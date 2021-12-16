using Dotmim.Sync;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Newtonsoft.Json;
using Orchestrators;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace HelloSync
{
    class Program
    {
        private static string serverConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=AdventureWorks;Integrated Security=true;";
        private static string clientConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=Client;Integrated Security=true;";

        static async Task Main(string[] args)
        {
            await AddingOneColumnInTrackingTable();

            Console.WriteLine("Done.");
            Console.ReadLine();
        }

        /// <summary>
        /// Create an orchestrator on the server database (to have an existing schema) and getting a table schema
        /// </summary>
        /// <returns></returns>
        private static async Task GetTableSchemaAsync()
        {
            var provider = new SqlSyncProvider(serverConnectionString);
            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "ProductCategory", "ProductModel", "Product" });
            var orchestrator = new RemoteOrchestrator(provider, options, setup);

            // working on the product Table
            var productSetupTable = setup.Tables["Product"];

            // Getting the table schema
            var productTable = await orchestrator.GetTableSchemaAsync(productSetupTable);

            foreach (var column in productTable.Columns)
                Console.WriteLine(column);
        }


        /// <summary>
        /// Create one stored procedure 
        /// </summary>
        private static async Task CreateOneStoredProcedure()
        {
            var provider = new SqlSyncProvider(serverConnectionString);
            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "ProductCategory", "ProductModel", "Product" });
            var orchestrator = new RemoteOrchestrator(provider, options, setup);

            // working on the product Table
            var productSetupTable = setup.Tables["Product"];

            var spExists = await orchestrator.ExistStoredProcedureAsync(productSetupTable, DbStoredProcedureType.SelectChanges);
            if (!spExists)
                await orchestrator.CreateStoredProcedureAsync(productSetupTable, DbStoredProcedureType.SelectChanges);

        }

        /// <summary>
        /// Create one tracking table
        /// </summary>
        private static async Task CreateOneTrackingTable()
        {
            var provider = new SqlSyncProvider(serverConnectionString);
            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "ProductCategory", "ProductModel", "Product" });
            var orchestrator = new RemoteOrchestrator(provider, options, setup);

            // working on the product Table
            var productSetupTable = setup.Tables["Product"];

            var spExists = await orchestrator.ExistTrackingTableAsync(productSetupTable);
            if (!spExists)
                await orchestrator.CreateTrackingTableAsync(productSetupTable);

        }

        /// <summary>
        /// Create one tracking table
        /// </summary>
        private static async Task AddingOneColumnInTrackingTable()
        {
            var provider = new SqlSyncProvider(serverConnectionString);
            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "ProductCategory", "ProductModel", "Product" });
            var orchestrator = new RemoteOrchestrator(provider, options, setup);

            // working on the product Table
            var productSetupTable = setup.Tables["Product"];

            orchestrator.OnTrackingTableCreating(ttca =>
            {
                var addingID = $" ALTER TABLE {ttca.TrackingTableName.Schema().Quoted()} ADD internal_id varchar(10) null";
                ttca.Command.CommandText += addingID;
            });

            var trExists = await orchestrator.ExistTrackingTableAsync(productSetupTable);
            if (!trExists)
                await orchestrator.CreateTrackingTableAsync(productSetupTable);

            orchestrator.OnTriggerCreating(tca =>
            {
                string val;
                if (tca.TriggerType == DbTriggerType.Insert)
                    val = "INS";
                else if (tca.TriggerType == DbTriggerType.Delete)
                    val = "DEL";
                else
                    val = "UPD";

                var cmdText = $"UPDATE Product_tracking " +
                              $"SET Product_tracking.internal_id='{val}' " +
                              $"FROM Product_tracking JOIN Inserted ON Product_tracking.ProductID = Inserted.ProductID;";

                tca.Command.CommandText += Environment.NewLine + cmdText;
            });

            var trgExists = await orchestrator.ExistTriggerAsync(productSetupTable, DbTriggerType.Insert);
            if (!trgExists)
                await orchestrator.CreateTriggerAsync(productSetupTable, DbTriggerType.Insert);

            trgExists = await orchestrator.ExistTriggerAsync(productSetupTable, DbTriggerType.Update);
            if (!trgExists)
                await orchestrator.CreateTriggerAsync(productSetupTable, DbTriggerType.Update);

            trgExists = await orchestrator.ExistTriggerAsync(productSetupTable, DbTriggerType.Delete);
            if (!trgExists)
                await orchestrator.CreateTriggerAsync(productSetupTable, DbTriggerType.Delete);

            orchestrator.OnTriggerCreating(null);
        }


        /// <summary>
        /// Drop one tracking table and one stored procedure
        /// </summary>
        private static async Task DropOneTrackingTableAndOneStoredProcedure()
        {
            var provider = new SqlSyncProvider(serverConnectionString);
            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "ProductCategory", "ProductModel", "Product" });
            var orchestrator = new RemoteOrchestrator(provider, options, setup);

            // working on the product Table
            var productSetupTable = setup.Tables["Product"];

            var trExists = await orchestrator.ExistTrackingTableAsync(productSetupTable);
            if (trExists)
                await orchestrator.DropTrackingTableAsync(productSetupTable);

            var spExists = await orchestrator.ExistStoredProcedureAsync(productSetupTable, DbStoredProcedureType.SelectChanges);
            if (spExists)
                await orchestrator.DropStoredProcedureAsync(productSetupTable, DbStoredProcedureType.SelectChanges);

        }

        /// <summary>
        /// Create a localorchestrator, and get changes that should be sent to server
        /// </summary>
        private static async Task GetClientChangesToSendToServerAsync()
        {
            var serverProvider = new SqlSyncProvider(serverConnectionString);
            var clientProvider = new SqlSyncProvider(clientConnectionString);

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider, Config.GetClientOptions(), Config.GetSetup());

            // Making a first sync, will initialize everything we need
            await agent.SynchronizeAsync();

            // Get the localorchestrator (you can create a new instance as well)
            var localOrchestrator = agent.LocalOrchestrator;

            // Create a productcategory item
            await Helper.InsertOneProductCategoryAsync(clientProvider.CreateConnection(), "New Product Category 2");
            await Helper.InsertOneProductCategoryAsync(clientProvider.CreateConnection(), "New Product Category 3");
            await Helper.InsertOneCustomerAsync(clientProvider.CreateConnection(), "John", "Doe");
            await Helper.InsertOneCustomerAsync(clientProvider.CreateConnection(), "Sébastien", "Pertus");

            // Get changes to be populated to the server
            var changes = await localOrchestrator.GetChangesAsync(progress: Config.GetProgress());

            // enumerate changes retrieved
            foreach (var tableChanges in changes.ClientChangesSelected.TableChangesSelected)
            {
                foreach (var bpi in changes.ClientBatchInfo.BatchPartsInfo)
                {
                    // only one table in each bpi
                    var table = bpi.Tables[0];
                    var path = changes.ClientBatchInfo.GetBatchPartInfoPath(bpi).FullPath;
                    var schemaTable = changes.ClientBatchInfo.SanitizedSchema.Tables[table.TableName, table.SchemaName];

                    Console.ForegroundColor = ConsoleColor.White;
                    Console.WriteLine($"Changes for table {table.TableName}. Rows:{table.RowsCount}");
                    Console.ResetColor();
                    foreach (var row in agent.Options.LocalSerializerFactory.GetLocalSerializer().ReadRowsFromFile(path, schemaTable))
                    {
                        Console.WriteLine(row);
                    }
                }
            }
        }

        /// <summary>
        /// Create a localorchestrator, and get changes that should be sent to server
        /// </summary>
        private static async Task GetServerChangesToSendToClientAsync()
        {
            var serverProvider = new SqlSyncProvider(serverConnectionString);
            var clientProvider = new SqlSyncProvider(clientConnectionString);

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider, Config.GetClientOptions(), Config.GetSetup());

            // Making a first sync, will initialize everything we need
            await agent.SynchronizeAsync();

            // Get the orchestrators (you can create a new instance as well)
            var localOrchestrator = agent.LocalOrchestrator;
            var remoteOrchestrator = agent.RemoteOrchestrator;

            // Create a productcategory item
            await Helper.InsertOneProductCategoryAsync(serverProvider.CreateConnection(), "New Product Category 2");
            await Helper.InsertOneProductCategoryAsync(serverProvider.CreateConnection(), "New Product Category 3");
            await Helper.InsertOneCustomerAsync(serverProvider.CreateConnection(), "John", "Doe");
            await Helper.InsertOneCustomerAsync(serverProvider.CreateConnection(), "Sébastien", "Pertus");

            // Get client scope
            var clientScope = await localOrchestrator.GetClientScopeAsync();

            // Simulate a full get changes (initialization step)
            // clientScope.IsNewScope = true;

            // Get changes to be populated to the server
            var changes = await remoteOrchestrator.GetChangesAsync(clientScope: clientScope, progress: Config.GetProgress());

            // enumerate changes retrieved
            foreach (var tableChanges in changes.ServerChangesSelected.TableChangesSelected)
            {

                foreach (var bpi in changes.ServerBatchInfo.BatchPartsInfo)
                {
                    // only one table in each bpi
                    var table = bpi.Tables[0];
                    var path = changes.ServerBatchInfo.GetBatchPartInfoPath(bpi).FullPath;
                    var schemaTable = changes.ServerBatchInfo.SanitizedSchema.Tables[table.TableName, table.SchemaName];

                    Console.ForegroundColor = ConsoleColor.White;
                    Console.WriteLine($"Changes for table {table.TableName}. Rows:{table.RowsCount}");
                    Console.ResetColor();
                    foreach (var row in agent.Options.LocalSerializerFactory.GetLocalSerializer().ReadRowsFromFile(path, schemaTable))
                    {
                        Console.WriteLine(row);
                    }
                }
            }
        }
    }
}
