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

            await GetTableSchemaAsync();
            await CreateOneTrackingTable();
            await CreateOneStoredProcedure();
            await DropOneTrackingTableAndOneStoredProcedure();

            await CreateOneTrackingTable();
            await CreateOneStoredProcedure();

            await DropSync();

            Console.WriteLine("Done. Please reset database for next operations");

            await GetClientChangesToSendToServerAsync();
            await GetServerChangesToSendToClientAsync();

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
            var setup = new SyncSetup("ProductCategory", "ProductModel", "Product");
            var orchestrator = new RemoteOrchestrator(provider, options);

            var serverScope = await orchestrator.GetServerScopeInfoAsync(setup);

            foreach (var column in serverScope.Schema.Tables["Product"].Columns)
                Console.WriteLine(column);
        }


        /// <summary>
        /// Create one stored procedure 
        /// </summary>
        private static async Task CreateOneStoredProcedure()
        {
            var provider = new SqlSyncProvider(serverConnectionString);
            var options = new SyncOptions();
            var setup = new SyncSetup("ProductCategory", "ProductModel", "Product");
            var orchestrator = new RemoteOrchestrator(provider, options);

            var serverScope = await orchestrator.GetServerScopeInfoAsync("v1", setup);

            var spExists = await orchestrator.ExistStoredProcedureAsync(serverScope, "Product", null, DbStoredProcedureType.SelectChanges);
            if (!spExists)
                await orchestrator.CreateStoredProcedureAsync(serverScope, "Product", null, DbStoredProcedureType.SelectChanges);

        }

        /// <summary>
        /// Create one tracking table
        /// </summary>
        private static async Task CreateOneTrackingTable()
        {
            var provider = new SqlSyncProvider(serverConnectionString);
            var options = new SyncOptions();
            var setup = new SyncSetup("ProductCategory", "ProductModel", "Product");
            var orchestrator = new RemoteOrchestrator(provider, options);

            var serverScope = await orchestrator.GetServerScopeInfoAsync(setup);

            var spExists = await orchestrator.ExistTrackingTableAsync(serverScope, "Product"); ;
            if (!spExists)
                await orchestrator.CreateTrackingTableAsync(serverScope, "Product");

        }

        /// <summary>
        /// Drop one tracking table and one stored procedure
        /// </summary>
        private static async Task DropOneTrackingTableAndOneStoredProcedure()
        {
            var provider = new SqlSyncProvider(serverConnectionString);
            var options = new SyncOptions();
            var setup = new SyncSetup("ProductCategory", "ProductModel", "Product");
            var orchestrator = new RemoteOrchestrator(provider, options);

            var serverScope = await orchestrator.GetServerScopeInfoAsync(setup);

            var trExists = await orchestrator.ExistTrackingTableAsync(serverScope, "Product");
            if (trExists)
                await orchestrator.DropTrackingTableAsync(serverScope, "Product");

            var spExists = await orchestrator.ExistStoredProcedureAsync(serverScope, "Product", null, DbStoredProcedureType.SelectChanges);
            if (spExists)
                await orchestrator.DropStoredProcedureAsync(serverScope, "Product", null, DbStoredProcedureType.SelectChanges);

        }

        private static async Task DropSync()
        {
            var provider = new SqlSyncProvider(serverConnectionString);
            var options = new SyncOptions();
            var setup = new SyncSetup("ProductCategory", "ProductModel", "Product");
            var orchestrator = new RemoteOrchestrator(provider, options);

            await orchestrator.DropAllAsync();

        }

            /// <summary>
            /// Create a localorchestrator, and get changes that should be sent to server
            /// </summary>
            private static async Task GetClientChangesToSendToServerAsync()
        {
            var serverProvider = new SqlSyncProvider(serverConnectionString);
            var clientProvider = new SqlSyncProvider(clientConnectionString);
            var setup = Config.GetSetup();

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider, Config.GetClientOptions());

            // Making a first sync, will initialize everything we need
            await agent.SynchronizeAsync(setup);

            // Get the localorchestrator (you can create a new instance as well)
            var localOrchestrator = agent.LocalOrchestrator;

            // Create a productcategory item
            await Helper.InsertOneProductCategoryAsync(clientProvider.CreateConnection(), "New Product Category 2");
            await Helper.InsertOneProductCategoryAsync(clientProvider.CreateConnection(), "New Product Category 3");
            await Helper.InsertOneCustomerAsync(clientProvider.CreateConnection(), "John", "Doe");
            await Helper.InsertOneCustomerAsync(clientProvider.CreateConnection(), "Will", "Doe");

            // Get changes to be populated to the server
            var changes = await localOrchestrator.GetChangesAsync(progress: Config.GetProgress());

            foreach (var tableChanges in changes.ClientChangesSelected.TableChangesSelected)
            {
                var syncTable = await localOrchestrator.LoadTableFromBatchInfoAsync(changes.ClientBatchInfo, tableChanges.TableName, tableChanges.SchemaName);

                Console.ForegroundColor = ConsoleColor.White;
                Console.WriteLine($"Changes for table {syncTable.TableName}. Rows:{syncTable.Rows.Count}");
                Console.ResetColor();
                foreach (var row in syncTable.Rows)
                {
                    Console.WriteLine(row);
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
            var setup = Config.GetSetup();

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider, Config.GetClientOptions());

            // Making a first sync, will initialize everything we need
            await agent.SynchronizeAsync(setup);

            // Get the orchestrators (you can create a new instance as well)
            var localOrchestrator = agent.LocalOrchestrator;
            var remoteOrchestrator = agent.RemoteOrchestrator;

            // Create a productcategory item
            await Helper.InsertOneProductCategoryAsync(serverProvider.CreateConnection(), "New Product Category 4");
            await Helper.InsertOneProductCategoryAsync(serverProvider.CreateConnection(), "New Product Category 5");
            await Helper.InsertOneCustomerAsync(serverProvider.CreateConnection(), "Jane", "Doe");
            await Helper.InsertOneCustomerAsync(serverProvider.CreateConnection(), "Lisa", "Doe");

            // Get client scope
            var clientScope = await localOrchestrator.GetClientScopeInfoAsync();


            // Get changes to be populated to the server
            var changes = await remoteOrchestrator.GetChangesAsync(clientScope: clientScope, progress: Config.GetProgress());

            // enumerate changes retrieved
            foreach (var tableChanges in changes.ServerChangesSelected.TableChangesSelected)
            {
                var syncTable = await remoteOrchestrator.LoadTableFromBatchInfoAsync(changes.ServerBatchInfo, tableChanges.TableName, tableChanges.SchemaName);

                Console.ForegroundColor = ConsoleColor.White;
                Console.WriteLine($"Changes for table {syncTable.TableName}. Rows:{syncTable.Rows.Count}");
                Console.ResetColor();
                foreach (var row in syncTable.Rows)
                {
                    Console.WriteLine(row);
                }


            }
        }
    }
}
