using Dotmim.Sync;
using Dotmim.Sync.Builders;
using Dotmim.Sync.SqlServer;
using Orchestrators;
using System;
using System.Threading.Tasks;

namespace HelloSync
{
    internal class Program
    {
        private static string serverConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=AdventureWorks;Integrated Security=true;";
        private static string clientConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=Client;Integrated Security=true;";

        private static async Task Main(string[] args)
        {

            await GetTableSchemaAsync().ConfigureAwait(false);
            await CreateOneTrackingTable().ConfigureAwait(false);
            await CreateOneStoredProcedure().ConfigureAwait(false);
            await DropOneTrackingTableAndOneStoredProcedure().ConfigureAwait(false);

            await CreateOneTrackingTable().ConfigureAwait(false);
            await CreateOneStoredProcedure().ConfigureAwait(false);

            await DropSync().ConfigureAwait(false);

            Console.WriteLine("Done. Please reset database for next operations");

            await GetClientChangesToSendToServerAsync().ConfigureAwait(false);
            await GetServerChangesToSendToClientAsync().ConfigureAwait(false);

            Console.ReadLine();
        }

        /// <summary>
        /// Create an orchestrator on the server database (to have an existing schema) and getting a table schema.
        /// </summary>
        /// <returns><placeholder>A <see cref="Task"/> representing the asynchronous operation.</placeholder></returns>
        private static async Task GetTableSchemaAsync()
        {
            var provider = new SqlSyncProvider(serverConnectionString);
            var options = new SyncOptions();
            var setup = new SyncSetup("ProductCategory", "ProductModel", "Product");
            var orchestrator = new RemoteOrchestrator(provider, options);

            var serverSchema = await orchestrator.GetSchemaAsync(setup).ConfigureAwait(false);

            foreach (var column in serverSchema.Tables["Product"].Columns)
                Console.WriteLine(column);
        }

        /// <summary>
        /// Create one stored procedure.
        /// </summary>
        private static async Task CreateOneStoredProcedure()
        {
            var provider = new SqlSyncProvider(serverConnectionString);
            var options = new SyncOptions();
            var setup = new SyncSetup("ProductCategory", "ProductModel", "Product");
            var orchestrator = new RemoteOrchestrator(provider, options);

            var serverScope = await orchestrator.GetScopeInfoAsync("v1", setup).ConfigureAwait(false);

            var spExists = await orchestrator.ExistStoredProcedureAsync(serverScope, "Product", null, DbStoredProcedureType.SelectChanges).ConfigureAwait(false);
            if (!spExists)
                await orchestrator.CreateStoredProcedureAsync(serverScope, "Product", null, DbStoredProcedureType.SelectChanges).ConfigureAwait(false);
        }

        /// <summary>
        /// Create one tracking table.
        /// </summary>
        private static async Task CreateOneTrackingTable()
        {
            var provider = new SqlSyncProvider(serverConnectionString);
            var options = new SyncOptions();
            var setup = new SyncSetup("ProductCategory", "ProductModel", "Product");
            var orchestrator = new RemoteOrchestrator(provider, options);

            var serverScope = await orchestrator.GetScopeInfoAsync(setup).ConfigureAwait(false);

            var spExists = await orchestrator.ExistTrackingTableAsync(serverScope, "Product").ConfigureAwait(false);
            if (!spExists)
                await orchestrator.CreateTrackingTableAsync(serverScope, "Product").ConfigureAwait(false);
        }

        /// <summary>
        /// Drop one tracking table and one stored procedure.
        /// </summary>
        private static async Task DropOneTrackingTableAndOneStoredProcedure()
        {
            var provider = new SqlSyncProvider(serverConnectionString);
            var options = new SyncOptions();
            var setup = new SyncSetup("ProductCategory", "ProductModel", "Product");
            var orchestrator = new RemoteOrchestrator(provider, options);

            var serverScope = await orchestrator.GetScopeInfoAsync(setup).ConfigureAwait(false);

            var trExists = await orchestrator.ExistTrackingTableAsync(serverScope, "Product").ConfigureAwait(false);
            if (trExists)
                await orchestrator.DropTrackingTableAsync(serverScope, "Product").ConfigureAwait(false);

            var spExists = await orchestrator.ExistStoredProcedureAsync(serverScope, "Product", null, DbStoredProcedureType.SelectChanges).ConfigureAwait(false);
            if (spExists)
                await orchestrator.DropStoredProcedureAsync(serverScope, "Product", null, DbStoredProcedureType.SelectChanges).ConfigureAwait(false);
        }

        private static async Task DropSync()
        {
            var provider = new SqlSyncProvider(serverConnectionString);
            var options = new SyncOptions();
            var setup = new SyncSetup("ProductCategory", "ProductModel", "Product");
            var orchestrator = new RemoteOrchestrator(provider, options);

            await orchestrator.DropAllAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Create a localorchestrator, and get changes that should be sent to server.
        /// </summary>
        private static async Task GetClientChangesToSendToServerAsync()
        {
            var serverProvider = new SqlSyncProvider(serverConnectionString);
            var clientProvider = new SqlSyncProvider(clientConnectionString);
            var setup = Config.GetSetup();

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider, Config.GetClientOptions());

            // Making a first sync, will initialize everything we need
            await agent.SynchronizeAsync(setup).ConfigureAwait(false);

            // Get the localorchestrator (you can create a new instance as well)
            var localOrchestrator = agent.LocalOrchestrator;

            // Create a productcategory item
            await Helper.InsertOneProductCategoryAsync(clientProvider.CreateConnection(), "New Product Category 2").ConfigureAwait(false);
            await Helper.InsertOneProductCategoryAsync(clientProvider.CreateConnection(), "New Product Category 3").ConfigureAwait(false);
            await Helper.InsertOneCustomerAsync(clientProvider.CreateConnection(), "John", "Doe").ConfigureAwait(false);
            await Helper.InsertOneCustomerAsync(clientProvider.CreateConnection(), "Will", "Doe").ConfigureAwait(false);

            // Get changes to be populated to the server
            var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync().ConfigureAwait(false);
            var changes = await localOrchestrator.GetChangesAsync(cScopeInfoClient).ConfigureAwait(false);

            foreach (var tableChanges in changes.ClientChangesSelected.TableChangesSelected)
            {
                var syncTable = localOrchestrator.LoadTableFromBatchInfo(changes.ClientBatchInfo, tableChanges.TableName, tableChanges.SchemaName);

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
        /// Create a localorchestrator, and get changes that should be sent to server.
        /// </summary>
        private static async Task GetServerChangesToSendToClientAsync()
        {
            var serverProvider = new SqlSyncProvider(serverConnectionString);
            var clientProvider = new SqlSyncProvider(clientConnectionString);
            var setup = Config.GetSetup();

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider, Config.GetClientOptions());

            // Making a first sync, will initialize everything we need
            await agent.SynchronizeAsync(setup).ConfigureAwait(false);

            // Get the orchestrators (you can create a new instance as well)
            var localOrchestrator = agent.LocalOrchestrator;
            var remoteOrchestrator = agent.RemoteOrchestrator;

            // Create a productcategory item
            await Helper.InsertOneProductCategoryAsync(serverProvider.CreateConnection(), "New Product Category 4").ConfigureAwait(false);
            await Helper.InsertOneProductCategoryAsync(serverProvider.CreateConnection(), "New Product Category 5").ConfigureAwait(false);
            await Helper.InsertOneCustomerAsync(serverProvider.CreateConnection(), "Jane", "Doe").ConfigureAwait(false);
            await Helper.InsertOneCustomerAsync(serverProvider.CreateConnection(), "Lisa", "Doe").ConfigureAwait(false);

            // Get client scope
            var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync().ConfigureAwait(false);

            // Get changes to be populated to the server
            var changes = await remoteOrchestrator.GetChangesAsync(cScopeInfoClient).ConfigureAwait(false);

            // enumerate changes retrieved
            foreach (var tableChanges in changes.ServerChangesSelected.TableChangesSelected)
            {
                var syncTable = remoteOrchestrator.LoadTableFromBatchInfo(changes.ServerBatchInfo, tableChanges.TableName, tableChanges.SchemaName);

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