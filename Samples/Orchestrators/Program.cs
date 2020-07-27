using Dotmim.Sync;
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
            await GetServerChangesToSendToClientAsync();

            Console.ReadLine();
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
                foreach (var table in changes.ClientBatchInfo.GetTable(tableChanges.TableName, tableChanges.SchemaName))
                {
                    Console.ForegroundColor = ConsoleColor.White;
                    Console.WriteLine($"Changes for table {table.GetFullName()}");
                    Console.ResetColor();
                    foreach (var row in table.Rows)
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
                foreach (var table in changes.ServerBatchInfo.GetTable(tableChanges.TableName, tableChanges.SchemaName))
                {
                    Console.ForegroundColor = ConsoleColor.White;
                    Console.WriteLine($"Changes for table {table.GetFullName()}");
                    Console.ResetColor();
                    foreach (var row in table.Rows)
                    {
                        Console.WriteLine(row);
                    }
                }
            }

        }


    }
}
