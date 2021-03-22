using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using System;
using System.Threading.Tasks;

namespace ProvisionDeprovision
{
    class Program
    {
        private static string serverConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=AdventureWorks;Integrated Security=true;";
        private static string clientConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=Client;Integrated Security=true;";

        static async Task Main(string[] args)
        {
            await ProvisionServerManuallyAsync();
            await ProvisionClientManuallyAsync();

            await SynchronizeAsync();

            await DeprovisionServerManuallyAsync();
            await DeprovisionClientManuallyAsync();

            Console.WriteLine("Hello World!");
        }


        private static async Task SynchronizeAsync()
        {
            // Create 2 Sql Sync providers
            var serverProvider = new SqlSyncProvider(serverConnectionString);
            var clientProvider = new SqlSyncProvider(clientConnectionString);

            // Create standard Setup and Options
            var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress" });
            var options = new SyncOptions();

            var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

            var result = await agent.SynchronizeAsync();

            Console.WriteLine(result);
        }


        private static async Task ProvisionServerManuallyAsync()
        {
            // Create 2 Sql Sync providers
            var serverProvider = new SqlSyncProvider(serverConnectionString);

            // Create standard Setup and Options
            var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress" });
            var options = new SyncOptions();

            // -----------------------------------------------------------------
            // Server side
            // -----------------------------------------------------------------

            // This method is useful if you want to provision by yourself the server database
            // You will need to :
            // - Create a remote orchestrator with the correct setup to proivision
            // - Provision everything

            // Create a server orchestrator used to Deprovision and Provision only table Address
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);

            // Provision everything needed
            await remoteOrchestrator.ProvisionAsync();

        }


        private static async Task DeprovisionServerManuallyAsync()
        {
            // Create server provider
            var serverProvider = new SqlSyncProvider(serverConnectionString);

            // Create standard Setup and Options
            var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress" });
            var options = new SyncOptions();

            // Create a server orchestrator used to Deprovision everything on the server side
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);

            // Deprovision everything
            await remoteOrchestrator.DeprovisionAsync();
        }
        private static async Task DeprovisionClientManuallyAsync()
        {
            // Create client provider
            var clientProvider = new SqlSyncProvider(clientConnectionString);

            // Create standard Setup and Options
            var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress" });
            var options = new SyncOptions();

            // Create a local orchestrator used to Deprovision everything
            var localOrchestrator = new LocalOrchestrator(clientProvider, options, setup);

            // Deprovision everything
            await localOrchestrator.DeprovisionAsync();

        }



        private static async Task ProvisionClientManuallyAsync()
        {
            // Create 2 Sql Sync providers
            var serverProvider = new SqlSyncProvider(serverConnectionString);
            var clientProvider = new SqlSyncProvider(clientConnectionString);

            // Create standard Setup and Options
            var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress" });
            var options = new SyncOptions();

            // -----------------------------------------------------------------
            // Client side
            // -----------------------------------------------------------------

            // This method is useful if you want to provision by yourself the client database
            // You will need to :
            // - Create a local orchestrator with the correct setup to provision
            // - Get the schema from the server side using a RemoteOrchestrator or a WebClientOrchestrator
            // - Provision everything locally

            // Create a local orchestrator used to provision everything locally
            var localOrchestrator = new LocalOrchestrator(clientProvider, options, setup);

            // Because we need the schema from remote side, create a remote orchestrator
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);

            // Getting the schema from server side
            var serverSchema = await remoteOrchestrator.GetSchemaAsync();

            // At this point, if you need the schema and you are not able to create a RemoteOrchestrator,
            // You can create a WebClientOrchestrator and get the schema as well
            // var proxyClientProvider = new WebClientOrchestrator("https://localhost:44369/api/Sync");
            // var serverSchema = proxyClientProvider.GetSchemaAsync();

            // Provision everything needed (sp, triggers, tracking tables, AND TABLES)
            await localOrchestrator.ProvisionAsync(serverSchema);

        }
    }
}
