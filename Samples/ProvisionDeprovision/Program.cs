using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using System;
using System.Threading.Tasks;

namespace ProvisionDeprovision
{
    internal class Program
    {
        private static string serverConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=AdventureWorks;Integrated Security=true;";
        private static string clientConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=Client;Integrated Security=true;";

        private static async Task Main(string[] args)
        {
            await ProvisionServerManuallyAsync().ConfigureAwait(false);
            await ProvisionClientManuallyAsync().ConfigureAwait(false);

            await SynchronizeAsync().ConfigureAwait(false);

            await DeprovisionServerManuallyAsync().ConfigureAwait(false);
            await DeprovisionClientManuallyAsync().ConfigureAwait(false);

            Console.WriteLine("Hello World!");
        }

        private static async Task SynchronizeAsync()
        {
            // Create 2 Sql Sync providers
            var serverProvider = new SqlSyncProvider(serverConnectionString);
            var clientProvider = new SqlSyncProvider(clientConnectionString);

            var agent = new SyncAgent(clientProvider, serverProvider);

            // no need to specify setup / tables, since we have already provisionned everything
            // on both side
            var result = await agent.SynchronizeAsync().ConfigureAwait(false);

            Console.WriteLine(result);
        }

        private static async Task ProvisionServerManuallyAsync()
        {
            // Create 2 Sql Sync providers
            var serverProvider = new SqlSyncProvider(serverConnectionString);

            // Create standard Setup and Options
            var setup = new SyncSetup("Address", "Customer", "CustomerAddress");

            // -----------------------------------------------------------------
            // Server side
            // -----------------------------------------------------------------

            // This method is useful if you want to provision by yourself the server database
            // You will need to :
            // - Create a remote orchestrator with the correct setup to proivision
            // - Provision everything

            // Create a server orchestrator
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider);

            // Provision everything needed by the setup
            await remoteOrchestrator.ProvisionAsync(setup).ConfigureAwait(false);
        }

        private static async Task DeprovisionServerManuallyAsync()
        {
            // Create server provider
            var serverProvider = new SqlSyncProvider(serverConnectionString);

            // Create a server orchestrator used to Deprovision everything on the server side
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider);

            // Deprovision everything
            var p = SyncProvision.ScopeInfo | SyncProvision.ScopeInfoClient |
                    SyncProvision.StoredProcedures | SyncProvision.TrackingTable |
                    SyncProvision.Triggers;

            // Deprovision everything
            await remoteOrchestrator.DeprovisionAsync(p).ConfigureAwait(false);
        }

        private static async Task DeprovisionClientManuallyAsync()
        {
            // Create client provider
            var clientProvider = new SqlSyncProvider(clientConnectionString);

            // Create a local orchestrator used to Deprovision everything
            var localOrchestrator = new LocalOrchestrator(clientProvider);

            var p = SyncProvision.ScopeInfo | SyncProvision.ScopeInfoClient |
                    SyncProvision.StoredProcedures | SyncProvision.TrackingTable |
                    SyncProvision.Triggers;

            // Deprovision everything
            await localOrchestrator.DeprovisionAsync(p).ConfigureAwait(false);
        }

        private static async Task ProvisionClientManuallyAsync()
        {
            // Create 2 Sql Sync providers
            var serverProvider = new SqlSyncProvider(serverConnectionString);
            var clientProvider = new SqlSyncProvider(clientConnectionString);

            // -----------------------------------------------------------------
            // Client side
            // -----------------------------------------------------------------

            // This method is useful if you want to provision by yourself the client database
            // You will need to :
            // - Create a local orchestrator with the correct setup to provision
            // - Get the ServerScopeInfo from the server side using a RemoteOrchestrator or a WebRemoteOrchestrator
            // - Provision everything locally

            // Create a local orchestrator used to provision everything locally
            var localOrchestrator = new LocalOrchestrator(clientProvider);

            // Because we need the schema from remote side, create a remote orchestrator
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider);

            // Getting the server scope from server side
            var serverScope = await remoteOrchestrator.GetScopeInfoAsync().ConfigureAwait(false);

            // You can create a WebRemoteOrchestrator and get the ServerScope as well
            // var proxyClientProvider = new WebRemoteOrchestrator("https://localhost:44369/api/Sync");
            // var serverScope = proxyClientProvider.GetScopeInfoAsync();

            // Provision everything needed (sp, triggers, tracking tables, AND TABLES)
            await localOrchestrator.ProvisionAsync(serverScope).ConfigureAwait(false);
        }
    }
}