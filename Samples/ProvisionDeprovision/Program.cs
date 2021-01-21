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

            await DeprovisionServerManuallyAsync();
            await DeprovisionClientManuallyAsync();

            Console.WriteLine("Hello World!");
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
            // - Get the server scope that will contains after provisioning, the serialized version of your scope / schema
            // - Provision everything
            // - Save the server scope information

            // Create a server orchestrator used to Deprovision and Provision only table Address
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);

            // Get the server scope
            var serverScope = await remoteOrchestrator.GetServerScopeAsync();

            // Server scope is created on the server side.
            // but Setup and Schema are both null, since nothing have been created so far
            //
            // serverScope.Setup = null;
            // serverScope.Schema = null;
            //
            // Provision everything needed (sp, triggers, tracking tables)
            // Internally provision will fectch the schema a will return it to the caller. 
            var newSchema = await remoteOrchestrator.ProvisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable);

            // affect good values
            serverScope.Setup = setup;
            serverScope.Schema = newSchema;

            // save the server scope
            await remoteOrchestrator.SaveServerScopeAsync(serverScope);
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

            // Get the server scope
            var serverScope = await remoteOrchestrator.GetServerScopeAsync();

            // Deprovision everything
            await remoteOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures
                | SyncProvision.Triggers | SyncProvision.TrackingTable);

            // Affect good values
            serverScope.Setup = null;
            serverScope.Schema = null;

            // save the server scope
            await remoteOrchestrator.SaveServerScopeAsync(serverScope);
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

            // Get the local scope
            var clientScope = await localOrchestrator.GetClientScopeAsync();

            // Deprovision everything
            await localOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures
                | SyncProvision.Triggers | SyncProvision.TrackingTable | SyncProvision.Table);

            // affect good values
            clientScope.Setup = null;
            clientScope.Schema = null;

            // save the local scope
            await localOrchestrator.SaveClientScopeAsync(clientScope);
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
            // - Get the local scope that will contains after provisioning, the serialized version of your scope / schema
            // - Get the schema from the server side using a RemoteOrchestrator or a WebClientOrchestrator
            // - Provision everything locally
            // - Save the local scope information

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

            // get the local scope
            var clientScope = await localOrchestrator.GetClientScopeAsync();

            // Provision everything needed (sp, triggers, tracking tables, AND TABLES)
            await localOrchestrator.ProvisionAsync(serverSchema, SyncProvision.StoredProcedures
                    | SyncProvision.Triggers | SyncProvision.TrackingTable | SyncProvision.Table);

            // affect good values
            clientScope.Setup = setup;
            clientScope.Schema = serverSchema;

            // save the client scope
            await localOrchestrator.SaveClientScopeAsync(clientScope);

        }
    }
}
