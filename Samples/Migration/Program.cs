using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using System;
using System.Threading.Tasks;

namespace Migration
{
    class Program
    {
        private static string serverConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=AdventureWorks;Integrated Security=true;";
        private static string clientConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=Client;Integrated Security=true;";

        static async Task Main(string[] args)
        {
            await SynchronizeThenDeprovisionThenProvisionAsync();

        }

        private static async Task SynchronizeThenDeprovisionThenProvisionAsync()
        {
            // Create 2 Sql Sync providers
            var serverProvider = new SqlSyncProvider(serverConnectionString);
            var clientProvider = new SqlSyncProvider(clientConnectionString);

            // Create standard Setup and Options
            var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress" });
            var options = new SyncOptions();

            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

            // Using the Progress pattern to handle progession during the synchronization
            var progress = new SynchronousProgress<ProgressArgs>(s => Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}"));

            // First sync to have a starting point
            var s1 = await agent.SynchronizeAsync(progress);

            Console.WriteLine(s1);

            // -----------------------------------------------------------------
            // Migrating a table by adding a new column
            // -----------------------------------------------------------------

            // Adding a new column called CreatedDate to Address table, on the server, and on the client.
            await Helper.AddNewColumnToAddressAsync(serverProvider.CreateConnection());
            await Helper.AddNewColumnToAddressAsync(clientProvider.CreateConnection());

            // -----------------------------------------------------------------
            // Server side
            // -----------------------------------------------------------------

            // Creating a setup regarding only the table Address
            var setupAddress = new SyncSetup(new string[] { "Address" });

            // Create a server orchestrator used to Deprovision and Provision only table Address
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setupAddress);

            // Unprovision the Address triggers / stored proc. 
            // We can conserve the Address tracking table, since we just add a column, 
            // that is not a primary key used in the tracking table
            // That way, we are preserving historical data
            await remoteOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers);

            // Provision the Address triggers / stored proc again, 
            // This provision method will fetch the address schema from the database, 
            // so it will contains all the columns, including the new Address column added
            await remoteOrchestrator.ProvisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers);

            // Now we need the full setup to get the full schema.
            // Setup includes [Address] [Customer] and [CustomerAddress]
            remoteOrchestrator.Setup = setup;
            var newSchema = await remoteOrchestrator.GetSchemaAsync();

            // Now we need to save this new schema to the serverscope table
            // get the server scope again
            var serverScope = await remoteOrchestrator.GetServerScopeAsync();

            // affect good values
            serverScope.Setup = setup;
            serverScope.Schema = newSchema;

            // save it
            await remoteOrchestrator.SaveServerScopeAsync(serverScope);

            // -----------------------------------------------------------------
            // Client side
            // -----------------------------------------------------------------

            // Now go for local orchestrator
            var localOrchestrator = new LocalOrchestrator(clientProvider, options, setupAddress);

            // Unprovision the Address triggers / stored proc. We can conserve tracking table, since we just add a column, that is not a primary key used in the tracking table
            // In this case, we will 
            await localOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers);

            // Provision the Address triggers / stored proc again, 
            // This provision method will fetch the address schema from the database, so it will contains all the columns, including the new one added
            await localOrchestrator.ProvisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers);

            // Now we need to save this to clientscope
            // get the server scope again
            var clientScope = await localOrchestrator.GetClientScopeAsync();

            // At this point, if you need the schema and you are not able to create a RemoteOrchestrator,
            // You can create a WebClientOrchestrator and get the schema as well
            // var proxyClientProvider = new WebClientOrchestrator("https://localhost:44369/api/Sync");
            // var newSchema = proxyClientProvider.GetSchemaAsync();

            // affect good values
            clientScope.Setup = setup;
            clientScope.Schema = newSchema;

            // save it
            await localOrchestrator.SaveClientScopeAsync(clientScope);

            // Now test a new sync, everything should work as expected.
            do
            {
                // Console.Clear();
                Console.WriteLine("Sync Start");
                try
                {
                    var s2 = await agent.SynchronizeAsync();

                    // Write results
                    Console.WriteLine(s2);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }

            } while (Console.ReadKey().Key != ConsoleKey.Escape);

            Console.WriteLine("End");

        }
    }
}
