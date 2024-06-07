using Dotmim.Sync.SqlServer;
using Dotmim.Sync;
using Dotmim.Sync.Enumerations;

namespace UseBackupOnClient
{
    internal class Program
    {
        // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql 

        internal static string sqlConnectionString = "Data Source=(localdb)\\mssqllocaldb; Initial Catalog={0};Integrated Security=true;";

        internal static string advConnectionString = string.Format(sqlConnectionString, "AdventureWorks");
        internal static string clientConnectionString = string.Format(sqlConnectionString, "Client");
        internal static string masterConnectionString = string.Format(sqlConnectionString, "master");


        // create the Setup
        internal static SyncSetup setup = new("ProductCategory", "ProductModel", "Product",
                    "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail");

        static async Task Main(string[] args)
        {

            // Dropping the client database if it exists
            Console.WriteLine("Dropping Client database if exists");
            Helper.DropSqlDatabase("Client", string.Format(sqlConnectionString, "master"));

            // Unprovision the server database if it's already provisioned, to start from scratch
            Console.WriteLine("Unprovisioning the server database if it's already provisioned, to start from scratch");
            await DeprovisionServerAsync();

            // Now can start the sample:
            await RestoreDatabaseThenSetupSyncAsync();

            // Synchronize
            await SynchronizeAsync();

        }


        public static async Task DeprovisionServerAsync() {

            // creating the orchestrator we are going to need
            var serverProvider = new SqlSyncProvider(advConnectionString);
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider);

            await remoteOrchestrator.DropAllAsync();

        }

        public static async Task RestoreDatabaseThenSetupSyncAsync()
        {
            // creating the orchestrator we are going to need
            var serverProvider = new SqlSyncProvider(advConnectionString);
            var clientProvider = new SqlSyncProvider(clientConnectionString);
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider);
            var localOrchestrator = new LocalOrchestrator(clientProvider);

            // Assuming adventureworks is already created, with data, without any DMS metadata
            // We can make a backup of the database, and restore it on the client, as is.


            // backup AdventureWorks
            Helper.BackupDatabase("AdventureWorks", advConnectionString);

            // Once we have the backup, we need to ensure all the data that will be added / edited / deleted on the server will be tracked
            // We can do this by provision the server database
            await remoteOrchestrator.ProvisionAsync(setup);


            // get the correct timestamp from remote orchestrator.
            // this timestamp will be recorded on the client, as a "from now on" timestamp
            var serverTimestamp = await remoteOrchestrator.GetLocalTimestampAsync();


            // from now on, we know all the future modification on the server are tracked
            // As a test, we are going to add a random line in ProductCategory
            await Helper.AddProductCategoryRowAsync(advConnectionString);

            // restore AdventureWorks on the client, with a new name, called "Client"
            Helper.RestoreSqlDatabase("AdventureWorks", "Client", masterConnectionString);


            // get the server scope to be able to create the client scope
            var serverScope = await remoteOrchestrator.GetScopeInfoAsync();

            await localOrchestrator.ProvisionAsync(serverScope);
            var clientTimestamp = await localOrchestrator.GetLocalTimestampAsync();

            var clientScope = await localOrchestrator.GetScopeInfoClientAsync();

            // Override with correct values, to ensure next time we are running from the good starting point
            clientScope.LastServerSyncTimestamp = serverTimestamp; // server last timestamp sync
            clientScope.LastSyncTimestamp = clientTimestamp; // client last timestamp sync
            clientScope.IsNewScope = false; // since we are considering the client as a restored database with existing values, we are not new anymore
            clientScope.LastSync = DateTime.UtcNow; // just as human readable value, not used internally
            
            await localOrchestrator.SaveScopeInfoClientAsync(clientScope);
        }



        private static async Task SynchronizeAsync()
        {

            var advConnectionString = string.Format(sqlConnectionString, "AdventureWorks");
            var clientConnectionString = string.Format(sqlConnectionString, "Client");

            var serverProvider = new SqlSyncProvider(advConnectionString);
            var clientProvider = new SqlSyncProvider(clientConnectionString);

            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverProvider);


            do
            {
                // Launch the sync process
                var s1 = await agent.SynchronizeAsync(setup);

                // Write results
                Console.WriteLine(s1);

            } while (Console.ReadKey().Key != ConsoleKey.Escape);

            Console.WriteLine("End");
        }

    }
}
