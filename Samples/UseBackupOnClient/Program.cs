using Dotmim.Sync;
using Dotmim.Sync.SqlServer;

namespace UseBackupOnClient
{
    internal class Program
    {
        // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql
        internal static string SqlConnectionString = "Data Source=(localdb)\\mssqllocaldb; Initial Catalog={0};Integrated Security=true;";

        internal static string AdvConnectionString = string.Format(SqlConnectionString, "AdventureWorks");
        internal static string ClientConnectionString = string.Format(SqlConnectionString, "Client");
        internal static string MasterConnectionString = string.Format(SqlConnectionString, "master");

        // create the Setup
        internal static SyncSetup Setup = new("ProductCategory", "ProductModel", "Product",
                    "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail");

        private static async Task Main(string[] args)
        {

            // Dropping the client database if it exists
            Console.WriteLine("Dropping Client database if exists");
            Helper.DropSqlDatabase("Client", string.Format(SqlConnectionString, "master"));

            // Unprovision the server database if it's already provisioned, to start from scratch
            Console.WriteLine("Unprovisioning the server database if it's already provisioned, to start from scratch");
            await DeprovisionServerAsync().ConfigureAwait(false);

            // Now can start the sample:
            await RestoreDatabaseThenSetupSyncAsync().ConfigureAwait(false);

            // Synchronize
            await SynchronizeAsync().ConfigureAwait(false);
        }

        public static async Task DeprovisionServerAsync()
        {

            // creating the orchestrator we are going to need
            var serverProvider = new SqlSyncProvider(AdvConnectionString);
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider);

            await remoteOrchestrator.DropAllAsync().ConfigureAwait(false);
        }

        public static async Task RestoreDatabaseThenSetupSyncAsync()
        {
            // creating the orchestrator we are going to need
            var serverProvider = new SqlSyncProvider(AdvConnectionString);
            var clientProvider = new SqlSyncProvider(ClientConnectionString);
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider);
            var localOrchestrator = new LocalOrchestrator(clientProvider);

            // Assuming adventureworks is already created, with data, without any DMS metadata
            // We can make a backup of the database, and restore it on the client, as is.

            // backup AdventureWorks
            Helper.BackupDatabase("AdventureWorks", AdvConnectionString);

            // Once we have the backup, we need to ensure all the data that will be added / edited / deleted on the server will be tracked
            // We can do this by provision the server database
            await remoteOrchestrator.ProvisionAsync(Setup).ConfigureAwait(false);

            // get the correct timestamp from remote orchestrator.
            // this timestamp will be recorded on the client, as a "from now on" timestamp
            var serverTimestamp = await remoteOrchestrator.GetLocalTimestampAsync().ConfigureAwait(false);

            // from now on, we know all the future modification on the server are tracked
            // As a test, we are going to add a random line in ProductCategory
            await Helper.AddProductCategoryRowAsync(AdvConnectionString).ConfigureAwait(false);

            // restore AdventureWorks on the client, with a new name, called "Client"
            Helper.RestoreSqlDatabase("AdventureWorks", "Client", MasterConnectionString);

            // get the server scope to be able to create the client scope
            var serverScope = await remoteOrchestrator.GetScopeInfoAsync().ConfigureAwait(false);

            await localOrchestrator.ProvisionAsync(serverScope).ConfigureAwait(false);
            var clientTimestamp = await localOrchestrator.GetLocalTimestampAsync().ConfigureAwait(false);

            var clientScope = await localOrchestrator.GetScopeInfoClientAsync().ConfigureAwait(false);

            // Override with correct values, to ensure next time we are running from the good starting point
            clientScope.LastServerSyncTimestamp = serverTimestamp; // server last timestamp sync
            clientScope.LastSyncTimestamp = clientTimestamp; // client last timestamp sync
            clientScope.IsNewScope = false; // since we are considering the client as a restored database with existing values, we are not new anymore
            clientScope.LastSync = DateTime.UtcNow; // just as human readable value, not used internally

            await localOrchestrator.SaveScopeInfoClientAsync(clientScope).ConfigureAwait(false);
        }

        private static async Task SynchronizeAsync()
        {

            var advConnectionString = string.Format(SqlConnectionString, "AdventureWorks");
            var clientConnectionString = string.Format(SqlConnectionString, "Client");

            var serverProvider = new SqlSyncProvider(advConnectionString);
            var clientProvider = new SqlSyncProvider(clientConnectionString);

            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverProvider);

            do
            {
                // Launch the sync process
                var s1 = await agent.SynchronizeAsync(Setup).ConfigureAwait(false);

                // Write results
                Console.WriteLine(s1);
            }
            while (Console.ReadKey().Key != ConsoleKey.Escape);

            Console.WriteLine("End");
        }
    }
}