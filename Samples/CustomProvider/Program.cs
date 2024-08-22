using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using System;
using System.Threading.Tasks;

namespace CustomProvider
{
    internal class Program
    {
        private static string serverConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=AdventureWorks;Integrated Security=true;";

        private static async Task Main() => await SynchronizeAsync().ConfigureAwait(false);

        private static async Task SynchronizeAsync()
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql
            var serverProvider = new SqlSyncProvider(serverConnectionString);

            // Tables involved in the sync process:
            var tables = new string[]
            {
                "ProductCategory", "ProductModel", "Product",
                "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail",
            };

            // downloadOnly provider for Sqlite
            var clientSqliteProvider = new SqliteSyncDownloadOnlyProvider("adv.db");

            var options = new SyncOptions { ErrorResolutionPolicy = ErrorResolution.RetryOneMoreTimeAndContinueOnError };

            do
            {
                // Creating an agent that will handle all the process
                var agentSqlite = new SyncAgent(clientSqliteProvider, serverProvider, options);
                var sqliteResults = await agentSqlite.SynchronizeAsync(tables).ConfigureAwait(false);
                Console.WriteLine(sqliteResults);
            }
            while (Console.ReadKey().Key != ConsoleKey.Escape);

            Console.WriteLine("End");
        }
    }
}