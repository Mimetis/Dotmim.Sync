using Dotmim.Sync;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using System;
using System.Threading.Tasks;

namespace CustomProvider
{
    internal class Program
    {
        private static string serverConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=AdventureWorks;Integrated Security=true;";
        private static string clientConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=Client;Integrated Security=true;";

        static async Task Main()
        {
            await SynchronizeAsync();
        }

        private static async Task SynchronizeAsync()
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql 

            var serverProvider = new SqlSyncProvider(serverConnectionString);

            // Tables involved in the sync process:
            var tables = new string[] {"ProductCategory", "ProductModel", "Product",
                        "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" };

            // First DownloadOnly provider for Sqlite
            var clientSqliteProvider = new SqliteSyncDownloadOnlyProvider("adv.db");

            // Second DownloadOnly provider for SqlServer
            var clientSqlServerProvider = new SqlSyncDownloadOnlyProvider(clientConnectionString);

            do
            {
                // Creating an agent that will handle all the process
                var agentSqlite = new SyncAgent(clientSqliteProvider, serverProvider);
                var sqliteResults = await agentSqlite.SynchronizeAsync(tables);
                Console.WriteLine(sqliteResults);

                var agentSqlServer = new SyncAgent(clientSqlServerProvider, serverProvider);
                var sqlServerResults = await agentSqlServer.SynchronizeAsync(tables);
                Console.WriteLine(sqlServerResults);

            } while (Console.ReadKey().Key != ConsoleKey.Escape);

            Console.WriteLine("End");
        }

    }
}
