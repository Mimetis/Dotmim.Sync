using Dotmim.Sync;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using System;
using System.Threading.Tasks;

namespace HelloMultiScopesSync
{
    class Program
    {
        private static string serverConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=AdventureWorks;Integrated Security=true;";
        private static string clientConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=Client;Integrated Security=true;";

        static async Task Main() => await SynchronizeAsync();


        private static async Task SynchronizeAsync()
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql 

            // Create 2 Sql Sync providers
            // First provider is using the Sql change tracking feature. Don't forget to enable it on your database until running this code !
            // For instance, use this SQL statement on your server database : ALTER DATABASE AdventureWorks  SET CHANGE_TRACKING = ON  (CHANGE_RETENTION = 10 DAYS, AUTO_CLEANUP = ON)  
            // Otherwise, if you don't want to use Change Tracking feature, just change 'SqlSyncChangeTrackingProvider' to 'SqlSyncProvider'
            var serverProvider = new SqlSyncProvider(serverConnectionString);
       
            // Second provider is using plain old Sql Server provider, relying on triggers and tracking tables to create the sync environment
            var clientProvider = new SqliteSyncProvider("adv.db");

            // Tables involved in the first scope sync process
            var tables1 = new string[] {"ProductCategory", "ProductModel", "Product" };

            // Tables involved in the second scope sync process:
            var tables2 = new string[] {"Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" };

            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverProvider);

            do
            {
                // Launch the sync process
                var s1 = await agent.SynchronizeAsync(tables1, "products");
                // Write results
                Console.WriteLine(s1);

                s1 = await agent.SynchronizeAsync(tables2, "customers");
                // Write results
                Console.WriteLine(s1);

            } while (Console.ReadKey().Key != ConsoleKey.Escape);

            Console.WriteLine("End");
        }
    }
}
