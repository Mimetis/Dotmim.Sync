using Dotmim.Sync;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Microsoft.Extensions.Configuration;
using System;
using System.Threading.Tasks;

namespace SqliteEncryption
{
    internal class Program
    {
        private static IConfigurationRoot configuration;

        private static async Task Main()
        {
            configuration = new ConfigurationBuilder()
              .AddJsonFile("appsettings.json", false, true)
              .AddJsonFile("appsettings.local.json", true, true)
              .Build();

            await SynchronizeAsync().ConfigureAwait(false);
        }

        private static async Task SynchronizeAsync()
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql

            var sqlConnectionString = configuration.GetConnectionString("SqlConnection");
            var serverProvider = new SqlSyncProvider(sqlConnectionString);

            // connection string should be something like "Data Source=AdventureWorks.db;Password=..."
            var sqliteConnectionString = configuration.GetConnectionString("SqliteConnection");
            var clientProvider = new SqliteSyncProvider(sqliteConnectionString);

            // You can use a SqliteConnectionStringBuilder() as well, like this:
            // var builder = new SqliteConnectionStringBuilder();
            // builder.DataSource = "AdventureWorks.db";
            // builder.Password = "...";

            // Tables involved in the sync process:
            var tables = new string[]
            {
                        "ProductCategory", "ProductModel", "Product",
                        "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail",
            };

            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverProvider, tables);

            do
            {
                // Launch the sync process
                var s1 = await agent.SynchronizeAsync().ConfigureAwait(false);

                // Write results
                Console.WriteLine(s1);
            }
            while (Console.ReadKey().Key != ConsoleKey.Escape);

            Console.WriteLine("End");
        }
    }
}