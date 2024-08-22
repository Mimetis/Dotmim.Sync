using Dotmim.Sync;
using Dotmim.Sync.SqlServer;
using System;
using System.Threading.Tasks;

namespace Conflict
{
    internal class Program
    {

        private static string serverConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=AdventureWorks;Integrated Security=true;";
        private static string clientConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=Client;Integrated Security=true;";

        private static async Task Main() => await ConflictAsync().ConfigureAwait(false);

        private static async Task ConflictAsync()
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql

            // Create 2 Sql Sync providers
            var serverProvider = new SqlSyncProvider(serverConnectionString);

            // Second provider is using plain old Sql Server provider, relying on triggers and tracking tables to create the sync environment
            var clientProvider = new SqlSyncProvider(clientConnectionString);

            // Tables involved in the sync process:
            var tables = new string[]
            {
                "ProductCategory", "ProductModel", "Product",
                "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail",
            };

            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverProvider);

            Console.WriteLine("- Initialize the databases with initial data");

            // Make a first sync to have everything in place
            Console.WriteLine(await agent.SynchronizeAsync(tables).ConfigureAwait(false));

            Console.WriteLine("- Insert data in client and server databases to generate a conflict Insert Client - Insert Server");

            var id = new Random(50000).Next();

            // Insert a value on client
            await Helper.InsertNConflictsCustomerAsync(clientProvider.CreateConnection(), 10, id, "John", "Clientdoe").ConfigureAwait(false);

            // Insert a value on server with same key, to generate a conflict
            await Helper.InsertNConflictsCustomerAsync(serverProvider.CreateConnection(), 10, id, "John", "Serverdoe").ConfigureAwait(false);

            do
            {
                try
                {
                    Console.WriteLine("- Launch synchronization");
                    var res = await agent.SynchronizeAsync().ConfigureAwait(false);
                    Console.WriteLine(res);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
            while (Console.ReadKey().Key != ConsoleKey.Escape);

            Console.WriteLine("End");
        }
    }
}