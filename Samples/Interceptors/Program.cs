using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using System;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Interceptors
{
    class Program
    {
        private static string serverConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=AdventureWorks;Integrated Security=true;";
        private static string clientConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=Client;Integrated Security=true;";

        static async Task Main(string[] args)
        {
            await PreventDeletionAsync();
        }


        private static async Task PreventDeletionAsync()
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql 

            var serverProvider = new SqlSyncProvider(serverConnectionString);
            var clientProvider = new SqlSyncProvider(clientConnectionString);

            // Tables involved in the sync process:
            var tables = new string[] {"Product" };

            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverProvider, tables);

            // First sync to have some rows on client
            var s1 = await agent.SynchronizeAsync();
            // Write results
            Console.WriteLine(s1);

           
            // do not delete product row. it's your choice !
            agent.LocalOrchestrator.OnTableChangesApplying(args =>
            {
                if (args.State == DataRowState.Deleted && args.SchemaTable.TableName == "Product")
                {
                    Console.WriteLine($"Preventing deletion on {args.BatchPartInfos.Sum(bpi => bpi.RowsCount)} rows.");
                    args.Cancel = true;
                }
            });

            var c = serverProvider.CreateConnection();
            var cmd = c.CreateCommand();
            cmd.Connection = c;
            cmd.CommandText = "DELETE FROM Product WHERE ProductId >= 750 AND ProductId < 760";
            c.Open();
            cmd.ExecuteNonQuery();
            c.Close();

            // Second sync
            s1 = await agent.SynchronizeAsync();
            // Write results
            Console.WriteLine(s1);

            // Third sync
            s1 = await agent.SynchronizeAsync();
            // Write results
            Console.WriteLine(s1);

        }
    }
}
