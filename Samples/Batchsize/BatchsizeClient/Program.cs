using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using System;
using System.Threading.Tasks;

namespace BatchsizeClient
{
    class Program
    {
        private static string clientConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=Client;Integrated Security=true;";

        static async Task Main(string[] args)
        {
            Console.WriteLine("Be sure the web api has started. Then click enter..");
            Console.ReadLine();
            await SynchronizeAsync();
        }

        private static async Task SynchronizeAsync()
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql 

            var serverOrchestrator = new WebClientOrchestrator("https://localhost:44342/api/sync");

            //var clientProvider = new SqlSyncProvider(clientConnectionString);
            var clientProvider = new SqliteSyncProvider("adv.db");

            var clientOptions = new SyncOptions { BatchSize = 100, ProgressLevel = SyncProgressLevel.Debug };



            var progress = new SynchronousProgress<ProgressArgs>(
                s => Console.WriteLine($"{s.ProgressPercentage:p}:  \t[{s.Source[..Math.Min(4, s.Source.Length)]}] {s.TypeName}:\t{s.Message}"));

            var agent = new SyncAgent(clientProvider, serverOrchestrator, clientOptions);

            do
            {
                // Launch the sync process
                var s1 = await agent.SynchronizeAsync(progress);
                // Write results
                Console.WriteLine(s1);

            } while (Console.ReadKey().Key != ConsoleKey.Escape);

            Console.WriteLine("End");
        }
    }
}
