using Dotmim.Sync;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;

namespace FilterWebSyncClient
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
                                                                
            // Second provider is using plain old Sql Server provider, relying on triggers and tracking tables to create the sync environment
            var clientProvider = new SqlSyncProvider(clientConnectionString);

            // Set the web server Options
            var options = new SyncOptions
            {
                BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "client")
            };

            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverOrchestrator, options);

            var progress = new SynchronousProgress<ProgressArgs>(
               pa => Console.WriteLine($"{pa.ProgressPercentage:p}\t {pa.Message}"));

            var parameters = new SyncParameters
            {
                { "City", "Toronto" },
                // Because I've specified that "postal" could be null, 
                // I can set the value to DBNull.Value (and the get all postal code in Toronto city)
                { "postal", DBNull.Value }
            };
            do
            {
                try
                {
                    // Launch the sync process
                    var s1 = await agent.SynchronizeAsync(progress);
                    // Write results
                    Console.WriteLine(s1);

                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }

            } while (Console.ReadKey().Key != ConsoleKey.Escape);

            Console.WriteLine("End");
        }
    }
}
