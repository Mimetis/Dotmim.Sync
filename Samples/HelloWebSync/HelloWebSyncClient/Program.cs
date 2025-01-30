using Dotmim.Sync;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.Web.Client;
using System;
using System.Threading.Tasks;

namespace HelloWebSyncClient
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            Console.WriteLine("Be sure the web api has started. Then click enter..");
            Console.ReadLine();
            await SynchronizeAsync().ConfigureAwait(false);
        }

        private static async Task SynchronizeAsync()
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql
            var serverOrchestrator = new WebRemoteOrchestrator("http://localhost:5213/api/sync");

            // Second provider is using plain old Sql Server provider, relying on triggers and tracking tables to create the sync environment
            // var clientProvider = new SqlSyncProvider(clientConnectionString);
            var clientProvider = new SqliteSyncProvider("adv.db");

            var options = new SyncOptions
            {
                BatchSize = 1000,
            };

            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverOrchestrator, options);

            do
            {
                try
                {
                    var progress = new SynchronousProgress<ProgressArgs>(args => Console.WriteLine($"{args.ProgressPercentage:p}:\t{args.Message}"));

                    // Launch the sync process
                    var s1 = await agent.SynchronizeAsync(Dotmim.Sync.Enumerations.SyncType.Reinitialize, progress).ConfigureAwait(false);

                    // Write results
                    Console.WriteLine(s1);
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