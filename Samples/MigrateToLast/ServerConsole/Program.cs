using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using System;
using System.Threading.Tasks;

namespace ServerConsole
{
    class Program
    {
        private static string serverConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=AdventureWorks;Integrated Security=true;";

        static async Task Main() => await UpgradeAsync();


        private static async Task UpgradeAsync()
        {
            var serverProvider = new SqlSyncProvider(serverConnectionString);

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider);

            do
            {
                var progress = new SynchronousProgress<ProgressArgs>(args => Console.WriteLine($"{args.ProgressPercentage:p}:\t{args.Message}"));

                await remoteOrchestrator.UpgradeAsync(progress:progress);

                // Write results
                Console.WriteLine("Upgrade to last version done");

            } while (Console.ReadKey().Key != ConsoleKey.Escape);

            Console.WriteLine("End");
        }
    }
}
