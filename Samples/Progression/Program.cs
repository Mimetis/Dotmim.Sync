using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Progression
{
    class Program
    {
        private static string serverConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=AdventureWorks;Integrated Security=true;";
        private static string clientConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=Client;Integrated Security=true;";

        static async Task Main(string[] args)
        {
            await SynchronizeAsync();
        }


        private static async Task SynchronizeAsync()
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql 

            // Create 2 Sql Sync providers
            // First provider is using the Sql change tracking feature. Don't forget to enable it on your database until running this code !
            // For instance, use this SQL statement on your server database : ALTER DATABASE AdventureWorks  SET CHANGE_TRACKING = ON  (CHANGE_RETENTION = 10 DAYS, AUTO_CLEANUP = ON)  
            // Otherwise, if you don't want to use Change Tracking feature, just change 'SqlSyncChangeTrackingProvider' to 'SqlSyncProvider'
            var serverProvider = new SqlSyncChangeTrackingProvider(serverConnectionString);
            var clientProvider = new SqlSyncProvider(clientConnectionString);

            // Tables involved in the sync process:
            var tables = new string[] {"ProductCategory", "ProductModel", "Product",
            "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" };

            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverProvider, tables);

            // Using the IProgress<T> pattern to handle progession dring the synchronization
            // Be careful, Progress<T> is not synchronous. Use SynchronousProgress<T> instead !
            var progress = new SynchronousProgress<ProgressArgs>(args => Console.WriteLine($"{args.Context.SyncStage}:\t{args.Message}"));

            // Because we are in a TCP project, we are able to reach the RemoteOrchestrator progress
            // This "trick" will not work on a HTTP mode sync project
            var remoteProgress = new SynchronousProgress<ProgressArgs>(s =>
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
                Console.ResetColor();
            });
            agent.AddRemoteProgress(remoteProgress);

            // --------------------------------------------
            // Using Interceptors
            // --------------------------------------------
            var cts = new CancellationTokenSource();

            agent.LocalOrchestrator.OnTableChangesApplying((args) =>
            {
                if (args.SchemaTable.TableName == "Table_That_Should_Not_Be_Sync")
                    cts.Cancel();
            });

            agent.LocalOrchestrator.OnTableChangesSelecting(args =>
            {
                Console.WriteLine($"-------- Getting changes from table {args.TableName} ...");
            });

            agent.LocalOrchestrator.OnTableChangesSelected(args =>
            {
                if (args.Changes == null || args.Changes.Rows.Count == 0)
                    return;

                foreach (var row in args.Changes.Rows)
                    Console.WriteLine(row);
            });

            do
            {
                // Launch the sync process
                var s1 = await agent.SynchronizeAsync(SyncType.Normal, cts.Token, progress);
                // Write results
                Console.WriteLine(s1);

            } while (Console.ReadKey().Key != ConsoleKey.Escape);

            Console.WriteLine("End");
        }
    }
}
