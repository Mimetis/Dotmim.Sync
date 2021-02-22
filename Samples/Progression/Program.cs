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

            agent.Options.BatchSize = 20;

            // Using the IProgress<T> pattern to handle progession dring the synchronization
            // Be careful, Progress<T> is not synchronous. Use SynchronousProgress<T> instead !
            var progress = new SynchronousProgress<ProgressArgs>(args => Console.WriteLine($"{args.PogressPercentageString}:\t{args.Message}"));

            // --------------------------------------------
            // Using Interceptors
            // --------------------------------------------

            // CancellationTokenSource is used to cancel a sync process in the next example
            var cts = new CancellationTokenSource();

            // Intercept a table changes selecting
            // Because the changes are not yet selected, we can easily interrupt the process with the cancellation token
            agent.LocalOrchestrator.OnTableChangesSelecting(args =>
            {
                Console.WriteLine($"-------- Getting changes from table {args.Table.GetFullName()} ...");

                if (args.Table.TableName == "Table_That_Should_Not_Be_Sync")
                    cts.Cancel();
            });

            // Intercept a table changes applying with a particular state [Upsert] or [Deleted]
            // The rows included in the args.Changes table will be applied right after.
            agent.LocalOrchestrator.OnTableChangesBatchApplying(args =>
            {
                Console.WriteLine($"-------- Applying changes {args.State} to table {args.Changes.GetFullName()} ...");

                if (args.Changes == null || args.Changes.Rows.Count == 0)
                    return;

                foreach (var row in args.Changes.Rows)
                    Console.WriteLine(row);
            });

            // Intercept a table changes selected.
            // The rows included in the args.Changes have been selected from the datasource and will be sent to the server
            agent.LocalOrchestrator.OnTableChangesSelected(args =>
            {
                if (args.Changes == null || args.Changes.Rows.Count == 0)
                    return;

                foreach (var row in args.Changes.Rows)
                    Console.WriteLine(row);
            });

            // ------------------------
            // Because we are in a TCP mode, we can hook the server side events
            // In an HTTP mode, the code below won't work
            // ------------------------

            agent.RemoteOrchestrator.OnTableChangesSelecting(args =>
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"-------- Getting changes from table {args.Table.GetFullName()} ...");
                Console.ResetColor();
            });

            agent.RemoteOrchestrator.OnTableChangesSelected(args =>
            {
                if (args.Changes == null || args.Changes.Rows.Count == 0)
                    return;

                Console.ForegroundColor = ConsoleColor.Yellow;
                foreach (var row in args.Changes.Rows)
                    Console.WriteLine(row);
                Console.ResetColor();
            });

            agent.RemoteOrchestrator.OnTableChangesBatchApplying(args =>
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"-------- Applying changes {args.State} to table {args.Changes.GetFullName()} ...");

                if (args.Changes == null || args.Changes.Rows.Count == 0)
                    return;

                foreach (var row in args.Changes.Rows)
                    Console.WriteLine(row);
                Console.ResetColor();
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
