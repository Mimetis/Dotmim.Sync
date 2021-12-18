using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using System;
using System.Linq;
using System.Runtime.CompilerServices;
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
            var progress = new SynchronousProgress<ProgressArgs>(args => Console.WriteLine($"{args.ProgressPercentage:p}:\t{args.Message}"));

            // --------------------------------------------
            // Using Interceptors
            // --------------------------------------------

            // CancellationTokenSource is used to cancel a sync process in the next example
            var cts = new CancellationTokenSource();


            // Intercept a table changes selecting
            // Because the changes are not yet selected, we can easily interrupt the process with the cancellation token
            agent.LocalOrchestrator.OnTableChangesSelecting(args =>
            {
                Console.WriteLine($"-------- Getting changes from table {args.SchemaTable.GetFullName()} ...");

                if (args.SchemaTable.TableName == "Table_That_Should_Not_Be_Sync")
                    cts.Cancel();
            });

            // Row has been selected from datasource.
            // You can change the synrow before the row is serialized on the disk.
            agent.LocalOrchestrator.OnTableChangesSelectedSyncRow(args =>
            {
                Console.Write(".");
            });

            // Tables changes have been selected
            // we can have all the batch part infos generated
            agent.RemoteOrchestrator.OnTableChangesSelected(tcsa =>
            {
                Console.WriteLine($"Table {tcsa.SchemaTable.GetFullName()}: " +
                    $"Files generated count:{tcsa.BatchPartInfos.Count()}. " +
                    $"Rows Count:{tcsa.TableChangesSelected.TotalChanges}");
            });


            // This event is raised when a table is applying some rows, available on the disk
            agent.LocalOrchestrator.OnTableChangesApplying(args =>
            {
                Console.WriteLine($"Table {args.SchemaTable.GetFullName()}: " +
                    $"Applying changes from {args.BatchPartInfos.Count()} files. " +
                    $"{args.BatchPartInfos.Sum(bpi => bpi.RowsCount)} rows.");
            });

            // This event is raised for each batch rows (maybe 1 or more in each batch)
            // that will be applied on the datasource
            // You can change something to the rows before they are applied here
            agent.LocalOrchestrator.OnTableChangesApplyingSyncRows(args =>
            {
                foreach (var syncRow in args.SyncRows)
                    Console.Write(".");
            });

            // This event is raised once all rows for a table have been applied
            agent.LocalOrchestrator.OnTableChangesApplied(args =>
            {
                Console.WriteLine();
                Console.WriteLine($"Table applied: ");
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
