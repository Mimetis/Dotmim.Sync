using Dotmim.Sync;
using Dotmim.Sync.Batch;
using Dotmim.Sync.SqlServer;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Spy
{

    class Program
    {
        private static string serverConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=AdventureWorks;Integrated Security=true;";
        private static string clientConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=Client;Integrated Security=true;";

        static async Task Main(string[] args)
        {
            await SpyWhenSyncAsync();
        }




        private static async Task SpyWhenSyncAsync()
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql 

            var serverProvider = new SqlSyncProvider(serverConnectionString);
            var clientProvider = new SqlSyncProvider(clientConnectionString);
            var tables = new string[] {"ProductCategory", "ProductModel", "Product",
            "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" };

            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverProvider, tables);

            // First sync to initialize everything
            var s1 = await agent.SynchronizeAsync();
            // Write results
            Console.WriteLine(s1);

            // Make some changes on the server side
            var sc = serverProvider.CreateConnection();
            await Helper.InsertOneCustomerAsync(sc, "John", "Doe");
            await Helper.InsertOneProductCategoryAsync(sc, "Shoes 2020");
            await Helper.DeleteOneSalesDetailOrderAsync(sc, 113141);
            await Helper.DeleteOneSalesDetailOrderAsync(sc, 113142);
            await Helper.DeleteOneSalesDetailOrderAsync(sc, 113143);
            await Helper.DeleteOneSalesDetailOrderAsync(sc, 113144);


            // Get local orchestrator to get some info during sync
            var localOrchestrator = agent.LocalOrchestrator;

            // Just before applying something locally, at the database level
            localOrchestrator.OnDatabaseChangesApplying(async args =>
            {
                Console.WriteLine($"--------------------------------------------");
                Console.WriteLine($"Applying changes to the local database:");
                Console.WriteLine($"--------------------------------------------");
                Console.WriteLine($"Policy applied : {args.ApplyChanges.Policy}");
                Console.WriteLine($"Last timestamp used to compare local rows : {args.ApplyChanges.LastTimestamp}");
                Console.WriteLine("List of ALL rows to be sync locally:");

                foreach (var table in args.ApplyChanges.Setup.Tables)
                {
                    // Get all batch part
                    var bpiTables = args.ApplyChanges.Changes.GetBatchPartsInfo(table.TableName, table.SchemaName);

                    foreach(var bpiTable in bpiTables)
                    {
                        // Get path to the bpiTable
                        var path = args.ApplyChanges.Changes.GetBatchPartInfoPath(bpiTable).FullPath;
                        // Get the schema table
                        var schemaTable = args.ApplyChanges.Changes.SanitizedSchema.Tables[table.TableName, table.SchemaName];

                        Console.ForegroundColor = ConsoleColor.White;
                        Console.WriteLine($"Changes for table {table.TableName}. Rows:{bpiTable.RowsCount}");
                        Console.ResetColor();
                        foreach (var row in agent.Options.LocalSerializer.ReadRowsFromFile(path, schemaTable))
                        {
                            Console.WriteLine(row);
                        }

                    }

                }
            });

            // Just before applying changes locally, at the table level
            localOrchestrator.OnTableChangesBatchApplying(args =>
            {
                if (args.Changes != null && args.Changes.HasRows)
                {
                    Console.WriteLine($"- --------------------------------------------");
                    Console.WriteLine($"- Applying [{args.State}] changes to Table {args.Changes.GetFullName()}");

                    foreach (var row in args.Changes.Rows)
                        Console.WriteLine($"- {row}");

                }
            });

            // Once changes are applied
            localOrchestrator.OnTableChangesApplied(args =>
            {
                Console.WriteLine($"- Applied [{args.TableChangesApplied.State}] to table [{args.TableChangesApplied.TableName}]: Applied:{args.TableChangesApplied.Applied}. Failed:{args.TableChangesApplied.Failed}. Conflicts:{args.TableChangesApplied.ResolvedConflicts}. ");
            });


            // Just before applying something locally, at the database level
            localOrchestrator.OnDatabaseChangesApplied(args =>
            {
                Console.WriteLine($"--------------------------------------------");
                Console.WriteLine($"Changes applied to the local database:");
                Console.WriteLine($"--------------------------------------------");

                Console.WriteLine(args.ChangesApplied);
            });

            // Launch the sync process
            var s2 = await agent.SynchronizeAsync();
            // Write results
            Console.WriteLine(s2);

        }

    }

}
