﻿using Dotmim.Sync;
using Dotmim.Sync.Batch;
using Dotmim.Sync.SqlServer;
using System;
using System.IO;
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
            var agent = new SyncAgent(clientProvider, serverProvider);

            // First sync to initialize everything
            var s1 = await agent.SynchronizeAsync(tables);
            // Write results
            Console.WriteLine(s1);

            // Make some changes on the server side
            var sc = serverProvider.CreateConnection();
            await Helper.InsertOneCustomerAsync(sc, "John", "Doe");
            await Helper.InsertOneProductCategoryAsync(sc, Guid.NewGuid(), "Shoes 2020" + Path.GetRandomFileName());
            await Helper.DeleteOneSalesDetailOrderAsync(sc, 113141);
            await Helper.DeleteOneSalesDetailOrderAsync(sc, 113142);
            await Helper.DeleteOneSalesDetailOrderAsync(sc, 113143);
            await Helper.DeleteOneSalesDetailOrderAsync(sc, 113144);

            // Get local orchestrator to get some info during sync
            var localOrchestrator = agent.LocalOrchestrator;
            var remoteOrchestrator = agent.LocalOrchestrator;

            remoteOrchestrator.OnDatabaseChangesSelecting(args =>
            {
                Console.WriteLine($"Getting changes from local database:");
                Console.WriteLine($"Batch directory: {args.BatchDirectory}. Batch size: {args.BatchSize}. Is first sync: {args.IsNew}");
                Console.WriteLine($"From: {args.FromTimestamp}. To: {args.ToTimestamp}.");
                Console.WriteLine($"--------------------------------------------");
            });


            remoteOrchestrator.OnTableChangesSelecting(args =>
            {
                Console.WriteLine($"Getting changes from local database for table:{args.SchemaTable.GetFullName()}");
                Console.WriteLine($"{args.Command.CommandText}");
                Console.WriteLine($"--------------------------------------------");

            });

            remoteOrchestrator.OnRowsChangesSelected(args =>
            {
                Console.WriteLine($"Row read from local database for table:{args.SchemaTable.GetFullName()}");
                Console.WriteLine($"{args.SyncRow}");
                Console.WriteLine($"--------------------------------------------");
            });


            remoteOrchestrator.OnTableChangesSelected(args =>
            {
                Console.WriteLine($"Table: {args.SchemaTable.GetFullName()} read. Rows count:{args.BatchInfo.RowsCount}.");
                Console.WriteLine($"Directory: {args.BatchInfo.DirectoryName}. Number of files: {args.BatchPartInfos?.Count()} ");
                Console.WriteLine($"Changes: {args.TableChangesSelected.TotalChanges} ({args.TableChangesSelected.Upserts}/{args.TableChangesSelected.Deletes})");
                Console.WriteLine($"--------------------------------------------");

            });

            remoteOrchestrator.OnDatabaseChangesSelected(args =>
            {
                Console.WriteLine($"Directory: {args.BatchInfo.DirectoryName}. Number of files: {args.BatchInfo.BatchPartsInfo?.Count()} ");
                Console.WriteLine($"Total: {args.ChangesSelected.TotalChangesSelected} " +
                                  $"({args.ChangesSelected.TotalChangesSelectedUpdates}/{args.ChangesSelected.TotalChangesSelectedDeletes})");
                foreach (var table in args.ChangesSelected.TableChangesSelected)
                    Console.WriteLine($"Table: {table.TableName}. Total: {table.TotalChanges} ({table.Upserts} / {table.Deletes})");
                Console.WriteLine($"--------------------------------------------");
            });


            // Just before applying something locally, at the database level
            localOrchestrator.OnDatabaseChangesApplying(args =>
            {
                Console.WriteLine($"Directory: {args.ApplyChanges.Changes.DirectoryName}. " +
                    $"Number of files: {args.ApplyChanges.Changes.BatchPartsInfo?.Count()} ");

                Console.WriteLine($"Total: {args.ApplyChanges.Changes.RowsCount} ");
                Console.WriteLine($"--------------------------------------------");
            });

            // Just before applying changes locally, at the table level
            localOrchestrator.OnTableChangesApplying(args =>
            {
                if (args.BatchPartInfos != null)
                {
                    var syncTable = localOrchestrator.LoadTableFromBatchInfo(
                        args.BatchInfo, args.SchemaTable.TableName, args.SchemaTable.SchemaName, args.State);

                    if (syncTable.HasRows)
                    {

                        Console.WriteLine($"- --------------------------------------------");
                        Console.WriteLine($"- Applying [{args.State}] changes to Table {args.SchemaTable.GetFullName()}");
                        Console.WriteLine($"Changes for table {args.SchemaTable.TableName}. Rows:{syncTable.Rows.Count}");
                        foreach (var row in syncTable.Rows)
                            Console.WriteLine(row);
                    }

                }
            });


            localOrchestrator.OnRowsChangesApplying(async args =>
            {
                Console.WriteLine($"- --------------------------------------------");
                Console.WriteLine($"- In memory rows that are going to be Applied");
                foreach (var row in args.SyncRows)
                    Console.WriteLine(row);

                Console.WriteLine();
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
