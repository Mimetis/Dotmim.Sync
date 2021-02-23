using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SampleConsole;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Events;
using Npgsql;
using Microsoft.Extensions.Configuration;
using NpgsqlTypes;
using Newtonsoft.Json;
using System.Collections.Generic;
using Dotmim.Sync.Postgres;
using Dotmim.Sync.Postgres.Builders;
using Dotmim.Sync.MySql;
using System.Linq;
using System.Transactions;
using System.Threading;
using MySql.Data.MySqlClient;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Builder;
using System.Text;
using System.Diagnostics;
using Dotmim.Sync.Serialization;

internal class Program
{
    public static string serverDbName = "AdventureWorks";
    public static string serverProductCategoryDbName = "AdventureWorksProductCategory";
    public static string clientDbName = "Client";
    public static string[] allTables = new string[] {"ProductDescription", "ProductCategory",
                                                    "ProductModel", "Product",
                                                    "Address", "Customer", "CustomerAddress",
                                                    "SalesOrderHeader", "SalesOrderDetail" };

    public static string[] oneTable = new string[] { "ProductCategory" };
    private static async Task Main(string[] args)
    {
        // await SynchronizeWithFiltersAndMultiScopesAsync();
        // await TestMultiCallToMethodsAsync();
        //await CreateSnapshotAsync();
        // await SyncHttpThroughKestrellAsync();
        // await SyncThroughWebApiAsync();
        //await SynchronizeWithFiltersAsync();
        await SynchronizeAsync();
    }

    private static async Task Snapshot_Then_ReinitializeAsync()
    {
        var clientFileName = "AdventureWorks.db";

        var tables = new string[] { "Customer" };

        var setup = new SyncSetup(tables)
        {
            // optional :
            StoredProceduresPrefix = "ussp_",
            StoredProceduresSuffix = "",
            TrackingTablesPrefix = "",
            TrackingTablesSuffix = "_tracking"
        };
        setup.Tables["Customer"].SyncDirection = SyncDirection.DownloadOnly;

        var options = new SyncOptions();

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s => Console.WriteLine($"{s.PogressPercentageString}:\t{s.Source}:\t{s.Message}"));

        // Be sure client database file is deleted is already exists
        if (File.Exists(clientFileName))
            File.Delete(clientFileName);

        // Create 2 Sql Sync providers
        // sql with change tracking enabled
        var serverProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqliteSyncProvider(clientFileName);

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

        Console.WriteLine();
        Console.WriteLine("----------------------");
        Console.WriteLine("0 - Initiliaze. Initialize Client database and get all Customers");

        // First sync to initialize
        var r = await agent.SynchronizeAsync(progress);
        Console.WriteLine(r);


        // DeprovisionAsync
        Console.WriteLine();
        Console.WriteLine("----------------------");
        Console.WriteLine("1 - Deprovision The Server");

        var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);
        // We are in change tracking mode, so no need to deprovision triggers and tracking table. But it's part of the sample
        await remoteOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable, progress: progress);

        var serverScope = await remoteOrchestrator.GetServerScopeAsync(progress: progress);

        serverScope.Setup = null;
        serverScope.Schema = null;

        // save the server scope
        await remoteOrchestrator.SaveServerScopeAsync(serverScope, progress: progress);


        // DeprovisionAsync
        Console.WriteLine();
        Console.WriteLine("----------------------");
        Console.WriteLine("2 - Provision Again With New Setup");

        tables = new string[] { "Customer", "ProductCategory" };

        setup = new SyncSetup(tables)
        {
            // optional :
            StoredProceduresPrefix = "ussp_",
            StoredProceduresSuffix = "",
            TrackingTablesPrefix = "",
            TrackingTablesSuffix = "_tracking"
        };
        setup.Tables["Customer"].SyncDirection = SyncDirection.DownloadOnly;
        setup.Tables["ProductCategory"].SyncDirection = SyncDirection.DownloadOnly;

        remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);
        serverScope = await remoteOrchestrator.GetServerScopeAsync(progress: progress);

        var newSchema = await remoteOrchestrator.ProvisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable, progress: progress);

        serverScope.Setup = setup;
        serverScope.Schema = newSchema;

        // save the server scope
        await remoteOrchestrator.SaveServerScopeAsync(serverScope, progress: progress);

        // Snapshot
        Console.WriteLine();
        Console.WriteLine("----------------------");
        Console.WriteLine("3 - Create Snapshot");

        var snapshotDirctory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "Snapshots");

        options = new SyncOptions
        {
            SnapshotsDirectory = snapshotDirctory,
            BatchSize = 5000
        };

        remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);
        // Create a snapshot
        var bi = await remoteOrchestrator.CreateSnapshotAsync(progress: progress);

        Console.WriteLine("Create snapshot done.");
        Console.WriteLine($"Rows Count in the snapshot:{bi.RowsCount}");
        foreach (var bpi in bi.BatchPartsInfo)
            foreach (var table in bpi.Tables)
                Console.WriteLine($"File: {bpi.FileName}. Table {table.TableName}: Rows Count:{table.RowsCount}");

        // Snapshot
        Console.WriteLine();
        Console.WriteLine("----------------------");
        Console.WriteLine("4 - Sync again with Reinitialize Mode");


        agent = new SyncAgent(clientProvider, serverProvider, options, setup);

        r = await agent.SynchronizeAsync(SyncType.Reinitialize, progress);
        Console.WriteLine(r);


        Console.WriteLine();
        Console.WriteLine("----------------------");
        Console.WriteLine("5 - Check client rows");

        using var sqliteConnection = new SqliteConnection(clientProvider.ConnectionString);

        sqliteConnection.Open();

        var command = new SqliteCommand("Select count(*) from Customer", sqliteConnection);
        var customerCount = (long)command.ExecuteScalar();

        command = new SqliteCommand("Select count(*) from ProductCategory", sqliteConnection);
        var productCategoryCount = (long)command.ExecuteScalar();

        Console.WriteLine($"Customer Rows Count on Client Database:{customerCount} rows");
        Console.WriteLine($"ProductCategory Rows Count on Client Database:{productCategoryCount} rows");

        sqliteConnection.Close();

    }


    private static async Task SynchronizeComputedColumnAsync()
    {
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString("tcp_sv_yq0h0pxbfu3"));
        var clientProvider = new SqliteSyncProvider("sv0DAD1.db");
        var tables = new string[] { "PricesListDetail" };

        var snapshotProgress = new SynchronousProgress<ProgressArgs>(pa =>
        {
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine($"{pa.PogressPercentageString}\t {pa.Message}");
            Console.ResetColor();
        });

        var options = new SyncOptions { BatchSize = 1000 };

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, tables);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.PogressPercentageString}:\t{s.Source}:\t{s.Message}");
            Console.ResetColor();
        });

        do
        {
            // Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                var r = await agent.SynchronizeAsync(SyncType.Reinitialize, progress);
                // Write results
                Console.WriteLine(r);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }


            //Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }

    private static async Task SynchronizeAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
        //var clientProvider = new SqliteSyncProvider("advdazdazd.db");


        var snapshotProgress = new SynchronousProgress<ProgressArgs>(pa =>
        {
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine($"{pa.PogressPercentageString}\t {pa.Message}");
            Console.ResetColor();
        });
        //var snapshotDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "Snapshots");

        var options = new SyncOptions { BatchSize = 1000 };

        //Console.ForegroundColor = ConsoleColor.Gray;
        //Console.WriteLine($"Creating snapshot");
        //var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, new SyncSetup(allTables));
        //remoteOrchestrator.CreateSnapshotAsync(progress: snapshotProgress).GetAwaiter().GetResult();

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, allTables);


        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.PogressPercentageString}:\t{s.Source}:\t{s.Message}");
            Console.ResetColor();
        });

        do
        {
            // Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                // Upgrade to last version
                //if (await agent.RemoteOrchestrator.NeedsToUpgradeAsync())
                //    await agent.RemoteOrchestrator.UpgradeAsync(progress:progress);

                var r = await agent.SynchronizeAsync(progress);
                Console.WriteLine(r);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }


            //Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }

    private static async Task CreateSnapshotAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));

        var snapshotProgress = new SynchronousProgress<ProgressArgs>(pa =>
        {
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine($"{pa.PogressPercentageString}\t {pa.Message}");
            Console.ResetColor();
        });
        var snapshotDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "Snapshots");

        var options = new SyncOptions() { BatchSize = 1000, SnapshotsDirectory = snapshotDirectory };

        Console.ForegroundColor = ConsoleColor.Gray;
        Console.WriteLine($"Creating snapshot ");
        var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, new SyncSetup(allTables));
        var stopwatch = Stopwatch.StartNew();

        remoteOrchestrator.OnTableChangesSelected(args =>
        {
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine($"{args.PogressPercentageString}\t {args.Message}");
            Console.ResetColor();

        });

        await remoteOrchestrator.CreateSnapshotAsync(progress: snapshotProgress);
        stopwatch.Stop();

        var str = $"Snapshot created: {stopwatch.Elapsed.Minutes}:{stopwatch.Elapsed.Seconds}.{stopwatch.Elapsed.Milliseconds}";


        Console.WriteLine(str);
    }


    public static async Task SyncHttpThroughKestrellAsync()
    {
        // server provider
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
        //var clientProvider = new SqliteSyncProvider("AdvHugeD.db");

        // ----------------------------------
        // Client & Server side
        // ----------------------------------
        // snapshot directory
        // Sync options
        var options = new SyncOptions
        {
            SnapshotsDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "Snapshots"),
            BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "Tmp"),
            BatchSize = 10000,
        };

        // Create the setup used for your sync process
        //var tables = new string[] { "Employees" };


        var remoteProgress = new SynchronousProgress<ProgressArgs>(pa =>
        {
            Console.ForegroundColor = ConsoleColor.Gray;
            Console.WriteLine($"{pa.PogressPercentageString}\t {pa.Message}");
            Console.ResetColor();
        });

        var snapshotProgress = new SynchronousProgress<ProgressArgs>(pa =>
        {
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine($"{pa.PogressPercentageString}\t {pa.Message}");
            Console.ResetColor();
        });

        var localProgress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.PogressPercentageString}:\t{s.Message}");
            Console.ResetColor();
        });

        var configureServices = new Action<IServiceCollection>(services =>
        {
            services.AddSyncServer<SqlSyncProvider>(serverProvider.ConnectionString, allTables, options);

            // ----------------------------------
            // Create a snapshot
            // ----------------------------------
            //Console.ForegroundColor = ConsoleColor.Gray;
            //Console.WriteLine($"Creating snapshot");
            //var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, new SyncSetup(allTables));

            //remoteOrchestrator.CreateSnapshotAsync(progress: snapshotProgress).GetAwaiter().GetResult();


        });

        var serverHandler = new RequestDelegate(async context =>
        {
            var webServerManager = context.RequestServices.GetService(typeof(WebServerManager)) as WebServerManager;

            var webServerOrchestrator = webServerManager.GetOrchestrator(context);

            //webServerOrchestrator.OnHttpGettingRequest(req =>
            //    Console.WriteLine("Receiving Client Request:" + req.Context.SyncStage + ". " + req.HttpContext.Request.Host.Host + "."));

            //webServerOrchestrator.OnHttpSendingResponse(res =>
            //    Console.WriteLine("Sending Client Response:" + res.Context.SyncStage + ". " + res.HttpContext.Request.Host.Host));

            //webServerOrchestrator.OnHttpGettingChanges(args => Console.WriteLine("Getting Client Changes" + args));
            //webServerOrchestrator.OnHttpSendingChanges(args => Console.WriteLine("Sending Server Changes" + args));

            await webServerManager.HandleRequestAsync(context);

        });

        using var server = new KestrellTestServer(configureServices);
        var clientHandler = new ResponseDelegate(async (serviceUri) =>
        {
            do
            {
                Console.WriteLine("Web sync start");
                try
                {

                    var localOrchestrator = new WebClientOrchestrator(serviceUri, SerializersCollection.Utf8JsonSerializer);

                    var agent = new SyncAgent(clientProvider, localOrchestrator, options);
                    var s = await agent.SynchronizeAsync(SyncType.Reinitialize, localProgress);
                    Console.WriteLine(s);
                }
                catch (SyncException e)
                {
                    Console.WriteLine(e.ToString());
                }
                catch (Exception e)
                {
                    Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
                }


                Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
            } while (Console.ReadKey().Key != ConsoleKey.Escape);


        });
        await server.Run(serverHandler, clientHandler);

    }



    /// <summary>
    /// Test a client syncing through a web api
    /// </summary>
    private static async Task SyncThroughWebApiAsync()
    {
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        var handler = new HttpClientHandler { AutomaticDecompression = DecompressionMethods.GZip };
        var client = new HttpClient(handler) { Timeout = TimeSpan.FromMinutes(5) };

        var proxyClientProvider = new WebClientOrchestrator("https://localhost:44313/api/Sync", null, null, client);

        var options = new SyncOptions
        {
            BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "Tmp"),
            BatchSize = 2000,
        };

        // Create the setup used for your sync process
        //var tables = new string[] { "Employees" };


        var remoteProgress = new SynchronousProgress<ProgressArgs>(pa =>
        {
            Console.ForegroundColor = ConsoleColor.Gray;
            Console.WriteLine($"{pa.PogressPercentageString}\t {pa.Message}");
            Console.ResetColor();
        });

        var snapshotProgress = new SynchronousProgress<ProgressArgs>(pa =>
        {
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine($"{pa.PogressPercentageString}\t {pa.Message}");
            Console.ResetColor();
        });

        var localProgress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.PogressPercentageString}:\t{s.Message}");
            Console.ResetColor();
        });


        var agent = new SyncAgent(clientProvider, proxyClientProvider, options);


        Console.WriteLine("Press a key to start (be sure web api is running ...)");
        Console.ReadKey();
        do
        {
            Console.Clear();
            Console.WriteLine("Web sync start");
            try
            {

                var s = await agent.SynchronizeAsync(SyncType.Reinitialize, localProgress);
                Console.WriteLine(s);

            }
            catch (SyncException e)
            {
                Console.WriteLine(e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
            }


            Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");

    }

    private static async Task SynchronizeWithFiltersAndMultiScopesAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider1 = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
        var clientProvider2 = new SqliteSyncProvider("clientX3.db");


        var configureServices = new Action<IServiceCollection>(services =>
        {

            // Setup 1 : contains all tables, all columns with filter
            var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" });

            setup.Filters.Add("Customer", "CompanyName");

            var addressCustomerFilter = new SetupFilter("CustomerAddress");
            addressCustomerFilter.AddParameter("CompanyName", "Customer");
            addressCustomerFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            addressCustomerFilter.AddWhere("CompanyName", "Customer", "CompanyName");
            setup.Filters.Add(addressCustomerFilter);

            var addressFilter = new SetupFilter("Address");
            addressFilter.AddParameter("CompanyName", "Customer");
            addressFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "AddressId", "Address", "AddressId");
            addressFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            addressFilter.AddWhere("CompanyName", "Customer", "CompanyName");
            setup.Filters.Add(addressFilter);

            var orderHeaderFilter = new SetupFilter("SalesOrderHeader");
            orderHeaderFilter.AddParameter("CompanyName", "Customer");
            orderHeaderFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
            orderHeaderFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            orderHeaderFilter.AddWhere("CompanyName", "Customer", "CompanyName");
            setup.Filters.Add(orderHeaderFilter);

            var orderDetailsFilter = new SetupFilter("SalesOrderDetail");
            orderDetailsFilter.AddParameter("CompanyName", "Customer");
            orderDetailsFilter.AddJoin(Join.Left, "SalesOrderHeader").On("SalesOrderDetail", "SalesOrderID", "SalesOrderHeader", "SalesOrderID");
            orderDetailsFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
            orderDetailsFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            orderDetailsFilter.AddWhere("CompanyName", "Customer", "CompanyName");
            setup.Filters.Add(orderDetailsFilter);

            // Add pref suf
            setup.StoredProceduresPrefix = "filtered";
            setup.StoredProceduresSuffix = "";
            setup.TrackingTablesPrefix = "t";
            setup.TrackingTablesSuffix = "";

            var options = new SyncOptions();

            services.AddSyncServer<SqlSyncProvider>(serverProvider.ConnectionString, "Filters", setup);

            //contains only some tables with subset of columns
            var setup2 = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress" });

            setup2.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "FirstName", "LastName" });
            setup2.StoredProceduresPrefix = "restricted";
            setup2.StoredProceduresSuffix = "";
            setup2.TrackingTablesPrefix = "t";
            setup2.TrackingTablesSuffix = "";

            services.AddSyncServer<SqlSyncProvider>(serverProvider.ConnectionString, "Restricted", setup2, options);

        });

        var serverHandler = new RequestDelegate(async context =>
        {
            var webServerManager = context.RequestServices.GetService(typeof(WebServerManager)) as WebServerManager;

            var progress = new SynchronousProgress<ProgressArgs>(pa =>
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"{pa.PogressPercentageString}\t {pa.Message}");
                Console.ResetColor();
            });

            await webServerManager.HandleRequestAsync(context, default, progress);
        });


        using var server = new KestrellTestServer(configureServices);

        var clientHandler = new ResponseDelegate(async (serviceUri) =>
        {
            do
            {
                Console.Clear();
                Console.WriteLine("Web sync start");
                try
                {
                    var webClientOrchestrator = new WebClientOrchestrator(serviceUri);
                    var agent = new SyncAgent(clientProvider1, webClientOrchestrator, "Filters");

                    // Launch the sync process
                    if (!agent.Parameters.Contains("CompanyName"))
                        agent.Parameters.Add("CompanyName", "Professional Sales and Service");

                    // Using the Progress pattern to handle progession during the synchronization
                    var progress = new SynchronousProgress<ProgressArgs>(s =>
                        {
                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine($"{s.PogressPercentageString}:\t{s.Source}:\t{s.Message}");
                            Console.ResetColor();
                        });

                    var s = await agent.SynchronizeAsync(progress);
                    Console.WriteLine(s);


                    var agent2 = new SyncAgent(clientProvider2, webClientOrchestrator, "Restricted");

                    // Using the Progress pattern to handle progession during the synchronization
                    var progress2 = new SynchronousProgress<ProgressArgs>(s =>
                        {
                            Console.ForegroundColor = ConsoleColor.DarkGreen;
                            Console.WriteLine($"{s.PogressPercentageString}:\t{s.Source}:\t{s.Message}");
                            Console.ResetColor();
                        });
                    s = await agent2.SynchronizeAsync(progress2);
                    Console.WriteLine(s);
                }
                catch (SyncException e)
                {
                    Console.WriteLine(e.ToString());
                }
                catch (Exception e)
                {
                    Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
                }


                Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
            } while (Console.ReadKey().Key != ConsoleKey.Escape);


        });
        await server.Run(serverHandler, clientHandler);
    }


    private static async Task TestMultiCallToMethodsAsync()
    {
        var loop = 5000;

        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));

        //var clientDatabaseName = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db";
        //var clientProvider = new SqliteSyncProvider(clientDatabaseName);

        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        //var clientProvider = new MySqlSyncProvider(DBHelper.GetMySqlDatabaseConnectionString(clientDbName));


        var options = new SyncOptions();
        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, allTables);
        var orchestrator = agent.LocalOrchestrator;

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.PogressPercentageString}:\t{s.Message}");
            Console.ResetColor();
        });

        var r = await agent.SynchronizeAsync(progress);
        Console.WriteLine(r);

        // Be sure commands are loaded
        //await orchestrator.GetEstimatedChangesCountAsync().ConfigureAwait(false); ;
        //await orchestrator.ExistTableAsync(agent.Setup.Tables[0]).ConfigureAwait(false); ;
        //await orchestrator.GetLocalTimestampAsync().ConfigureAwait(false);
        //await orchestrator.GetSchemaAsync().ConfigureAwait(false);
        //await orchestrator.GetChangesAsync().ConfigureAwait(false);

        await orchestrator.ExistScopeInfoTableAsync(Dotmim.Sync.Builders.DbScopeType.Client, options.ScopeInfoTableName).ConfigureAwait(false);
        await orchestrator.ExistTableAsync(agent.Setup.Tables[0]).ConfigureAwait(false);
        await orchestrator.GetClientScopeAsync();

        var stopwatch = Stopwatch.StartNew();

        for (int i = 0; i < loop; i++)
        {
            //await orchestrator.GetEstimatedChangesCountAsync().ConfigureAwait(false);
            //await orchestrator.ExistTableAsync(agent.Setup.Tables[0]).ConfigureAwait(false);
            //await orchestrator.GetLocalTimestampAsync().ConfigureAwait(false);
            //await orchestrator.GetSchemaAsync().ConfigureAwait(false);
            //await orchestrator.GetChangesAsync().ConfigureAwait(false);

            await orchestrator.ExistScopeInfoTableAsync(Dotmim.Sync.Builders.DbScopeType.Client, options.ScopeInfoTableName).ConfigureAwait(false);
            await orchestrator.ExistTableAsync(agent.Setup.Tables[0]).ConfigureAwait(false);
            await orchestrator.GetClientScopeAsync();
        }

        stopwatch.Stop();
        var str = $"SQL Server [Connection Pooling, Connection not shared]: {stopwatch.Elapsed.Minutes}:{stopwatch.Elapsed.Seconds}.{stopwatch.Elapsed.Milliseconds}";
        Console.WriteLine(str);

        var stopwatch2 = Stopwatch.StartNew();
        using (var connection = agent.LocalOrchestrator.Provider.CreateConnection())
        {
            connection.Open();
            using (var transaction = connection.BeginTransaction())
            {
                for (int i = 0; i < loop; i++)
                {
                    //await orchestrator.GetEstimatedChangesCountAsync(connection: connection, transaction: transaction).ConfigureAwait(false);
                    //await orchestrator.ExistTableAsync(agent.Setup.Tables[0], connection: connection, transaction: transaction).ConfigureAwait(false);
                    //await orchestrator.GetLocalTimestampAsync(connection: connection, transaction: transaction).ConfigureAwait(false);
                    //await orchestrator.GetSchemaAsync(connection: connection, transaction: transaction).ConfigureAwait(false);
                    //await orchestrator.GetChangesAsync(connection: connection, transaction: transaction).ConfigureAwait(false);

                    await orchestrator.ExistScopeInfoTableAsync(Dotmim.Sync.Builders.DbScopeType.Client, options.ScopeInfoTableName, connection, transaction).ConfigureAwait(false);
                    await orchestrator.ExistTableAsync(agent.Setup.Tables[0], connection, transaction).ConfigureAwait(false);
                    await orchestrator.GetClientScopeAsync(connection, transaction);
                }
                transaction.Commit();
            }
            connection.Close();
        }
        stopwatch2.Stop();

        var str2 = $"SQL Server [Connection Pooling, Connection shared]: {stopwatch2.Elapsed.Minutes}:{stopwatch2.Elapsed.Seconds}.{stopwatch2.Elapsed.Milliseconds}";
        Console.WriteLine(str2);

        Console.WriteLine("End");
    }

   
    private static async Task SynchronizeWithFiltersAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
        //var clientProvider = new SqliteSyncProvider("clientX.db");

        var setup = new SyncSetup(new string[] {"ProductCategory",
                  "ProductModel", "Product",
                  "Address", "Customer", "CustomerAddress",
                  "SalesOrderHeader", "SalesOrderDetail" });

        // ----------------------------------------------------
        // Horizontal Filter: On rows. Removing rows from source
        // ----------------------------------------------------
        // Over all filter : "we Want only customer from specific city and specific postal code"
        // First level table : Address
        // Second level tables : CustomerAddress
        // Third level tables : Customer, SalesOrderHeader
        // Fourth level tables : SalesOrderDetail

        // Create a filter on table Address on City Washington
        // Optional : Sub filter on PostalCode, for testing purpose
        var addressFilter = new SetupFilter("Address");

        // For each filter, you have to provider all the input parameters
        // A parameter could be a parameter mapped to an existing colum : That way you don't have to specify any type, length and so on ...
        // We can specify if a null value can be passed as parameter value : That way ALL addresses will be fetched
        // A default value can be passed as well, but works only on SQL Server (MySql is a damn shity thing)
        addressFilter.AddParameter("City", "Address", true);

        // Or a parameter could be a random parameter bound to anything. In that case, you have to specify everything
        // (This parameter COULD BE bound to a column, like City, but for the example, we go for a custom parameter)
        addressFilter.AddParameter("postal", DbType.String, true, null, 20);

        // Then you map each parameter on wich table / column the "where" clause should be applied
        addressFilter.AddWhere("City", "Address", "City");
        addressFilter.AddWhere("PostalCode", "Address", "postal");
        setup.Filters.Add(addressFilter);

        var addressCustomerFilter = new SetupFilter("CustomerAddress");
        addressCustomerFilter.AddParameter("City", "Address", true);
        addressCustomerFilter.AddParameter("postal", DbType.String, true, null, 20);

        // You can join table to go from your table up (or down) to your filter table
        addressCustomerFilter.AddJoin(Join.Left, "Address").On("CustomerAddress", "AddressId", "Address", "AddressId");

        // And then add your where clauses
        addressCustomerFilter.AddWhere("City", "Address", "City");
        addressCustomerFilter.AddWhere("PostalCode", "Address", "postal");
        setup.Filters.Add(addressCustomerFilter);

        var customerFilter = new SetupFilter("Customer");
        customerFilter.AddParameter("City", "Address", true);
        customerFilter.AddParameter("postal", DbType.String, true, null, 20);
        customerFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
        customerFilter.AddJoin(Join.Left, "Address").On("CustomerAddress", "AddressId", "Address", "AddressId");
        customerFilter.AddWhere("City", "Address", "City");
        customerFilter.AddWhere("PostalCode", "Address", "postal");
        setup.Filters.Add(customerFilter);

        var orderHeaderFilter = new SetupFilter("SalesOrderHeader");
        orderHeaderFilter.AddParameter("City", "Address", true);
        orderHeaderFilter.AddParameter("postal", DbType.String, true, null, 20);
        orderHeaderFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
        orderHeaderFilter.AddJoin(Join.Left, "Address").On("CustomerAddress", "AddressId", "Address", "AddressId");
        orderHeaderFilter.AddWhere("City", "Address", "City");
        orderHeaderFilter.AddWhere("PostalCode", "Address", "postal");
        setup.Filters.Add(orderHeaderFilter);

        var orderDetailsFilter = new SetupFilter("SalesOrderDetail");
        orderDetailsFilter.AddParameter("City", "Address", true);
        orderDetailsFilter.AddParameter("postal", DbType.String, true, null, 20);
        orderDetailsFilter.AddJoin(Join.Left, "SalesOrderHeader").On("SalesOrderHeader", "SalesOrderID", "SalesOrderDetail", "SalesOrderID");
        orderDetailsFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
        orderDetailsFilter.AddJoin(Join.Left, "Address").On("CustomerAddress", "AddressId", "Address", "AddressId");
        orderDetailsFilter.AddWhere("City", "Address", "City");
        orderDetailsFilter.AddWhere("PostalCode", "Address", "postal");
        setup.Filters.Add(orderDetailsFilter);


        var options = new SyncOptions();

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.PogressPercentageString}:\t{s.Message}");
            Console.ResetColor();
        });

        do
        {
            // Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {

                if (!agent.Parameters.Contains("City"))
                    agent.Parameters.Add("City", "Toronto");

                // Because I've specified that "postal" could be null, I can set the value to DBNull.Value (and then get all postal code in Toronto city)
                if (!agent.Parameters.Contains("postal"))
                    agent.Parameters.Add("postal", DBNull.Value);

                var s1 = await agent.SynchronizeAsync();

                // Write results
                Console.WriteLine(s1);

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }


            //Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }

    private static async Task SynchronizeWithLoggerAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
        //var clientProvider = new SqliteSyncProvider("clientX.db");

        var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" });
        //var setup = new SyncSetup(new string[] { "Customer" });
        //var setup = new SyncSetup(new[] { "Customer" });
        //setup.Tables["Customer"].Columns.AddRange(new[] { "CustomerID", "FirstName", "LastName" });


        //Log.Logger = new LoggerConfiguration()
        //    .Enrich.FromLogContext()
        //    .MinimumLevel.Verbose()
        //    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
        //    .WriteTo.Console()
        //    .CreateLogger();

        // 1) create a console logger
        //var loggerFactory = LoggerFactory.Create(builder => { builder.AddConsole().SetMinimumLevel(LogLevel.Trace); });
        //var logger = loggerFactory.CreateLogger("Dotmim.Sync");
        //options.Logger = logger;


        // 2) create a serilog logger
        //var loggerFactory = LoggerFactory.Create(builder => { builder.AddSerilog().SetMinimumLevel(LogLevel.Trace); });
        //var logger = loggerFactory.CreateLogger("SyncAgent");
        //options.Logger = logger;

        //3) Using Serilog with Seq
        var serilogLogger = new LoggerConfiguration()
            .Enrich.FromLogContext()
            .MinimumLevel.Debug()
            .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
            .WriteTo.Seq("http://localhost:5341")
            .CreateLogger();


        //var actLogging = new Action<SyncLoggerOptions>(slo =>
        //{
        //    slo.AddConsole();
        //    slo.SetMinimumLevel(LogLevel.Information);
        //});

        ////var loggerFactory = LoggerFactory.Create(builder => builder.AddSerilog().AddConsole().SetMinimumLevel(LogLevel.Information));

        var loggerFactory = LoggerFactory.Create(builder => builder.AddSerilog(serilogLogger));


        //loggerFactory.AddSerilog(serilogLogger);

        //options.Logger = loggerFactory.CreateLogger("dms");

        // 2nd option to add serilog
        //var loggerFactorySerilog = new SerilogLoggerFactory();
        //var logger = loggerFactorySerilog.CreateLogger<SyncAgent>();
        //options.Logger = logger;

        //options.Logger = new SyncLogger().AddConsole().AddDebug().SetMinimumLevel(LogLevel.Trace);

        //var snapshotDirectory = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "Snapshots");
        //options.BatchSize = 500;
        //options.SnapshotsDirectory = snapshotDirectory;
        //var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);
        //remoteOrchestrator.CreateSnapshotAsync().GetAwaiter().GetResult();

        var options = new SyncOptions();
        options.BatchSize = 500;
        options.Logger = new SyncLogger().AddConsole().SetMinimumLevel(LogLevel.Debug);


        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.PogressPercentageString}:\t{s.Message}");
            Console.ResetColor();
        });

        do
        {
            // Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                // Launch the sync process
                //if (!agent.Parameters.Contains("CompanyName"))
                //    agent.Parameters.Add("CompanyName", "Professional Sales and Service");

                var s1 = await agent.SynchronizeAsync(progress);

                // Write results
                Console.WriteLine(s1);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }


            //Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }


}
