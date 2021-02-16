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

        await Snapshot_Then_ReinitializeAsync();
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


    private static async Task SynchronizeAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
        //var clientProvider = new SqliteSyncProvider("adv.db");


        var snapshotProgress = new SynchronousProgress<ProgressArgs>(pa =>
        {
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine($"{pa.PogressPercentageString}\t {pa.Message}");
            Console.ResetColor();
        });
        var snapshotDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "Snapshots");

        var options = new SyncOptions { BatchSize = 1000, SnapshotsDirectory = snapshotDirectory };

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
                if (await agent.RemoteOrchestrator.NeedsToUpgradeAsync())
                    await agent.RemoteOrchestrator.UpgradeAsync();

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

    public static async Task TestHelloKestrellAsync()
    {

        var hostBuilder = new WebHostBuilder()
            .UseKestrel()
            .UseUrls("http://127.0.0.1:0/");

        hostBuilder.Configure(app =>
        {
            app.Run(async context => await context.Response.WriteAsync("Hello world"));

        });

        var host = hostBuilder.Build();
        host.Start();
        string serviceUrl = $"http://localhost:{host.GetPort()}/";

        var client = new HttpClient();
        var s = await client.GetAsync(serviceUrl);

        Console.WriteLine(await s.Content.ReadAsStringAsync());

    }

    public static async Task TestWebSendAsync()
    {
        // server provider
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqliteSyncProvider("testblob2.db");

        var configureServices = new Action<IServiceCollection>(services =>
        {
            services.AddSyncServer<SqlSyncProvider>(serverProvider.ConnectionString, new string[] { "Product" });

        });

        var serverHandler = new RequestDelegate(async context =>
        {
            await context.Response.WriteAsync("Hello");
        });

        using var server = new KestrellTestServer(configureServices);

        var clientHandler = new ResponseDelegate(async (serviceUri) =>
        {
            var client = new HttpClient();
            var s = await client.GetAsync(serviceUri);

        });
        await server.Run(serverHandler, clientHandler);

    }


    private static async Task SynchronizeWithFiltersAndCustomerSerializerAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new MySqlSyncProvider(DBHelper.GetMariadbDatabaseConnectionString(clientDbName));
        //var clientProvider = new SqliteSyncProvider("clientX.db");


        var configureServices = new Action<IServiceCollection>(services =>
        {
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
            setup.StoredProceduresPrefix = "s";
            setup.StoredProceduresSuffix = "";
            setup.TrackingTablesPrefix = "t";
            setup.TrackingTablesSuffix = "";

            var options = new SyncOptions();

            // To add a converter, create an instance and add it to the special WebServerOptions
            var webServerOptions = new WebServerOptions();
            webServerOptions.Serializers.Add(new CustomMessagePackSerializerFactory());


            services.AddSyncServer<SqlSyncProvider>(serverProvider.ConnectionString, setup, options, webServerOptions);

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
                    var webClientOrchestrator = new WebClientOrchestrator(serviceUri, new CustomMessagePackSerializerFactory());
                    var agent = new SyncAgent(clientProvider, webClientOrchestrator);

                    // Launch the sync process
                    if (!agent.Parameters.Contains("CompanyName"))
                        agent.Parameters.Add("CompanyName", "Professional Sales and Service");

                    var progress = new SynchronousProgress<ProgressArgs>(pa => Console.WriteLine($"{pa.PogressPercentageString}\t {pa.Message}"));

                    var s = await agent.SynchronizeAsync(SyncType.Reinitialize, progress);
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


    public static async Task SyncBlobTypeToSqliteThroughKestrellAsync()
    {
        // server provider
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqliteSyncProvider("testblob2.db");

        var configureServices = new Action<IServiceCollection>(services =>
        {
            services.AddSyncServer<SqlSyncProvider>(serverProvider.ConnectionString, new string[] { "Product" });

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
                    var agent = new SyncAgent(clientProvider, new WebClientOrchestrator(serviceUri));
                    var progress = new SynchronousProgress<ProgressArgs>(pa => Console.WriteLine($"{pa.PogressPercentageString}\t {pa.Message}"));
                    var s = await agent.SynchronizeAsync(SyncType.Reinitialize, progress);
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


    private static async Task TestSqlSyncBlobAsync()
    {
        var serverProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString("AdventureWorks"));
        var clientProvider = new SqliteSyncProvider("testblob.db");

        var options = new SyncOptions { ConflictResolutionPolicy = ConflictResolutionPolicy.ServerWins };
        // adding 2 new tables
        var tables = new string[] { "Product" };

        var agent = new SyncAgent(clientProvider, serverProvider, tables);

        var progress = new SynchronousProgress<ProgressArgs>(args =>
                Console.WriteLine($"{args.PogressPercentageString}:{args.Message} "));

        do
        {
            // Launch the sync process
            var s1 = await agent.SynchronizeAsync(progress);
            // Write results
            Console.WriteLine(s1);

        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");

    }

    private static async Task SynchronizeHeavyTableAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString("HeavyTables"));
        var clientProvider = new SqliteSyncProvider("heavyTwo.db");

        var setup = new SyncSetup(new string[] { "Customer" });

        var options = new SyncOptions();
        options.BatchSize = 4000;

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
                var r = await agent.SynchronizeAsync(progress);
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
    private static async Task TestRemovingAColumnWithInterceptorAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" });

        var options = new SyncOptions();
        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, allTables);

        agent.RemoteOrchestrator.OnProvisioning(args =>
        {
            var schema = args.Schema;

            foreach (var table in schema.Tables)
            {
                var columnsToRemove = new string[] { "ModifiedDate", "rowguid" };
                foreach (var columnName in columnsToRemove)
                {
                    var column = table.Columns[columnName];

                    if (column != null)
                        table.Columns.Remove(column);
                }
            }

        });

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

    private static async Task TestMySqlToSqliteSyncAsync()
    {
        // Create Sqlite database and table
        CreateSqliteDatabaseAndTable("Scenario01", "Customer");

        // Create MySql database and table
        CreateMySqlDatabase("Scenario01", null);
        CreateMySqlTable("Scenario01", "Customer", null);

        // Add one mysql record
        AddMySqlRecord("Scenario01", "Customer");

        // Add one record in sqlite
        AddSqliteRecord("Scenario01", "Customer");

        // sync with a client database with rows, before sync is enabled, and who needs to be sent to server anyway
        await TestSyncAsync("Scenario01", "Scenario01", "Customer", false);

        // Add two records in sqlite
        AddSqliteRecord("Scenario01", "Customer");
        AddSqliteRecord("Scenario01", "Customer");

        // Add two records in mysql
        AddMySqlRecord("Scenario01", "Customer");
        AddMySqlRecord("Scenario01", "Customer");

        await TestSyncAsync("Scenario01", "Scenario01", "Customer", true);
    }

    private static void CreateMySqlTable(string databaseName, string tableName, MySqlConnection mySqlConnection)
    {

        string sql = $"DROP TABLE IF EXISTS `{tableName}`; " +
            $" CREATE TABLE `F` (" +
            $" `ID` char(36) NOT NULL " +
            $",`F1` int NULL" +
            $",`F2` longtext NOT NULL" +
            $",`F3` longtext NULL" +
            $", PRIMARY KEY(`ID`))";

        var cmd = new MySqlCommand(sql, mySqlConnection);
        cmd.ExecuteNonQuery();
    }
    private static void CreateSqliteDatabaseAndTable(string fileName, string tableName)
    {

        // Delete sqlite database
        string filePath = null;
        try
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();

            filePath = Path.Combine(Directory.GetCurrentDirectory(), $"{fileName}.db");

            if (File.Exists(filePath))
                File.Delete(filePath);

        }
        catch (Exception)
        {
        }



        var builder = new SqliteConnectionStringBuilder();
        builder.DataSource = $"{fileName}.db";

        if (!File.Exists(fileName))
        {
            string sql = $"CREATE TABLE [{tableName}] (" +
                        " [ID] text NOT NULL UNIQUE" +
                        ",[F1] integer NULL" +
                        ",[F2] text NOT NULL COLLATE NOCASE" +
                        ",[F3] text NULL COLLATE NOCASE" +
                        ", PRIMARY KEY([ID]))";

            using SqliteConnection dBase = new SqliteConnection(builder.ConnectionString);
            try
            {
                dBase.Open();
                SqliteCommand cmd = new SqliteCommand(sql, dBase);
                cmd.ExecuteNonQuery();
                dBase.Close();
                Console.WriteLine("Database created: " + fileName);
            }
            catch (SqliteException ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

    }


    private static MySqlConnection GetMyConnection(string databaseName = null)
    {
        var builder = new MySqlConnectionStringBuilder();

        if (!string.IsNullOrEmpty(databaseName))
            builder.Database = databaseName;

        builder.Port = 3307;
        builder.UserID = "root";
        builder.Password = "Password12!";

        var mySqlConnection = new MySqlConnection(builder.ConnectionString);
        return mySqlConnection;

    }

    private static void BugExecutingProcedureMySql()
    {
        var databaseName = "test";

        // creating database
        using (var connection = GetMyConnection())
        {
            connection.Open();
            CreateMySqlDatabase(databaseName, connection);
            connection.Close();
        }

        // creating 1st version of table and stored proc (then call it)
        using (var connection = GetMyConnection(databaseName))
        {
            connection.Open();

            CreateMySqlTable(databaseName, connection);
            CreateMySqlProcedure(databaseName, connection);
            // call 1st version of my stored procedure
            InsertMySqlRecord(connection);

            connection.Close();
        }

        // creating 2nd version of table and stored proc (then call it)
        using (var connection = GetMyConnection(databaseName))
        {
            connection.Open();
            // Adding one column to table
            AlterMySqlTable(databaseName, connection);
            // Creating 2nd version of my stored procedure
            CreateMySqlProcedure2(databaseName, connection);
            // FAIL: Trying to call this new stored procedure
            InsertMySqlRecord2(connection);

            connection.Close();
        }

    }

    // Creating my database
    private static void CreateMySqlDatabase(string databaseName, MySqlConnection mySqlConnection)
    {
        string sqlDB = $"DROP DATABASE IF EXISTS `{databaseName}`; CREATE DATABASE `{databaseName}`;";
        var cmd = new MySqlCommand(sqlDB, mySqlConnection);
        cmd.ExecuteNonQuery();

    }

    // Creating my table
    private static void CreateMySqlTable(string databaseName, MySqlConnection mySqlConnection)
    {

        string sql = $"DROP TABLE IF EXISTS `F`; " +
            $" CREATE TABLE `F` (" +
            $" `ID` char(36) NOT NULL " +
            $",`F1` int NULL" +
            $",`F2` longtext NOT NULL" +
            $",`F3` longtext NULL" +
            $", PRIMARY KEY(`ID`))";

        var cmd = new MySqlCommand(sql, mySqlConnection);
        cmd.ExecuteNonQuery();
    }

    // Adding new column to my table
    private static void AlterMySqlTable(string databaseName, MySqlConnection mySqlConnection)
    {
        var cmd = new MySqlCommand($"ALTER TABLE `F` ADD `F4` longtext NULL;", mySqlConnection);
        cmd.ExecuteNonQuery();
    }

    // Creating 1st version of the stored procedure
    private static void CreateMySqlProcedure(string databaseName, MySqlConnection mySqlConnection)
    {
        var procedure = new StringBuilder();
        procedure.AppendLine($"DROP PROCEDURE IF EXISTS `insert`;");
        procedure.AppendLine($"CREATE PROCEDURE `insert` (");
        procedure.AppendLine($" in_ID char(36)");
        procedure.AppendLine($",in_F1 int");
        procedure.AppendLine($",in_F2 longtext");
        procedure.AppendLine($",in_F3 longtext)");
        procedure.AppendLine($"BEGIN");
        procedure.AppendLine($"  INSERT INTO `F` (");
        procedure.AppendLine($"  `ID`, `F1`, `F2`, `F3`) ");
        procedure.AppendLine($"VALUES (");
        procedure.AppendLine($"  in_ID, in_F1, in_F2, in_F3);");
        procedure.AppendLine($"END;");

        var cmd = new MySqlCommand(procedure.ToString(), mySqlConnection);

        cmd.ExecuteNonQuery();
    }

    // Creating 2nd version of the stored procedure
    private static void CreateMySqlProcedure2(string databaseName, MySqlConnection mySqlConnection)
    {
        var procedure = new StringBuilder();
        procedure.AppendLine($"DROP PROCEDURE IF EXISTS `insert`;");
        procedure.AppendLine($"CREATE PROCEDURE `insert` (");
        procedure.AppendLine($" in_ID char(36)");
        procedure.AppendLine($",in_F1 int");
        procedure.AppendLine($",in_F2 longtext");
        procedure.AppendLine($",in_F3 longtext");
        procedure.AppendLine($",in_F4 longtext)");
        procedure.AppendLine($"BEGIN");
        procedure.AppendLine($"  INSERT INTO `F` (");
        procedure.AppendLine($"  `ID`, `F1`, `F2`, `F3`, `F4`) ");
        procedure.AppendLine($"VALUES (");
        procedure.AppendLine($"  in_ID, in_F1, in_F2, in_F3, in_F4);");
        procedure.AppendLine($"END;");

        var cmd = new MySqlCommand(procedure.ToString(), mySqlConnection);

        cmd.ExecuteNonQuery();
    }

    // Executing 1st version of the stored procedure
    private static void InsertMySqlRecord(MySqlConnection mySqlConnection)
    {
        var cmd = new MySqlCommand
        {
            CommandText = "`insert`",
            CommandType = CommandType.StoredProcedure,
            Connection = mySqlConnection
        };

        cmd.Parameters.AddWithValue("in_ID", Guid.NewGuid().ToString());
        cmd.Parameters.AddWithValue("in_F1", 1);
        cmd.Parameters.AddWithValue("in_F2", "Hello");
        cmd.Parameters.AddWithValue("in_F3", "world");

        cmd.ExecuteNonQuery();
    }

    // Executing 2nd version of the stored procedure
    private static void InsertMySqlRecord2(MySqlConnection mySqlConnection)
    {
        var cmd = new MySqlCommand
        {
            CommandText = "`insert`",
            CommandType = CommandType.StoredProcedure,
            Connection = mySqlConnection
        };

        cmd.Parameters.AddWithValue("in_ID", Guid.NewGuid().ToString());
        cmd.Parameters.AddWithValue("in_F1", 1);
        cmd.Parameters.AddWithValue("in_F2", "Hello");
        cmd.Parameters.AddWithValue("in_F3", "world");
        cmd.Parameters.AddWithValue("in_F4", "again !");

        cmd.ExecuteNonQuery();
    }


    private static void AddMySqlRecord(string databaseName, string tableName)
    {

        var builder = new MySqlConnectionStringBuilder();

        builder.Database = databaseName;
        builder.Port = 3307;
        builder.UserID = "root";
        builder.Password = "Password12!";

        string sql = $"INSERT INTO `{tableName}` (ID,F1,F2,F3) VALUES (@ID, 2, '2st2tem', '1111111')";

        using MySqlConnection dBase = new MySqlConnection(builder.ConnectionString);
        try
        {
            MySqlCommand cmd = new MySqlCommand(sql, dBase);
            var parameter = new MySqlParameter("@ID", Guid.NewGuid());
            cmd.Parameters.Add(parameter);

            dBase.Open();
            cmd.ExecuteNonQuery();
            dBase.Close();
            Console.WriteLine("Record added into table " + tableName);
        }
        catch (SqliteException ex)
        {
            Console.WriteLine(ex.Message);
        }
    }
    private static void AddSqliteRecord(string fileName, string tableName)
    {

        var builder = new SqliteConnectionStringBuilder();
        builder.DataSource = $"{fileName}.db";

        string sql = $"INSERT INTO [{tableName}] (ID,F1,F2,F3) VALUES ( @ID, 1, '1stItem', '1111111')";

        using SqliteConnection dBase = new SqliteConnection(builder.ConnectionString);
        try
        {
            SqliteCommand cmd = new SqliteCommand(sql, dBase);
            var parameter = new SqliteParameter("@ID", Guid.NewGuid());
            cmd.Parameters.Add(parameter);


            dBase.Open();
            cmd.ExecuteNonQuery();
            dBase.Close();
            Console.WriteLine("Record added into table " + tableName);
        }
        catch (SqliteException ex)
        {
            Console.WriteLine(ex.Message);
        }
    }


    private static async Task TestSyncAsync(string mySqlDatabaseName, string sqliteFileName, string tableName, bool uploadOnly = false)
    {
        var builder = new MySqlConnectionStringBuilder();

        builder.Database = mySqlDatabaseName;
        builder.Port = 3307;
        builder.UserID = "root";
        builder.Password = "Password12!";

        var serverProvider = new MySqlSyncProvider(builder.ConnectionString);

        var sqlitebuilder = new SqliteConnectionStringBuilder();
        sqlitebuilder.DataSource = $"{sqliteFileName}.db";

        var clientProvider = new SqliteSyncProvider(sqlitebuilder.ConnectionString);

        var setup = new SyncSetup(new string[] { tableName });
        setup.Tables[tableName].SyncDirection = uploadOnly ? SyncDirection.UploadOnly : SyncDirection.Bidirectional;

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

                var s = await agent.SynchronizeAsync();
                Console.WriteLine(s);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }


            //Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }

    private static async Task TestClientHasRowsToSyncAnywayAsync(string mySqlDatabaseName, string sqliteFileName, string tableName)
    {
        var builder = new MySqlConnectionStringBuilder();

        builder.Database = mySqlDatabaseName;
        builder.Port = 3307;
        builder.UserID = "root";
        builder.Password = "Password12!";

        var serverProvider = new MySqlSyncProvider(builder.ConnectionString);

        var sqlitebuilder = new SqliteConnectionStringBuilder();
        sqlitebuilder.DataSource = $"{sqliteFileName}.db";

        var clientProvider = new SqliteSyncProvider(sqlitebuilder.ConnectionString);

        var setup = new SyncSetup(new string[] { tableName });

        var options = new SyncOptions { ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins };

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

                //var s = await agent.SynchronizeAsync();
                //Console.WriteLine(s);

                var schema = await agent.RemoteOrchestrator.GetSchemaAsync();

                await agent.LocalOrchestrator.UpdateUntrackedRowsAsync(schema);

                var s2 = await agent.SynchronizeAsync();
                Console.WriteLine(s2);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }


            //Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }


    private static async Task TestInsertJsonArrayPostgreSqlAsync()
    {
        var connection = new NpgsqlConnection("Host=localhost;Database=Client;User ID=postgres;Password=azerty31*;");

        var lstscopes = new List<scope>
        {
            new scope { sync_scope_id = Guid.NewGuid(), sync_scope_name = "jsonscope1" },
            new scope { sync_scope_id = Guid.NewGuid(), sync_scope_name = "jsonscope2" },
            new scope { sync_scope_id = Guid.NewGuid(), sync_scope_name = "jsonscope3" },
            new scope { sync_scope_id = Guid.NewGuid(), sync_scope_name = "jsonscope4" },
            new scope { sync_scope_id = Guid.NewGuid(), sync_scope_name = "jsonscope5" },
            new scope { sync_scope_id = Guid.NewGuid(), sync_scope_name = "jsonscope6" }
        };


        var p = new NpgsqlParameter
        {
            ParameterName = "scopes",
            NpgsqlDbType = NpgsqlDbType.Json,
            NpgsqlValue = JsonConvert.SerializeObject(lstscopes)
        };

        try
        {
            using var command = new NpgsqlCommand("fn_upsert_scope_info_json", connection);
            command.Parameters.Add(p);
            command.CommandType = CommandType.StoredProcedure;

            await connection.OpenAsync().ConfigureAwait(false);

            await command.ExecuteNonQueryAsync();



            connection.Close();
        }
        catch (Exception)
        {

            throw;
        }

    }



    private static async Task TestNpgsqlAsync()
    {
        var commandText = $"SELECT * " +
               " FROM information_schema.tables " +
               " WHERE table_type = 'BASE TABLE' " +
               " AND table_schema != 'pg_catalog' AND table_schema != 'information_schema' " +
               " AND table_name=@tableName AND table_schema=@schemaName " +
               " ORDER BY table_schema, table_name " +
               " LIMIT 1";

        var schemaNameString = "public";
        var tableNameString = "actor";

        var connection = new NpgsqlConnection(DBHelper.GetNpgsqlDatabaseConnectionString(clientDbName));

        using var command = new NpgsqlCommand(commandText, connection);
        command.Parameters.AddWithValue("@tableName", tableNameString);
        command.Parameters.AddWithValue("@schemaName", schemaNameString);

        await connection.OpenAsync().ConfigureAwait(false);

        using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
        {
            while (reader.Read())
            {
                Console.WriteLine(reader["table_schema"].ToString() + "." + reader["table_name"].ToString());
            }
        }

        connection.Close();
    }


    private static async Task LongTransactionBeforeRuningSyncAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqliteSyncProvider("longrunning.sqlite");

        // Create standard Setup and Options
        var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress" });
        var options = new SyncOptions();

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

        // First Sync
        // ----------------------
        Console.WriteLine("FIRST INITIALIZE SYNC");
        Console.WriteLine("--------------------");
        var s1 = await agent.SynchronizeAsync();
        Console.WriteLine(s1);

        // Second Sync
        // ----------------------
        Console.WriteLine();
        Console.WriteLine("SECOND PARALLEL SYNC");
        Console.WriteLine("--------------------");


        agent.LocalOrchestrator.OnDatabaseChangesSelecting(dcsa =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"[{DateTime.Now:ss.ms}] - Last Sync TS start (T0) : " + dcsa.ChangesRequest.LastTimestamp.ToString().Substring(dcsa.ChangesRequest.LastTimestamp.ToString().Length - 4, 4));
            Console.ResetColor();
            //await InsertIntoSqliteWithCurrentTransactionAsync(dcsa.Connection, dcsa.Transaction);
        });

        agent.LocalOrchestrator.OnDatabaseChangesSelected(dcsa =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"[{DateTime.Now:ss.ms}] - Last Sync TS for next run (T10) : " + dcsa.Timestamp.ToString().Substring(dcsa.Timestamp.ToString().Length - 4, 4));
            Console.ResetColor();
        });


        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"[{DateTime.Now:ss.ms}] - {s.PogressPercentageString}:\t{s.Message}");
            Console.ResetColor();
        });

        var t1 = LongInsertIntoSqliteAsync(clientProvider.CreateConnection());
        var t2 = agent.SynchronizeAsync(progress);

        await Task.WhenAll(t1, t2);
        Console.WriteLine(t2.Result);

        Console.WriteLine();
        Console.WriteLine("THIRD FINAL SYNC");
        Console.WriteLine("--------------------");

        agent.LocalOrchestrator.OnDatabaseChangesSelecting(null);
        agent.LocalOrchestrator.OnDatabaseChangesSelected(null);
        // Third Sync
        // ----------------------
        // First Sync
        var s3 = await agent.SynchronizeAsync();
        Console.WriteLine(s3);

    }



    private static async Task LongInsertIntoSqliteAsync(DbConnection connection)
    {
        var addressline1 = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant();

        var command = connection.CreateCommand();
        command.CommandText =
            $"Insert into Address (AddressLine1, City, StateProvince, CountryRegion, PostalCode, rowguid, ModifiedDate) Values ('{addressline1}', 'Toulouse', 'Haute Garonne', 'Occitanie', '31000', '{Guid.NewGuid()}', '2020-02-02');" +
            $"Select timestamp from Address_tracking where AddressID = (Select AddressID from Address where AddressLine1 = '{addressline1}')";

        connection.Open();

        using (var transaction = connection.BeginTransaction())
        {
            command.Transaction = transaction;
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"[{DateTime.Now:ss.ms}] - Inserting row ");
            var ts = await command.ExecuteScalarAsync();
            Console.WriteLine($"[{DateTime.Now:ss.ms}] - SQLite Timestamp inserted (T9) : " + ts.ToString().Substring(ts.ToString().Length - 4, 4));

            Console.WriteLine($"[{DateTime.Now:ss.ms}] - Waiting 10 sec before commit");
            await Task.Delay(10000);

            transaction.Commit();
            Console.WriteLine($"[{DateTime.Now:ss.ms}] - Transaction commit");
            Console.ResetColor();

        }
        connection.Close();
    }

    private static async Task InsertIntoSqliteWithCurrentTransactionAsync(DbConnection connection, DbTransaction transaction)
    {


        try
        {
            var addressline1 = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant();

            var command = connection.CreateCommand();
            command.CommandText =
                $"Insert into Address (AddressLine1, City, StateProvince, CountryRegion, PostalCode, rowguid, ModifiedDate) Values ('{addressline1}', 'Toulouse', 'Haute Garonne', 'Occitanie', '31000', '{Guid.NewGuid()}', '2020-02-02');" +
                $"Select timestamp from Address_tracking where AddressID = (Select AddressID from Address where AddressLine1 = '{addressline1}')";

            command.Transaction = transaction;
            Console.WriteLine("Inserting row ");

            var ts = await command.ExecuteScalarAsync();

            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("SQLite Timestamp inserted: " + ts.ToString().Substring(ts.ToString().Length - 4, 4));
            Console.ResetColor();
        }
        catch (Exception)
        {

            throw;
        }

    }


    private static async Task SynchronizeThenDeprovisionThenProvisionAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        // Create standard Setup and Options
        var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress" });
        var options = new SyncOptions();

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s => Console.WriteLine($"{s.PogressPercentageString}:\t{s.Message}"));

        // First sync to have a starting point
        var s1 = await agent.SynchronizeAsync(progress);

        Console.WriteLine(s1);

        // -----------------------------------------------------------------
        // Migrating a table by adding a new column
        // -----------------------------------------------------------------

        // Adding a new column called CreatedDate to Address table, on the server, and on the client.
        await AddNewColumnToAddressAsync(serverProvider.CreateConnection());
        await AddNewColumnToAddressAsync(clientProvider.CreateConnection());

        // -----------------------------------------------------------------
        // Server side
        // -----------------------------------------------------------------

        // Creating a setup regarding only the table Address
        var setupAddress = new SyncSetup(new string[] { "Address" });

        // Create a server orchestrator used to Deprovision and Provision only table Address
        var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setupAddress);

        // Unprovision the Address triggers / stored proc. 
        // We can conserve the Address tracking table, since we just add a column, 
        // that is not a primary key used in the tracking table
        // That way, we are preserving historical data
        await remoteOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers);

        // Provision the Address triggers / stored proc again, 
        // This provision method will fetch the address schema from the database, 
        // so it will contains all the columns, including the new Address column added
        await remoteOrchestrator.ProvisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers);

        // Now we need the full setup to get the full schema.
        // Setup includes [Address] [Customer] and [CustomerAddress]
        remoteOrchestrator.Setup = setup;
        var newSchema = await remoteOrchestrator.GetSchemaAsync();

        // Now we need to save this new schema to the serverscope table
        // get the server scope again
        var serverScope = await remoteOrchestrator.GetServerScopeAsync();

        // affect good values
        serverScope.Setup = setup;
        serverScope.Schema = newSchema;

        // save it
        await remoteOrchestrator.SaveServerScopeAsync(serverScope);

        // -----------------------------------------------------------------
        // Client side
        // -----------------------------------------------------------------

        // Now go for local orchestrator
        var localOrchestrator = new LocalOrchestrator(clientProvider, options, setupAddress);

        // Unprovision the Address triggers / stored proc. We can conserve tracking table, since we just add a column, that is not a primary key used in the tracking table
        // In this case, we will 
        await localOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers);

        // Provision the Address triggers / stored proc again, 
        // This provision method will fetch the address schema from the database, so it will contains all the columns, including the new one added
        await localOrchestrator.ProvisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers);

        // Now we need to save this to clientscope
        // get the server scope again
        var clientScope = await localOrchestrator.GetClientScopeAsync();

        // At this point, if you need the schema and you are not able to create a RemoteOrchestrator,
        // You can create a WebClientOrchestrator and get the schema as well
        // var proxyClientProvider = new WebClientOrchestrator("https://localhost:44369/api/Sync");
        // var newSchema = proxyClientProvider.GetSchemaAsync();

        // affect good values
        clientScope.Setup = setup;
        clientScope.Schema = newSchema;

        // save it
        await localOrchestrator.SaveClientScopeAsync(clientScope);





        // Now test a new sync, everything should work as expected.
        do
        {
            // Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                var s2 = await agent.SynchronizeAsync();

                // Write results
                Console.WriteLine(s2);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");

    }

    private static void TestSqliteDoubleStatement()
    {
        var clientProvider = new SqliteSyncProvider(@"C:\PROJECTS\DOTMIM.SYNC\Tests\Dotmim.Sync.Tests\bin\Debug\netcoreapp2.0\st_r55jmmolvwg.db");
        var clientConnection = new SqliteConnection(clientProvider.ConnectionString);

        var commandText = "Update ProductCategory Set Name=@Name Where ProductCategoryId=@Id; " +
                          "Select * from ProductCategory Where ProductCategoryId=@Id;";

        using DbCommand command = clientConnection.CreateCommand();
        command.Connection = clientConnection;
        command.CommandText = commandText;
        var p = command.CreateParameter();
        p.ParameterName = "@Id";
        p.DbType = DbType.String;
        p.Value = "FTNLBJ";
        command.Parameters.Add(p);

        p = command.CreateParameter();
        p.ParameterName = "@Name";
        p.DbType = DbType.String;
        p.Value = "Awesome Bike";
        command.Parameters.Add(p);

        clientConnection.Open();

        using (var reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                var name = reader["Name"];
                Console.WriteLine(name);
            }
        }

        clientConnection.Close();

    }
    private static async Task TestDeleteWithoutBulkAsync()
    {
        var cs = DBHelper.GetDatabaseConnectionString(serverProductCategoryDbName);
        var cc = DBHelper.GetDatabaseConnectionString(clientDbName);

        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(cs);
        var serverConnection = new SqlConnection(cs);

        //var clientProvider = new SqlSyncProvider(cc);
        //var clientConnection = new SqlConnection(cc);

        var clientProvider = new SqliteSyncProvider("advworks2.db");
        var clientConnection = new SqliteConnection(clientProvider.ConnectionString);

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, new string[] { "ProductCategory" });

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"{s.PogressPercentageString}:\t{s.Message}");
            Console.ResetColor();
        });

        var remoteProgress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.PogressPercentageString}:\t{s.Message}");
            Console.ResetColor();
        });
        // agent.AddRemoteProgress(remoteProgress);

        agent.Options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "sync");
        agent.Options.BatchSize = 0;
        agent.Options.UseBulkOperations = false;
        agent.Options.DisableConstraintsOnApplyChanges = false;
        agent.Options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

        Console.WriteLine("Sync Start");
        var s1 = await agent.SynchronizeAsync(progress);
        Console.WriteLine(s1);

        Console.WriteLine("Insert product category on Server");
        var id = InsertOneProductCategoryId(serverConnection, "GLASSES");
        Console.WriteLine("Update Done.");

        Console.WriteLine("Update product category on Server");
        UpdateOneProductCategoryId(serverConnection, id, "OVERGLASSES");
        Console.WriteLine("Update Done.");

        Console.WriteLine("Sync Start");
        s1 = await agent.SynchronizeAsync(progress);
        Console.WriteLine(s1);

        Console.WriteLine("End");
    }

    private static async Task AddNewColumnToAddressAsync(DbConnection c)
    {
        using var command = c.CreateCommand();
        command.CommandText = "ALTER TABLE dbo.Address ADD CreatedDate datetime NULL;";
        c.Open();
        await command.ExecuteNonQueryAsync();
        c.Close();
    }
    private static int InsertOneProductCategoryId(IDbConnection c, string updatedName)
    {
        using var command = c.CreateCommand();
        command.CommandText = "Insert Into ProductCategory (Name) Values (@Name); SELECT SCOPE_IDENTITY();";
        var p = command.CreateParameter();
        p.DbType = DbType.String;
        p.Value = updatedName;
        p.ParameterName = "@Name";
        command.Parameters.Add(p);

        c.Open();
        var id = command.ExecuteScalar();
        c.Close();

        return Convert.ToInt32(id);
    }
    private static void UpdateOneProductCategoryId(IDbConnection c, int productCategoryId, string updatedName)
    {
        using var command = c.CreateCommand();
        command.CommandText = "Update ProductCategory Set Name = @Name Where ProductCategoryId = @Id";
        var p = command.CreateParameter();
        p.DbType = DbType.String;
        p.Value = updatedName;
        p.ParameterName = "@Name";
        command.Parameters.Add(p);

        p = command.CreateParameter();
        p.DbType = DbType.Int32;
        p.Value = productCategoryId;
        p.ParameterName = "@Id";
        command.Parameters.Add(p);

        c.Open();
        command.ExecuteNonQuery();
        c.Close();
    }
    private static void DeleteOneLine(DbConnection c, int productCategoryId)
    {
        using var command = c.CreateCommand();
        command.CommandText = "Delete from ProductCategory Where ProductCategoryId = @Id";

        var p = command.CreateParameter();
        p.DbType = DbType.Int32;
        p.Value = productCategoryId;
        p.ParameterName = "@Id";
        command.Parameters.Add(p);

        c.Open();
        command.ExecuteNonQuery();
        c.Close();
    }


    private static async Task SynchronizeMultiScopesAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        // Create 2 tables list (one for each scope)
        string[] productScopeTables = new string[] { "ProductCategory", "ProductModel", "Product" };
        string[] customersScopeTables = new string[] { "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" };

        // Create 2 sync setup with named scope 
        var setupProducts = new SyncSetup(productScopeTables);
        var setupCustomers = new SyncSetup(customersScopeTables);

        var syncOptions = new SyncOptions();

        // Create 2 agents, one for each scope
        var agentProducts = new SyncAgent(clientProvider, serverProvider, syncOptions, setupProducts, "productScope");
        var agentCustomers = new SyncAgent(clientProvider, serverProvider, syncOptions, setupCustomers, "customerScope");

        // Using the Progress pattern to handle progession during the synchronization
        // We can use the same progress for each agent
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.PogressPercentageString}:\t{s.Message}");
            Console.ResetColor();
        });


        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                Console.WriteLine("Hit 1 for sync Products. Hit 2 for sync customers and sales");
                var k = Console.ReadKey().Key;

                if (k == ConsoleKey.D1)
                {
                    Console.WriteLine("Sync Products:");
                    var s1 = await agentProducts.SynchronizeAsync(progress);
                    Console.WriteLine(s1);
                }
                else
                {
                    Console.WriteLine("Sync Customers and Sales:");
                    var s1 = await agentCustomers.SynchronizeAsync(progress);
                    Console.WriteLine(s1);
                }

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }


    private static async Task TestAvbAsync()
    {
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString("TestAvg"));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        var setup = new SyncSetup(new String[] { "avb.avb", "avb.avbg" });

        // filter on table avb.avbg
        var avbGFilter = new SetupFilter("avbg", "avb");

        // add parameter from table avb.avb Column avb.avb.UserId
        avbGFilter.AddParameter("UserId", "avb", "avb");

        // join to table avb.avbg
        avbGFilter.AddJoin(Join.Left, "avb.avb").On("avb.avb", "Id", "avb.avbg", "avbId");
        avbGFilter.AddWhere("UserId", "avb", "UserId", "avb");

        setup.Filters.Add(avbGFilter);

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, new SyncOptions(), setup);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.PogressPercentageString}:\t{s.Message}");
            Console.ResetColor();
        });

        Guid userId = new Guid("a7ca90ef-f6b1-4a19-9e31-01a215abbb95");

        agent.Parameters.Add("UserId", userId);

        var s1 = await agent.SynchronizeAsync(progress);

        Console.WriteLine(s1);
    }

    private static async Task SynchronizeWithFiltersAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
        //var clientProvider = new SqliteSyncProvider("clientX.db");

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
        //setup.StoredProceduresPrefix = "s";
        //setup.StoredProceduresSuffix = "";
        //setup.TrackingTablesPrefix = "t";
        //setup.TrackingTablesSuffix = "";

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
                // Launch the sync process
                if (!agent.Parameters.Contains("CompanyName"))
                    agent.Parameters.Add("CompanyName", "Professional Sales and Service");

                var s1 = await agent.SynchronizeAsync(SyncType.Reinitialize);

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


    private static async Task SynchronizeThenChangeSetupAsync2()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new MySqlSyncProvider(DBHelper.GetMySqlDatabaseConnectionString(clientDbName));
        //var clientProvider = new SqlSyncProvider(DBHelper.GetMySqlDatabaseConnectionString(clientDbName));
        //var clientProvider = new SqliteSyncProvider("client.db");

        var setup = new SyncSetup(new string[] { "Customer" });
        setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "NameStyle", "FirstName", "LastName", "EmailAddress" });

        // Add pref suf
        setup.StoredProceduresPrefix = "s";
        setup.StoredProceduresSuffix = "";
        setup.TrackingTablesPrefix = "t";
        setup.TrackingTablesSuffix = "";

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, new SyncOptions(), setup);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.PogressPercentageString}:\t{s.Message}");
            Console.ResetColor();
        });

        var s1 = await agent.SynchronizeAsync(SyncType.Reinitialize, progress);

        Console.WriteLine(s1);

    }

    private static async Task SynchronizeThenChangeSetupAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new MySqlSyncProvider(DBHelper.GetMySqlDatabaseConnectionString(clientDbName));
        //var clientProvider = new SqlSyncProvider(DBHelper.GetMySqlDatabaseConnectionString(clientDbName));
        //var clientProvider = new SqliteSyncProvider("client.db");

        var setup = new SyncSetup(new string[] { "Customer" });
        setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "NameStyle", "FirstName", "LastName" });

        // Add pref suf
        setup.StoredProceduresPrefix = "s";
        setup.StoredProceduresSuffix = "";
        setup.TrackingTablesPrefix = "t";
        setup.TrackingTablesSuffix = "";

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, new SyncOptions(), setup);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.PogressPercentageString}:\t{s.Message}");
            Console.ResetColor();
        });

        var s1 = await agent.SynchronizeAsync(progress);

        Console.WriteLine(s1);

        // Adding a new column to Customer
        setup.Tables["Customer"].Columns.Add("EmailAddress");

        // fake old setup
        var oldSetup = new SyncSetup(new string[] { "Customer" });
        oldSetup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "NameStyle", "FirstName", "LastName" });

        await agent.RemoteOrchestrator.MigrationAsync(oldSetup);
        var newSchema = await agent.RemoteOrchestrator.GetSchemaAsync();

        await agent.LocalOrchestrator.MigrationAsync(oldSetup, newSchema);


        //var r1 = await agent.SynchronizeAsync(SyncType.Reinitialize, progress);

        Console.WriteLine("End");
    }

}

internal class scope
{
    public Guid sync_scope_id { get; set; }
    public string sync_scope_name { get; set; }
}