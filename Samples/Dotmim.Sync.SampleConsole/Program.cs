using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SampleConsole;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using System.Linq;
using System.Collections.Generic;
using NLog.Web;

#if NET6_0 || NET8_0
using MySqlConnector;
#elif NETCOREAPP3_1
using MySql.Data.MySqlClient;
#endif

internal class Program
{
    public static string serverDbName = "AdventureWorks";
    public static string serverProductCategoryDbName = "AdventureWorksProductCategory";
    public static string clientDbName = "Client";
    public static string[] allTables = new string[] {"ProductDescription", "ProductCategory",
                                                    "ProductModel", "Product",
                                                    "Address", "Customer", "CustomerAddress",
                                                    "SalesOrderHeader", "SalesOrderDetail"};

    public static string[] oneTable = new string[] { "ProductCategory" };


    private static async Task Main(string[] args)
    {

        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        //var serverProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        //var serverProvider = new NpgsqlSyncProvider(DBHelper.GetNpgsqlDatabaseConnectionString("Wasim"));
        //var serverProvider = new MariaDBSyncProvider(DBHelper.GetMariadbDatabaseConnectionString(serverDbName));
        // var serverProvider = new MySqlSyncProvider(DBHelper.GetMySqlDatabaseConnectionString(serverDbName));

        //var clientProvider = new SqliteSyncProvider(Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db");
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
        //var clientProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
        //var clientProvider = new NpgsqlSyncProvider(DBHelper.GetNpgsqlDatabaseConnectionString(clientDbName));
        //clientProvider.UseBulkOperations = false;
        //var clientProvider = new MariaDBSyncProvider(DBHelper.GetMariadbDatabaseConnectionString(clientDbName));
        //var clientProvider = new MySqlSyncProvider(DBHelper.GetMySqlDatabaseConnectionString(clientDbName));

        var setup = new SyncSetup(allTables);

        var options = new SyncOptions();
        //options.Logger = new SyncLogger().AddDebug().SetMinimumLevel(LogLevel.Information);
        //options.UseVerboseErrors = true;

        //setup.Tables["ProductCategory"].Columns.AddRange(new string[] { "ProductCategoryID", "ParentProductCategoryID", "Name" });
        //setup.Tables["ProductDescription"].Columns.AddRange(new string[] { "ProductDescriptionID", "Description" });
        //setup.Filters.Add("ProductCategory", "ParentProductCategoryID", null, true);

        //var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString("vaguegitserver"));
        //var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString("vaguegitclient"));

        //var setup = new SyncSetup(new string[] { "SubscriptionTransactions" });
        //var options = new SyncOptions();

        //var loggerFactory = LoggerFactory.Create(builder => { builder.AddSeq().SetMinimumLevel(LogLevel.Debug); });
        //var logger = loggerFactory.CreateLogger("Dotmim.Sync");
        //options.Logger = logger;
        //options.SnapshotsDirectory = Path.Combine("C:\\Tmp\\Snapshots");


        //await SyncHttpThroughKestrellAsync(clientProvider, serverProvider, setup, options);

        // await SynchronizeAsync(clientProvider, serverProvider, setup, options);

        //await SynchronizeAsync(clientProvider, serverProvider, setup, options);
        await SyncWithReinitialiazeWithChangeTrackingAsync();

        //await CreateSnapshotAsync();
    }

    private static async Task SyncWithReinitialiazeWithChangeTrackingAsync()
    {
        var serverProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        var setup = new SyncSetup("ProductCategory");

        var options = new SyncOptions
        {
            DisableConstraintsOnApplyChanges = true
        };

        var progress = new SynchronousProgress<ProgressArgs>(s =>
            Console.WriteLine($"{s.ProgressPercentage:p}:  " +
            $"\t[{s?.Source?[..Math.Min(4, s.Source.Length)]}] {s.TypeName}: {s.Message}"));


        var agent = new SyncAgent(clientProvider, serverProvider, options);

        //var s = await agent.SynchronizeAsync(setup, progress: progress);
        //Console.WriteLine(s);

        //await DBHelper.AddProductCategoryRowAsync(clientProvider);

        var s2 = await agent.SynchronizeAsync(setup, SyncType.Reinitialize, progress: progress);
        Console.WriteLine(s2);


    }

    private static async Task CreateSnapshotAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        var setup = new SyncSetup("SalesLT.ProductCategory", "SalesLT.Product");

        var productCategoryFilter = new SetupFilter("ProductCategory", "SalesLT");
        productCategoryFilter.AddParameter("ProductCategoryID", "ProductCategory", "SalesLT", true);
        productCategoryFilter.AddWhere("ProductCategoryID", "ProductCategory", "ProductCategoryID", "SalesLT");
        setup.Filters.Add(productCategoryFilter);

        // snapshot directory
        var snapshotDirectoryName = "Snapshots";
        var directory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), snapshotDirectoryName);

        // snapshot directory
        var options = new SyncOptions
        {
            SnapshotsDirectory = directory,
            BatchSize = 3000
        };

        SyncParameters parameters = new() { new("ProductCategoryID", default) };

        // Create a remote orchestrator
        var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

        // Create a snapshot
        await remoteOrchestrator.CreateSnapshotAsync(setup, parameters);
    }
    private static async Task SynchronizeWithFiltersJoinsAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        var setup = new SyncSetup("SalesLT.ProductCategory", "SalesLT.Product");

        var productCategoryFilter = new SetupFilter("ProductCategory", "SalesLT");
        productCategoryFilter.AddParameter("ProductCategoryID", "ProductCategory", "SalesLT");
        productCategoryFilter.AddWhere("ProductCategoryID", "ProductCategory", "ProductCategoryID", "SalesLT");
        setup.Filters.Add(productCategoryFilter);

        var productFilter = new SetupFilter("Product", "SalesLT");
        productFilter.AddParameter("ProductCategoryID", "ProductCategory", "SalesLT");
        productFilter.AddJoin(Join.Left, "ProductCategory", "SalesLT").On("ProductCategory", "ProductCategoryID", "Product", "ProductCategoryId", "SalesLT", "SalesLT");
        productFilter.AddWhere("ProductCategoryID", "ProductCategory", "ProductCategoryID", "SalesLT");
        setup.Filters.Add(productFilter);


        var options = new SyncOptions();

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.ProgressPercentage:p}:\t{s.Message}");
            Console.ResetColor();
        });

        do
        {
            var s = await agent.SynchronizeAsync(setup, progress: progress);

            Console.WriteLine($"DONE.");
            Console.WriteLine($"----------------------------------------");

        } while (Console.ReadKey().Key != ConsoleKey.Escape);
    }

    private static async Task SynchronizeAsync(CoreProvider clientProvider, CoreProvider serverProvider, SyncSetup setup, SyncOptions options, string scopeName = SyncOptions.DefaultScopeName)
    {
        options.DisableConstraintsOnApplyChanges = true;

        var progress = new SynchronousProgress<ProgressArgs>(s =>
            Console.WriteLine($"{s.ProgressPercentage:p}:  " +
            $"\t[{s?.Source?[..Math.Min(4, s.Source.Length)]}] {s.TypeName}: {s.Message}"));


        var agent = new SyncAgent(clientProvider, serverProvider, options);

        do
        {
            try
            {
                Console.ForegroundColor = ConsoleColor.Green;
                var s = await agent.SynchronizeAsync(scopeName, setup, SyncType.Normal, default, default, progress);
                Console.WriteLine(s);
            }
            catch (SyncException e)
            {
                Console.ResetColor();
                Console.WriteLine(e.Message);
            }
            catch (Exception e)
            {
                Console.ResetColor();
                Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
            }
            Console.WriteLine("--------------------");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

    }
    public static async Task SyncHttpThroughKestrellAsync(CoreProvider clientProvider, CoreProvider serverProvider, SyncSetup setup, SyncOptions options)
    {
        var configureServices = new Action<IServiceCollection>(services => services.AddSyncServer<SqlSyncProvider>(serverProvider.ConnectionString, setup, options, null, "01"));

        var serverHandler = new RequestDelegate(async context =>
        {
            try
            {
                var webServerAgents = context.RequestServices.GetService(typeof(IEnumerable<WebServerAgent>)) as IEnumerable<WebServerAgent>;

                var scopeName = context.GetScopeName();
                var identifier = context.GetIdentifier();

                var clientScopeId = context.GetClientScopeId();

                var webServerAgent = webServerAgents.First(wsa => wsa.Identifier == identifier);

                await webServerAgent.HandleRequestAsync(context);

            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex);
                throw;
            }

        });

        using var server = new KestrellTestServer(configureServices, false);

        var clientHandler = new ResponseDelegate(async (serviceUri) =>
        {
            do
            {
                Console.WriteLine("Web sync start");
                try
                {
                    var startTime = DateTime.Now;


                    // Using the Progress pattern to handle progession during the synchronization
                    var progress = new SynchronousProgress<ProgressArgs>(s =>
                        Console.WriteLine($"{s.ProgressPercentage:p}:  \t[{s?.Source[..Math.Min(4, s.Source.Length)]}] {s.TypeName}: {s.Message}"));

                    var localProgress = new SynchronousProgress<ProgressArgs>(s =>
                    {
                        var tsEnded = TimeSpan.FromTicks(DateTime.Now.Ticks);
                        var tsStarted = TimeSpan.FromTicks(startTime.Ticks);
                        var durationTs = tsEnded.Subtract(tsStarted);

                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine($"{durationTs:mm\\:ss\\.fff} {s.ProgressPercentage:p}:\t[{s?.Source[..Math.Min(4, s.Source.Length)]}] {s.TypeName}: {s.Message}");
                        Console.ResetColor();
                    });

                    options.ProgressLevel = SyncProgressLevel.Debug;

                    var remoteOrchestrator = new WebRemoteOrchestrator(serviceUri, identifier: "01");

                    // create the agent
                    var agent = new SyncAgent(clientProvider, remoteOrchestrator, options);


                    // make a synchronization to get all rows between backup and now
                    var s = await agent.SynchronizeAsync(progress: localProgress);

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

    private static async Task SynchronizeWithLoggerAsync()
    {

        //docker run -it --name seq -p 5341:80 -e ACCEPT_EULA=Y datalust/seq

        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
        //var clientProvider = new SqliteSyncProvider("clientX.db");

        var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" });
        //var setup = new SyncSetup(new string[] { "Customer" });
        //var setup = new SyncSetup(new[] { "Customer" });
        //setup.Tables["Customer"].Columns.AddRange(new[] { "CustomerID", "FirstName", "LastName" });

        var options = new SyncOptions();
        options.BatchSize = 500;

        //Log.Logger = new LoggerConfiguration()
        //    .Enrich.FromLogContext()
        //    .MinimumLevel.Verbose()
        //    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
        //    .WriteTo.Console()
        //    .CreateLogger();

        ILoggerFactory loggerFactory = null;
        Microsoft.Extensions.Logging.ILogger logger = null;

        // *) Synclogger
        //options.Logger = new SyncLogger().AddConsole().SetMinimumLevel(LogLevel.Debug);


        // *) create a NLog logger
        loggerFactory = LoggerFactory.Create(builder => { builder.AddNLogWeb(); });
        logger = loggerFactory.CreateLogger("Dotmim.Sync");
        options.Logger = logger;


        //// *) create a console logger
        //loggerFactory = LoggerFactory.Create(builder => { builder.AddConsole().SetMinimumLevel(LogLevel.Trace); });
        //logger = loggerFactory.CreateLogger("Dotmim.Sync");
        //options.Logger = logger;

        //// *) create a seq logger
        //loggerFactory = LoggerFactory.Create(builder => { builder.AddSeq().SetMinimumLevel(LogLevel.Debug); });
        //logger = loggerFactory.CreateLogger("Dotmim.Sync");
        //options.Logger = logger;


        //// *) create a serilog logger
        //loggerFactory = LoggerFactory.Create(builder => { builder.AddSerilog().SetMinimumLevel(LogLevel.Trace); });
        //logger = loggerFactory.CreateLogger("Dotmim.Sync");
        //options.Logger = logger;

        // *) Using Serilog with Seq
        //var serilogLogger = new LoggerConfiguration()
        //    .Enrich.FromLogContext()
        //    .MinimumLevel.Debug()
        //    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
        //    .WriteTo.Seq("http://localhost:5341")
        //    .CreateLogger();


        //var actLogging = new Action<SyncLoggerOptions>(slo =>
        //{
        //    slo.AddConsole();
        //    slo.SetMinimumLevel(LogLevel.Information);
        //});

        ////var loggerFactory = LoggerFactory.Create(builder => builder.AddSerilog().AddConsole().SetMinimumLevel(LogLevel.Information));

        //var loggerFactory = LoggerFactory.Create(builder => builder.AddSerilog(serilogLogger));

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


        //options.Logger = new SyncLogger().AddConsole().SetMinimumLevel(LogLevel.Debug);


        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.ProgressPercentage:p}:\t{s.Message}");
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

                var s1 = await agent.SynchronizeAsync(setup, progress);

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
