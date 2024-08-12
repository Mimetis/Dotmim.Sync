using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.PostgreSql;
using Dotmim.Sync.SampleConsole;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NLog.Web;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

#if NET6_0 || NET8_0
#elif NETCOREAPP3_1
using MySql.Data.MySqlClient;
#endif

internal class Program
{
    public static string ServerDbName = "AdventureWorks";
    public static string ServerProductCategoryDbName = "AdventureWorksProductCategory";
    public static string ClientDbName = "Client";
    public static string[] AllTables = new string[]
    {
        "ProductDescription", "ProductCategory",
        "ProductModel", "Product",
        "Address", "Customer", "CustomerAddress",
        "SalesOrderHeader", "SalesOrderDetail",
    };

    public static string[] OneTable = new string[] { "ProductCategory" };
    internal static readonly string[] Tables = new string[] { "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" };

    private static async Task Main(string[] args)
    {

        // var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(ServerDbName));

        // var serverProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var serverProvider = new NpgsqlSyncProvider(DBHelper.GetNpgsqlDatabaseConnectionString("data"));
        // var serverProvider = new MariaDBSyncProvider(DBHelper.GetMariadbDatabaseConnectionString(serverDbName));
        // var serverProvider = new MySqlSyncProvider(DBHelper.GetMySqlDatabaseConnectionString(serverDbName));

        var clientProvider = new SqliteSyncProvider(Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db");
        //var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(ClientDbName));

        // var clientProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
        // var clientProvider = new NpgsqlSyncProvider(DBHelper.GetNpgsqlDatabaseConnectionString(clientDbName));
        // clientProvider.UseBulkOperations = false;
        // var clientProvider = new MariaDBSyncProvider(DBHelper.GetMariadbDatabaseConnectionString(clientDbName));
        // var clientProvider = new MySqlSyncProvider(DBHelper.GetMySqlDatabaseConnectionString(clientDbName));
        var setup = new SyncSetup("dataengine.trackplot_cog");

        var options = new SyncOptions();

        // options.Logger = new SyncLogger().AddDebug().SetMinimumLevel(LogLevel.Information);
        // options.UseVerboseErrors = true;

        // setup.Tables["ProductCategory"].Columns.AddRange(new string[] { "ProductCategoryID", "ParentProductCategoryID", "Name" });
        // setup.Tables["ProductDescription"].Columns.AddRange(new string[] { "ProductDescriptionID", "Description" });
        // setup.Filters.Add("ProductCategory", "ParentProductCategoryID", null, true);

        // var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString("vaguegitserver"));
        // var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString("vaguegitclient"));

        // var setup = new SyncSetup(new string[] { "SubscriptionTransactions" });
        // var options = new SyncOptions();

        // var loggerFactory = LoggerFactory.Create(builder => { builder.AddSeq().SetMinimumLevel(LogLevel.Debug); });
        // var logger = loggerFactory.CreateLogger("Dotmim.Sync");
        // options.Logger = logger;
        // options.SnapshotsDirectory = Path.Combine("C:\\Tmp\\Snapshots");

        // await SyncHttpThroughKestrelAsync(clientProvider, serverProvider, setup, options);

        // await SyncHttpThroughKestrelAsync(clientProvider, serverProvider, setup, options);
        //await CheckChanges(clientProvider, serverProvider, setup, options);

        //await SynchronizeAsync(clientProvider, serverProvider, setup, options);

        await ScenarioAsync();
        //await CheckProvisionTime();
    }

    public static async Task ScenarioAsync()
    {
        var serverProvider = new NpgsqlSyncProvider(DBHelper.GetNpgsqlDatabaseConnectionString("AdvData"));
        var clientProvider = new SqliteSyncProvider(Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db");

        //AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior", false);

        // reset
        await DropAllAsync(serverProvider);

        var setup = new SyncSetup("dataengine.trackplot_cog");
        var options = new SyncOptions();

        var progress = new SynchronousProgress<ProgressArgs>(s =>
            Console.WriteLine($"{s.ProgressPercentage:p}:  " +
            $"\t[{s?.Source?[..Math.Min(4, s.Source.Length)]}] {s.TypeName}: {s.Message}"));


        // Initial rows on server side
        await AddTrackPlotAsync(serverProvider, 12, "server 01");
        await AddTrackPlotAsync(serverProvider, 23, "server 01");
        await AddTrackPlotAsync(serverProvider, 34, "server 01");

        var agent = new SyncAgent(clientProvider, serverProvider, options);

        var syncResult = await agent.SynchronizeAsync(setup, progress);
        Console.WriteLine(syncResult);

        await AddTrackPlotAsync(clientProvider, 12, "client 10");

        syncResult = await agent.SynchronizeAsync(setup, progress);
        Console.WriteLine(syncResult);


    }

    internal static async Task DropAllAsync(CoreProvider provider)
    {
        var remoteOrchestrator = new RemoteOrchestrator(provider);
        await remoteOrchestrator.DropAllAsync();

        var connection = provider.CreateConnection();

        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = "Delete from dataengine.trackplot_cog";
        command.Connection = connection;

        await command.ExecuteNonQueryAsync();

        connection.Close();

    }

    internal static async Task AddTrackPlotAsync(CoreProvider provider, int id, string name = default)
    {
        string commandText;
        if (provider.GetShortProviderTypeName().ToLower() == "sqlitesyncprovider")
            commandText = $"Insert into trackplot_cog (id, timestamp, name, lat, long) " +
                                 $"Values (@id, @timestamp, @name, @lat, @long)";
        else
            commandText = $"Insert into dataengine.trackplot_cog (id, timestamp, name, lat, long) " +
                                 $"Values (@id, @timestamp, @name, @lat, @long)";

        var connection = provider.CreateConnection();

        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = commandText;
        command.Connection = connection;

        var p = command.CreateParameter();
        p.DbType = DbType.Int32;
        p.ParameterName = "@id";
        p.Value = id;
        command.Parameters.Add(p);

        p = command.CreateParameter();
        p.DbType = DbType.DateTime;
        p.ParameterName = "@timestamp";
        p.Value = DateTime.UtcNow;
        command.Parameters.Add(p);

        p = command.CreateParameter();
        p.DbType = DbType.String;
        p.ParameterName = "@name";
        p.Value = string.IsNullOrEmpty(name) ? Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ' ' + Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() : name;
        command.Parameters.Add(p);

        p = command.CreateParameter();
        p.DbType = DbType.Double;
        p.ParameterName = "@lat";
        p.Value = 1.0;
        command.Parameters.Add(p);

        p = command.CreateParameter();
        p.DbType = DbType.Double;
        p.ParameterName = "@long";
        p.Value = 1.0;
        command.Parameters.Add(p);

        await command.ExecuteNonQueryAsync();

        connection.Close();
    }

    public static async Task CheckChanges(CoreProvider clientProvider, CoreProvider serverProvider, SyncSetup setup, SyncOptions options, string scopeName = SyncOptions.DefaultScopeName)
    {
        // Creating a remote orchestrator
        var remoteOrchestrator = new RemoteOrchestrator(serverProvider);

        // Getting all scope client to evaluate the changes
        var scopeInfoClients = await remoteOrchestrator.GetAllScopeInfoClientsAsync();

        // for each scope, get the changes
        foreach (var scopeInfoClient in scopeInfoClients)
        {
            // First idea: Just get the estimated changes count, as the method is faster than getting all rows
            var estimatedChanges = await remoteOrchestrator.GetEstimatedChangesCountAsync(scopeInfoClient);

            Console.WriteLine($"Estimated tables changes count for scope {scopeInfoClient.Id} : {estimatedChanges.ServerChangesSelected.TableChangesSelected.Count} tables");
            Console.WriteLine($"Estimated rows changes count for scope {scopeInfoClient.Id} : {estimatedChanges.ServerChangesSelected.TotalChangesSelected} rows");


            // Second idea: Get all changes
            // Get the batches changes serialized on disk
            var changes = await remoteOrchestrator.GetChangesAsync(scopeInfoClient);

            // load all the tables in memory
            var tables = remoteOrchestrator.LoadTablesFromBatchInfo(changes.ServerBatchInfo);

            // iterate
            foreach (var table in tables.ToList())
            {
                Console.WriteLine($"Changes for table {table.GetFullName()}");

                foreach (var row in table.Rows)
                {
                    Console.WriteLine($"Row {row}");
                }
            }

        }

        await remoteOrchestrator.ProvisionAsync(setup).ConfigureAwait(false);
        //await remoteOrchestrator.DeprovisionAsync(setup).ConfigureAwait(false);

    }

    //private static async Task SyncWithReinitialiazeWithChangeTrackingAsync()
    //{
    //    var serverProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(ServerDbName));
    //    var clientProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(ClientDbName));

    //    var setup = new SyncSetup("ProductCategory");

    //    var options = new SyncOptions
    //    {
    //        DisableConstraintsOnApplyChanges = true,
    //    };

    //    var progress = new SynchronousProgress<ProgressArgs>(s =>
    //        Console.WriteLine($"{s.ProgressPercentage:p}:  " +
    //        $"\t[{s?.Source?[..Math.Min(4, s.Source.Length)]}] {s.TypeName}: {s.Message}"));

    //    var agent = new SyncAgent(clientProvider, serverProvider, options);

    //    // var s = await agent.SynchronizeAsync(setup, progress: progress);
    //    // Console.WriteLine(s);

    //    // await DBHelper.AddProductCategoryRowAsync(clientProvider);
    //    var s2 = await agent.SynchronizeAsync(setup, SyncType.Reinitialize, progress: progress);
    //    Console.WriteLine(s2);
    //}

    private static async Task CreateSnapshotAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(ServerDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(ClientDbName));

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
            BatchSize = 3000,
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
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(ServerDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(ClientDbName));

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
        }
        while (Console.ReadKey().Key != ConsoleKey.Escape);
    }

    private static async Task SynchronizeAsync(CoreProvider clientProvider, CoreProvider serverProvider, SyncSetup setup, SyncOptions options, string scopeName = SyncOptions.DefaultScopeName)
    {
        options.DisableConstraintsOnApplyChanges = true;

        var progress = new SynchronousProgress<ProgressArgs>(s =>
            Console.WriteLine($"{s.ProgressPercentage:p}:  " +
            $"\t[{s?.Source?[..Math.Min(4, s.Source.Length)]}] {s.TypeName}: {s.Message}"));

        //options.Logger = new SyncLogger().AddConsole().SetMinimumLevel(LogLevel.Debug);

        // options.ErrorResolutionPolicy = ErrorResolution.ContinueOnError;
        var agent = new SyncAgent(clientProvider, serverProvider, options);

        agent.LocalOrchestrator.OnApplyChangesErrorOccured(args =>
        {
            Console.WriteLine("error on client for row " + args.ErrorRow);
            args.Resolution = ErrorResolution.ContinueOnError;
        });

        agent.RemoteOrchestrator.OnApplyChangesErrorOccured(args =>
        {
            Console.WriteLine("error on server for row " + args.ErrorRow);
            args.Resolution = ErrorResolution.ContinueOnError;
        });

        ;
        do
        {
            try
            {
                Console.ForegroundColor = ConsoleColor.Green;
                var s = await agent.SynchronizeAsync(scopeName, setup, progress: progress);
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
        }
        while (Console.ReadKey().Key != ConsoleKey.Escape);
    }

    public static async Task SyncHttpThroughKestrelAsync(CoreProvider clientProvider, CoreProvider serverProvider, SyncSetup setup, SyncOptions options)
    {

        options.ErrorResolutionPolicy = ErrorResolution.ContinueOnError;

        var configureServices = new Action<IServiceCollection>(services => services.AddSyncServer(serverProvider, setup, options, null, default, "01"));

        var serverHandler = new RequestDelegate(async context =>
        {
            try
            {
                var webServerAgents = context.RequestServices.GetService(typeof(IEnumerable<WebServerAgent>)) as IEnumerable<WebServerAgent>;

                var scopeName = context.GetScopeName();
                var identifier = context.GetIdentifier();

                var clientScopeId = context.GetClientScopeId();

                var webServerAgent = webServerAgents.First(wsa => wsa.Identifier == identifier);

                var errors = new Dictionary<string, string>();

                webServerAgent.RemoteOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    // not mandatory, if you have set the ErrorResolutionPolicy in the options
                    args.Resolution = ErrorResolution.ContinueOnError;

                    // add the error to the AdditionalProperties dictionary:
                    if (args.Context.AdditionalProperties == null)
                        args.Context.AdditionalProperties = new Dictionary<string, string>();

                    args.Context.AdditionalProperties.Add(args.ErrorRow.ToString(), args.Exception.Message);
                });

                await webServerAgent.HandleRequestAsync(context);
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex);
                throw;
            }
        });

        using var server = new KestrelTestServer(configureServices, false);

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

                    var remoteOrchestrator = new WebRemoteOrchestrator(new Uri(serviceUri), identifier: "01");

                    // create the agent
                    var agent = new SyncAgent(clientProvider, remoteOrchestrator, options);

                    agent.LocalOrchestrator.OnSessionEnd(args =>
                    {
                        if (args.Context.AdditionalProperties != null && args.Context.AdditionalProperties.Count > 0)
                        {
                            Console.WriteLine("Errors on server side");
                            foreach (var kvp in args.Context.AdditionalProperties)
                                Console.WriteLine($"Row {kvp.Key} \n Error:{kvp.Value}");
                        }
                    });

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
            }
            while (Console.ReadKey().Key != ConsoleKey.Escape);
        });
        await server.Run(serverHandler, clientHandler);
    }

    private static async Task SynchronizeWithLoggerAsync()
    {

        // docker run -it --name seq -p 5341:80 -e ACCEPT_EULA=Y datalust/seq

        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(ServerDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(ClientDbName));

        // var clientProvider = new SqliteSyncProvider("clientX.db");
        var setup = new SyncSetup(Tables);

        // var setup = new SyncSetup(new string[] { "Customer" });
        // var setup = new SyncSetup(new[] { "Customer" });
        // setup.Tables["Customer"].Columns.AddRange(new[] { "CustomerID", "FirstName", "LastName" });
        var options = new SyncOptions();
        options.BatchSize = 500;

        // Log.Logger = new LoggerConfiguration()
        //    .Enrich.FromLogContext()
        //    .MinimumLevel.Verbose()
        //    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
        //    .WriteTo.Console()
        //    .CreateLogger();
        ILoggerFactory loggerFactory = null;
        Microsoft.Extensions.Logging.ILogger logger = null;

        // *) Synclogger
        // options.Logger = new SyncLogger().AddConsole().SetMinimumLevel(LogLevel.Debug);

        // *) create a NLog logger
        loggerFactory = LoggerFactory.Create(builder => { builder.AddNLogWeb(); });
        logger = loggerFactory.CreateLogger("Dotmim.Sync");
        options.Logger = logger;

        //// *) create a console logger
        // loggerFactory = LoggerFactory.Create(builder => { builder.AddConsole().SetMinimumLevel(LogLevel.Trace); });
        // logger = loggerFactory.CreateLogger("Dotmim.Sync");
        // options.Logger = logger;

        //// *) create a seq logger
        // loggerFactory = LoggerFactory.Create(builder => { builder.AddSeq().SetMinimumLevel(LogLevel.Debug); });
        // logger = loggerFactory.CreateLogger("Dotmim.Sync");
        // options.Logger = logger;

        //// *) create a serilog logger
        // loggerFactory = LoggerFactory.Create(builder => { builder.AddSerilog().SetMinimumLevel(LogLevel.Trace); });
        // logger = loggerFactory.CreateLogger("Dotmim.Sync");
        // options.Logger = logger;

        // *) Using Serilog with Seq
        // var serilogLogger = new LoggerConfiguration()
        //    .Enrich.FromLogContext()
        //    .MinimumLevel.Debug()
        //    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
        //    .WriteTo.Seq("http://localhost:5341")
        //    .CreateLogger();

        // var actLogging = new Action<SyncLoggerOptions>(slo =>
        // {
        //    slo.AddConsole();
        //    slo.SetMinimumLevel(LogLevel.Information);
        // });

        ////var loggerFactory = LoggerFactory.Create(builder => builder.AddSerilog().AddConsole().SetMinimumLevel(LogLevel.Information));

        // var loggerFactory = LoggerFactory.Create(builder => builder.AddSerilog(serilogLogger));

        // loggerFactory.AddSerilog(serilogLogger);

        // options.Logger = loggerFactory.CreateLogger("dms");

        // 2nd option to add serilog
        // var loggerFactorySerilog = new SerilogLoggerFactory();
        // var logger = loggerFactorySerilog.CreateLogger<SyncAgent>();
        // options.Logger = logger;

        // options.Logger = new SyncLogger().AddConsole().AddDebug().SetMinimumLevel(LogLevel.Trace);

        // var snapshotDirectory = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "Snapshots");
        // options.BatchSize = 500;
        // options.SnapshotsDirectory = snapshotDirectory;
        // var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);
        // remoteOrchestrator.CreateSnapshotAsync().GetAwaiter().GetResult();

        // options.Logger = new SyncLogger().AddConsole().SetMinimumLevel(LogLevel.Debug);

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
                // if (!agent.Parameters.Contains("CompanyName"))
                //    agent.Parameters.Add("CompanyName", "Professional Sales and Service");
                var s1 = await agent.SynchronizeAsync(setup, progress);

                // Write results
                Console.WriteLine(s1);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

            // Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        }
        while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }
}