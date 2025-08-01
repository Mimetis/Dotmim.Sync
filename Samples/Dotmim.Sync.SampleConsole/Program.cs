using Dotmim.Sync;
using Dotmim.Sync.DatabaseStringParsers;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.MySql;
using Dotmim.Sync.SampleConsole;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.SqlServer.Builders;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NLog.Web;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

#if NETCOREAPP3_1
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

    public static string[] OneTable = new string[] { "[Customers E-Mails]" };
    public static string[] TwoTableS = new string[] { "ProductCategory", "ProductDescription" };

    private static async Task Main(string[] args)
    {
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(ServerDbName));

        //var serverProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(ServerDbName));

        // var serverProvider = new NpgsqlSyncProvider(DBHelper.GetNpgsqlDatabaseConnectionString("data"));
        // var serverProvider = new MariaDBSyncProvider(DBHelper.GetMariadbDatabaseConnectionString(ServerDbName));
        // var serverProvider = new MySqlSyncProvider(DBHelper.GetMySqlDatabaseConnectionString(ServerDbName));

        //var clientProvider = new SqliteSyncProvider(Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db");
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(ClientDbName));
        // var clientProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(ClientDbName));
        // var clientProvider = new NpgsqlSyncProvider(DBHelper.GetNpgsqlDatabaseConnectionString(clientDbName));
        // clientProvider.UseBulkOperations = false;
        // var clientProvider = new MariaDBSyncProvider(DBHelper.GetMariadbDatabaseConnectionString(clientDbName));
        // var clientProvider = new MySqlSyncProvider(DBHelper.GetMySqlDatabaseConnectionString(clientDbName));
        var setup = new SyncSetup(OneTable);

        // options.Logger = new SyncLogger().AddDebug().SetMinimumLevel(LogLevel.Information);
        // options.UseVerboseErrors = true;

        // setup.Tables["ProductCategory"].Columns.AddRange(new string[] { "ProductCategoryID", "ParentProductCategoryID", "Name" });
        // setup.Tables["ProductDescription"].Columns.AddRange(new string[] { "ProductDescriptionID", "Description" });
        // setup.Filters.Add("ProductCategory", "ParentProductCategoryID", null, true);

        // var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString("vaguegitserver"));
        // var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString("vaguegitclient"));

        // var setup = new SyncSetup(new string[] { "SubscriptionTransactions" });
        var options = new SyncOptions();

        // var loggerFactory = LoggerFactory.Create(builder => { builder.AddSeq().SetMinimumLevel(LogLevel.Debug); });
        // var logger = loggerFactory.CreateLogger("Dotmim.Sync");
        // options.Logger = logger;
        // options.SnapshotsDirectory = Path.Combine("C:\\Tmp\\Snapshots");

        // await SyncHttpThroughKestrelAsync(clientProvider, serverProvider, setup, options);

        //await SyncHttpThroughKestrelAsync(clientProvider, serverProvider, setup, options);
        await SynchronizeAsync(clientProvider, serverProvider, setup, options);

        //await ScenarioAsync();
        //await CheckProvisionTime();
        //await SyncHttpThroughKestrelAsync(clientProvider, serverProvider, setup, options);
        //await MMCAsync();
    }

    private static async Task SynchronizeAsync(CoreProvider clientProvider, CoreProvider serverProvider, SyncSetup setup, SyncOptions options, string scopeName = SyncOptions.DefaultScopeName)
    {
        options.DisableConstraintsOnApplyChanges = true;
        options.TransactionMode = TransactionMode.None;

        var progress = new SynchronousProgress<ProgressArgs>(s =>
            Console.WriteLine($"{s.ProgressPercentage:p}:  " +
            $"\t[{s?.Source?[..Math.Min(4, s.Source.Length)]}] {s.TypeName}: {s.Message}"));

        // options.Logger = new SyncLogger().AddConsole().SetMinimumLevel(LogLevel.Debug);
        // options.ErrorResolutionPolicy = ErrorResolution.ContinueOnError;
        var agent = new SyncAgent(clientProvider, serverProvider, options);

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



    /// <summary>
    /// This method returns the full quoted name of the tracking table
    /// </summary>
    private static string GetTrackingTableFullQuotedName(SyncTable tableDescription, SyncSetup setup)
    {
        var trakingTableNameString = string.IsNullOrEmpty(setup.TrackingTablesPrefix) && string.IsNullOrEmpty(setup.TrackingTablesSuffix)
            ? $"{tableDescription.TableName}_tracking"
            : $"{setup.TrackingTablesPrefix}{tableDescription.TableName}{setup.TrackingTablesSuffix}";

        if (!string.IsNullOrEmpty(tableDescription.SchemaName))
            trakingTableNameString = $"{tableDescription.SchemaName}.{trakingTableNameString}";

        var trackingTableParser = new TableParser(trakingTableNameString, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
        return trackingTableParser.QuotedFullName;

    }

    /// <summary>
    /// This method returns the unquoted schema and table names
    /// </summary>
    private static (string SchemaName, string TableName) GetTableNameUnquotedName(SyncTable tableDescription)
    {
        //-------------------------------------------------
        // define tracking table name with prefix and suffix.
        // if no pref / suf, use default value
        var tableNameString = tableDescription.TableName;

        if (!string.IsNullOrEmpty(tableDescription.SchemaName))
            tableNameString = $"{tableDescription.SchemaName}.{tableNameString}";
        else
            tableNameString = $"dbo.{tableNameString}";

        var tableParser = new TableParser(tableNameString, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
        return (tableParser.SchemaName, tableParser.TableName);

    }

    /// <summary>
    /// Get the update trigger command text as a replacement, to handle omitted columns
    /// </summary>
    private static string GetUpdateTrigger(SyncTable tableDescription, SyncSetup setup)
    {
        var trackingTableName = GetTrackingTableFullQuotedName(tableDescription, setup);
        var tableName = GetTableNameUnquotedName(tableDescription);
        var triggerNormalizedName = $"{setup.TriggersPrefix}{tableName.TableName}{setup.TriggersSuffix}_";
        var triggerName = string.Format("[{0}].[{1}update_trigger]", tableName.SchemaName, triggerNormalizedName);

        var stringBuilder = new StringBuilder();
        stringBuilder.AppendLine($"CREATE TRIGGER {triggerName} ON [{tableName.SchemaName}].[{tableName.TableName}] FOR UPDATE AS");
        stringBuilder.AppendLine();
        stringBuilder.AppendLine("SET NOCOUNT ON;");
        stringBuilder.AppendLine();
        stringBuilder.AppendLine("UPDATE [side] ");
        stringBuilder.AppendLine("SET \t[update_scope_id] = NULL -- since the update if from local, it's a NULL");
        stringBuilder.AppendLine("\t,[last_change_datetime] = GetUtcDate()");
        stringBuilder.AppendLine();

        stringBuilder.AppendLine($"FROM {trackingTableName} [side]");
        stringBuilder.Append($"JOIN INSERTED AS [i] ON ");
        stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(tableDescription.PrimaryKeys, "[side]", "[i]"));

        if (tableDescription.GetMutableColumns().Count() > 0)
        {
            stringBuilder.Append($"JOIN DELETED AS [d] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(tableDescription.PrimaryKeys, "[d]", "[i]"));

            stringBuilder.AppendLine("WHERE (");
            string or = "";
            foreach (var column in tableDescription.GetMutableColumns())
            {
                var columnParser = new ObjectParser(column.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);

                stringBuilder.Append("\t");
                stringBuilder.Append(or);
                stringBuilder.Append("ISNULL(");
                stringBuilder.Append("NULLIF(");
                stringBuilder.Append("[d].");
                stringBuilder.Append(columnParser.QuotedShortName);
                stringBuilder.Append(", ");
                stringBuilder.Append("[i].");
                stringBuilder.Append(columnParser.QuotedShortName);
                stringBuilder.Append(")");
                stringBuilder.Append(", ");
                stringBuilder.Append("NULLIF(");
                stringBuilder.Append("[i].");
                stringBuilder.Append(columnParser.QuotedShortName);
                stringBuilder.Append(", ");
                stringBuilder.Append("[d].");
                stringBuilder.Append(columnParser.QuotedShortName);
                stringBuilder.Append(")");
                stringBuilder.AppendLine(") IS NOT NULL");

                or = " OR ";
            }
            stringBuilder.AppendLine(") ");
        }

        stringBuilder.AppendLine($"INSERT INTO {trackingTableName} (");

        var stringBuilderArguments = new StringBuilder();
        var stringBuilderArguments2 = new StringBuilder();
        var stringPkAreNull = new StringBuilder();

        string argComma = " ";
        string argAnd = string.Empty;
        var primaryKeys = tableDescription.GetPrimaryKeysColumns();

        foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
        {
            var columnParser = new ObjectParser(mutableColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);

            stringBuilderArguments.AppendLine($"\t{argComma}[i].{columnParser.QuotedShortName}");
            stringBuilderArguments2.AppendLine($"\t{argComma}{columnParser.QuotedShortName}");
            stringPkAreNull.Append($"{argAnd}[side].{columnParser.QuotedShortName} IS NULL");
            argComma = ",";
            argAnd = " AND ";
        }

        stringBuilder.Append(stringBuilderArguments2.ToString());
        stringBuilder.AppendLine("\t,[update_scope_id]");
        stringBuilder.AppendLine("\t,[sync_row_is_tombstone]");
        stringBuilder.AppendLine("\t,[last_change_datetime]");
        stringBuilder.AppendLine(") ");
        stringBuilder.AppendLine("SELECT");
        stringBuilder.Append(stringBuilderArguments.ToString());
        stringBuilder.AppendLine("\t,NULL");
        stringBuilder.AppendLine("\t,0");
        stringBuilder.AppendLine("\t,GetUtcDate()");
        stringBuilder.AppendLine("FROM INSERTED [i]");
        stringBuilder.Append($"JOIN DELETED AS [d] ON ");
        stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(tableDescription.PrimaryKeys, "[d]", "[i]"));
        stringBuilder.Append($"LEFT JOIN {trackingTableName} [side] ON ");
        stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(tableDescription.PrimaryKeys, "[i]", "[side]"));
        stringBuilder.Append("WHERE ");
        stringBuilder.AppendLine(stringPkAreNull.ToString());

        if (tableDescription.GetMutableColumns().Count() > 0)
        {
            stringBuilder.AppendLine("AND (");
            string or = "";
            foreach (var column in tableDescription.GetMutableColumns())
            {
                var columnParser = new ObjectParser(column.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);

                stringBuilder.Append("\t");
                stringBuilder.Append(or);
                stringBuilder.Append("ISNULL(");
                stringBuilder.Append("NULLIF(");
                stringBuilder.Append("[d].");
                stringBuilder.Append(columnParser.QuotedShortName);
                stringBuilder.Append(", ");
                stringBuilder.Append("[i].");
                stringBuilder.Append(columnParser.QuotedShortName);
                stringBuilder.Append(")");
                stringBuilder.Append(", ");
                stringBuilder.Append("NULLIF(");
                stringBuilder.Append("[i].");
                stringBuilder.Append(columnParser.QuotedShortName);
                stringBuilder.Append(", ");
                stringBuilder.Append("[d].");
                stringBuilder.Append(columnParser.QuotedShortName);
                stringBuilder.Append(")");
                stringBuilder.AppendLine(") IS NOT NULL");

                or = " OR ";
            }
            stringBuilder.AppendLine(") ");
        }
        return stringBuilder.ToString();
    }

    public static async Task ProvisionAsync()
    {
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString("1001_SearchManager"));
        var setup = new SyncSetup("AssignedKITS", "AssignedMEMBERS", "AssignedSUPPORT", "Basic_Info", "CircleGroups",
        "Circles", "Columns", "Consensus", "Consensus_Areas", "Consensus_Areas_GUI", "Consensus_Areas_StatusX",
        "Consensus_Areas_Terrain", "Consensus_Members", "Consensus_Members_History", "Consensus_Method",
        "Consensus_Paths", "Consensus_Scoring_Letters", "Consensus_Scoring_Numbers", "ConsensusNO", "DateTimeColumns",
        "DeviceData", "DeviceGroups", "Devices", "DispersionAngles", "EventsLOG", "ExtraCircles", "FlowPaths",
        "FlowPoints", "Grid_Cells", "Grids", "GUI_COLORS", "IIMARCH", "Incident_REFERENCE_POINTS_Options",
        "IncidentINFO", "Incidents", "Incidents_Reference_Points", "Incidents_Reference_Points_Extra", "InitDATABASE",
        "Kopija sPARTY_STATUS", "Labels", "Labels_Content", "Masts", "Media", "MISPERBehaviour", "MISPERCategory",
        "MISPERSet", "MortBsePlt_AssessmentMatrixData", "MortBsePlt_Target_Corners", "MortBsePlt_Targets",
        "MortBsePlt_TargetContent", "MortBsePlt_Weapons", "MortBsePlt_WeaponContent", "MR_LOG_DATA",
        "MR_LOG_OPTIONS", "MRequipment", "MRmembers", "MRsupport", "MRteamsIMPORT", "PARTIES_STATUS", "PARTIES_STATUS_COPY",
        "Person_data_Template_CasEvac", "Person_data_Template_Search", "Persons", "RadioChannels", "Rankings", "SCENARIO",
        "SCENARIO_ITEMS", "Search_Progress_Colors", "SearchGradedResponseMatrix", "SearchUrgency", "sPARTY_STATUS", "SPOT2FeedSettings",
        "SpotData", "StandardCircles", "STRATEGY", "TASK_ITEMS", "TaskingData", "Terrain", "TerrainEDIT", "TextTASKS", "UserRoles",
        "Users", "X_BAP_Points", "X_BAP_Points_Types", "X_MR_Settings", "X_MRAC_Areas", "X_MRAC_Areas_Colors", "X_MRAC_Points",
        "X_MRGM_GpxFiles", "X_MRPC_Paths", "X_MRPC_Paths_Colors", "X_MRPC_Points", "X_MRPC_Reports", "X_MRPTC_Content",
        "X_MRPTC_Points", "X_MRPTC_Points_Types", "X_MRPTC_Reports", "Y_TRACKER_Data", "Y_TRACKER_Data_Received", "y_TRACKER_DataS",
        "y_TRACKER_FILE_PARSER", "y_TRACKER_PROPERTIES", "y_TRACKER_TRANSPARENCY", "YellowBrickData", "Z_DBversion");

        var watch = Stopwatch.StartNew();
        var options = new SyncOptions { DisableConstraintsOnApplyChanges = false, TransactionMode = TransactionMode.None };
        serverProvider.IsolationLevel = IsolationLevel.ReadUncommitted;

        var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

        var p = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.ScopeInfo | SyncProvision.ScopeInfoClient;
        Console.WriteLine("Deprovisioning");
        await remoteOrchestrator.DeprovisionAsync(p);

        Console.WriteLine("Provisioning");
        await remoteOrchestrator.ProvisionAsync(setup);

        watch.Stop();
        var ellapsedTime = $"{watch.Elapsed.Minutes}:{watch.Elapsed.Seconds}.{watch.Elapsed.Milliseconds}";

        Console.WriteLine($"Ellapsed time:{ellapsedTime}");
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

                var webServerAgent = string.IsNullOrEmpty(identifier) ? webServerAgents.First() : webServerAgents.First(wsa => wsa.Identifier == identifier);

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
        var setup = new SyncSetup(TwoTableS);

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