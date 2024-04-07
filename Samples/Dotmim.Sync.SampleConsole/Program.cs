using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SampleConsole;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using System.Linq;
using Dotmim.Sync.Tests.Models;
using System.Collections.Generic;
using Newtonsoft.Json;
using Dotmim.Sync.Builders;
using NLog.Web;
using System.Threading;
using Dotmim.Sync.Tests;


#if NET5_0 || NET6_0 || NET7_0
using MySqlConnector;
#elif NETSTANDARD
using MySql.Data.MySqlClient;
#endif

using System.Diagnostics;


internal class Program
{
    public static string serverDbName = "AdventureWorks";
    public static string serverProductCategoryDbName = "AdventureWorksProductCategory";
    public static string clientDbName = "Client";
    public static string[] allTables = new string[] {"ProductDescription", "ProductCategory",
                                                    "ProductModel", "Product",
                                                    "Address", "Customer", "CustomerAddress",
                                                    "SalesOrderHeader", "SalesOrderDetail"};

    public static string[] oneTable = new string[] { "MensCurves" };


    private static async Task Main(string[] args)
    {

        //var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var serverProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        //var serverProvider = new NpgsqlSyncProvider(DBHelper.GetNpgsqlDatabaseConnectionString("Wasim"));
        //var serverProvider = new MariaDBSyncProvider(DBHelper.GetMariadbDatabaseConnectionString(serverDbName));
        // var serverProvider = new MySqlSyncProvider(DBHelper.GetMySqlDatabaseConnectionString(serverDbName));

        // var clientProvider = new SqliteSyncProvider(Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db");
        var clientProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
        //var clientProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
        var clientProvider = new NpgsqlSyncProvider(DBHelper.GetNpgsqlDatabaseConnectionString(clientDbName));
        //clientProvider.UseBulkOperations = false;
        //var clientProvider = new MariaDBSyncProvider(DBHelper.GetMariadbDatabaseConnectionString(clientDbName));
        //var clientProvider = new MySqlSyncProvider(DBHelper.GetMySqlDatabaseConnectionString(clientDbName));

        var setup = new SyncSetup(oneTable);

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

        //await SynchronizeAsync(clientProvider, serverProvider, setup, options);

        //await SynchronizeAsync(clientProvider, serverProvider, setup, options);
        // await SynchronizeUniqueIndexAsync();
        await SyncAsync();

        await TestSyncSqliteWithFiftyColumnsAndOneThousandRowsAsync();

        await CreateSnapshotAsync();
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
            //await LoopAsync(100);
            //await LoopAsync(1000);
            //await LoopAsync(10000);
            await LoopAsync(20000);
            await LoopAsync(20000);
            await LoopAsync(40000);
            //await LoopAsync(50000);
            //await LoopAsync(100000);

            Console.WriteLine($"DONE.");
            Console.WriteLine($"----------------------------------------");

        } while (Console.ReadKey().Key != ConsoleKey.Escape);
    }

    private static async Task LoopAsync(int rowsNumber)
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqliteSyncProvider("db/" + rowsNumber.ToString() + "_" + Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db");
        var downloadOnlyclientProvider = new SqliteSyncDownloadOnlyProvider("db/" + rowsNumber.ToString() + "_" + Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db");
        var sqliteOnlyProvider = new SqliteSyncDownloadOnlyProvider("db/" + rowsNumber.ToString() + "_" + Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db");

        var setup = new SyncSetup("Codes");

        var options1 = new SyncOptions { BatchSize = 150000 };
        var options2 = new SyncOptions { BatchSize = 150000 };

        // Creating an agent that will handle all the process
        var agent1 = new SyncAgent(clientProvider, serverProvider, options1);
        var agent2 = new SyncAgent(downloadOnlyclientProvider, serverProvider, options2);
        var agent3 = new SyncAgent(sqliteOnlyProvider, serverProvider, options2);

        await DropAllAsync(agent1.RemoteOrchestrator);

        // no lines, create structure
        await agent1.SynchronizeAsync(setup);
        await agent2.SynchronizeAsync(setup);
        await agent3.SynchronizeAsync(setup);
        Console.WriteLine($"databases created and setup.");
        Console.WriteLine($"----------------------------------------");

        var result = InsertNCodesInProvider(agent1.RemoteOrchestrator.Provider, rowsNumber);
        Console.WriteLine($"Insert {rowsNumber} rows into server (SQL Server): {result:hh\\.mm\\:ss\\.fff}");

        var sqliteWatch = new Stopwatch();
        agent1.LocalOrchestrator.OnBatchChangesApplying(args => sqliteWatch.Start());
        agent1.LocalOrchestrator.OnBatchChangesApplied(args => sqliteWatch.Stop());
        var s1 = await agent1.SynchronizeAsync(setup);
        //var durationTs1 = TimeSpan.FromTicks(s1.CompleteTime.Ticks).Subtract(TimeSpan.FromTicks(s1.StartTime.Ticks));
        Console.WriteLine($"{agent1.LocalOrchestrator.Provider.GetShortProviderTypeName()}:{sqliteWatch.Elapsed:hh\\.mm\\:ss\\.fff}");

        var sqliteWatch2 = new Stopwatch();
        agent2.LocalOrchestrator.OnBatchChangesApplying(args => sqliteWatch2.Start());
        agent2.LocalOrchestrator.OnBatchChangesApplied(args => sqliteWatch2.Stop());
        var s2 = await agent2.SynchronizeAsync(setup);
        //var durationTs2 = TimeSpan.FromTicks(s2.CompleteTime.Ticks).Subtract(TimeSpan.FromTicks(s2.StartTime.Ticks));
        Console.WriteLine($"{agent2.LocalOrchestrator.Provider.GetShortProviderTypeName()}:{sqliteWatch2.Elapsed:hh\\.mm\\:ss\\.fff}");

        var cscopeClient = await agent3.LocalOrchestrator.GetScopeInfoClientAsync();
        var serverChanges = await agent3.RemoteOrchestrator.GetChangesAsync(cscopeClient);
        var bi = serverChanges.ServerBatchInfo.BatchPartsInfo[0];
        result = InsertRowsFromFile(agent3.LocalOrchestrator.Provider, serverChanges.ServerBatchInfo.GetBatchPartInfoPath(bi));
        Console.WriteLine($"Insert {rowsNumber} rows into client (SQLite): {result:hh\\.mm\\:ss\\.fff}");

        Console.WriteLine($"----------------------------------------");
    }

    private static async Task DropAllAsync(RemoteOrchestrator remoteOrchestrator)
    {
        var progress = new SynchronousProgress<ProgressArgs>(s =>
            Console.WriteLine($"{s.ProgressPercentage:p}:  " +
            $"\t[{s?.Source?[..Math.Min(4, s.Source.Length)]}] {s.TypeName}: {s.Message}"));

        options.DisableConstraintsOnApplyChanges = true;
        options.TransactionMode = TransactionMode.PerBatch;

        var commandDel = connection.CreateCommand();
        commandDel.Connection = connection;
        commandDel.CommandText = "Delete from Codes";
        connection.Open();
        commandDel.ExecuteNonQuery();
        connection.Close();

        setup = new SyncSetup("Items");
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

    private static TimeSpan InsertRowsFromFile(CoreProvider coreProvider, string fullPath)
    {
        var localSerializer = new LocalJsonSerializer();
        var (schemaTable, rowsCount, state) = LocalJsonSerializer.GetSchemaTableFromFile(fullPath);
        using var connection = coreProvider.CreateConnection();


        string sql = @"Insert into Codes(Code, 
            Name1 , Name2 , Name3 , Name4 , Name5 , Name6 , Name7 , Name8 , Name9 , Date1 , Date2 , Date3 , Date4 , Date5 , Date6 , Date7 , Date8 , Date9 , Date10, Date11, Date12, Date13, Date14, Date15, Date16, Date17, Date18, Date19, Date20, Date21, Date22, Date23, Date24, Date25, Date26, Date27, Date28, Date29, Date30, Date31, Date32, Date33, Date34, Date35, Date36, Date37, Date38, Date39, Date40
            ) VALUES(@Code,
            @Name1 , @Name2 , @Name3 , @Name4 , @Name5 , @Name6 , @Name7 , @Name8 , @Name9 , @Date1 , @Date2 , @Date3 , @Date4 , @Date5 , @Date6 , @Date7 , @Date8 , @Date9 , @Date10, @Date11, @Date12, @Date13, @Date14, @Date15, @Date16, @Date17, @Date18, @Date19, @Date20, @Date21, @Date22, @Date23, @Date24, @Date25, @Date26, @Date27, @Date28, @Date29, @Date30, @Date31, @Date32, @Date33, @Date34, @Date35, @Date36, @Date37, @Date38, @Date39, @Date40
            )";


        var stopWatch = new Stopwatch();
        stopWatch.Start();

        var command = connection.CreateCommand();
        command.Connection = connection;
        command.CommandText = sql;

        DbParameter p;

        p = command.CreateParameter();
        p.DbType = DbType.Int32;
        p.ParameterName = "@Code";
        command.Parameters.Add(p);

        for (int i = 1; i <= 9; i++)
        {
            p = command.CreateParameter();
            p.DbType = DbType.String;
            p.Size = 36;
            p.ParameterName = "@Name" + i.ToString();
            command.Parameters.Add(p);
        }

        for (int i = 1; i <= 40; i++)
        {
            p = command.CreateParameter();
            p.DbType = DbType.DateTime;
            p.ParameterName = "@Date" + i.ToString();
            command.Parameters.Add(p);
        }

        connection.Open();

        command.Prepare();
        using (var transaction = connection.BeginTransaction())
        {
            foreach (var syncRow in localSerializer.GetRowsFromFile(fullPath, schemaTable))
            {
                command.Transaction = transaction;
                command.Parameters[0].Value = syncRow[0];

                for (int k = 1; k <= 49; k++)
                    command.Parameters[k].Value = syncRow[k];
                command.ExecuteNonQuery();

            }

            transaction.Commit();
        }

        connection.Close();

        stopWatch.Stop();

        return stopWatch.Elapsed;



    }

    private static TimeSpan InsertNCodesInProvider(CoreProvider coreProvider, int count)
    {

        using var connection = coreProvider.CreateConnection();

        var commandMax = connection.CreateCommand();
        commandMax.Connection = connection;
        commandMax.CommandText = "Select max(Code) from Codes";
        connection.Open();
        var maxCodeO = commandMax.ExecuteScalar();
        var maxCode = maxCodeO == DBNull.Value ? 0 : Convert.ToInt32(maxCodeO) + 1;
        connection.Close();

        string sql = @"Insert into Codes(Code, 
            Name1 , Name2 , Name3 , Name4 , Name5 , Name6 , Name7 , Name8 , Name9 , Date1 , Date2 , Date3 , Date4 , Date5 , Date6 , Date7 , Date8 , Date9 , Date10, Date11, Date12, Date13, Date14, Date15, Date16, Date17, Date18, Date19, Date20, Date21, Date22, Date23, Date24, Date25, Date26, Date27, Date28, Date29, Date30, Date31, Date32, Date33, Date34, Date35, Date36, Date37, Date38, Date39, Date40
            ) VALUES(@Code,
            @Name1 , @Name2 , @Name3 , @Name4 , @Name5 , @Name6 , @Name7 , @Name8 , @Name9 , @Date1 , @Date2 , @Date3 , @Date4 , @Date5 , @Date6 , @Date7 , @Date8 , @Date9 , @Date10, @Date11, @Date12, @Date13, @Date14, @Date15, @Date16, @Date17, @Date18, @Date19, @Date20, @Date21, @Date22, @Date23, @Date24, @Date25, @Date26, @Date27, @Date28, @Date29, @Date30, @Date31, @Date32, @Date33, @Date34, @Date35, @Date36, @Date37, @Date38, @Date39, @Date40
            )";

        var stopWatch = new Stopwatch();
        stopWatch.Start();

        var command = connection.CreateCommand();
        command.Connection = connection;
        command.CommandText = sql;

        DbParameter p;

        p = command.CreateParameter();
        p.DbType = DbType.Int32;
        p.ParameterName = "@Code";
        command.Parameters.Add(p);

        for (int i = 1; i <= 9; i++)
        {
            p = command.CreateParameter();
            p.DbType = DbType.String;
            p.Size = 36;
            p.ParameterName = "@Name" + i.ToString();
            command.Parameters.Add(p);
        }

        for (int i = 1; i <= 40; i++)
        {
            p = command.CreateParameter();
            p.DbType = DbType.DateTime;
            p.ParameterName = "@Date" + i.ToString();
            command.Parameters.Add(p);
        }

        connection.Open();

        command.Prepare();
        using (var transaction = connection.BeginTransaction())
        {
            for (var i = maxCode; i < count + maxCode; i++)
            {
                command.Transaction = transaction;
                command.Parameters[0].Value = i;

                for (int k = 1; k <= 9; k++)
                    command.Parameters[k].Value = "Product " + i.ToString() + k.ToString();
                for (int k = 10; k < 50; k++)
                    command.Parameters[k].Value = DateTime.Now;
                command.ExecuteNonQuery();
            }
            transaction.Commit();
        }

        connection.Close();

        stopWatch.Stop();

        return stopWatch.Elapsed;
    }


    private static async Task SynchronizeWithFiltersJoinsAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        // var clientProvider = new SqliteSyncProvider(Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db");
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        var setup = new SyncSetup("SalesLT.ProductCategory");

        var productCategoryFilter = new SetupFilter("ProductCategory", "SalesLT");
        productCategoryFilter.AddParameter("ProductCategoryID", "ProductCategory", "SalesLT");
        productCategoryFilter.AddWhere("ProductCategoryID", "ProductCategory", "ProductCategoryID", "SalesLT");
        setup.Filters.Add(productCategoryFilter);


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
            // Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {

                // classic sync for A_BIKES
                var paramsABikes = new SyncParameters { { "ProductCategoryId", "A_BIKES" } };

                // Sync server. Create the local schema, Get all the A_BIKES rows, and apply them locally
                var s1 = await agent.SynchronizeAsync(setup, paramsABikes, progress);
                Console.WriteLine(s1);

                // Adding a R_BIKES product category, and we want to upload it, despite the fact we don't have yet a correct scope
                await DBHelper.AddProductCategoryRowAsync(agent.LocalOrchestrator.Provider, "R_BIKES", default, "R Bikes category");

                // the params to use                
                var paramsRBikes = new SyncParameters { { "ProductCategoryId", "R_BIKES" } };

                // You need to do this trick only ONE TIME.
                // Once it's done, you don't have to do it on every sync, of course
                // -----------------------------------------------------------------
                // Get the scope client for R_BIKES from local storage
                // The GetScopeInfoClientAsync method will create the scope if it does not exists in the local database
                var scopeClientRBikes = await agent.LocalOrchestrator.GetScopeInfoClientAsync(syncParameters: paramsRBikes);
                // hack the last sync datetime
                scopeClientRBikes.LastSyncTimestamp = 0;
                scopeClientRBikes.LastSync = DateTime.Now.AddDays(-1);
                // Save it
                await agent.LocalOrchestrator.SaveScopeInfoClientAsync(scopeClientRBikes);
                // -----------------------------------------------------------------

                // now sync
                var s2 = await agent.SynchronizeAsync(setup, paramsRBikes, progress);
                Console.WriteLine(s2);


                // Write results

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }


            //Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }
    private static async Task SynchronizeAsync(CoreProvider clientProvider, CoreProvider serverProvider, SyncSetup setup, SyncOptions options, string scopeName = SyncOptions.DefaultScopeName)
    {
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

        var configureServices = new Action<IServiceCollection>(services =>
        {
            services.AddSyncServer(serverProvider.GetType(), serverProvider.ConnectionString, setup: setup, options: options, identifier: "01");
        });

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



    ///// <summary>
    ///// Test a client syncing through a web api
    ///// </summary>
    //private static async Task SyncThroughWebApiAsync()
    //{
    //    var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

    //    var handler = new HttpClientHandler { AutomaticDecompression = DecompressionMethods.GZip };
    //    var client = new HttpClient(handler) { Timeout = TimeSpan.FromMinutes(5) };

    //    var proxyClientProvider = new WebRemoteOrchestrator("https://localhost:44313/api/Sync", client: client);

    //    var options = new SyncOptions
    //    {
    //        BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "Tmp"),
    //        BatchSize = 2000,
    //    };

    //    // Create the setup used for your sync process
    //    //var tables = new string[] { "Employees" };


    //    var remoteProgress = new SynchronousProgress<ProgressArgs>(pa =>
    //    {
    //        Console.ForegroundColor = ConsoleColor.Gray;
    //        Console.WriteLine($"{pa.ProgressPercentage:p}\t {pa.Message}");
    //        Console.ResetColor();
    //    });

    //    var snapshotProgress = new SynchronousProgress<ProgressArgs>(pa =>
    //    {
    //        Console.ForegroundColor = ConsoleColor.Blue;
    //        Console.WriteLine($"{pa.ProgressPercentage:p}\t {pa.Message}");
    //        Console.ResetColor();
    //    });

    //    var localProgress = new SynchronousProgress<ProgressArgs>(s =>
    //    {
    //        Console.ForegroundColor = ConsoleColor.Green;
    //        Console.WriteLine($"{s.ProgressPercentage:p}:\t{s.Message}");
    //        Console.ResetColor();
    //    });


    //    var agent = new SyncAgent(clientProvider, proxyClientProvider, options);


    //    Console.WriteLine("Press a key to start (be sure web api is running ...)");
    //    Console.ReadKey();
    //    do
    //    {
    //        Console.Clear();
    //        Console.WriteLine("Web sync start");
    //        try
    //        {

    //            var s = await agent.SynchronizeAsync(SyncType.Reinitialize, localProgress);
    //            Console.WriteLine(s);

    //        }
    //        catch (SyncException e)
    //        {
    //            Console.WriteLine(e.Message);
    //        }
    //        catch (Exception e)
    //        {
    //            Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
    //        }


    //        Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
    //    } while (Console.ReadKey().Key != ConsoleKey.Escape);

    //    Console.WriteLine("End");

    //}



    //private static async Task SynchronizeWithFiltersAsync()
    //{
    //    // Create 2 Sql Sync providers
    //    var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
    //    //var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

    //    var clientDatabaseName = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db";
    //    var clientProvider = new SqliteSyncProvider(clientDatabaseName);

    //    var setup = new SyncSetup(new string[] {"ProductCategory",
    //              "ProductModel", "Product",
    //              "Address", "Customer", "CustomerAddress",
    //              "SalesOrderHeader", "SalesOrderDetail" });

    //    // ----------------------------------------------------
    //    // Horizontal Filter: On rows. Removing rows from source
    //    // ----------------------------------------------------
    //    // Over all filter : "we Want only customer from specific city and specific postal code"
    //    // First level table : Address
    //    // Second level tables : CustomerAddress
    //    // Third level tables : Customer, SalesOrderHeader
    //    // Fourth level tables : SalesOrderDetail

    //    // Create a filter on table Address on City Washington
    //    // Optional : Sub filter on PostalCode, for testing purpose
    //    var addressFilter = new SetupFilter("Address");

    //    // For each filter, you have to provider all the input parameters
    //    // A parameter could be a parameter mapped to an existing colum : That way you don't have to specify any type, length and so on ...
    //    // We can specify if a null value can be passed as parameter value : That way ALL addresses will be fetched
    //    // A default value can be passed as well, but works only on SQL Server (MySql is a damn shity thing)
    //    addressFilter.AddParameter("City", "Address", true);

    //    // Or a parameter could be a random parameter bound to anything. In that case, you have to specify everything
    //    // (This parameter COULD BE bound to a column, like City, but for the example, we go for a custom parameter)
    //    addressFilter.AddParameter("postal", DbType.String, true, null, 20);

    //    // Then you map each parameter on wich table / column the "where" clause should be applied
    //    addressFilter.AddWhere("City", "Address", "City");
    //    addressFilter.AddWhere("PostalCode", "Address", "postal");
    //    setup.Filters.Add(addressFilter);

    //    var addressCustomerFilter = new SetupFilter("CustomerAddress");
    //    addressCustomerFilter.AddParameter("City", "Address", true);
    //    addressCustomerFilter.AddParameter("postal", DbType.String, true, null, 20);

    //    // You can join table to go from your table up (or down) to your filter table
    //    addressCustomerFilter.AddJoin(Join.Left, "Address").On("CustomerAddress", "AddressId", "Address", "AddressId");

    //    // And then add your where clauses
    //    addressCustomerFilter.AddWhere("City", "Address", "City");
    //    addressCustomerFilter.AddWhere("PostalCode", "Address", "postal");
    //    setup.Filters.Add(addressCustomerFilter);

    //    var customerFilter = new SetupFilter("Customer");
    //    customerFilter.AddParameter("City", "Address", true);
    //    customerFilter.AddParameter("postal", DbType.String, true, null, 20);
    //    customerFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
    //    customerFilter.AddJoin(Join.Left, "Address").On("CustomerAddress", "AddressId", "Address", "AddressId");
    //    customerFilter.AddWhere("City", "Address", "City");
    //    customerFilter.AddWhere("PostalCode", "Address", "postal");
    //    setup.Filters.Add(customerFilter);

    //    var orderHeaderFilter = new SetupFilter("SalesOrderHeader");
    //    orderHeaderFilter.AddParameter("City", "Address", true);
    //    orderHeaderFilter.AddParameter("postal", DbType.String, true, null, 20);
    //    orderHeaderFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
    //    orderHeaderFilter.AddJoin(Join.Left, "Address").On("CustomerAddress", "AddressId", "Address", "AddressId");
    //    orderHeaderFilter.AddWhere("City", "Address", "City");
    //    orderHeaderFilter.AddWhere("PostalCode", "Address", "postal");
    //    setup.Filters.Add(orderHeaderFilter);

    //    var orderDetailsFilter = new SetupFilter("SalesOrderDetail");
    //    orderDetailsFilter.AddParameter("City", "Address", true);
    //    orderDetailsFilter.AddParameter("postal", DbType.String, true, null, 20);
    //    orderDetailsFilter.AddJoin(Join.Left, "SalesOrderHeader").On("SalesOrderHeader", "SalesOrderID", "SalesOrderDetail", "SalesOrderID");
    //    orderDetailsFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
    //    orderDetailsFilter.AddJoin(Join.Left, "Address").On("CustomerAddress", "AddressId", "Address", "AddressId");
    //    orderDetailsFilter.AddWhere("City", "Address", "City");
    //    orderDetailsFilter.AddWhere("PostalCode", "Address", "postal");
    //    setup.Filters.Add(orderDetailsFilter);


    //    var options = new SyncOptions();

    //    // Creating an agent that will handle all the process
    //    var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

    //    // Using the Progress pattern to handle progession during the synchronization
    //    var progress = new SynchronousProgress<ProgressArgs>(s =>
    //    {
    //        Console.ForegroundColor = ConsoleColor.Green;
    //        Console.WriteLine($"{s.ProgressPercentage:p}:\t{s.Message}");
    //        Console.ResetColor();
    //    });

    //    do
    //    {
    //        // Console.Clear();
    //        Console.WriteLine("Sync Start");
    //        try
    //        {

    //            if (!agent.Parameters.Contains("City"))
    //                agent.Parameters.Add("City", "Toronto");

    //            // Because I've specified that "postal" could be null, I can set the value to DBNull.Value (and then get all postal code in Toronto city)
    //            if (!agent.Parameters.Contains("postal"))
    //                agent.Parameters.Add("postal", DBNull.Value);

    //            var s1 = await agent.SynchronizeAsync();

    //            // Write results
    //            Console.WriteLine(s1);

    //        }
    //        catch (Exception e)
    //        {
    //            Console.WriteLine(e.Message);
    //        }


    //        //Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
    //    } while (Console.ReadKey().Key != ConsoleKey.Escape);

    //    Console.WriteLine("End");
    //}

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
