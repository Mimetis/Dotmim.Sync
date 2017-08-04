using DmBinaryFormatter;
using Dotmim.Sync.Core;
using Dotmim.Sync.Core.Builders;
using Dotmim.Sync.Core.Enumerations;
using Dotmim.Sync.Core.Manager;
using Dotmim.Sync.Core.Proxy;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SampleConsole;
using Dotmim.Sync.SampleConsole.shared;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.SqlServer.Builders;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Server.Kestrel.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Net.Http.Headers;
using System.Data.SqlClient;
using Dotmim.Sync.SQLite;
using Dotmim.Sync.Core.Scope;
using System.Data.SQLite;

class Program
{
    static void Main(string[] args)
    {


        //TestSync().Wait();

        //TestSyncThroughKestrellAsync().Wait();

        //TestAllAvailablesColumns().Wait();

        FilterSync().Wait();

        Console.ReadLine();

    }

    private static async Task TestSQLiteSyncScopeBuilder()
    {
        var path = Path.Combine(Environment.CurrentDirectory, "db.sqlite");

        var builder = new System.Data.SQLite.SQLiteConnectionStringBuilder
        {
            DataSource = path
        };

        var sqliteConnectionString = builder.ConnectionString;

        SQLiteSyncProvider sqliteSyncProvider = new SQLiteSyncProvider(sqliteConnectionString);

        var tbl = new DmTable("ServiceTickets");
        var id = new DmColumn<Guid>("ServiceTicketID");
        tbl.Columns.Add(id);
        var key = new DmKey(new DmColumn[] { id });
        tbl.PrimaryKey = key;
        tbl.Columns.Add(new DmColumn<string>("Title"));
        tbl.Columns.Add(new DmColumn<bool>("IsAware"));
        tbl.Columns.Add(new DmColumn<string>("Description"));
        tbl.Columns.Add(new DmColumn<int>("StatusValue"));
        tbl.Columns.Add(new DmColumn<long>("EscalationLevel"));
        tbl.Columns.Add(new DmColumn<DateTime>("Opened"));
        tbl.Columns.Add(new DmColumn<DateTime>("Closed"));
        tbl.Columns.Add(new DmColumn<int>("CustomerID"));

        var dbTableBuilder = sqliteSyncProvider.GetDatabaseBuilder(tbl, DbBuilderOption.CreateOrUseExistingSchema | DbBuilderOption.CreateOrUseExistingTrackingTables);

        using (var sqliteConnection = new SQLiteConnection(sqliteConnectionString))
        {
            try
            {
                await sqliteConnection.OpenAsync();

                var script = dbTableBuilder.Script(sqliteConnection);

                dbTableBuilder.Apply(sqliteConnection);

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                throw;
            }


        }

    }


    private static async Task TestSyncWithTestServer()
    {
        var builder = new WebHostBuilder()
               .UseKestrel()
               .UseUrls("http://127.0.0.1:0/")
               .Configure(app =>
               {
                   app.UseSession();

                   app.Run(context =>
                   {
                       int? value = context.Session.GetInt32("Key");
                       if (context.Request.Path == new PathString("/first"))
                       {
                           Console.WriteLine("value.HasValue : " + value.HasValue);
                           value = 0;
                       }
                       Console.WriteLine("value.HasValue " + value.HasValue);
                       context.Session.SetInt32("Key", value.Value + 1);
                       return context.Response.WriteAsync(value.Value.ToString());

                   });
               })
               .ConfigureServices(services =>
               {
                   services.AddDistributedMemoryCache();
                   services.AddSession();
               });

        using (var server = new TestServer(builder))
        {
            var client = server.CreateClient();

            // Nothing here seems to work
            // client.BaseAddress = new Uri("http://localhost.fiddler/");

            var response = await client.GetAsync("first");
            response.EnsureSuccessStatusCode();
            Console.WriteLine("Server result : " + await response.Content.ReadAsStringAsync());

            client = server.CreateClient();
            var cookie = SetCookieHeaderValue.ParseList(response.Headers.GetValues("Set-Cookie").ToList()).First();
            client.DefaultRequestHeaders.Add("Cookie", new CookieHeaderValue(cookie.Name, cookie.Value).ToString());

            Console.WriteLine("Server result : " + await client.GetStringAsync("/"));
            Console.WriteLine("Server result : " + await client.GetStringAsync("/"));
            Console.WriteLine("Server result : " + await client.GetStringAsync("/"));

        }
    }

    /// <summary>
    /// Test syncking through Kestrell server
    /// </summary>
    private static async Task TestSyncThroughKestrellAsync()
    {
        var id = Guid.NewGuid();

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.AddJsonFile("config.json", true);
        IConfiguration Configuration = configurationBuilder.Build();
        var serverConfig = Configuration["AppConfiguration:ServerConnectionString"];
        var clientConfig = Configuration["AppConfiguration:ClientConnectionString"];

        // Server side
        var serverHandler = new RequestDelegate(async context =>
        {
            // Create the internal provider
            SqlSyncProvider serverProvider = new SqlSyncProvider(serverConfig);
            // Create the configuration stuff
            ServiceConfiguration configuration = new ServiceConfiguration(new string[] { "ServiceTickets" });
            configuration.DownloadBatchSizeInKB = 500;
            serverProvider.SetConfiguration(configuration);

            // Create the proxy provider
            WebProxyServerProvider proxyServerProvider = new WebProxyServerProvider(serverProvider, SerializationFormat.Json);

            serverProvider.SyncProgress += ServerProvider_SyncProgress;

            try
            {
                CancellationTokenSource cts = new CancellationTokenSource();
                //   cts.CancelAfter(60000);
                CancellationToken token = cts.Token;
                await proxyServerProvider.HandleRequestAsync(context, token);

            }
            catch (WebSyncException webSyncException)
            {
                Console.WriteLine("Proxy Server WebSyncException : " + webSyncException.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("Proxy Server Exception : " + e.Message);
                throw e;
            }
        });

        var clientHandler = new ResponseDelegate(async (serviceUri) =>
        {
            var proxyProvider = new WebProxyClientProvider(new Uri(serviceUri), SerializationFormat.Json);
            var clientProvider = new SqlSyncProvider(clientConfig);

            SyncAgent agent = new SyncAgent(clientProvider, proxyProvider);

            agent.SyncProgress += SyncProgress;
            do
            {
                try
                {
                    CancellationTokenSource cts = new CancellationTokenSource();
                    //cts.CancelAfter(1000);
                    CancellationToken token = cts.Token;
                    var s = await agent.SynchronizeAsync(token);

                }
                catch (WebSyncException webSyncException)
                {
                    Console.WriteLine("Proxy Client WebSyncException : " + webSyncException.Message);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Proxy Client Exception : " + e.Message);
                }

                Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
            } while (Console.ReadKey().Key != ConsoleKey.Escape);



        });


        await TestKestrelHttpServer.LaunchKestrellAsync(serverHandler, clientHandler);
    }

    private static async Task FilterSync()
    {
        // Get SQL Server connection string
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.AddJsonFile("config.json", true);
        IConfiguration Configuration = configurationBuilder.Build();
        var serverConfig = Configuration["AppConfiguration:ServerFilteredConnectionString"];
        var clientConfig = "sqlitefiltereddb.db";

        SqlSyncProvider serverProvider = new SqlSyncProvider(serverConfig);
        SQLiteSyncProvider clientProvider = new SQLiteSyncProvider(clientConfig);

        // With a config when we are in local mode (no proxy)
        ServiceConfiguration configuration = new ServiceConfiguration(new string[] { "ServiceTickets" });
        //configuration.DownloadBatchSizeInKB = 500;
        configuration.UseBulkOperations = false;
        // Adding filters on schema
        configuration.Filters.Add("ServiceTickets", "CustomerID");
       
        SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);

        // Adding a parameter for this agent
        agent.Parameters.Add("ServiceTickets", "CustomerID", 1);

        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                var s = await agent.SynchronizeAsync();

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

        Console.WriteLine("End");
    }


    private static async Task TestSyncSQLite()
    {
        // Get SQL Server connection string
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.AddJsonFile("config.json", true);
        IConfiguration Configuration = configurationBuilder.Build();
        var serverConfig = Configuration["AppConfiguration:ServerConnectionString"];
        var clientConfig = Configuration["AppConfiguration:ClientSQLiteConnectionString"];
        var clientConfig2 = Configuration["AppConfiguration:ClientSQLiteConnectionString2"];
        var clientConfig3 = Configuration["AppConfiguration:ClientConnectionString"];

        SqlSyncProvider serverProvider = new SqlSyncProvider(serverConfig);
        SQLiteSyncProvider clientProvider = new SQLiteSyncProvider(clientConfig);
        SQLiteSyncProvider clientProvider2 = new SQLiteSyncProvider(clientConfig2);
        SqlSyncProvider clientProvider3 = new SqlSyncProvider(clientConfig3);

        // With a config when we are in local mode (no proxy)
        ServiceConfiguration configuration = new ServiceConfiguration(new string[] { "ServiceTickets" });
        //configuration.DownloadBatchSizeInKB = 500;
        configuration.UseBulkOperations = false;

        SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);
        SyncAgent agent2 = new SyncAgent(clientProvider2, serverProvider, configuration);
        SyncAgent agent3 = new SyncAgent(clientProvider3, serverProvider, configuration);

        agent.SyncProgress += SyncProgress;
        agent2.SyncProgress += SyncProgress;
        agent3.SyncProgress += SyncProgress;
        // agent.ApplyChangedFailed += ApplyChangedFailed;

        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                var s = await agent.SynchronizeAsync();
                var s2 = await agent2.SynchronizeAsync();
                var s3 = await agent3.SynchronizeAsync();

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

        Console.WriteLine("End");
    }


    private static async Task TestSync()
    {
        // Get SQL Server connection string
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.AddJsonFile("config.json", true);
        IConfiguration Configuration = configurationBuilder.Build();
        var serverConfig = Configuration["AppConfiguration:ServerConnectionString"];
        var clientConfig = Configuration["AppConfiguration:ClientConnectionString"];


        Guid id = Guid.NewGuid();

        using (var sqlConnection = new SqlConnection(clientConfig))
        {
            var script = $@"INSERT [ServiceTickets] 
                            ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                            VALUES 
                            (N'{id.ToString()}', N'Conflict Line Client', N'Description client', 1, 0, getdate(), NULL, 1)";

            using (var sqlCmd = new SqlCommand(script, sqlConnection))
            {
                sqlConnection.Open();
                sqlCmd.ExecuteNonQuery();
                sqlConnection.Close();
            }
        }

        using (var sqlConnection = new SqlConnection(serverConfig))
        {
            var script = $@"INSERT [ServiceTickets] 
                            ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                            VALUES 
                            (N'{id.ToString()}', N'Conflict Line Server', N'Description client', 1, 0, getdate(), NULL, 1)";

            using (var sqlCmd = new SqlCommand(script, sqlConnection))
            {
                sqlConnection.Open();
                sqlCmd.ExecuteNonQuery();
                sqlConnection.Close();
            }
        }


        SqlSyncProvider serverProvider = new SqlSyncProvider(serverConfig);
        SqlSyncProvider clientProvider = new SqlSyncProvider(clientConfig);

        // With a config when we are in local mode (no proxy)
        ServiceConfiguration configuration = new ServiceConfiguration(new string[] { "ServiceTickets" });
        //configuration.DownloadBatchSizeInKB = 500;
        SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);

        agent.SyncProgress += SyncProgress;
        agent.ApplyChangedFailed += ApplyChangedFailed;

        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                CancellationTokenSource cts = new CancellationTokenSource();
                CancellationToken token = cts.Token;
                var s = await agent.SynchronizeAsync(token);

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

        Console.WriteLine("End");
    }

    private static void ServerProvider_SyncProgress(object sender, SyncProgressEventArgs e)
    {
        SyncProgress(e, ConsoleColor.Red);
    }

    private static void SyncProgress(object sender, SyncProgressEventArgs e)
    {
        SyncProgress(e);
    }

    private static void SyncProgress(SyncProgressEventArgs e, ConsoleColor? consoleColor = null)
    {
        var sessionId = e.Context.SessionId.ToString();

        if (consoleColor.HasValue)
            Console.ForegroundColor = consoleColor.Value;

        switch (e.Context.SyncStage)
        {
            case SyncStage.BeginSession:
                Console.WriteLine($"Begin Session.");
                break;
            case SyncStage.EndSession:
                Console.WriteLine($"End Session.");
                break;
            case SyncStage.EnsureMetadata:
                if (e.Configuration != null)
                {
                    var ds = e.Configuration.ScopeSet;

                    Console.WriteLine($"Configuration readed. {ds.Tables.Count} table(s) involved.");

                    Func<JsonSerializerSettings> settings = new Func<JsonSerializerSettings>(() =>
                    {
                        var s = new JsonSerializerSettings();
                        s.Formatting = Formatting.Indented;
                        s.StringEscapeHandling = StringEscapeHandling.Default;
                        return s;
                    });
                    JsonConvert.DefaultSettings = settings;
                    var dsString = JsonConvert.SerializeObject(new DmSetSurrogate(ds));

                    //Console.WriteLine(dsString);
                }
                if (e.DatabaseScript != null)
                {
                    Console.WriteLine($"Database is created");
                    //Console.WriteLine(e.DatabaseScript);
                }
                break;
            case SyncStage.SelectedChanges:
                Console.WriteLine($"Selected changes : {e.ChangesStatistics.TotalSelectedChanges}");

                //Console.WriteLine($"{sessionId}. Selected added Changes : {e.ChangesStatistics.TotalSelectedChangesInserts}");
                //Console.WriteLine($"{sessionId}. Selected updates Changes : {e.ChangesStatistics.TotalSelectedChangesUpdates}");
                //Console.WriteLine($"{sessionId}. Selected deleted Changes : {e.ChangesStatistics.TotalSelectedChangesDeletes}");
                break;

            case SyncStage.AppliedChanges:
                Console.WriteLine($"Applied changes : {e.ChangesStatistics.TotalAppliedChanges}");
                break;
            //case SyncStage.ApplyingInserts:
            //    Console.WriteLine($"{sessionId}. Applying Inserts : {e.ChangesStatistics.AppliedChanges.Where(ac => ac.State == DmRowState.Added).Sum(ac => ac.ChangesApplied) }");
            //    break;
            //case SyncStage.ApplyingDeletes:
            //    Console.WriteLine($"{sessionId}. Applying Deletes : {e.ChangesStatistics.AppliedChanges.Where(ac => ac.State == DmRowState.Deleted).Sum(ac => ac.ChangesApplied) }");
            //    break;
            //case SyncStage.ApplyingUpdates:
            //    Console.WriteLine($"{sessionId}. Applying Updates : {e.ChangesStatistics.AppliedChanges.Where(ac => ac.State == DmRowState.Modified).Sum(ac => ac.ChangesApplied) }");
            //    break;
            case SyncStage.WriteMetadata:
                if (e.Scopes != null)
                {
                    Console.WriteLine($"Writing Scopes : ");
                    e.Scopes.ForEach(sc => Console.WriteLine($"\t{sc.Id} synced at {sc.LastSync}. "));
                }
                break;
            case SyncStage.CleanupMetadata:
                Console.WriteLine($"CleanupMetadata");
                break;
        }

        Console.ResetColor();
    }

    static void ApplyChangedFailed(object sender, ApplyChangeFailedEventArgs e)
    {
        // Note: LocalChange table name may be null if the record does not exist on the server. So use the remote table name.
        string tableName = e.Conflict.RemoteChanges.TableName;

        // Line exist on client, not on server, force to create it
        if (e.Conflict.Type == ConflictType.RemoteInsertLocalNoRow || e.Conflict.Type == ConflictType.RemoteUpdateLocalNoRow)
            e.Action = ApplyAction.RetryWithForceWrite;
        else
            e.Action = ApplyAction.RetryWithForceWrite;

    }
}