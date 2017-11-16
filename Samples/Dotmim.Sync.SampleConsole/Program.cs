using Dotmim.Sync;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SampleConsole;
using Dotmim.Sync.SQLite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Net.Http.Headers;
using Newtonsoft.Json;
using System;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

class Program
{
    static void Main(string[] args)
    {



        //TestSync().GetAwaiter().GetResult();

        //TestSyncThroughKestrellAsync().GetAwaiter().GetResult();

        //TestAllAvailablesColumns().GetAwaiter().GetResult();

        //TestSyncSQLite().GetAwaiter().GetResult();

        //TestMySqlSync().GetAwaiter().GetResult();


        TestSyncThroughWebApi().GetAwaiter().GetResult();

        Console.ReadLine();

    }


    private static async Task TestSyncThroughWebApi()
    {
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.AddJsonFile("config.json", true);
        IConfiguration Configuration = configurationBuilder.Build();

        //var clientConfig = Configuration["AppConfiguration:ClientSQLiteConnectionString"];
        //var clientProvider = new SQLiteSyncProvider(clientConfig);

        var clientConfig = Configuration["AppConfiguration:ClientConnectionString"];
        var clientProvider = new SqlSyncProvider(clientConfig);

        var proxyClientProvider = new WebProxyClientProvider(new Uri("http://localhost:56782/api/values"));

        var agent = new SyncAgent(clientProvider, proxyClientProvider);

        agent.SyncProgress += SyncProgress;
        agent.ApplyChangedFailed += ApplyChangedFailed;

        Console.WriteLine("Press a key to start...");
        Console.ReadKey();
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
        SyncConfiguration configuration = new SyncConfiguration(new string[] { "ServiceTickets" });
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

        SqlSyncProvider serverProvider = new SqlSyncProvider(serverConfig);
        SQLiteSyncProvider clientProvider = new SQLiteSyncProvider(clientConfig);

        // With a config when we are in local mode (no proxy)
        SyncConfiguration configuration = new SyncConfiguration(new string[] { "Customers", "ServiceTickets" });

        SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);

        agent.SyncProgress += SyncProgress;
            //(s,e) =>
        //{
        //    switch (e.Context.SyncStage)
        //    {
        //        case SyncStage.EnsureConfiguration:
        //            break;
        //        case SyncStage.EnsureDatabase:
        //            if (e.DatabaseScript != null)
        //                e.Action = ChangeApplicationAction.Rollback;
        //            break;
        //        case SyncStage.SelectedChanges:
        //            Console.WriteLine($"Selected changes : {e.ChangesStatistics.TotalSelectedChanges}. I:{e.ChangesStatistics.TotalSelectedChangesInserts}. U:{e.ChangesStatistics.TotalSelectedChangesUpdates}. D:{e.ChangesStatistics.TotalSelectedChangesDeletes}");
        //            break;
        //        case SyncStage.ApplyingChanges:
        //            Console.WriteLine($"Going to apply changes.");
        //            e.Action = ChangeApplicationAction.Continue;
        //            break;
        //        case SyncStage.AppliedChanges:
        //            Console.WriteLine($"Applied changes : {e.ChangesStatistics.TotalAppliedChanges}");
        //            e.Action = ChangeApplicationAction.Continue;
        //            break;
        //        case SyncStage.ApplyingInserts:
        //            Console.WriteLine($"Applying Inserts : {e.ChangesStatistics.AppliedChanges.Where(ac => ac.State == DmRowState.Added).Sum(ac => ac.ChangesApplied) }");
        //            e.Action = ChangeApplicationAction.Continue;
        //            break; 
        //        case SyncStage.ApplyingDeletes:
        //            Console.WriteLine($"Applying Deletes : {e.ChangesStatistics.AppliedChanges.Where(ac => ac.State == DmRowState.Deleted).Sum(ac => ac.ChangesApplied) }");
        //            break;
        //        case SyncStage.ApplyingUpdates:
        //            Console.WriteLine($"Applying Updates : {e.ChangesStatistics.AppliedChanges.Where(ac => ac.State == DmRowState.Modified).Sum(ac => ac.ChangesApplied) }");
        //            e.Action = ChangeApplicationAction.Continue;
        //            break;
        //    }
        //};
        agent.ApplyChangedFailed += ApplyChangedFailed;

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


    //private static async Task TestMySqlSync()
    //{
    //    // Get SQL Server connection string
    //    ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
    //    configurationBuilder.AddJsonFile("config.json", true);
    //    IConfiguration Configuration = configurationBuilder.Build();
    //    var serverConfig = Configuration["AppConfiguration:MySqlServerConnectionString"];
    //    var clientConfig = Configuration["AppConfiguration:MySqlClientConnectionString"];

    //    MySqlSyncProvider serverProvider = new MySqlSyncProvider(serverConfig);
    //    MySqlSyncProvider clientProvider = new MySqlSyncProvider(clientConfig);

    //    // With a config when we are in local mode (no proxy)
    //    SyncConfiguration configuration = new SyncConfiguration(new string[] { "ServiceTickets" });


    //    //configuration.DownloadBatchSizeInKB = 500;
    //    SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);

    //    agent.SyncProgress += SyncProgress;
    //    agent.ApplyChangedFailed += ApplyChangedFailed;

    //    do
    //    {
    //        Console.Clear();
    //        Console.WriteLine("Sync Start");
    //        try
    //        {
    //            CancellationTokenSource cts = new CancellationTokenSource();
    //            CancellationToken token = cts.Token;
    //            var s = await agent.SynchronizeAsync(token);

    //        }
    //        catch (SyncException e)
    //        {
    //            Console.WriteLine(e.ToString());
    //        }
    //        catch (Exception e)
    //        {
    //            Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
    //        }


    //        Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
    //    } while (Console.ReadKey().Key != ConsoleKey.Escape);

    //    Console.WriteLine("End");
    //}

    private static async Task TestSync()
    {
        // Get SQL Server connection string
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.AddJsonFile("config.json", true);
        IConfiguration Configuration = configurationBuilder.Build();
        var serverConfig = @"Data Source=(localdb)\MSSQLLocalDB; Initial Catalog=DotminXAF; Integrated Security=true;";
        var clientConfig = @"Data Source=(localdb)\MSSQLLocalDB; Initial Catalog=DotminXAFClient; Integrated Security=true;";

        SqlSyncProvider serverProvider = new SqlSyncProvider(serverConfig);
        SqlSyncProvider clientProvider = new SqlSyncProvider(clientConfig);

        // With a config when we are in local mode (no proxy)
        SyncConfiguration configuration = new SyncConfiguration(new string[] {
        "Analysis", "Event", "FileData", "HCategory", "ModelDifference", "PermissionPolicyUser",
        "Resource", "XPObjectType", "C4File", "ModelDifferenceAspect", "PermissionPolicyRole",
        "ReportDataV2", "ResourceResources_EventEvents",
        "PermissionPolicyNavigationPermissionsObject", "PermissionPolicyTypePermissionsObject",
        "PermissionPolicyUserUsers_PermissionPolicyRoleRoles", "PermissionPolicyMemberPermissionsObject",
        "PermissionPolicyObjectPermissionsObject"});

        //var configuration = new SyncConfiguration(new string[] {
        //"Analysis", "Event", "FileData","Resource", "C4File"});


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
            case SyncStage.EnsureScopes:
                Console.WriteLine($"Ensure Scopes");
                break;
            case SyncStage.EnsureConfiguration:
                Console.WriteLine($"Ensure Configuration");
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
                break;
            case SyncStage.EnsureDatabase:
                Console.WriteLine($"Ensure Database");
                break;
            case SyncStage.SelectingChanges:
                Console.WriteLine($"Selecting changes...");
                break;
            case SyncStage.SelectedChanges:
                Console.WriteLine($"Changes selected : {e.ChangesStatistics.TotalSelectedChanges}");
                break;
            case SyncStage.ApplyingChanges:
                Console.WriteLine($"Applying changes...");
                break;
            case SyncStage.ApplyingInserts:
                Console.WriteLine($"\tApplying Inserts : {e.ChangesStatistics.AppliedChanges.Where(ac => ac.State == DmRowState.Added).Sum(ac => ac.ChangesApplied) }");
                break;
            case SyncStage.ApplyingDeletes:
                Console.WriteLine($"\tApplying Deletes : {e.ChangesStatistics.AppliedChanges.Where(ac => ac.State == DmRowState.Deleted).Sum(ac => ac.ChangesApplied) }");
                break;
            case SyncStage.ApplyingUpdates:
                Console.WriteLine($"\tApplying Updates : {e.ChangesStatistics.AppliedChanges.Where(ac => ac.State == DmRowState.Modified).Sum(ac => ac.ChangesApplied) }");
                break;
            case SyncStage.AppliedChanges:
                Console.WriteLine($"Changes applied : {e.ChangesStatistics.TotalAppliedChanges}");
                break;
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

        e.Action = ApplyAction.Continue;
        return;
        // tables name
        //string serverTableName = e.Conflict.RemoteChanges.TableName;
        //string clientTableName = e.Conflict.LocalChanges.TableName;

        //// server row in conflict
        //var dmRowServer = e.Conflict.RemoteChanges.Rows[0];
        //var dmRowClient = e.Conflict.LocalChanges.Rows[0];

        //// Example 1 : Resolution based on rows values
        //if ((int)dmRowServer["ClientID"] == 100 && (int)dmRowClient["ClientId"] == 0)
        //    e.Action = ApplyAction.Continue;
        //else
        //    e.Action = ApplyAction.RetryWithForceWrite;

        // Example 2 : resolution based on conflict type
        // Line exist on client, not on server, force to create it
        //if (e.Conflict.Type == ConflictType.RemoteInsertLocalNoRow || e.Conflict.Type == ConflictType.RemoteUpdateLocalNoRow)
        //    e.Action = ApplyAction.RetryWithForceWrite;
        //else
        //    e.Action = ApplyAction.RetryWithForceWrite;
    }
}