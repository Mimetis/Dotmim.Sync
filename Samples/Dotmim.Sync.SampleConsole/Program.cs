using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SampleConsole;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using SerializationFormat = Dotmim.Sync.Enumerations.SerializationFormat;

internal class Program
{
    public static string serverDbName = "Server";
    public static string clientDbName = "Client";
    public static string[] allTables = new string[] {"ProductCategory",
                                                    "ProductModel", "Product",
                                                    "Address", "Customer", "CustomerAddress",
                                                    "SalesOrderHeader", "SalesOrderDetail" };
    private static void Main(string[] args)
    {


        TrySyncAzureSqlDbAsync().GetAwaiter().GetResult();
        Console.ReadLine();
    }

    private async static Task RunAsync()
    {
        // Create databases 
        await DbHelper.EnsureDatabasesAsync(serverDbName);
        await DbHelper.CreateDatabaseAsync(clientDbName);

        // Launch Sync
        await SynchronizeAsync();
    }


    private static async Task TrySyncAzureSqlDbAsync()
    {
        // Sql Server provider, the master.
        var serverProvider = new SqlSyncProvider(
            @"Data Source=sebpertus.database.windows.net;Initial Catalog=AdventureWorks;User Id=YOUR_ID;Password=YOUR_PASSWORD;");

        // Sqlite Client provider for a Sql Server <=> Sqlite sync
        var clientProvider = new SqliteSyncProvider("advfromazure.db");

        // Tables involved in the sync process:
        var tables = new string[] { "Address" };

        // Sync orchestrator
        var agent = new SyncAgent(clientProvider, serverProvider, tables);

        do
        {
            var s = await agent.SynchronizeAsync();
            Console.WriteLine($"Total Changes downloaded : {s.TotalChangesDownloaded}");

        } while (Console.ReadKey().Key != ConsoleKey.Escape);
    }

    private static async Task SyncAdvAsync()
    {
        // Sql Server provider, the master.
        var serverProvider = new SqlSyncProvider(
            @"Data Source=.;Initial Catalog=AdventureWorks;User Id=sa;Password=Password12!;");

        // Sqlite Client provider for a Sql Server <=> Sqlite sync
        var clientProvider = new SqliteSyncProvider("advworks2.db");

        // Tables involved in the sync process:
        var tables = new string[] {"ProductCategory",
                "ProductDescription", "ProductModel",
                "Product", "ProductModelProductDescription",
                "Address", "Customer", "CustomerAddress",
                "SalesOrderHeader", "SalesOrderDetail" };

        // Sync orchestrator
        var agent = new SyncAgent(clientProvider, serverProvider, tables);


        do
        {
            var s = await agent.SynchronizeAsync();
            Console.WriteLine($"Total Changes downloaded : {s.TotalChangesDownloaded}");

        } while (Console.ReadKey().Key != ConsoleKey.Escape);
    }


    /// <summary>
    /// Launch a simple sync, over TCP network, each sql server (client and server are reachable through TCP cp
    /// </summary>
    /// <returns></returns>
    private static async Task SynchronizeAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(clientDbName));

        // Tables involved in the sync process:
        var tables = allTables;

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, tables);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new Progress<ProgressArgs>(s => Console.WriteLine($"[client]: {s.Context.SyncStage}:\t{s.Message}"));


        // Setting configuration options
        agent.SetConfiguration(s =>
        {
            s.ScopeInfoTableName = "tscopeinfo";
            s.SerializationFormat = Dotmim.Sync.Enumerations.SerializationFormat.Binary;
            s.StoredProceduresPrefix = "s";
            s.StoredProceduresSuffix = "";
            s.TrackingTablesPrefix = "t";
            s.TrackingTablesSuffix = "";
        });

        agent.SetOptions(opt =>
        {
            opt.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "sync");
            opt.BatchSize = 100;
            opt.CleanMetadatas = true;
            opt.UseBulkOperations = true;
            opt.UseVerboseErrors = false;
        });


        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                // Launch the sync process
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

    /// <summary>
    /// Launch a simple sync, over TCP network, each sql server (client and server are reachable through TCP cp
    /// </summary>
    /// <returns></returns>
    private static async Task SynchronizeExistingTablesAsync()
    {
        string serverName = "ServerTablesExist";
        string clientName = "ClientsTablesExist";

        await DbHelper.EnsureDatabasesAsync(serverName);
        await DbHelper.EnsureDatabasesAsync(clientName);

        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(serverName));
        var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(clientName));

        // Tables involved in the sync process:
        var tables = allTables;

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, tables);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new Progress<ProgressArgs>(s => Console.WriteLine($"[client]: {s.Context.SyncStage}:\t{s.Message}"));




        // Setting configuration options
        agent.SetConfiguration(s =>
        {
            s.ScopeInfoTableName = "tscopeinfo";
            s.SerializationFormat = Dotmim.Sync.Enumerations.SerializationFormat.Binary;
            s.StoredProceduresPrefix = "s";
            s.StoredProceduresSuffix = "";
            s.TrackingTablesPrefix = "t";
            s.TrackingTablesSuffix = "";
        });

        agent.SetOptions(opt =>
        {
            opt.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "sync");
            opt.BatchSize = 100;
            opt.CleanMetadatas = true;
            opt.UseBulkOperations = true;
            opt.UseVerboseErrors = false;
        });


        var remoteProvider = agent.RemoteProvider as CoreProvider;

        var dpAction = new Action<DatabaseProvisionedArgs>(args =>
        {
            Console.WriteLine($"-- [InterceptDatabaseProvisioned] -- ");

            var sql = $"Update tscopeinfo set scope_last_sync_timestamp = 0 where [scope_is_local] = 1";

            var cmd = args.Connection.CreateCommand();
            cmd.Transaction = args.Transaction;
            cmd.CommandText = sql;

            cmd.ExecuteNonQuery();

        });

        remoteProvider.OnDatabaseProvisioned(dpAction);

        agent.LocalProvider.OnDatabaseProvisioned(dpAction);

        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                // Launch the sync process
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



    private static async Task SynchronizeOSAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString("OptionsServer"));
        var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString("OptionsClient"));

        // Tables involved in the sync process:
        var tables = new string[] { "ObjectSettings", "ObjectSettingValues" };

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, tables);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new Progress<ProgressArgs>(s => Console.WriteLine($"[client]: {s.Context.SyncStage}:\t{s.Message}"));

        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                // Launch the sync process
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




    public static async Task SyncHttpThroughKestellAsync()
    {
        // server provider
        var serverProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(serverDbName));

        // client provider
        var client1Provider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(clientDbName));
        // proxy client provider 
        var proxyClientProvider = new WebProxyClientProvider();

        var tables = new string[] {"ProductCategory",
                "ProductDescription", "ProductModel",
                "Product", "ProductModelProductDescription",
                "Address", "Customer", "CustomerAddress",
                "SalesOrderHeader", "SalesOrderDetail" };

        var configuration = new Action<SyncConfiguration>(conf =>
        {
            conf.ScopeName = "AdventureWorks";
            conf.ScopeInfoTableName = "tscopeinfo";
            conf.SerializationFormat = Dotmim.Sync.Enumerations.SerializationFormat.Binary;
            conf.StoredProceduresPrefix = "s";
            conf.StoredProceduresSuffix = "";
            conf.TrackingTablesPrefix = "t";
            conf.TrackingTablesSuffix = "";
            conf.Add(tables);
        });


        var optionsClient = new Action<SyncOptions>(opt =>
        {
            opt.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "client");
            opt.BatchSize = 100;
            opt.CleanMetadatas = true;
            opt.UseBulkOperations = true;
            opt.UseVerboseErrors = false;

        });

        var optionsServer = new Action<SyncOptions>(opt =>
        {
            opt.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "server");
            opt.BatchSize = 100;
            opt.CleanMetadatas = true;
            opt.UseBulkOperations = true;
            opt.UseVerboseErrors = false;

        });



        var serverHandler = new RequestDelegate(async context =>
        {
            var proxyServerProvider = WebProxyServerProvider.Create(context, serverProvider, configuration, optionsServer);

            await proxyServerProvider.HandleRequestAsync(context);
        });
        using (var server = new KestrellTestServer())
        {
            var clientHandler = new ResponseDelegate(async (serviceUri) =>
            {
                proxyClientProvider.ServiceUri = new Uri(serviceUri);

                var syncAgent = new SyncAgent(client1Provider, proxyClientProvider);

                do
                {
                    Console.Clear();
                    Console.WriteLine("Sync Start");
                    try
                    {
                        var cts = new CancellationTokenSource();

                        Console.WriteLine("--------------------------------------------------");
                        Console.WriteLine("1 : Normal synchronization.");
                        Console.WriteLine("2 : Synchronization with reinitialize");
                        Console.WriteLine("3 : Synchronization with upload and reinitialize");
                        Console.WriteLine("--------------------------------------------------");
                        Console.WriteLine("What's your choice ? ");
                        Console.WriteLine("--------------------------------------------------");
                        var choice = Console.ReadLine();

                        if (int.TryParse(choice, out var choiceNumber))
                        {
                            Console.WriteLine($"You choose {choice}. Start operation....");
                            switch (choiceNumber)
                            {
                                case 1:
                                    var s1 = await syncAgent.SynchronizeAsync(cts.Token);
                                    Console.WriteLine(s1);
                                    break;
                                case 2:
                                    s1 = await syncAgent.SynchronizeAsync(SyncType.Reinitialize, cts.Token);
                                    Console.WriteLine(s1);
                                    break;
                                case 3:
                                    s1 = await syncAgent.SynchronizeAsync(SyncType.ReinitializeWithUpload, cts.Token);
                                    Console.WriteLine(s1);
                                    break;

                                default:
                                    break;

                            }
                        }
                    }
                    catch (SyncException e)
                    {
                        Console.WriteLine(e.ToString());
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
                    }


                    Console.WriteLine("--------------------------------------------------");
                    Console.WriteLine("Press a key to choose again, or Escapte to end");

                } while (Console.ReadKey().Key != ConsoleKey.Escape);


            });
            await server.Run(serverHandler, clientHandler);
        }

    }

    /// <summary>
    /// Test a client syncing through a web api
    /// </summary>
    private static async Task TestSyncThroughWebApi()
    {
        var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(clientDbName));

        var proxyClientProvider = new WebProxyClientProvider(
            new Uri("http://localhost:52288/api/Sync"));

        var agent = new SyncAgent(clientProvider, proxyClientProvider);

        Console.WriteLine("Press a key to start (be sure web api is running ...)");
        Console.ReadKey();
        do
        {
            Console.Clear();
            Console.WriteLine("Web sync start");
            try
            {
                var progress = new Progress<ProgressArgs>(pa => Console.WriteLine($"{pa.Context.SessionId} - {pa.Context.SyncStage}\t {pa.Message}"));

                var s = await agent.SynchronizeAsync(progress);

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

        Console.WriteLine("End");

    }



}