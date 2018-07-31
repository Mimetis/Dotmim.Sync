﻿using Dotmim.Sync;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.MySql;
using Dotmim.Sync.SampleConsole;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

class Program
{
    static void Main(string[] args)
    {
        SyncHttpThroughKestellAsync().GetAwaiter().GetResult();

        Console.ReadLine();
    }

    public static String GetDatabaseConnectionString(string dbName) =>
        $"Data Source=.\\SQLEXPRESS; Initial Catalog={dbName}; Integrated Security=true;";

    public static string GetMySqlDatabaseConnectionString(string dbName) =>
        $@"Server=127.0.0.1; Port=3306; Database={dbName}; Uid=root; Pwd=azerty31$;";

    public async static Task SyncHttpThroughKestellAsync()
    {
        // server provider
        var serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
        // proxy server based on server provider
        var proxyServerProvider = new WebProxyServerProvider(serverProvider);

        // client provider
        var client1Provider = new SqlSyncProvider(GetDatabaseConnectionString("Adv"));
        // proxy client provider 
        var proxyClientProvider = new WebProxyClientProvider();

        var tables = new string[] {"ProductCategory",
                "ProductDescription", "ProductModel",
                "Product", "ProductModelProductDescription",
                "Address", "Customer", "CustomerAddress",
                "SalesOrderHeader", "SalesOrderDetail" };

        var configuration = new SyncConfiguration(tables)
        {
            ScopeName = "AdventureWorks",
            ScopeInfoTableName = "tscopeinfo",
            SerializationFormat = SerializationFormat.Binary,
            DownloadBatchSizeInKB = 400,
            StoredProceduresPrefix = "s",
            StoredProceduresSuffix = "",
            TrackingTablesPrefix = "t",
            TrackingTablesSuffix = "",
        };


        var serverHandler = new RequestDelegate(async context =>
        {
           proxyServerProvider.Configuration = configuration;
          
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
                        CancellationTokenSource cts = new CancellationTokenSource();

                        Console.WriteLine("--------------------------------------------------");
                        Console.WriteLine("1 : Normal synchronization.");
                        Console.WriteLine("2 : Fill configuration from server side");
                        Console.WriteLine("3 : Synchronization with reinitialize");
                        Console.WriteLine("4 : Synchronization with upload and reinitialize");
                        Console.WriteLine("5 : Deprovision everything from client side (tables included)");
                        Console.WriteLine("6 : Deprovision everything from server side (tables not included)");
                        Console.WriteLine("7 : Provision everything on the client side (tables included)");
                        Console.WriteLine("8 : Provision everything on the server side (tables not included)");
                        Console.WriteLine("--------------------------------------------------");
                        Console.WriteLine("What's your choice ? ");
                        Console.WriteLine("--------------------------------------------------");
                        var choice = Console.ReadLine();
               
                        if (int.TryParse(choice, out int choiceNumber))
                        {
                            Console.WriteLine($"You choose {choice}. Start operation....");
                            switch (choiceNumber)
                            {
                                case 1:
                                    var s1 = await syncAgent.SynchronizeAsync(cts.Token);
                                    Console.WriteLine(s1);
                                    break;
                                case 2:
                                    SyncContext ctx = new SyncContext(Guid.NewGuid());
                                    SqlSyncProvider syncConfigProvider = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
                                    (ctx, configuration.Schema) = await syncConfigProvider.EnsureSchemaAsync(ctx, new Dotmim.Sync.Messages.MessageEnsureSchema
                                    {
                                        Schema = configuration.Schema,
                                        SerializationFormat = SerializationFormat.Json
                                    });
                                    break;
                                case 3:
                                    s1 = await syncAgent.SynchronizeAsync(SyncType.Reinitialize, cts.Token);
                                    Console.WriteLine(s1);
                                    break;
                                case 4:
                                    s1 = await syncAgent.SynchronizeAsync(SyncType.ReinitializeWithUpload, cts.Token);
                                    Console.WriteLine(s1);
                                    break;
                                case 5:
                                    SqlSyncProvider clientSyncProvider = syncAgent.LocalProvider as SqlSyncProvider;
                                    await clientSyncProvider.DeprovisionAsync(configuration, SyncProvision.All | SyncProvision.Table);
                                    Console.WriteLine("Deprovision complete on client");
                                    break;
                                case 6:
                                    SqlSyncProvider remoteSyncProvider = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
                                    await remoteSyncProvider.DeprovisionAsync(configuration, SyncProvision.All);
                                    Console.WriteLine("Deprovision complete on remote");
                                    break;

                                case 7:
                                    SqlSyncProvider clientSyncProvider2 = syncAgent.LocalProvider as SqlSyncProvider;
                                    await clientSyncProvider2.ProvisionAsync(configuration, SyncProvision.All | SyncProvision.Table);
                                    Console.WriteLine("Provision complete on client");
                                    break;
                                case 8:
                                    SqlSyncProvider remoteSyncProvider2 = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
                                    await remoteSyncProvider2.ProvisionAsync(configuration, SyncProvision.All);
                                    Console.WriteLine("Provision complete on remote");
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
        var clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("NW1"));

        var proxyClientProvider = new WebProxyClientProvider(
            new Uri("http://localhost:54347/api/values"));

        var agent = new SyncAgent(clientProvider, proxyClientProvider);

        Console.WriteLine("Press a key to start (be sure web api is running ...)");
        Console.ReadKey();
        do
        {
            Console.Clear();
            Console.WriteLine("Web sync start");
            try
            {
                var s = await agent.SynchronizeAsync();

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

    private static async Task TestSync()
    {
        //CreateDatabase("NW1", true);
        SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
        SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("Adv"));

        // Tables involved in the sync process:
        var tables = new string[] {"ProductCategory",
                "ProductDescription", "ProductModel",
                "Product", "ProductModelProductDescription",
                "Address", "Customer", "CustomerAddress",
                "SalesOrderHeader", "SalesOrderDetail" };

        SyncAgent agent = new SyncAgent(clientProvider, serverProvider, tables);

        agent.Configuration.StoredProceduresPrefix = "sp";
        agent.Configuration.TrackingTablesPrefix = "sync";
        agent.Configuration.ScopeInfoTableName = "syncscope";

        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                CancellationTokenSource cts = new CancellationTokenSource();
                CancellationToken token = cts.Token;

                var s1 = await agent.SynchronizeAsync(SyncType.ReinitializeWithUpload, token);

                Console.WriteLine(s1);
            }
            catch (SyncException e)
            {
                Console.WriteLine(e.ToString());
            }
            catch (Exception e)
            {
                Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
            }


            //Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }

    public static void DeleteDatabase(string dbName)
    {
        SqlConnection masterConnection = null;
        SqlCommand cmdDb = null;
        masterConnection = new SqlConnection(GetDatabaseConnectionString("master"));

        masterConnection.Open();
        cmdDb = new SqlCommand(GetDeleteDatabaseScript(dbName), masterConnection);
        cmdDb.ExecuteNonQuery();
        masterConnection.Close();
    }

    private static string GetDeleteDatabaseScript(string dbName)
    {
        return $@"if (exists (Select * from sys.databases where name = '{dbName}'))
                    begin
	                    alter database [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	                    drop database {dbName}
                    end";
    }

    private static string GetCreationDBScript(string dbName, Boolean recreateDb = true)
    {
        if (recreateDb)
            return $@"if (exists (Select * from sys.databases where name = '{dbName}'))
                    begin
	                    alter database [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	                    drop database {dbName}
                    end
                    Create database {dbName}";
        else
            return $@"if not (exists (Select * from sys.databases where name = '{dbName}')) 
                          Create database {dbName}";

    }

    public static void CreateDatabase(string dbName, bool recreateDb = true)
    {
        SqlConnection masterConnection = null;
        SqlCommand cmdDb = null;
        masterConnection = new SqlConnection(GetDatabaseConnectionString("master"));

        masterConnection.Open();
        cmdDb = new SqlCommand(GetCreationDBScript(dbName, recreateDb), masterConnection);
        cmdDb.ExecuteNonQuery();
        masterConnection.Close();
    }


}