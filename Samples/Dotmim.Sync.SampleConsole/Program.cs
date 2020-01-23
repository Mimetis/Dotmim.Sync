using Dotmim.Sync;

using Dotmim.Sync.Enumerations;
using Dotmim.Sync.MySql;
using Dotmim.Sync.SampleConsole;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using MessagePack;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Http2.HPack;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration.EnvironmentVariables;
using Newtonsoft.Json;
using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using System.Threading.Tasks;


internal class Program
{
    public static string serverDbName = "AdventureWorks";
    public static string serverProductCategoryDbName = "AdventureWorksProductCategory";
    public static string clientDbName = "Client";
    public static string[] allTables = new string[] {"ProductCategory",
                                                    "ProductModel", "Product",
                                                    "Address", "Customer", "CustomerAddress",
                                                    "SalesOrderHeader", "SalesOrderDetail" };

    public static string[] oneTable = new string[] { "ProductCategory" };
    private static void Main(string[] args)
    {
        SyncHttpThroughKestellAsync().GetAwaiter().GetResult();
    }

    private static void TestSqliteDoubleStatement()
    {
        var clientProvider = new SqliteSyncProvider(@"C:\PROJECTS\DOTMIM.SYNC\Tests\Dotmim.Sync.Tests\bin\Debug\netcoreapp2.0\st_r55jmmolvwg.db");
        var clientConnection = new SqliteConnection(clientProvider.ConnectionString);

        var commandText = "Update ProductCategory Set Name=@Name Where ProductCategoryId=@Id; " +
                          "Select * from ProductCategory Where ProductCategoryId=@Id;";

        using (DbCommand command = clientConnection.CreateCommand())
        {
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
            p.Value = "Awesom Bike";
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

    }
    private static async Task TestDeleteWithoutBulkAsync()
    {
        var cs = DbHelper.GetDatabaseConnectionString(serverProductCategoryDbName);
        var cc = DbHelper.GetDatabaseConnectionString(clientDbName);

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
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });

        var remoteProgress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
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
    private static int InsertOneProductCategoryId(IDbConnection c, string updatedName)
    {
        using (var command = c.CreateCommand())
        {
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
    }
    private static void UpdateOneProductCategoryId(IDbConnection c, int productCategoryId, string updatedName)
    {
        using (var command = c.CreateCommand())
        {
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
    }
    private static void DeleteOneLine(DbConnection c, int productCategoryId)
    {
        using (var command = c.CreateCommand())
        {
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
    }

    private static async Task SynchronizeWithSyncAgent2Async()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString("AdventureWorks"));
        var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString("Client"));

        // Tables involved in the sync process:
        var tables = new string[] { "ProductCategory", "ProductModel", "Product" };

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, tables);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });

        var remoteProgress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });
        agent.AddRemoteProgress(remoteProgress);

        // Setting configuration options
        agent.Schema.StoredProceduresPrefix = "s";
        agent.Schema.StoredProceduresSuffix = "";
        agent.Schema.TrackingTablesPrefix = "t";
        agent.Schema.TrackingTablesSuffix = "";

        agent.Options.ScopeInfoTableName = "tscopeinfo";
        agent.Options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "sync");
        agent.Options.BatchSize = 100;
        agent.Options.CleanMetadatas = true;
        agent.Options.UseBulkOperations = true;
        agent.Options.UseVerboseErrors = false;

        agent.LocalOrchestrator.OnTransactionOpen(to =>
        {
            var dt = DateTime.Now;
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Transaction Opened\t {dt.ToLongTimeString()}.{dt.Millisecond}");
            Console.ResetColor();
        });
        agent.LocalOrchestrator.OnTransactionCommit(to =>
        {
            var dt = DateTime.Now;
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Transaction Commited\t {dt.ToLongTimeString()}.{dt.Millisecond}");
            Console.ResetColor();
        });


        agent.RemoteOrchestrator.OnTransactionOpen(to =>
        {
            var dt = DateTime.Now;
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"Transaction Opened\t {dt.ToLongTimeString()}.{dt.Millisecond}");
            Console.ResetColor();
        });
        agent.RemoteOrchestrator.OnTransactionCommit(to =>
        {
            var dt = DateTime.Now;
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"Transaction Commited\t {dt.ToLongTimeString()}.{dt.Millisecond}");
            Console.ResetColor();
        });

        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                // Launch the sync process
                var s1 = await agent.SynchronizeAsync(SyncType.Normal, CancellationToken.None, progress);

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

    private async static Task RunAsync()
    {
        // Create databases 
        await DbHelper.EnsureDatabasesAsync(serverDbName);
        await DbHelper.CreateDatabaseAsync(clientDbName);

        // Launch Sync
        await SyncHttpThroughKestellAsync();
    }


    //private static async Task SyncAdvAsync()
    //{
        //// Sql Server provider, the master.
        //var serverProvider = new SqlSyncProvider(
        //    @"Data Source=.;Initial Catalog=AdventureWorks;User Id=sa;Password=Password12!;");

        //// Sqlite Client provider for a Sql Server <=> Sqlite sync
        //var clientProvider = new SqliteSyncProvider("advworks2.db");

        //// Tables involved in the sync process:
        //var tables = new string[] {"ProductCategory",
        //        "ProductDescription", "ProductModel",
        //        "Product", "ProductModelProductDescription",
        //        "Address", "Customer", "CustomerAddress",
        //        "SalesOrderHeader", "SalesOrderDetail" };

        //// Sync orchestrator
        //var agent = new SyncAgent(clientProvider, serverProvider, tables);


        //do
        //{
        //    var s = await agent.SynchronizeAsync();
        //    Console.WriteLine($"Total Changes downloaded : {s.TotalChangesDownloaded}");

        //} while (Console.ReadKey().Key != ConsoleKey.Escape);
   // }


    /// <summary>
    /// Launch a simple sync, over TCP network, each sql server (client and server are reachable through TCP cp
    /// </summary>
    /// <returns></returns>
    private static async Task SynchronizeAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(clientDbName));
        //var clientProvider = new SqliteSyncProvider("client.db");

        // specific Setup with only 2 tables, and one filtered
        var setup = new SyncSetup(new[] { "ProductCategory" });
       
        // Add a table with less columns
        //setup.Tables.Add("Product")
        //    .Columns.AddRange(new string[] { "ProductId", "Name", "ProductCategoryID", "ProductNumber", "StandardCost", "ListPrice", "SellStartDate", "rowguid", "ModifiedDate" });

        //// Add filters
        //setup.Filters.Add("ProductCategory", "ParentProductCategoryID")
        //             .Add("ProductCategory", "Name");

        // Add pref suf
        setup.StoredProceduresPrefix = "s";
        setup.StoredProceduresSuffix = "";
        setup.TrackingTablesPrefix = "t";
        setup.TrackingTablesSuffix = "";

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, setup);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });

        var remoteProgress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });

        agent.AddRemoteProgress(remoteProgress);

        //agent.Options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "sync");
        agent.Options.BatchSize = 1000;
        //agent.Options.CleanMetadatas = true;
        agent.Options.UseBulkOperations = false;
        agent.Options.DisableConstraintsOnApplyChanges = false;
        agent.Options.ConflictResolutionPolicy = ConflictResolutionPolicy.ServerWins;
        //agent.Options.UseVerboseErrors = false;
        //agent.Options.ScopeInfoTableName = "tscopeinfo";


        //agent.OnApplyChangesFailed(acf =>
        //{
        //    // Check conflict is correctly set
        //    var localRow = acf.Conflict.LocalRow;
        //    var remoteRow = acf.Conflict.RemoteRow;

        //    // Merge row
        //    acf.Resolution = ConflictResolution.MergeRow;

        //    acf.FinalRow["Name"] = "Prout";

        //});

        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                // Launch the sync process
                //agent.Parameters.Add("ProductCategory", "ParentProductCategoryID", "", 1);
                //agent.Parameters.Add("ProductCategory", "Name", "", "Touring Bikes");

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
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(clientDbName));

        // Tables involved in the sync process:
        // var tables = allTables;
        //var tables = new string[] { "SalesLT.Product", "SalesLT.ProductCategory" };

        // ----------------------------------
        // Client side
        // ----------------------------------
        var clientOptions = new SyncOptions
        {
            ScopeInfoTableName = "client_scopeinfo",
            BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "sync_client"),
            BatchSize = 10,
            CleanMetadatas = true,
            UseBulkOperations = true,
            UseVerboseErrors = false,
        };

        // Create the web proxy client provider with specific options
        var proxyClientProvider = new WebClientOrchestrator
        {
            SerializerFactory = new CustomMessagePackSerializerFactory(),
            Converter = new CustomConverter()
        };


        // ----------------------------------
        // Web Server side
        // ----------------------------------
        // specific Setup with only 2 tables, and one filtered
        var setup = new SyncSetup(new string[] { "Product" });
        //setup.Tables["Product"].Columns.AddRange(new string[] { "ProductId", "Name", "ProductCategoryID", "ProductNumber", "StandardCost", "ListPrice", "SellStartDate", "rowguid", "ModifiedDate" });

        // Add pref suf for v0.4
        setup.StoredProceduresPrefix = "s";
        setup.StoredProceduresSuffix = "";
        setup.TrackingTablesPrefix = "t";
        setup.TrackingTablesSuffix = "";
        setup.TriggersPrefix = "";
        setup.TriggersSuffix = "";

        var webServerOptions = new WebServerOptions
        {
            BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "sync_server"),
            CleanMetadatas = true,
            UseBulkOperations = true,
            UseVerboseErrors = false
        };
        webServerOptions.Serializers.Add(new CustomMessagePackSerializerFactory());
        webServerOptions.Converters.Add(new CustomConverter());

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, proxyClientProvider);
        agent.Options = clientOptions;


        var serverHandler = new RequestDelegate(async context =>
        {
            var proxyServerProvider = WebProxyServerOrchestrator.Create(context, serverProvider, setup, webServerOptions);

            var webServerOrchestrator = proxyServerProvider.GetLocalOrchestrator(context);

            webServerOrchestrator.OnDatabaseChangesApplying(dca =>
            {
                Console.WriteLine("On db changes applying. Press enter");
                Console.ReadLine();
            });

            await proxyServerProvider.HandleRequestAsync(context);
        });
        using (var server = new KestrellTestServer())
        {
            var clientHandler = new ResponseDelegate(async (serviceUri) =>
            {
                proxyClientProvider.ServiceUri = serviceUri;
                Console.Clear();
                Console.WriteLine("Sync Start");
                var s1 = await agent.SynchronizeAsync();
                Console.WriteLine(s1);
                Console.WriteLine("--------------------------------------------------");


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

        var proxyClientProvider = new WebClientOrchestrator("http://localhost:52288/api/Sync");

        // ----------------------------------
        // Client side
        // ----------------------------------
        var clientOptions = new SyncOptions
        {
            ScopeInfoTableName = "client_scopeinfo",
            BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "sync_client"),
            BatchSize = 50,
            CleanMetadatas = true,
            UseBulkOperations = true,
            UseVerboseErrors = false,
        };

        var clientSetup = new SyncSetup
        {
            StoredProceduresPrefix = "cli",
            StoredProceduresSuffix = "",
            TrackingTablesPrefix = "cli",
            TrackingTablesSuffix = "",
            TriggersPrefix = "",
            TriggersSuffix = "",
        };


        var agent = new SyncAgent(clientProvider, proxyClientProvider, clientSetup, clientOptions);

        agent.LocalOrchestrator.OnDatabaseChangesApplied(dcaa =>
        {
            Console.WriteLine("DCAA");
            Console.ReadLine();
        });

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

