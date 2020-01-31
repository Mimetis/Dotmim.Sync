using Dotmim.Sync;

using Dotmim.Sync.Enumerations;
using Dotmim.Sync.MySql;
using Dotmim.Sync.SampleConsole;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using MessagePack;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Http2.HPack;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration.EnvironmentVariables;
using Microsoft.Extensions.DependencyInjection;
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
    private static async Task Main(string[] args)
    {
        await SynchronizeAsync();
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
        var serverProvider = new SqlSyncChangeTrackingProvider(DbHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(clientDbName));
        //var clientProvider = new SqliteSyncProvider("client2.db");

        // specific Setup with only 2 tables, and one filtered
        var setup = new SyncSetup(allTables);
        

        // ----------------------------------------------------
        // Vertical Filter: On columns. Removing columns from source
        // ----------------------------------------------------

        // Add a table with less columns
        setup.Tables["Product"]
            .Columns.AddRange(new string[] { "ProductId", "Name", "ProductCategoryID", "ProductNumber", "StandardCost", "ListPrice", "SellStartDate", "rowguid", "ModifiedDate" });

        // ----------------------------------------------------
        // Horizontal Filter: On rows. Removing rows from source
        // ----------------------------------------------------
        // Over all filter : "we Want only customer from specific city and specific postal code"
        // First level table : Address
        // Second level tables : CustomerAddress
        // Third level tables : Customer, SalesOrderHeader
        // Fourth level tables : SalesOrderDetail

        // Create a filter on table Address on City Washington
        // Optional : Sub filter on PostalCode, for testing purpose
        var addressFilter = new SetupFilter("Address");

        // For each filter, you have to provider all the input parameters
        // A parameter could be a parameter mapped to an existing colum : That way you don't have to specify any type, length and so on ...
        // We can specify if a null value can be passed as parameter value : That way ALL addresses will be fetched
        // A default value can be passed as well, but works only on SQL Server (MySql is a damn shity thing)
        addressFilter.AddParameter("City", "Address", true);

        // Or a parameter could be a random parameter bound to anything. In that case, you have to specify everything
        // (This parameter COULD BE bound to a column, like City, but for the example, we go for a custom parameter)
        addressFilter.AddParameter("postal", DbType.String, true, null, 20);

        // Then you map each parameter on wich table / column the "where" clause should be applied
        addressFilter.AddWhere("City", "Address", "City");
        addressFilter.AddWhere("PostalCode", "Address", "postal");
        setup.Filters.Add(addressFilter);

        var addressCustomerFilter = new SetupFilter("CustomerAddress");
        addressCustomerFilter.AddParameter("City", "Address", true);
        addressCustomerFilter.AddParameter("postal", DbType.String, true, null, 20);

        // You can join table to go from your table up (or down) to your filter table
        addressCustomerFilter.AddJoin(Join.Left, "Address").On("CustomerAddress", "AddressId", "Address", "AddressId");

        // And then add your where clauses
        addressCustomerFilter.AddWhere("City", "Address", "City");
        addressCustomerFilter.AddWhere("PostalCode", "Address", "postal");
        
        setup.Filters.Add(addressCustomerFilter);

        var customerFilter = new SetupFilter("Customer");
        customerFilter.AddParameter("City", "Address", true);
        customerFilter.AddParameter("postal", DbType.String, true, null, 20);
        customerFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
        customerFilter.AddJoin(Join.Left, "Address").On("CustomerAddress", "AddressId", "Address", "AddressId");
        customerFilter.AddWhere("City", "Address", "City");
        customerFilter.AddWhere("PostalCode", "Address", "postal");
        setup.Filters.Add(customerFilter);

        var orderHeaderFilter = new SetupFilter("SalesOrderHeader");
        orderHeaderFilter.AddParameter("City", "Address", true);
        orderHeaderFilter.AddParameter("postal", DbType.String, true, null, 20);
        orderHeaderFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
        orderHeaderFilter.AddJoin(Join.Left, "Address").On("CustomerAddress", "AddressId", "Address", "AddressId");
        orderHeaderFilter.AddWhere("City", "Address", "City");
        orderHeaderFilter.AddWhere("PostalCode", "Address", "postal");
        setup.Filters.Add(orderHeaderFilter);

        var orderDetailsFilter = new SetupFilter("SalesOrderDetail");
        orderDetailsFilter.AddParameter("City", "Address", true);
        orderDetailsFilter.AddParameter("postal", DbType.String, true, null, 20);
        orderDetailsFilter.AddJoin(Join.Left, "SalesOrderHeader").On("SalesOrderHeader", "SalesOrderID", "SalesOrderHeader", "SalesOrderID");
        orderDetailsFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
        orderDetailsFilter.AddJoin(Join.Left, "Address").On("CustomerAddress", "AddressId", "Address", "AddressId");
        orderDetailsFilter.AddWhere("City", "Address", "City");
        orderDetailsFilter.AddWhere("PostalCode", "Address", "postal");
        setup.Filters.Add(orderDetailsFilter);

        // ----------------------------------------------------

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
        agent.Options.CleanMetadatas = true;
        agent.Options.UseBulkOperations = true;
        agent.Options.DisableConstraintsOnApplyChanges = false;
        //agent.Options.ConflictResolutionPolicy = ConflictResolutionPolicy.ServerWins;
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
                if (!agent.Parameters.Contains("City"))
                    agent.Parameters.Add("City", "Toronto");

                if (!agent.Parameters.Contains("postal"))
                    agent.Parameters.Add("postal", DBNull.Value);


                var s1 = await agent.SynchronizeAsync(progress);

                await agent.RemoteOrchestrator.DeleteMetadatasAsync(s1, setup, 4000);


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
            UseVerboseErrors = false,
            ClientCacheSlidingExpiration = TimeSpan.FromSeconds(20)
        };
        webServerOptions.Serializers.Add(new CustomMessagePackSerializerFactory());
        webServerOptions.Converters.Add(new CustomConverter());

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, proxyClientProvider);
        agent.Options = clientOptions;


        var configureServices = new Action<IServiceCollection>(services =>
        {
            // add a SqlSyncProvider acting as the server hub
            services.AddSyncServer<SqlSyncProvider>(serverProvider.ConnectionString, setup, webServerOptions);

        });

        var serverHandler = new RequestDelegate(async context =>
        {
            var webProxyServer = context.RequestServices.GetService(typeof(WebProxyServerOrchestrator)) as WebProxyServerOrchestrator;
            await webProxyServer.HandleRequestAsync(context);
        });
        using (var server = new KestrellTestServer(configureServices))
        {
            var clientHandler = new ResponseDelegate(async (serviceUri) =>
            {
                proxyClientProvider.ServiceUri = serviceUri;
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

