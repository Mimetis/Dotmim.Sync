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
using System.Net;
using System.Net.Http;
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
        await SyncThroughWebApiAsync();
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
        //agent.Options.BatchSize = 100;
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


    private static async Task SyncProductCategoryAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(clientDbName));
        // specific Setup with only 2 tables, and one filtered
        var setup = new SyncSetup(new string[] { "ProductCategory" });

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

        agent.Options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "syncproduct");
        agent.Options.BatchSize = 10;
        agent.Options.CleanMetadatas = true;
        agent.Options.UseBulkOperations = true;
        agent.Options.DisableConstraintsOnApplyChanges = false;

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

    private static async Task CreateSnapshotAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(serverDbName));
        var remoteOrchestrator = new RemoteOrchestrator(serverProvider);

        // specific Setup with only 2 tables, and one filtered
        var setup = new SyncSetup(allTables);

        // snapshot directory
        var directory = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "Snapshots");


        await remoteOrchestrator.CreateSnapshotAsync(new SyncContext(), setup, directory, 500, CancellationToken.None);
        // client provider
        var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(clientDbName));

        // Insert a value after snapshot created
        using (var c = serverProvider.CreateConnection())
        {
            var command = c.CreateCommand();
            command.CommandText = "INSERT INTO [dbo].[ProductCategory] ([Name]) VALUES ('Bikes revolution');";
            c.Open();
            command.ExecuteNonQuery();
            c.Close();
        }

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, setup);

        var syncOptions = new SyncOptions { SnapshotsDirectory = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "Snapshots") };

        // Setting the snapshots directory for client
        agent.Options.SnapshotsDirectory = directory;

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


        //// Launch the sync process
        //if (!agent.Parameters.Contains("City"))
        //    agent.Parameters.Add("City", "Toronto");

        //if (!agent.Parameters.Contains("postal"))
        //    agent.Parameters.Add("postal", "NULL");



        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                var s1 = await agent.SynchronizeAsync(progress);
                Console.WriteLine(s1);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }

    private static async Task SynchronizePalmyraAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString("ImpactPalmyraOct2019"));
        //var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(clientDbName));
        var clientProvider = new SqliteSyncProvider("palmyra.db");

        allTables = new string[]
        {
            "Branch_Details", "Counters", "Debtors", "Debtor_Accounts", "Departments", "Department_Links",
               "DocNumbers", "Gratuity", "Happy_Hour", "Happy_Hour1", "Locations", "Location_Links",
               "OrderNoTracking", "Printer_Footer", "Printer_Header", "Product_Prices", "Products",
               "Quantities", "Recipes", "Sales_Journal", "Settings", "Specials", "Sub_Recipes",
               "Tab_Listing", "Table_Listing","Table_Details","Tax_Rates", "Users"
        };

        // specific Setup with only 2 tables, and one filtered
        var setup = new SyncSetup(allTables);

  
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

    private static async Task SynchronizeMultiScopesAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncChangeTrackingProvider(DbHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(clientDbName));

        // Create 2 tables list (one for each scope)
        string[] productScopeTables = new string[] { "ProductCategory", "ProductModel", "Product" };
        string[] customersScopeTables = new string[] { "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" };

        // Create 2 sync setup with named scope 
        var setupProducts = new SyncSetup(productScopeTables, "productScope");
        var setupCustomers = new SyncSetup(customersScopeTables, "customerScope");

        // Create 2 agents, one for each scope
        var agentProducts = new SyncAgent(clientProvider, serverProvider, setupProducts);
        var agentCustomers = new SyncAgent(clientProvider, serverProvider, setupCustomers);

        // Using the Progress pattern to handle progession during the synchronization
        // We can use the same progress for each agent
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

        // Spying what's going on the server side
        agentProducts.AddRemoteProgress(remoteProgress);
        agentCustomers.AddRemoteProgress(remoteProgress);


        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                Console.WriteLine("Hit 1 for sync Products. Hit 2 for sync customers and sales");
                var k = Console.ReadKey().Key;

                if (k == ConsoleKey.D1)
                {
                    Console.WriteLine("Sync Products:");
                    var s1 = await agentProducts.SynchronizeAsync(progress);
                    Console.WriteLine(s1);
                }
                else
                {
                    Console.WriteLine("Sync Customers and Sales:");
                    var s1 = await agentCustomers.SynchronizeAsync(progress);
                    Console.WriteLine(s1);
                }

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
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
        //var clientProvider = new SqliteSyncProvider("client2.db");

        // specific Setup with only 2 tables, and one filtered
        var setup = new SyncSetup(allTables);



        // 1) EASY Way:
        setup.Filters.Add("Customer", "CompanyName");

        var customerAddressFilter = new SetupFilter("CustomerAddress");
        customerAddressFilter.AddParameter("CompanyName", "Customer");
        customerAddressFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
        customerAddressFilter.AddWhere("CompanyName", "Customer", "CompanyName");
        setup.Filters.Add(customerAddressFilter);

        var addressFilter = new SetupFilter("Address");
        addressFilter.AddParameter("CompanyName", "Customer");
        addressFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "AddressId", "Address", "AddressId");
        addressFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
        addressFilter.AddWhere("CompanyName", "Customer", "CompanyName");
        setup.Filters.Add(addressFilter);

        var salesOrderHeaderFilter = new SetupFilter("SalesOrderHeader");
        salesOrderHeaderFilter.AddParameter("CompanyName", "Customer");
        salesOrderHeaderFilter.AddJoin(Join.Left, "Customer").On("SalesOrderHeader", "CustomerId", "Customer", "CustomerId");
        salesOrderHeaderFilter.AddWhere("CompanyName", "Customer", "CompanyName");
        setup.Filters.Add(salesOrderHeaderFilter);

        var salesOrderDetailFilter = new SetupFilter("SalesOrderDetail");
        salesOrderDetailFilter.AddParameter("CompanyName", "Customer");
        salesOrderDetailFilter.AddJoin(Join.Left, "SalesOrderHeader").On("SalesOrderHeader", "SalesOrderId", "SalesOrderDetail", "SalesOrderId");
        salesOrderDetailFilter.AddJoin(Join.Left, "Customer").On("SalesOrderHeader", "CustomerId", "Customer", "CustomerId");
        salesOrderDetailFilter.AddWhere("CompanyName", "Customer", "CompanyName");
        setup.Filters.Add(salesOrderDetailFilter);


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
        agent.Options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;
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
                if (!agent.Parameters.Contains("CompanyName"))
                    agent.Parameters.Add("CompanyName", "Professional Sales and Service");

                //if (!agent.Parameters.Contains("postal"))
                //    agent.Parameters.Add("postal", DBNull.Value);


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
       

        // ----------------------------------
        // Client side
        // ----------------------------------
        var clientOptions = new SyncOptions { BatchSize = 500 };

        var proxyClientProvider = new WebClientOrchestrator();

        // ----------------------------------
        // Web Server side
        // ----------------------------------
        var setup = new SyncSetup(allTables)
        {
            ScopeName = "all_tables_scope",
            StoredProceduresPrefix = "s",
            StoredProceduresSuffix = "",
            TrackingTablesPrefix = "t",
            TrackingTablesSuffix = "",
            TriggersPrefix = "",
            TriggersSuffix = "t"
        };

        // snapshot directory
        var directory = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "Snapshots");

        // ----------------------------------
        // Create a snapshot
        // ----------------------------------
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine($"Creating snapshot");
        var remoteOrchestrator = new RemoteOrchestrator(serverProvider);
        await remoteOrchestrator.CreateSnapshotAsync(new SyncContext(), setup, directory, 500, CancellationToken.None);
        Console.WriteLine($"Done.");
        Console.ResetColor();


        // ----------------------------------
        // Insert a value after snapshot created
        // ----------------------------------
        using (var c = serverProvider.CreateConnection())
        {
            var command = c.CreateCommand();
            command.CommandText = "INSERT INTO [dbo].[ProductCategory] ([Name]) VALUES ('Bikes revolution');";
            c.Open();
            command.ExecuteNonQuery();
            c.Close();
        }

        var webServerOptions = new WebServerOptions { SnapshotsDirectory = directory };

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, proxyClientProvider);
        agent.Options = clientOptions;

        var configureServices = new Action<IServiceCollection>(services =>
        {
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
                        var progress = new SynchronousProgress<ProgressArgs>(pa => Console.WriteLine($"{pa.Context.SyncStage}\t {pa.Message}"));
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
    private static async Task SyncThroughWebApiAsync()
    {
        var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(clientDbName));

        var handler = new HttpClientHandler { AutomaticDecompression = DecompressionMethods.GZip };
        var client = new HttpClient(handler) { Timeout = TimeSpan.FromMinutes(5) };

        var proxyClientProvider = new WebClientOrchestrator("http://localhost:52288/api/Sync", null, null, client);



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

        Console.WriteLine("Press a key to start (be sure web api is running ...)");
        Console.ReadKey();
        do
        {
            Console.Clear();
            Console.WriteLine("Web sync start");
            try
            {
                var progress = new SynchronousProgress<ProgressArgs>(pa => Console.WriteLine($"{pa.Context.SessionId} - {pa.Context.SyncStage}\t {pa.Message}"));

                var s = await agent.SynchronizeAsync(progress);

                Console.WriteLine(s);

            }
            catch (SyncException e)
            {
                Console.WriteLine(e.Message);
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

