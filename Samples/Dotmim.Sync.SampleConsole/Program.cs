using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SampleConsole;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Data;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Events;
using System.Data.Common;
using Dotmim.Sync.MySql;
using System.Linq;
using Microsoft.Data.SqlClient;
using Dotmim.Sync.MariaDB;
using Dotmim.Sync.Tests.Models;
using System.Collections.Generic;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Serialization;

#if NET5_0 || NET6_0
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
                                                    "SalesOrderHeader", "SalesOrderDetail" };

    public static string[] oneTable = new string[] { "ProductCategory" };


    private static async Task Main(string[] args)
    {

        var serverProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        serverProvider.UseBulkOperations = false;

        //var serverProvider = new MariaDBSyncProvider(DBHelper.GetMariadbDatabaseConnectionString(serverDbName));
        //var serverProvider = new MySqlSyncProvider(DBHelper.GetMySqlDatabaseConnectionString(serverDbName));

        var clientProvider = new SqliteSyncProvider(Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db");
        //var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
        //clientProvider.UseBulkOperations = false;

        //var clientProvider = new MariaDBSyncProvider(DBHelper.GetMariadbDatabaseConnectionString(clientDbName));
        //var clientProvider = new MySqlSyncProvider(DBHelper.GetMySqlDatabaseConnectionString(clientDbName));

        var setup = new SyncSetup(allTables);
        //var setup = new SyncSetup("ProductCategory", "Product");
        //setup.Tables["Address"].Columns.AddRange("AddressID", "CreatedDate", "ModifiedDate");

        var options = new SyncOptions();

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

        //await GetChangesAsync(clientProvider, serverProvider, setup, options);
        //await ProvisionAsync(serverProvider, setup, options);
        //await CreateSnapshotAsync(serverProvider, setup, options);

        // await ScenarioAddColumnSyncAsync(clientProvider, serverProvider, setup, options);


        //await SyncHttpThroughKestrellAsync(clientProvider, serverProvider, setup, options);

        //await ScenarioPluginLogsAsync(clientProvider, serverProvider, setup, options, "all");

        //var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString("CliProduct"));
        //clientProvider.UseBulkOperations = false;
        // await FileTestAsync();

        //await TestItAsync();

        // await SynchronizeAsync(clientProvider, serverProvider, setup, options);
        await GenerateErrorsAsync();
    }

    private static async Task TestItAsync()
    {
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString("svProduct"));
        var client1Provider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString("CliProduct"));
        var client2Provider = new SqliteSyncProvider(Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db");

        var setup = new SyncSetup("Customer");
        setup.Filters.Add("Customer", "Country");

        var agent = new SyncAgent(client1Provider, serverProvider);

        var paramIndia = new SyncParameters(("Country", "India"));
        var paramSweden = new SyncParameters(("Country", "Sweden"));

        var resultIndia = await agent.SynchronizeAsync(setup, paramIndia);
        Console.WriteLine(resultIndia);

        await DBHelper.ExecuteScriptAsync("CliProduct",
            "INSERT [dbo].[Customer] ([CustomerID], [LastName], [Country]) VALUES (NEWID(), 'S1', 'Sweden')");
        await DBHelper.ExecuteScriptAsync("CliProduct",
            "INSERT [dbo].[Customer] ([CustomerID], [LastName], [Country]) VALUES (NEWID(), 'S2', 'Sweden')");

        // try to get sweden info client instance:
        // if it is not existing, DMS will create a new one, with IsNewScope set to true
        var swedenScopeInfoClient = await agent.LocalOrchestrator.GetScopeInfoClientAsync(syncParameters: paramSweden);

        if (swedenScopeInfoClient.IsNewScope)
        {
            swedenScopeInfoClient.IsNewScope = false;
            swedenScopeInfoClient.LastSync = DateTime.Now;
            swedenScopeInfoClient.LastSyncTimestamp = 0;
            swedenScopeInfoClient.LastServerSyncTimestamp = 0;

            await agent.LocalOrchestrator.SaveScopeInfoClientAsync(swedenScopeInfoClient);
        }

        // sweden rows are sent to server
        var resultSweden = await agent.SynchronizeAsync(setup, paramSweden);
        Console.WriteLine(resultSweden);

        agent = new SyncAgent(client2Provider, serverProvider);

        resultIndia = await agent.SynchronizeAsync(setup, paramIndia);
        Console.WriteLine(resultIndia);
        resultSweden = await agent.SynchronizeAsync(setup, paramSweden);
        Console.WriteLine(resultSweden);


    }

    private static async Task GenerateErrorsAsync()
    {
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString("svProduct"));
        serverProvider.UseBulkOperations = false;
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString("CliProduct"));
        clientProvider.UseBulkOperations = false;
        var setup = new SyncSetup("ProductCategory");
        var options = new SyncOptions();

        var getUpdateAndResolveForeignKeyErrorCommand = new Func<string>(() =>
        {
            var command = "UPDATE [ProductCategory] SET [ParentProductCategoryId] = NULL;";
            return command;
        });

        var getUpdateAndResolveUniqueNameErrorCommand = new Func<string>(() =>
        {
            var command = "UPDATE [ProductCategory] SET [Name] = Convert(varchar(50), [ProductCategoryID])";
            return command;
        });

        var getForeignKeyErrorCommand = new Func<string, string, string>((string subcat, string cat) =>
        {
            var command = @$"Begin Tran
                            ALTER TABLE [ProductCategory] NOCHECK CONSTRAINT ALL
                            INSERT [ProductCategory] ([ProductCategoryID], [ParentProductCategoryId], [Name]) VALUES (N'{subcat}', '{cat}', N'{subcat} Sub category')
                            INSERT [ProductCategory] ([ProductCategoryID], [ParentProductCategoryId], [Name]) VALUES (N'{cat}', NULL, N'{cat} Category');
                            ALTER TABLE [ProductCategory] CHECK CONSTRAINT ALL
                            Commit Tran";

            return command;
        });

        var getUniqueKeyErrorCommand = new Func<string>(() =>
        {
            var cat = string.Concat("Z_", string.Concat(Path.GetRandomFileName().Where(c => char.IsLetter(c))).ToUpperInvariant());
            cat = cat.Substring(0, Math.Min(cat.Length, 12));

            var command = @$"Begin Tran
                            ALTER TABLE [ProductCategory] NOCHECK CONSTRAINT ALL
                            INSERT [ProductCategory] ([ProductCategoryID], [ParentProductCategoryId], [Name]) VALUES (N'{cat}_1', NULL, N'{cat} Category');
                            INSERT [ProductCategory] ([ProductCategoryID], [ParentProductCategoryId], [Name]) VALUES (N'{cat}_2', NULL, N'{cat} Category');
                            ALTER TABLE [ProductCategory] CHECK CONSTRAINT ALL
                            Commit Tran";

            return command;
        });


        var getNotNullErrorCommand = new Func<string>(() =>
        {
            // Generate a null exception
            // On client ModifiedDate is not null allowed (but null allowed on server)
            return "UPDATE [ProductCategory] Set [ModifiedDate]=NULL WHERE [ProductCategoryID]='A_ACCESS'";

        });


        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options);

        var result = await agent.SynchronizeAsync(setup);

        agent.LocalOrchestrator.OnApplyChangesErrorOccured(args =>
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"ERROR: {args.Exception.Message}");
            Console.WriteLine($"ROW  : {args.ErrorRow}");
            Console.ResetColor();
            args.Resolution = ErrorResolution.RetryOnNextSync;
        });

        // Generate foreign key error
        await DBHelper.ExecuteScriptAsync("svProduct", getForeignKeyErrorCommand("B", "A"));

        //do
        //{
        try
        {
            Console.ResetColor();
            result = await agent.SynchronizeAsync(setup);
            Console.WriteLine(result);
            Console.WriteLine("FIRST SYNC ENDED. Press a key to start again, or Escapte to end");


            Console.ResetColor();
            result = await agent.SynchronizeAsync(setup);
            Console.WriteLine(result);
            Console.WriteLine("SECOND SYNC ENDED. Press a key to start again, or Escapte to end");

        }
        catch (Exception e)
        {
            Console.ResetColor();
            Console.WriteLine("Sync Rollbacked.");
        }
        //} while (Console.ReadKey().Key != ConsoleKey.Escape);


        Console.ResetColor();
        Console.WriteLine();
        // Loading all batch infos from tmp dir
        var batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();
        foreach (var batchInfo in batchInfos)
        {
            // Load all rows from error tables specifying the specific SyncRowState states
            var allTables = agent.LocalOrchestrator.LoadTablesFromBatchInfoAsync(batchInfo,
                SyncRowState.ApplyDeletedFailed | SyncRowState.ApplyModifiedFailed);

            // Enumerate all rows in error
            await foreach (var table in allTables)
                foreach (var row in table.Rows)
                    Console.WriteLine(row);
        }




    }


    private static async Task SynchronizeAsync(CoreProvider clientProvider, CoreProvider serverProvider, SyncSetup setup, SyncOptions options, string scopeName = SyncOptions.DefaultScopeName)
    {

        //var options = new SyncOptions();
        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.WriteLine($"{s.ProgressPercentage:p}:  \t[{s?.Source[..Math.Min(4, s.Source.Length)]}] {s.TypeName}: {s.Message}");
        });

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options);

        do
        {
            try
            {
                Console.Clear();
                Console.ForegroundColor = ConsoleColor.Green;
                var s = await agent.SynchronizeAsync(setup, progress: progress);
                Console.ResetColor();
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
            Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

    }


    private static async Task MultiFiltersAsync()
    {
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.ProgressPercentage:p}:  \t[{s.Source[..Math.Min(4, s.Source.Length)]}] {s.TypeName}:\t{s.Message}");
            Console.ResetColor();

        });

        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var remoteOrchestrator = new RemoteOrchestrator(serverProvider);


        // Now, we are creating a new setup with a filter on ProductCategory and Product
        var setup = new SyncSetup("ProductCategory", "Product", "Address", "Customer", "CustomerAddress");
        setup.Filters.Add("ProductCategory", "Name");

        var productFilter = new SetupFilter("Product");
        productFilter.AddParameter("Name", DbType.String);
        productFilter.AddJoin(Join.Left, "ProductCategory")
            .On("Product", "ProductCategoryID", "ProductCategory", "ProductCategoryID");
        productFilter.AddWhere("Name", "ProductCategory", "Name");
        setup.Filters.Add(productFilter);

        var schema2 = await remoteOrchestrator.ProvisionAsync(setup);

        // Testing a new client
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        var agent = new SyncAgent(clientProvider, serverProvider);

        var parameters = new SyncParameters(("Name", "Bikes"));
        var s = await agent.SynchronizeAsync(parameters, progress: progress);
        Console.WriteLine(s);

    }


    private static async Task TestsSetupInheritance()
    {
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.ProgressPercentage:p}:  \t[{s.Source[..Math.Min(4, s.Source.Length)]}] {s.TypeName}:\t{s.Message}");
            Console.ResetColor();

        });
        // Creating one Setup
        // This Setup contains all tables without filters
        // When provision, it will be called "default"
        var setup = new SyncSetup("ProductCategory", "Product", "Address",
            "Customer", "CustomerAddress");

        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var remoteOrchestrator = new RemoteOrchestrator(serverProvider);

        // Provision this "default" scope
        var serverScopeInfo = await remoteOrchestrator.ProvisionAsync("default", setup);

        // Now, we are creating a new setup with a filter on ProductCategory and Product
        var setup2 = new SyncSetup("ProductCategory", "Product", "Address",
            "Customer", "CustomerAddress");
        setup2.Filters.Add("ProductCategory", "Name");

        var productFilter = new SetupFilter("Product");
        productFilter.AddParameter("Name", DbType.String);
        productFilter.AddJoin(Join.Left, "ProductCategory")
            .On("Product", "ProductCategoryID", "ProductCategory", "ProductCategoryID");
        productFilter.AddWhere("Name", "ProductCategory", "Name");
        setup2.Filters.Add(productFilter);

        // Instead of Provisioning everything, we are 
        // Provisionning the Stored procedures specific to filters and then save this new Scope
        // Create a new ServerScopeInfo instance with schema and setup containing the filter
        var schema2 = await remoteOrchestrator.GetSchemaAsync(setup2);
        serverScopeInfo.Schema = schema2;
        serverScopeInfo.Setup = setup2;
        serverScopeInfo.Name = "filterproducts";

        // Only generate the get changes with filters 
        await remoteOrchestrator.CreateStoredProcedureAsync(serverScopeInfo,
            "ProductCategory", default, DbStoredProcedureType.SelectChangesWithFilters, true);
        await remoteOrchestrator.CreateStoredProcedureAsync(serverScopeInfo,
            "ProductCategory", default, DbStoredProcedureType.SelectInitializedChangesWithFilters, true);

        await remoteOrchestrator.CreateStoredProcedureAsync(serverScopeInfo,
            "Product", default, DbStoredProcedureType.SelectChangesWithFilters, true);
        await remoteOrchestrator.CreateStoredProcedureAsync(serverScopeInfo,
            "Product", default, DbStoredProcedureType.SelectInitializedChangesWithFilters, true);

        await remoteOrchestrator.SaveScopeInfoAsync(serverScopeInfo);

        // Testing a new client
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        var agent = new SyncAgent(clientProvider, serverProvider);

        // Eeach time a command is called on remote side, 
        // We are making a redirection to "default" stored procedures, except for filters specific stored proc
        agent.RemoteOrchestrator.OnGetCommand(args =>
        {
            if (args.Command.CommandType == CommandType.StoredProcedure)
            {
                switch (args.CommandType)
                {
                    case DbCommandType.SelectInitializedChangesWithFilters:
                    case DbCommandType.SelectChangesWithFilters:
                        break;
                    default:
                        args.Command.CommandText = args.Command.CommandText.Replace("_filterproducts_", "_default_");
                        break;
                }
                Console.WriteLine(args.Command.CommandText);
            }
        });

        var p = new SyncParameters(("Name", "Bikes"));
        var s = await agent.SynchronizeAsync("filterproducts", p, progress: progress);
        Console.WriteLine(s);

    }

    private static async Task ScenarioForeignKeyOnSameTableErrorsAsync()
    {
        var serverDbName = "PCServer";
        var clientDbName1 = "PCClient";

        await DBHelper.CreateDatabaseAsync(serverDbName, true);
        await DBHelper.CreateDatabaseAsync(clientDbName1, true);

        var script = @"CREATE TABLE [dbo].[ProductCategory](
	                    [ProductCategoryID] [uniqueidentifier] NOT NULL,
	                    [ParentProductCategoryID] [uniqueidentifier] NULL,
	                    [Name] [nvarchar](50) NOT NULL,
	                    [rowguid] [uniqueidentifier] ROWGUIDCOL  NULL,
	                    [ModifiedDate] [datetime] NULL,
                     CONSTRAINT [PK_ProductCategory_ProductCategoryID] PRIMARY KEY CLUSTERED ([ProductCategoryID] ASC));

                    ALTER TABLE [dbo].[ProductCategory] WITH CHECK 
                    ADD CONSTRAINT [FK_ProductCategory_ProductCategory_ParentProductCategoryID_ProductCategoryID] 
                    FOREIGN KEY([ParentProductCategoryID])
                    REFERENCES [dbo].[ProductCategory] ([ProductCategoryID])
                    
                    ALTER TABLE [dbo].[ProductCategory] CHECK CONSTRAINT [FK_ProductCategory_ProductCategory_ParentProductCategoryID_ProductCategoryID]

                    INSERT [dbo].[ProductCategory] ([ProductCategoryID], [ParentProductCategoryID], [Name], [rowguid], [ModifiedDate]) VALUES ('cfbda25c-df71-47a7-b81b-64ee161aa37c', NULL, N'Bikes', newid(), CAST(N'2002-06-01T00:00:00.000' AS DateTime));
                    INSERT [dbo].[ProductCategory] ([ProductCategoryID], [ParentProductCategoryID], [Name], [rowguid], [ModifiedDate]) VALUES ('ad364ade-264a-433c-b092-4fcbf3804e01', 'cfbda25c-df71-47a7-b81b-64ee161aa37c', N'Mountain Bikes', newid(), CAST(N'2002-06-01T00:00:00.000' AS DateTime));";

        await DBHelper.ExecuteScriptAsync(serverDbName, script);

        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.ProgressPercentage:p}:  \t[{s.Source[..Math.Min(4, s.Source.Length)]}] {s.TypeName}:\t{s.Message}");
            Console.ResetColor();

        });

        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName1))
        {
            UseBulkOperations = false
        };

        var setup = new SyncSetup("ProductCategory");

        try
        {
            var agent = new SyncAgent(clientProvider, serverProvider);

            agent.LocalOrchestrator.OnApplyChangesErrorOccured(args =>
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(args.ErrorRow);
                args.Resolution = ErrorResolution.ContinueOnError;
                Console.ForegroundColor = ConsoleColor.Green;
            });

            var result = await agent.SynchronizeAsync(setup, progress);
            Console.WriteLine(result);

        }
        catch (Exception ex)
        {
            Console.WriteLine(ex);
        }
    }


    private static async Task ScenarioMultiplesFiltersErrorAsync()
    {
        var serverDbName = "FServer";
        var clientDbName1 = "Employee1";
        var clientDbName2 = "Employee2";

        await DBHelper.CreateDatabaseAsync(serverDbName, true);
        await DBHelper.CreateDatabaseAsync(clientDbName1, true);
        await DBHelper.CreateDatabaseAsync(clientDbName2, true);

        var script = @"
        CREATE TABLE Customer (CustomerId int IDENTITY(1, 1) NOT NULL PRIMARY KEY, Name varchar(50) Not Null, EmployeeId int NOT NULL);

        CREATE TABLE Sales (SalesId int IDENTITY(1, 1) NOT NULL PRIMARY KEY, EmployeeId int NOT NULL, BuyerCustomerId int NOT NULL, Product varchar(50) NOT NULL,
        CONSTRAINT FK_Buyer_Customer FOREIGN KEY(BuyerCustomerId) REFERENCES Customer(CustomerId));

        SET IDENTITY_INSERT Customer ON
        INSERT Customer (CustomerId, [Name], EmployeeId) VALUES(5000, 'B. Gates', 1)
        INSERT Customer (CustomerId, [Name], EmployeeId) VALUES(6000, 'S. Nadela', 1)
        INSERT Customer (CustomerId, [Name], EmployeeId) VALUES(7000, 'S. Balmer', 1)
        INSERT Customer (CustomerId, [Name], EmployeeId) VALUES(8000, 'S. Jobs', 2)
        INSERT Customer (CustomerId, [Name], EmployeeId) VALUES(9000, 'T. Cook', 2)
        SET IDENTITY_INSERT Customer OFF

        INSERT Sales (EmployeeId, BuyerCustomerId, Product) VALUES (1, 5000, 'Stairs');
        INSERT Sales (EmployeeId, BuyerCustomerId, Product) VALUES (1, 6000, 'Doors');
        INSERT Sales (EmployeeId, BuyerCustomerId, Product) VALUES (2, 8000, 'Oranges');
        -- We have a problem here. An employee 1 sold something to a customer that is not in its customers list 
        -- Customer 9000 is affiliated to employee 2
        INSERT Sales (EmployeeId, BuyerCustomerId, Product) VALUES (1, 9000, 'Strawberries');
        ";

        await DBHelper.ExecuteScriptAsync(serverDbName, script);

        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.ProgressPercentage:p}:  \t[{s.Source[..Math.Min(4, s.Source.Length)]}] {s.TypeName}:\t{s.Message}");
            Console.ResetColor();

        });

        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));

        //var employee1Provider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName1))
        //{
        //    UseBulkOperations = false
        //};

        var employee1Provider = new SqliteSyncProvider(Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db");


        var setup = new SyncSetup("Customer", "Sales");
        setup.Filters.Add("Customer", "EmployeeId");
        setup.Filters.Add("Sales", "EmployeeId");

        try
        {

            var emp1Agent = new SyncAgent(employee1Provider, serverProvider);
            var emp1Params = new SyncParameters(("EmployeeId", 1));

            emp1Agent.LocalOrchestrator.OnApplyChangesErrorOccured(args =>
            {
                Console.WriteLine(args.ErrorRow);
                // We can do something here the failed row
                // ....
                // Then pass the resolution to Ignore to prevent a fail 
                args.Resolution = ErrorResolution.ContinueOnError;
            });

            var emp1result = await emp1Agent.SynchronizeAsync(setup, emp1Params, progress);
            Console.WriteLine(emp1result);

        }
        catch (Exception ex)
        {
            Console.WriteLine(ex);
        }
    }

    private static async Task ScenarioConflictOnApplyChangesChangeResolutionAsync()
    {
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.ProgressPercentage:p}:  \t[{s.Source[..Math.Min(4, s.Source.Length)]}] {s.TypeName}:\t{s.Message}");
            Console.ResetColor();

        });

        // Server provider
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        // Client 1 provider. Will migrate to the new schema (with one more column)
        var clientProvider1 = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        // Step 1. Add a lot of ProductCategory
        // --------------------------
        //for (int i = 0; i < 1000; i++)
        //    await AddProductCategoryRowAsync(serverProvider);


        // Step 2: Sync 
        // --------------------------
        var setup = new SyncSetup(new string[] { "ProductCategory" });
        var options = new SyncOptions()
        {
            ProgressLevel = SyncProgressLevel.Debug,
            DisableConstraintsOnApplyChanges = true,
            BatchSize = 1,
        };

        var agent = new SyncAgent(clientProvider1, serverProvider, options);
        Console.WriteLine(await agent.SynchronizeAsync(setup, progress: progress));

        // Step 3: Update all product on both sides
        // --------------------------
        await UpdateAllProductCategoryAsync(serverProvider, "server");
        await UpdateAllProductCategoryAsync(clientProvider1, "client");


        agent.RemoteOrchestrator.OnApplyChangesConflictOccured(async args =>
        {
            args.Resolution = ConflictResolution.ClientWins;
            var conflict = await args.GetSyncConflictAsync();
        });

        Console.WriteLine(await agent.SynchronizeAsync(setup, progress: progress));


        //// Step 4: Sync through kestrell
        //// --------------------------
        //var configureServices = new Action<IServiceCollection>(services =>
        //{
        //    services.AddSyncServer<SqlSyncProvider>(DBHelper.GetDatabaseConnectionString(serverDbName), setup, options);
        //});

        //var cpt = 0;
        //var serverHandler = new RequestDelegate(async context =>
        //{
        //    try
        //    {
        //        var webServerAgent = context.RequestServices.GetService<WebServerAgent>();

        //        var scopeName = context.GetScopeName();
        //        var clientScopeId = context.GetClientScopeId();

        //        webServerAgent.RemoteOrchestrator.OnApplyChangesFailed(args =>
        //        {
        //            args.Resolution = ConflictResolution.ClientWins;
        //            cpt++;
        //        });

        //        await webServerAgent.HandleRequestAsync(context);

        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine(ex);
        //        throw;
        //    }

        //});

        //using var server = new KestrellTestServer(configureServices, false);

        //var clientHandler = new ResponseDelegate(async (serviceUri) =>
        //{
        //    try
        //    {
        //        // create the agent
        //        var agent = new SyncAgent(clientProvider1, new WebRemoteOrchestrator(serviceUri), options);

        //        // make a synchronization to get all rows between backup and now
        //        var s = await agent.SynchronizeAsync(progress: progress);

        //        Console.WriteLine(cpt);
        //        Console.WriteLine(s);
        //    }
        //    catch (SyncException e)
        //    {
        //        Console.WriteLine(e.ToString());
        //    }
        //    catch (Exception e)
        //    {
        //        Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
        //    }

        //});
        //await server.Run(serverHandler, clientHandler);




    }


    static async Task ScenarioPluginLogsAsync(CoreProvider clientProvider, CoreProvider serverProvider, SyncSetup setup, SyncOptions options, string scopeName = SyncOptions.DefaultScopeName)
    {
        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options);

        var showHelp = new Action(() =>
        {
            Console.WriteLine("+ :\t\t Add 1 Product & 1 Product Category to server");
            Console.WriteLine("a :\t\t Add 1 Product & 1 Product Category to client");
            Console.WriteLine("c :\t\t Generate Product Category Conflict");
            Console.WriteLine("h :\t\t Show Help");
            Console.WriteLine("r :\t\t Reinitialize");
            Console.WriteLine("Esc :\t\t End");
            Console.WriteLine("Default :\t Synchronize");

        });

        await InteruptRemoteOrchestratorInterceptors(agent.RemoteOrchestrator);

        ConsoleKey key;

        showHelp();
        do
        {
            key = Console.ReadKey().Key;

            try
            {
                if (key != ConsoleKey.Escape)
                {
                    switch (key)
                    {
                        case ConsoleKey.H:
                        case ConsoleKey.Help:
                            showHelp();
                            break;
                        case ConsoleKey.C:
                            Console.WriteLine("Generating 1 conflict on Product Category");
                            var pId = Guid.NewGuid();
                            await DBHelper.AddProductCategoryRowAsync(serverProvider, pId);
                            await DBHelper.AddProductCategoryRowAsync(clientProvider, pId);
                            break;
                        case ConsoleKey.Add:
                            Console.WriteLine("Adding 1 product & 1 product category to server");
                            await DBHelper.AddProductCategoryRowAsync(serverProvider);
                            await AddProductRowAsync(serverProvider);
                            break;
                        case ConsoleKey.A:
                            Console.WriteLine("Adding 1 product & 1 product category to client");
                            await DBHelper.AddProductCategoryRowAsync(clientProvider);
                            await AddProductRowAsync(clientProvider);
                            break;
                        case ConsoleKey.R:
                            Console.WriteLine("Reinitialiaze");
                            await agent.SynchronizeAsync(scopeName, setup, SyncType.Reinitialize);
                            showHelp();
                            break;
                        default:
                            await agent.SynchronizeAsync(scopeName, setup);
                            showHelp();
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


            Console.WriteLine("---");
            Console.WriteLine();
        } while (key != ConsoleKey.Escape);
    }
    static async Task InteruptRemoteOrchestratorInterceptors(RemoteOrchestrator remoteOrchestrator)
    {
        using var syncLogContext = new SyncLogsContext(remoteOrchestrator.Provider.ConnectionString);
        await syncLogContext.Database.EnsureCreatedAsync();
        syncLogContext.EnsureTablesCreated();

        remoteOrchestrator.OnDatabaseChangesApplying(args =>
        {
            Console.WriteLine("-----------------------------------------------------");
            Console.WriteLine("OnDatabaseChangesApplying");
            Console.WriteLine($"{args.Context.SessionId}-{args.Context.ClientId}-{args.Context.StartTime}");
            Console.WriteLine($"SyncType: {args.Context.SyncType}");
            Console.WriteLine($"SyncWay: {args.Context.SyncWay}");
            Console.WriteLine($"SenderScopeId: {args.ApplyChanges.SenderScopeId}");
            Console.WriteLine($"Policy: {args.ApplyChanges.Policy}");

            using var syncLogContext = new SyncLogsContext(remoteOrchestrator.Provider.ConnectionString);

            var log = syncLogContext.SyncLog.Find(args.Context.SessionId, args.Context.ClientId);

            if (log == null)
            {
                log = new SyncLog { SessionId = args.Context.SessionId, ClientScopeId = args.Context.ClientId.Value };
                syncLogContext.SyncLog.Add(log);
            }

            log.StartTime = args.Context.StartTime;
            log.ScopeName = args.Context.ScopeName;
            log.SyncType = args.Context.SyncType;

            syncLogContext.SaveChanges();

        });

        remoteOrchestrator.OnDatabaseChangesApplied(args =>
        {

            Console.WriteLine("-----------------------------------------------------");
            Console.WriteLine("OnDatabaseChangesApplied");
            Console.WriteLine($"{args.Context.SessionId}-{args.Context.ClientId}-{args.Context.StartTime}");
            Console.WriteLine($"TotalAppliedChanges: {args.ChangesApplied.TotalAppliedChanges}");
            Console.WriteLine($"TotalAppliedChangesFailed: {args.ChangesApplied.TotalAppliedChangesFailed}");
            Console.WriteLine($"TotalResolvedConflicts: {args.ChangesApplied.TotalResolvedConflicts}");

            using var syncLogContext = new SyncLogsContext(remoteOrchestrator.Provider.ConnectionString);

            var log = syncLogContext.SyncLog.Find(args.Context.SessionId, args.Context.ClientId);

            log.TotalChangesApplied = args.ChangesApplied.TotalAppliedChanges;
            log.TotalResolvedConflicts = args.ChangesApplied.TotalResolvedConflicts;

            syncLogContext.SaveChanges();


        });

        remoteOrchestrator.OnDatabaseChangesSelecting(args =>
        {
            Console.WriteLine("-----------------------------------------------------");
            Console.WriteLine("OnDatabaseChangesSelecting");
            Console.WriteLine($"{args.Context.SessionId}-{args.Context.ClientId}-{args.Context.StartTime}");
            Console.WriteLine($"From/ To : {args.FromTimestamp}/{args.ToTimestamp}");
            Console.WriteLine($"IsNew : {args.IsNew}");

            using var syncLogContext = new SyncLogsContext(remoteOrchestrator.Provider.ConnectionString);

            var log = syncLogContext.SyncLog.Find(args.Context.SessionId, args.Context.ClientId);

            if (log == null)
            {
                log = new SyncLog { SessionId = args.Context.SessionId, ClientScopeId = args.Context.ClientId.Value };
                syncLogContext.SyncLog.Add(log);
            }

            log.StartTime = args.Context.StartTime;
            log.ScopeName = args.Context.ScopeName;
            log.FromTimestamp = args.FromTimestamp;
            log.ToTimestamp = args.ToTimestamp;
            log.SyncType = args.Context.SyncType;
            log.IsNew = args.IsNew;

            syncLogContext.SaveChanges();

        });

        remoteOrchestrator.OnTableChangesSelecting(args =>
        {
            Console.WriteLine("-----------------------------------------------------");
            Console.WriteLine("OnTableChangesSelecting");
            Console.WriteLine($"{args.Context.SessionId}-{args.Context.ClientId}-{args.Context.StartTime}");
            Console.WriteLine($"Table: {args.SchemaTable.GetFullName()}");

            using var syncLogContext = new SyncLogsContext(remoteOrchestrator.Provider.ConnectionString);

            var logTable = syncLogContext.SyncLogTable.Find(args.Context.SessionId, args.Context.ClientId, args.SchemaTable.GetFullName());

            if (logTable == null)
            {
                logTable = new SyncLogTable
                {
                    SessionId = args.Context.SessionId,
                    ScopeName = args.Context.ScopeName,
                    ClientScopeId = args.Context.ClientId.Value,
                    TableName = args.SchemaTable.GetFullName()
                };
                syncLogContext.SyncLogTable.Add(logTable);
            }

            var j = new { CommandText = args.Command.CommandText, Parameters = new List<(string Name, object value)>() };


            var parameters = new JArray();
            foreach (DbParameter p in args.Command.Parameters)
            {
                var pJ = new JObject();
                pJ.Add(p.ParameterName, JToken.FromObject(p.Value));
                parameters.Add(pJ);
            }


            var jObject = new JObject
            {
                { "CommandText", args.Command.CommandText },
                { "Parameters", parameters }
            };


            logTable.Command = jObject.ToString();
            syncLogContext.SaveChanges();
        });

        remoteOrchestrator.OnTableChangesSelected(args =>
        {
            Console.WriteLine("-----------------------------------------------------");
            Console.WriteLine("OnTableChangesSelected");
            Console.WriteLine($"{args.Context.SessionId}-{args.Context.ClientId}-{args.Context.StartTime}");
            Console.WriteLine($"Table: {args.SchemaTable.GetFullName()}");
            Console.WriteLine($"TableChangesSelected: {args.TableChangesSelected.TotalChanges} ({args.TableChangesSelected.Upserts}/{args.TableChangesSelected.Deletes})");

            using var syncLogContext = new SyncLogsContext(remoteOrchestrator.Provider.ConnectionString);
            var logTable = syncLogContext.SyncLogTable.Find(args.Context.SessionId, args.Context.ClientId, args.SchemaTable.GetFullName());

            logTable.TotalChangesSelected = args.TableChangesSelected.TotalChanges;
            logTable.TotalChangesSelectedUpdates = args.TableChangesSelected.Upserts;
            logTable.TotalChangesSelectedDeletes = args.TableChangesSelected.Deletes;
            syncLogContext.SaveChanges();

        });


        remoteOrchestrator.OnTableChangesApplied(args =>
        {
            Console.WriteLine("-----------------------------------------------------");
            Console.WriteLine("OnTableChangesApplied");
            Console.WriteLine($"{args.Context.SessionId}-{args.Context.ClientId}-{args.Context.StartTime}");

            using var syncLogContext = new SyncLogsContext(remoteOrchestrator.Provider.ConnectionString);
            var fullName = string.IsNullOrEmpty(args.TableChangesApplied.SchemaName) ? args.TableChangesApplied.TableName : $"{args.TableChangesApplied.SchemaName}.{args.TableChangesApplied.TableName}";

            var logTable = syncLogContext.SyncLogTable.Find(args.Context.SessionId, args.Context.ClientId, fullName);

            if (logTable == null)
            {
                logTable = new SyncLogTable
                {
                    SessionId = args.Context.SessionId,
                    ScopeName = args.Context.ScopeName,
                    ClientScopeId = args.Context.ClientId.Value,
                    TableName = fullName
                };
                syncLogContext.SyncLogTable.Add(logTable);
            }


            if (args.TableChangesApplied.Applied > 0)
                logTable.TotalChangesApplied = logTable.TotalChangesApplied.HasValue ? logTable.TotalChangesApplied += args.TableChangesApplied.Applied : args.TableChangesApplied.Applied;

            if (args.TableChangesApplied.ResolvedConflicts > 0)
                logTable.TotalResolvedConflicts = logTable.TotalResolvedConflicts.HasValue ? logTable.TotalResolvedConflicts += args.TableChangesApplied.ResolvedConflicts : args.TableChangesApplied.ResolvedConflicts;

            if (args.TableChangesApplied.State == SyncRowState.Modified)
                logTable.TotalChangesAppliedUpdates = args.TableChangesApplied.Applied;
            else
                logTable.TotalChangesAppliedDeletes = args.TableChangesApplied.Applied;


            syncLogContext.SaveChanges();

        });

        remoteOrchestrator.OnDatabaseChangesSelected(args =>
        {

            Console.WriteLine("-----------------------------------------------------");
            Console.WriteLine("OnDatabaseChangesSelected");
            Console.WriteLine($"{args.Context.SessionId}-{args.Context.ClientId}-{args.Context.StartTime}");
            Console.WriteLine($"From/ To : {args.FromTimestamp}/{args.ToTimestamp}");
            Console.WriteLine($"TotalChangesSelected :{args.ChangesSelected.TotalChangesSelected}");
            Console.WriteLine($"TotalChangesSelectedUpdates :{args.ChangesSelected.TotalChangesSelectedUpdates}");
            Console.WriteLine($"TotalChangesSelectedDeletes :{args.ChangesSelected.TotalChangesSelectedDeletes}");

            using var syncLogContext = new SyncLogsContext(remoteOrchestrator.Provider.ConnectionString);
            var log = syncLogContext.SyncLog.Find(args.Context.SessionId, args.Context.ClientId);
            log.TotalChangesSelected = args.ChangesSelected.TotalChangesSelected;
            log.TotalChangesSelectedUpdates = args.ChangesSelected.TotalChangesSelectedUpdates;
            log.TotalChangesSelectedDeletes = args.ChangesSelected.TotalChangesSelectedDeletes;
            syncLogContext.SaveChanges();

        });


    }
    private static async Task ScenarioMigrationAddingColumnsAndTableAsync()
    {
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.ProgressPercentage:p}:  \t[{s.Source[..Math.Min(4, s.Source.Length)]}] {s.TypeName}:\t{s.Message}");
            Console.ResetColor();

        });

        // Server provider
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        // Client 1 provider. Will migrate to the new schema (with one more column)
        var clientProvider1 = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
        // Client 2 provider: Will stay with old schema
        //var clientProvider2 = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString("Client2"));

        // --------------------------
        // Step 1: Create a default scope and Sync clients
        var setup = new SyncSetup(new string[] { "ProductCategory" });
        setup.Tables["ProductCategory"].Columns.AddRange(
            new string[] { "ProductCategoryID", "ParentProductCategoryID", "Name", "rowguid", "ModifiedDate" });

        var options = new SyncOptions() { ProgressLevel = SyncProgressLevel.Debug };

        // Sync 2 clients
        var agent = new SyncAgent(clientProvider1, serverProvider, options);
        Console.WriteLine(await agent.SynchronizeAsync(setup, progress: progress));
        var localOrchestrator = agent.LocalOrchestrator;
        var remoteOrchestrator = agent.RemoteOrchestrator;


        // --------------------------
        // Step2 : Adding a new column "CreatedDate datetime NULL" on the server
        //         Then create the corresponding scope (called "v1  ")
        await AddColumnsToProductCategoryAsync(serverProvider);

        // Step 2 : Add a new scope to server with this new column
        //          Creating a new scope called "V1" on server
        var setupV1 = new SyncSetup(new string[] { "ProductCategory", "Product" });
        setupV1.Tables["ProductCategory"].Columns.AddRange(
            new string[] { "ProductCategoryID", "ParentProductCategoryID", "Name", "rowguid", "ModifiedDate", "CreatedDate" });

        var serverScope = await remoteOrchestrator.ProvisionAsync("v1", setupV1);

        // Add a product category row on server (just to check we are still able to get this row on clients)
        await AddProductCategoryRowWithOneMoreColumnAsync(serverProvider);

        // Add a product category row on both client (just to check we are still able to get this row on server)
        await DBHelper.AddProductCategoryRowAsync(clientProvider1);
        //await AddProductCategoryRowAsync(clientProvider2);

        // --------------------------
        // Step 3 : Add the column to client 1, add the new scope "v1" and sync
        await AddColumnsToProductCategoryAsync(clientProvider1);

        // Step 4 Add product table
        await localOrchestrator.CreateTableAsync(serverScope, "Product");

        // Provision the "v1" scope on the client with the new setup
        await localOrchestrator.ProvisionAsync(serverScope);

        var defaultClientScopeInfo = await localOrchestrator.GetScopeInfoAsync(); // scope name is SyncOptions.DefaultScopeName, which is default value
        var v1ClientScopeInfo = await localOrchestrator.GetScopeInfoAsync("v1"); // scope name is SyncOptions.DefaultScopeName, which is default value

        throw new Exception("Do not work ");
        //v1ClientScopeInfo.LastServerSyncTimestamp = defaultClientScopeInfo.LastServerSyncTimestamp;
        //v1ClientScopeInfo.LastSyncTimestamp = defaultClientScopeInfo.LastSyncTimestamp;
        //v1ClientScopeInfo.LastSync = defaultClientScopeInfo.LastSync;
        //v1ClientScopeInfo.LastSyncDuration = defaultClientScopeInfo.LastSyncDuration;

        await localOrchestrator.SaveScopeInfoAsync(v1ClientScopeInfo);

        Console.WriteLine(await agent.SynchronizeAsync("v1", progress: progress));

        var deprovision = SyncProvision.StoredProcedures;
        await localOrchestrator.DeprovisionAsync(deprovision);



        // --------------------------
        // Step 4 : Do nothing on client 2 and see if we can still sync
        // Console.WriteLine(await agent2.SynchronizeAsync(progress));


    }


    private static async Task ScenarioMigrationAddingColumnsAndTableInSameScopeAsync()
    {
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.ProgressPercentage:p}:  \t[{s.Source[..Math.Min(4, s.Source.Length)]}] {s.TypeName}:\t{s.Message}");
            Console.ResetColor();

        });

        // Server provider
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        // Client 1 provider. 
        var clientProvider1 = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        // --------------------------
        // Step 1: Create a default scope and Sync clients
        var setup = new SyncSetup("ProductCategory");
        setup.Tables["ProductCategory"].Columns.AddRange("ProductCategoryID", "ParentProductCategoryID", "Name", "rowguid", "ModifiedDate");

        var options = new SyncOptions() { ProgressLevel = SyncProgressLevel.Debug };

        // Sync 2 clients
        var agent = new SyncAgent(clientProvider1, serverProvider, options);
        Console.WriteLine(await agent.SynchronizeAsync(setup, progress: progress));
        var localOrchestrator = agent.LocalOrchestrator;
        var remoteOrchestrator = agent.RemoteOrchestrator;


        // --------------------------
        // Step2 : Adding a new column "CreatedDate datetime NULL" on the server
        await AddColumnsToProductCategoryAsync(serverProvider);

        // Step 2 : Add new table in setup
        setup.Tables.Add("Product");
        // Remove all columns to get a * :D
        setup.Tables["ProductCategory"].Columns.Clear();

        // get existing scope
        var serverScope = await remoteOrchestrator.GetScopeInfoAsync();

        // You don't want to create a new scope, but instead editing the existing one
        // You need to get the new schema from the database containing this new table
        var schema = await remoteOrchestrator.GetSchemaAsync(setup);
        serverScope.Schema = schema;
        serverScope.Setup = setup;

        // You call the ProvisionAsync with an override of true to override all existing stored procs and so on
        // This method will save the server scope as well
        serverScope = await remoteOrchestrator.ProvisionAsync(serverScope, overwrite: true);

        // You call the ProvisionAsync with an override of true to override all existing stored procs and so on
        // This method will save the server scope as well
        var serverScopea = await remoteOrchestrator.ProvisionAsync("a", serverScope.Setup, overwrite: true);


        // Add a product category row on server (just to check we are still able to get this row on clients)
        await AddProductCategoryRowWithOneMoreColumnAsync(serverProvider);

        // --------------------------
        // Step 3 : Add the column to client 1, add the new scope "v1" and sync
        await AddColumnsToProductCategoryAsync(clientProvider1);

        // Step 4 Add product table
        await localOrchestrator.CreateTableAsync(serverScope, "Product");


        agent.LocalOrchestrator.OnConflictingSetup(async args =>
        {
            if (args.ServerScopeInfo != null)
            {
                args.ClientScopeInfo = await localOrchestrator.ProvisionAsync(args.ServerScopeInfo, overwrite: true);

                // this action will let the sync continue
                args.Action = ConflictingSetupAction.Continue;
            }
            else
            {
                // if we raise this step, just and the sync without raising an error
                args.Action = ConflictingSetupAction.Abort;

                // The Rollback Action will raise an error
                // args.Action = ConflictingSetupAction.Rollback;
            }
        });


        Console.WriteLine(await agent.SynchronizeAsync("a", progress: progress));



    }


    //private static async Task ScenarioMigrationRemovingColumnsAsync()
    //{
    //var progress = new SynchronousProgress<ProgressArgs>(s =>
    //{
    //    Console.ForegroundColor = ConsoleColor.Green;
    //    Console.WriteLine($"{s.ProgressPercentage:p}:  \t[{s.Source[..Math.Min(4, s.Source.Length)]}] {s.TypeName}:\t{s.Message}");
    //    Console.ResetColor();

    //});

    //    // Server provider
    //    var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));

    //    // Client 1 provider
    //    var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

    //    // First startup Setup with ALL columns
    //    var setup = new SyncSetup(new string[] { "ProductCategory" });
    //    setup.Tables["ProductCategory"].Columns.AddRange(
    //        new string[] { "ProductCategoryID", "ParentProductCategoryID", "Name", "rowguid", "ModifiedDate" });

    //    var options = new SyncOptions() { ProgressLevel = SyncProgressLevel.Information };

    //    // Creating an agent that will handle all the process
    //    var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

    //    // Make a first sync to get the client schema created and synced
    //    var s = await agent.SynchronizeAsync(progress);
    //    Console.WriteLine(s);

    //    // Step 1 : creating a new scope on server without columns "rowguid", "ModifiedDate"
    //    setup = new SyncSetup(new string[] { "ProductCategory" });
    //    setup.Tables["ProductCategory"].Columns.AddRange(
    //        new string[] { "ProductCategoryID", "ParentProductCategoryID", "Name" });

    //    // Creating a new scope called "V1" on server
    //    var orchestrator = new RemoteOrchestrator(serverProvider, options);
    //    await orchestrator.ProvisionAsync("v1", setup);

    //    // add a product category on server (just to check we are still able to get this row)
    //    await AddProductCategoryRowAsync(serverProvider);

    //    // Optional : Deprovision client old scope
    //    // var clientOrchestrator = new LocalOrchestrator(clientProvider, options, setup);
    //    // We just need to deprovision the stored proc
    //    // await clientOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures);

    //    // Removing columns from client
    //    await RemoveColumnsFromProductCategoryAsync(clientProvider);

    //    // Provision the "v1" scope on the client with the new setup
    //    var clientOrchestrator = new LocalOrchestrator(clientProvider, options);
    //    await clientOrchestrator.ProvisionAsync("v1", setup, SyncProvision.StoredProcedures);

    //    // create a new agent and make a sync on the "v1" scope
    //    agent = new SyncAgent(clientProvider, serverProvider, options, setup, "v1");

    //    var s2 = await agent.SynchronizeAsync(progress);
    //    Console.WriteLine(s2);

    //}


    //private static async Task ScenarioAddColumnSyncAsync(CoreProvider clientProvider, CoreProvider serverProvider, SyncSetup setup, SyncOptions options)
    //{
    //    //var options = new SyncOptions();
    //    // Using the Progress pattern to handle progession during the synchronization
    //    var progress = new SynchronousProgress<ProgressArgs>(s =>
    //    {
    //        Console.ForegroundColor = ConsoleColor.Green;
    //        Console.WriteLine($"{s.ProgressPercentage:p}:  \t[{s.Source[..Math.Min(4, s.Source.Length)]}] {s.TypeName}:\t{s.Message}");
    //        Console.ResetColor();
    //    });


    //    Console.Clear();
    //    Console.WriteLine("Sync start");
    //    try
    //    {
    //        // Creating an agent that will handle all the process
    //        var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

    //        var s = await agent.SynchronizeAsync(SyncType.Normal, progress);
    //        Console.WriteLine(s);

    //        // change Setup to remove a column from Server
    //        setup.Tables["ProductCategory"].Columns.AddRange(new string[] { "ProductCategoryID", "ParentProductCategoryID", "Name", "ModifiedDate" });

    //        // Creating an agent that will handle all the process
    //        agent = new SyncAgent(clientProvider, serverProvider, options, setup);

    //        await AddProductCategoryRowAsync(serverProvider);

    //        s = await agent.SynchronizeAsync(SyncType.Normal, progress);
    //        Console.WriteLine(s);

    //    }
    //    catch (SyncException e)
    //    {
    //        Console.WriteLine(e.ToString());
    //    }
    //    catch (Exception e)
    //    {
    //        Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
    //    }


    //    Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");

    //}

    //private static async Task RemoveColumnsFromProductCategoryAsync(CoreProvider provider)
    //{
    //    var commandText = @"ALTER TABLE dbo.ProductCategory DROP COLUMN rowguid, ModifiedDate;";

    //    var connection = provider.CreateConnection();

    //    connection.Open();

    //    var command = connection.CreateCommand();
    //    command.CommandText = commandText;
    //    command.Connection = connection;

    //    await command.ExecuteNonQueryAsync();

    //    connection.Close();
    //}

    private static async Task AddColumnsToProductCategoryAsync(CoreProvider provider)
    {
        var commandText = @"ALTER TABLE dbo.ProductCategory ADD CreatedDate datetime NULL;";

        var connection = provider.CreateConnection();

        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = commandText;
        command.Connection = connection;

        await command.ExecuteNonQueryAsync();

        connection.Close();
    }

    private static async Task UpdateAllProductCategoryAsync(CoreProvider provider, string addedString)
    {
        string commandText = "Update ProductCategory Set Name = Name + @addedString";
        var connection = provider.CreateConnection();

        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = commandText;
        command.Connection = connection;

        var p = command.CreateParameter();
        p.DbType = DbType.String;
        p.ParameterName = "@addedString";
        p.Value = addedString;
        command.Parameters.Add(p);

        await command.ExecuteNonQueryAsync();

        connection.Close();
    }


    private static async Task AddProductRowAsync(CoreProvider provider)
    {

        string commandText = "Insert into Product (Name, ProductNumber, StandardCost, ListPrice, SellStartDate) Values (@Name, @ProductNumber, @StandardCost, @ListPrice, @SellStartDate)";
        var connection = provider.CreateConnection();

        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = commandText;
        command.Connection = connection;

        var p = command.CreateParameter();
        p.DbType = DbType.String;
        p.ParameterName = "@Name";
        p.Value = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant();
        command.Parameters.Add(p);

        p = command.CreateParameter();
        p.DbType = DbType.String;
        p.ParameterName = "@ProductNumber";
        p.Value = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant().Substring(0, 6).ToUpperInvariant();
        command.Parameters.Add(p);

        p = command.CreateParameter();
        p.DbType = DbType.Double;
        p.ParameterName = "@StandardCost";
        p.Value = 100;
        command.Parameters.Add(p);

        p = command.CreateParameter();
        p.DbType = DbType.Double;
        p.ParameterName = "@ListPrice";
        p.Value = 100;
        command.Parameters.Add(p);

        p = command.CreateParameter();
        p.DbType = DbType.DateTime;
        p.ParameterName = "@SellStartDate";
        p.Value = DateTime.UtcNow;
        command.Parameters.Add(p);

        await command.ExecuteNonQueryAsync();

        connection.Close();

    }

    private static async Task AddProductCategoryRowWithOneMoreColumnAsync(CoreProvider provider)
    {

        string commandText = "Insert into ProductCategory (ProductCategoryId, Name, ModifiedDate, CreatedDate, rowguid) Values (@ProductCategoryId, @Name, @ModifiedDate, @CreatedDate, @rowguid)";
        var connection = provider.CreateConnection();

        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = commandText;
        command.Connection = connection;

        var p = command.CreateParameter();
        p.DbType = DbType.String;
        p.ParameterName = "@Name";
        p.Value = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant();
        command.Parameters.Add(p);

        p = command.CreateParameter();
        p.DbType = DbType.Guid;
        p.ParameterName = "@ProductCategoryId";
        p.Value = Guid.NewGuid();
        command.Parameters.Add(p);

        p = command.CreateParameter();
        p.DbType = DbType.DateTime;
        p.ParameterName = "@ModifiedDate";
        p.Value = DateTime.UtcNow;
        command.Parameters.Add(p);

        p = command.CreateParameter();
        p.DbType = DbType.DateTime;
        p.ParameterName = "@CreatedDate";
        p.Value = DateTime.UtcNow;
        command.Parameters.Add(p);

        p = command.CreateParameter();
        p.DbType = DbType.Guid;
        p.ParameterName = "@rowguid";
        p.Value = Guid.NewGuid();
        command.Parameters.Add(p);

        await command.ExecuteNonQueryAsync();

        connection.Close();

    }


    //private static async Task SynchronizeAsyncAndChangeTrackingKey(CoreProvider clientProvider, CoreProvider serverProvider, SyncSetup setup, SyncOptions options)
    //{
    //    //var options = new SyncOptions();
    //    // Using the Progress pattern to handle progession during the synchronization
    //    var progress = new SynchronousProgress<ProgressArgs>(s =>
    //    {
    //        Console.ForegroundColor = ConsoleColor.Green;
    //        Console.WriteLine($"{s.ProgressPercentage:p}:  \t[{s.Source[..Math.Min(4, s.Source.Length)]}] {s.TypeName}:\t{s.Message}");
    //        Console.ResetColor();
    //    });

    //    setup = new SyncSetup(new string[] { "Product" });

    //    // Creating an agent that will handle all the process
    //    var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

    //    // get scope based on the setup 
    //    // getting the scope will provision the database
    //    // so we will need to deprovision - provision again
    //    var serverScope = await agent.RemoteOrchestrator.GetServerScopeAsync();
    //    // TODO : if serverScope.Schema is null, should we Provision here ?

    //    // [Optional] Create table on client if not exists
    //    await agent.LocalOrchestrator.ProvisionAsync(serverScope, SyncProvision.Table);

    //    // Get client scope
    //    var clientScope = await agent.LocalOrchestrator.GetClientScopeAsync();

    //    // Deprovision the server scope because it has been created
    //    // And we want to replace all the metadatas where PK are used
    //    await agent.RemoteOrchestrator.DeprovisionAsync(
    //        SyncProvision.StoredProcedures |
    //        SyncProvision.TrackingTable |
    //        SyncProvision.Triggers);

    //    // get the schema and create a tmp Fake schema with another primary key
    //    var schema = serverScope.Schema;

    //    // Removing the primary key that is a auto inc column
    //    schema.Tables[0].PrimaryKeys.Clear();

    //    // Removing the primary key as a column as well
    //    // This column will never be synced anymore
    //    schema.Tables[0].Columns.Remove(
    //        serverScope.Schema.Tables[0].Columns.First(c => c.ColumnName == "ProductID"));

    //    // Add the NEW unique identifier as fake primary key
    //    schema.Tables[0].PrimaryKeys.Add("rowguid");

    //    // affect temporary schema for provisioning
    //    serverScope.Schema = schema;
    //    clientScope.Schema = schema;

    //    // Provision
    //    var p = SyncProvision.StoredProcedures | SyncProvision.TrackingTable | SyncProvision.Triggers;

    //    await agent.RemoteOrchestrator.ProvisionAsync(serverScope, p, true);

    //    await agent.LocalOrchestrator.ProvisionAsync(clientScope, p, true);


    //    // This event is raised before selecting the changes for a particular table
    //    // you still can change the DbCommand generated, if you need to
    //    agent.RemoteOrchestrator.OnTableChangesSelecting(tcsa =>
    //    {
    //        Console.WriteLine($"Table {tcsa.SchemaTable.GetFullName()}: " +
    //            $"Selecting rows from datasource {tcsa.Source}");
    //    });

    //    // This event is raised for each row read from the datasource.
    //    // You can change the values of args.SyncRow if you need to.
    //    // this row will be later serialized on disk
    //    agent.RemoteOrchestrator.OnTableChangesSelected(args =>
    //    {
    //        Console.Write(".");
    //    });

    //    //// The table is read. The batch parts infos are generated and already available on disk
    //    //agent.RemoteOrchestrator.OnTableChangesSelected(tcsa =>
    //    //{
    //    //    Console.WriteLine();
    //    //    Console.WriteLine($"Table {tcsa.SchemaTable.GetFullName()}: " +
    //    //        $"Files generated count:{tcsa.BatchPartInfos.Count()}. " +
    //    //        $"Rows Count:{tcsa.TableChangesSelected.TotalChanges}");
    //    //});




    //    // The table is read. The batch parts infos are generated and already available on disk
    //    agent.LocalOrchestrator.OnTableChangesSelected(async tcsa =>
    //    {
    //        foreach (var bpi in tcsa.BatchPartInfos)
    //        {
    //            var table = await tcsa.BatchInfo.LoadBatchPartInfoAsync(bpi);

    //            foreach (var row in table.Rows.ToArray())
    //            {

    //            }

    //            await tcsa.BatchInfo.SaveBatchPartInfoAsync(bpi, table);
    //        }
    //    });

    //    //agent.LocalOrchestrator.OnTableChangesApplyingSyncRows(args =>
    //    //{
    //    //    foreach (var syncRow in args.SyncRows)
    //    //        Console.Write(".");
    //    //});
    //    do
    //    {
    //        Console.Clear();
    //        Console.WriteLine("Sync start");
    //        try
    //        {
    //            var s = await agent.SynchronizeAsync(SyncType.Normal, progress);
    //            Console.WriteLine(s);
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

    //}

    private static async Task ProvisionAsync(CoreProvider serverProvider, SyncSetup setup, SyncOptions options)
    {
        Console.WriteLine($"Provision");

        var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

        try
        {
            Stopwatch stopw = new Stopwatch();
            stopw.Start();

            await remoteOrchestrator.ProvisionAsync();

            stopw.Stop();
            Console.WriteLine($"Total duration :{stopw.Elapsed:hh\\.mm\\:ss\\.fff}");
        }
        catch (Exception e)
        {
            Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
        }
    }

    private static async Task DeprovisionAsync(CoreProvider serverProvider, SyncSetup setup, SyncOptions options)
    {

        Console.WriteLine($"Deprovision ");

        var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

        try
        {
            Stopwatch stopw = new Stopwatch();
            stopw.Start();

            await remoteOrchestrator.DeprovisionAsync();

            stopw.Stop();
            Console.WriteLine($"Total duration :{stopw.Elapsed:hh\\.mm\\:ss\\.fff}");
        }
        catch (Exception e)
        {
            Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
        }
    }

    //private static async Task CreateSnapshotAsync(CoreProvider serverProvider, SyncSetup setup, SyncOptions options)
    //{
    //    var snapshotProgress = new SynchronousProgress<ProgressArgs>(s =>
    //    {
    //        Console.ForegroundColor = ConsoleColor.Blue;
    //        Console.WriteLine($"{s.ProgressPercentage:p}:  \t[{s.Source[..Math.Min(4, s.Source.Length)]}] {s.TypeName}:\t{s.Message}");
    //        Console.ResetColor();
    //    });

    //    Console.WriteLine($"Creating snapshot");

    //    var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

    //    try
    //    {
    //        Stopwatch stopw = new Stopwatch();
    //        stopw.Start();

    //        var bi = await remoteOrchestrator.CreateSnapshotAsync(progress: snapshotProgress);

    //        stopw.Stop();
    //        Console.WriteLine($"Total duration :{stopw.Elapsed:hh\\.mm\\:ss\\.fff}");
    //    }
    //    catch (Exception e)
    //    {
    //        Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
    //    }
    //}


    public static async Task SyncHttpThroughKestrellAsync(CoreProvider clientProvider, CoreProvider serverProvider, SyncSetup setup, SyncOptions options)
    {
        var configureServices = new Action<IServiceCollection>(services =>
        {
            services.AddSyncServer(serverProvider.GetType(), serverProvider.ConnectionString, "DefaultScope", setup, options);
            services.AddSyncServer(serverProvider.GetType(), serverProvider.ConnectionString, "pc", setup, options);
        });

        var serverHandler = new RequestDelegate(async context =>
        {
            try
            {
                var webServerAgents = context.RequestServices.GetService(typeof(IEnumerable<WebServerAgent>)) as IEnumerable<WebServerAgent>;

                var scopeName = context.GetScopeName();
                var clientScopeId = context.GetClientScopeId();

                var webServerAgent = webServerAgents.First(wsa => wsa.ScopeName == scopeName);

                webServerAgent.RemoteOrchestrator.OnGettingOperation(operationArgs =>
                {
                    var syncOperation = SyncOperation.Reinitialize;

                    // this operation will be applied for the current sync
                    operationArgs.Operation = syncOperation;
                });

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

                    var localProgress = new SynchronousProgress<ProgressArgs>(s =>
                    {
                        var tsEnded = TimeSpan.FromTicks(DateTime.Now.Ticks);
                        var tsStarted = TimeSpan.FromTicks(startTime.Ticks);
                        var durationTs = tsEnded.Subtract(tsStarted);

                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine($"{durationTs:mm\\:ss\\.fff} {s.ProgressPercentage:p}:\t{s.Message}");
                        Console.ResetColor();
                    });

                    // create the agent
                    var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                    // make a synchronization to get all rows between backup and now
                    var s = await agent.SynchronizeAsync("pc", progress: localProgress);

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

    //private static async Task AddNewColumn(DbConnection connection,
    //    string tableName, string columnName, string columnType,
    //    string defaultValue = default)
    //{
    //    var command = connection.CreateCommand();
    //    command.CommandText = $"ALTER TABLE {tableName} ADD {columnName} {columnType} NULL {defaultValue}";
    //    command.Connection = connection;
    //    command.CommandType = CommandType.Text;

    //    await connection.OpenAsync();
    //    command.ExecuteNonQuery();
    //    await connection.CloseAsync();


    //}


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

    //private static async Task SynchronizeWithLoggerAsync()
    //{

    //    //docker run -it --name seq -p 5341:80 -e ACCEPT_EULA=Y datalust/seq

    //    // Create 2 Sql Sync providers
    //    var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
    //    var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
    //    //var clientProvider = new SqliteSyncProvider("clientX.db");

    //    var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" });
    //    //var setup = new SyncSetup(new string[] { "Customer" });
    //    //var setup = new SyncSetup(new[] { "Customer" });
    //    //setup.Tables["Customer"].Columns.AddRange(new[] { "CustomerID", "FirstName", "LastName" });


    //    //Log.Logger = new LoggerConfiguration()
    //    //    .Enrich.FromLogContext()
    //    //    .MinimumLevel.Verbose()
    //    //    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
    //    //    .WriteTo.Console()
    //    //    .CreateLogger();

    //    // *) create a console logger
    //    //var loggerFactory = LoggerFactory.Create(builder => { builder.AddConsole().SetMinimumLevel(LogLevel.Trace); });
    //    //var logger = loggerFactory.CreateLogger("Dotmim.Sync");
    //    //options.Logger = logger;

    //    // *) create a seq logger
    //    var loggerFactory = LoggerFactory.Create(builder => { builder.AddSeq().SetMinimumLevel(LogLevel.Debug); });
    //    var logger = loggerFactory.CreateLogger("Dotmim.Sync");


    //    // *) create a serilog logger
    //    //var loggerFactory = LoggerFactory.Create(builder => { builder.AddSerilog().SetMinimumLevel(LogLevel.Trace); });
    //    //var logger = loggerFactory.CreateLogger("SyncAgent");
    //    //options.Logger = logger;

    //    // *) Using Serilog with Seq
    //    //var serilogLogger = new LoggerConfiguration()
    //    //    .Enrich.FromLogContext()
    //    //    .MinimumLevel.Debug()
    //    //    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
    //    //    .WriteTo.Seq("http://localhost:5341")
    //    //    .CreateLogger();


    //    //var actLogging = new Action<SyncLoggerOptions>(slo =>
    //    //{
    //    //    slo.AddConsole();
    //    //    slo.SetMinimumLevel(LogLevel.Information);
    //    //});

    //    ////var loggerFactory = LoggerFactory.Create(builder => builder.AddSerilog().AddConsole().SetMinimumLevel(LogLevel.Information));

    //    //var loggerFactory = LoggerFactory.Create(builder => builder.AddSerilog(serilogLogger));

    //    //loggerFactory.AddSerilog(serilogLogger);

    //    //options.Logger = loggerFactory.CreateLogger("dms");

    //    // 2nd option to add serilog
    //    //var loggerFactorySerilog = new SerilogLoggerFactory();
    //    //var logger = loggerFactorySerilog.CreateLogger<SyncAgent>();
    //    //options.Logger = logger;

    //    //options.Logger = new SyncLogger().AddConsole().AddDebug().SetMinimumLevel(LogLevel.Trace);

    //    //var snapshotDirectory = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "Snapshots");
    //    //options.BatchSize = 500;
    //    //options.SnapshotsDirectory = snapshotDirectory;
    //    //var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);
    //    //remoteOrchestrator.CreateSnapshotAsync().GetAwaiter().GetResult();

    //    var options = new SyncOptions();
    //    options.BatchSize = 500;
    //    options.Logger = logger;
    //    //options.Logger = new SyncLogger().AddConsole().SetMinimumLevel(LogLevel.Debug);


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
    //            // Launch the sync process
    //            //if (!agent.Parameters.Contains("CompanyName"))
    //            //    agent.Parameters.Add("CompanyName", "Professional Sales and Service");

    //            var s1 = await agent.SynchronizeAsync(progress);

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


}
