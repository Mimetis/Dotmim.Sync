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
using NLog.Extensions.Logging;
using NLog.Web;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;
using Dotmim.Sync.SqlServer.Builders;
using Microsoft.Extensions.Options;
using System.Security.Cryptography.X509Certificates;
using Dotmim.Sync.PostgreSql;
using Npgsql;
using MessagePack.Resolvers;
using MessagePack;
using System.Runtime.Serialization;
using System.Reflection.Metadata;
using Microsoft.AspNetCore.Hosting.Server;
using System.Threading;

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

    public static string[] oneTable = new string[] { "ProductCategory" };


    private static async Task Main(string[] args)
    {

        //var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        //var serverProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var serverProvider = new NpgsqlSyncProvider(DBHelper.GetNpgsqlDatabaseConnectionString(serverDbName));
        //var serverProvider = new MariaDBSyncProvider(DBHelper.GetMariadbDatabaseConnectionString(serverDbName));
        // var serverProvider = new MySqlSyncProvider(DBHelper.GetMySqlDatabaseConnectionString(serverDbName));

        //var clientProvider = new SqliteSyncProvider(Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db");
        //var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
        //var clientProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
        var clientProvider = new NpgsqlSyncProvider(DBHelper.GetNpgsqlDatabaseConnectionString(clientDbName));
        //clientProvider.UseBulkOperations = false;
        //var clientProvider = new MariaDBSyncProvider(DBHelper.GetMariadbDatabaseConnectionString(clientDbName));
        //var clientProvider = new MySqlSyncProvider(DBHelper.GetMySqlDatabaseConnectionString(clientDbName));

        //var setup = new SyncSetup(allTables);
        var setup = new SyncSetup(oneTable);

        var options = new SyncOptions();
        options.ErrorResolutionPolicy = ErrorResolution.RetryOneMoreTimeAndThrowOnError;
        //options.CleanFolder = false;
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

        await SynchronizeAsync(clientProvider, serverProvider, setup, options);

        //await AddRemoveRemoveAsync();

        //await SynchronizeWithFiltersJoinsAsync();

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
    //private static async Task SynchronizeWithFiltersJoinsAsync()
    //{
    //    // Create 2 Sql Sync providers
    //    var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
    //    var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

    //    var setup = new SyncSetup("SalesLT.ProductCategory", "SalesLT.Product");

    //    var productCategoryFilter = new SetupFilter("ProductCategory", "SalesLT");
    //    productCategoryFilter.AddParameter("ProductCategoryID", "ProductCategory", "SalesLT");
    //    productCategoryFilter.AddWhere("ProductCategoryID", "ProductCategory", "ProductCategoryID", "SalesLT");
    //    setup.Filters.Add(productCategoryFilter);

    //    var productFilter = new SetupFilter("Product", "SalesLT");
    //    productFilter.AddParameter("ProductCategoryID", "ProductCategory", "SalesLT");
    //    productFilter.AddJoin(Join.Left, "ProductCategory", "SalesLT").On("ProductCategory", "ProductCategoryID", "Product", "ProductCategoryId", "SalesLT", "SalesLT");
    //    productFilter.AddWhere("ProductCategoryID", "ProductCategory", "ProductCategoryID", "SalesLT");
    //    setup.Filters.Add(productFilter);


    //    var options = new SyncOptions();

    //    // Creating an agent that will handle all the process
    //    var agent = new SyncAgent(clientProvider, serverProvider, options);

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
    //            agent.LocalOrchestrator.OnExecuteCommand(eca =>
    //            {
    //                if (eca.CommandType == DbCommandType.UpdateRow || eca.CommandType == DbCommandType.UpdateRows
    //                || eca.CommandType == DbCommandType.InsertRow || eca.CommandType == DbCommandType.InsertRows)
    //                {
    //                    var command = eca.Command;
    //                }
    //            });

    //            var p = new SyncParameters { { "ProductCategoryId", "ROADFR" } };

    //            var s1 = await agent.SynchronizeAsync(setup, p, progress);

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

    private static async Task AddRemoveRemoveAsync()
    {
        // Using the Progress pattern to handle progression during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
            Console.WriteLine($"{s.ProgressPercentage:p}:  " +
            $"\t[{s?.Source[..Math.Min(4, s.Source.Length)]}] " +
            $"{s.TypeName}: {s.Message}"));

        // Server provider
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));

        // Clients 1 & 2 providers
        var clientProvider1 = new SqliteSyncProvider(
            Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db");
        var clientProvider2 = new SqliteSyncProvider(
            Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db");

        var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

        var setup = new SyncSetup(oneTable);

        try
        {
            var agent1 = new SyncAgent(clientProvider1, serverProvider, options);
            var agent2 = new SyncAgent(clientProvider2, serverProvider, options);

            // Sync client 1 to create table and gell all product categories
            var result1 = await agent1.SynchronizeAsync(setup, progress: progress);
            Console.WriteLine(result1);
            // Total changes  uploaded: 0
            // Total changes  downloaded: 42
            // Total changes  applied on client: 42

            // Sync client 2 to create table and get all product categories
            var result2 = await agent2.SynchronizeAsync(setup, progress: progress);
            Console.WriteLine(result2);
            // Total changes  uploaded: 0
            // Total changes  downloaded: 42
            // Total changes  applied on client: 42

            // Add a product category on server
            var productCategoryId = await DBHelper.AddProductCategoryRowAsync(serverProvider);

            // Sync client 1 to get this new created server product category on client 1
            result1 = await agent1.SynchronizeAsync(setup, progress: progress);
            Console.WriteLine(result1);
            // Total changes  uploaded: 0
            // Total changes  downloaded: 1
            // Total changes  applied on client: 1

            // Sync client 2 to get this new created server product category on client 2
            result2 = await agent2.SynchronizeAsync(setup, progress: progress);
            Console.WriteLine(result2);
            // Total changes  uploaded: 0
            // Total changes  downloaded: 1
            // Total changes  applied on client: 1

            // Now delete server product category
            await DBHelper.DeleteProductCategoryRowAsync(serverProvider, productCategoryId);

            // Sync client 1 to sync the deleted product category from server
            result1 = await agent1.SynchronizeAsync(setup, progress: progress);
            Console.WriteLine(result1);
            // Total changes  uploaded: 0
            // Total changes  downloaded: 1
            // Total changes  applied on client: 1

            // Sync client 1 to sync the deleted product category from server
            result2 = await agent2.SynchronizeAsync(setup, progress: progress);
            Console.WriteLine(result2);
            // Total changes  uploaded: 0
            // Total changes  downloaded: 1
            // Total changes  applied on client: 1
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }
        Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");

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
    static async Task ScenarioPluginLogsAsync(CoreProvider clientProvider, CoreProvider serverProvider, SyncSetup setup, SyncOptions options, string scopeName = SyncOptions.DefaultScopeName)
    {
        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options);

        //agent.LocalOrchestrator.OnApplyChangesErrorOccured(args => args.Resolution = ErrorResolution.ContinueOnError);

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

        var parameters = scopeName == "filter" ? new SyncParameters(("ParentProductCategoryID", new Guid("10A7C342-CA82-48D4-8A38-46A2EB089B74"))) : null;

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
                            var reinitResut = await agent.SynchronizeAsync(scopeName, setup, SyncType.Reinitialize, parameters);
                            Console.WriteLine(reinitResut);
                            showHelp();
                            break;
                        default:
                            var r = await agent.SynchronizeAsync(scopeName, setup, parameters);
                            Console.WriteLine(r);
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

        var network = remoteOrchestrator.GetType().Name == "WebRemoteOrchestrator" ? "Http" : "Tcp";

        using (var syncLogContext = new SyncLogsContext(remoteOrchestrator.Provider.ConnectionString))
        {
            await syncLogContext.Database.EnsureCreatedAsync();
            syncLogContext.EnsureTablesCreated();
        }


        var ensureSyncLog = new Func<SyncContext, SyncLogsContext, SyncLog>((syncContext, ctx) =>
        {
            var log = ctx.SyncLog.Find(syncContext.SessionId);

            if (log != null)
                return log;

            log = new SyncLog
            {
                SessionId = syncContext.SessionId,
                ClientScopeId = syncContext.ClientId.Value,
                ScopeParameters = syncContext.Parameters != null ? JsonConvert.SerializeObject(syncContext.Parameters) : null,
                Network = network,
            };

            ctx.SyncLog.Add(log);

            return log;
        });

        var ensureSyncLogTable = new Func<SyncContext, string, SyncLogsContext, SyncLogTable>((syncContext, fullTableName, ctx) =>
        {
            var logTable = ctx.SyncLogTable.Find(syncContext.SessionId, fullTableName);

            if (logTable != null)
                return logTable;

            logTable = new SyncLogTable
            {
                SessionId = syncContext.SessionId,
                ScopeName = syncContext.ScopeName,
                ClientScopeId = syncContext.ClientId.Value,
                TableName = fullTableName,
                ScopeParameters = syncContext.Parameters != null ? JsonConvert.SerializeObject(syncContext.Parameters) : null
            };
            ctx.SyncLogTable.Add(logTable);

            return logTable;
        });


        remoteOrchestrator.OnSessionEnd(args =>
        {
            using var syncLogContext = new SyncLogsContext(remoteOrchestrator.Provider.ConnectionString);
            //syncLogContext.Database.UseTransaction(args.Transaction);

            var log = ensureSyncLog(args.Context, syncLogContext);

            log.ScopeName = args.Context.ScopeName;
            log.SyncType = args.Context.SyncType;

            log.StartTime = args.SyncResult.StartTime;
            log.EndTime = args.SyncResult.CompleteTime;


            if (args.SyncResult.ChangesAppliedOnServer != null && args.SyncResult.ChangesAppliedOnServer.TableChangesApplied != null && args.SyncResult.ChangesAppliedOnServer.TableChangesApplied.Count > 0)
                log.ChangesAppliedOnServer = JsonConvert.SerializeObject(args.SyncResult?.ChangesAppliedOnServer);
            else
                log.ChangesAppliedOnServer = null;

            if (args.SyncResult.ChangesAppliedOnClient != null && args.SyncResult.ChangesAppliedOnClient.TableChangesApplied != null && args.SyncResult.ChangesAppliedOnClient.TableChangesApplied.Count > 0)
                log.ChangesAppliedOnClient = JsonConvert.SerializeObject(args.SyncResult?.ChangesAppliedOnClient);
            else
                log.ChangesAppliedOnClient = null;

            if (args.SyncResult.ClientChangesSelected != null && args.SyncResult.ClientChangesSelected.TableChangesSelected != null && args.SyncResult.ClientChangesSelected.TableChangesSelected.Count > 0)
                log.ClientChangesSelected = JsonConvert.SerializeObject(args.SyncResult?.ClientChangesSelected);
            else
                log.ClientChangesSelected = null;

            if (args.SyncResult.ServerChangesSelected != null && args.SyncResult.ServerChangesSelected.TableChangesSelected != null && args.SyncResult.ServerChangesSelected.TableChangesSelected.Count > 0)
                log.ServerChangesSelected = JsonConvert.SerializeObject(args.SyncResult?.ServerChangesSelected);
            else
                log.ServerChangesSelected = null;

            if (args.SyncResult.SnapshotChangesAppliedOnClient != null && args.SyncResult.SnapshotChangesAppliedOnClient.TableChangesApplied != null && args.SyncResult.ServerChangesSelected.TableChangesSelected.Count > 0)
                log.SnapshotChangesAppliedOnClient = JsonConvert.SerializeObject(args.SyncResult?.SnapshotChangesAppliedOnClient);
            else
                log.SnapshotChangesAppliedOnClient = null;


            if (args.SyncException != null)
            {
                log.State = "Error";
                log.Error = args.SyncException.Message;
            }
            else
            {
                if (args.SyncResult?.TotalChangesFailedToApplyOnClient > 0 || args.SyncResult?.TotalChangesFailedToApplyOnServer > 0)
                    log.State = "Partial";
                else
                    log.State = "Success";
            }


            syncLogContext.SaveChanges();
        });

        remoteOrchestrator.OnDatabaseChangesApplying(args =>
        {
            using var syncLogContext = new SyncLogsContext(remoteOrchestrator.Provider.ConnectionString);
            //syncLogContext.Database.UseTransaction(args.Transaction);

            var log = ensureSyncLog(args.Context, syncLogContext);

            log.StartTime = args.Context.StartTime;
            log.ScopeName = args.Context.ScopeName;
            log.SyncType = args.Context.SyncType;
            log.ScopeParameters = args.Context.Parameters != null ? JsonConvert.SerializeObject(args.Context.Parameters) : null;
            log.State = "DatabaseApplyingChanges";

            syncLogContext.SaveChanges();

        });

        remoteOrchestrator.OnDatabaseChangesApplied(args =>
        {
            using var syncLogContext = new SyncLogsContext(remoteOrchestrator.Provider.ConnectionString);
            //syncLogContext.Database.UseTransaction(args.Transaction);

            var log = ensureSyncLog(args.Context, syncLogContext);

            log.ChangesAppliedOnServer = args.ChangesApplied != null ? JsonConvert.SerializeObject(args.ChangesApplied) : null;
            log.State = "DatabaseChangesApplied";

            syncLogContext.SaveChanges();


        });

        remoteOrchestrator.OnDatabaseChangesSelecting(args =>
        {
            using var syncLogContext = new SyncLogsContext(remoteOrchestrator.Provider.ConnectionString);
            //syncLogContext.Database.UseTransaction(args.Transaction);

            var log = ensureSyncLog(args.Context, syncLogContext);

            log.StartTime = args.Context.StartTime;
            log.ScopeName = args.Context.ScopeName;
            log.FromTimestamp = args.FromTimestamp;
            log.ToTimestamp = args.ToTimestamp;
            log.SyncType = args.Context.SyncType;
            log.IsNew = args.IsNew;
            log.State = "DatabaseChangesSelecting";

            syncLogContext.SaveChanges();

        });

        remoteOrchestrator.OnDatabaseChangesSelected(args =>
        {
            using var syncLogContext = new SyncLogsContext(remoteOrchestrator.Provider.ConnectionString);
            ////syncLogContext.Database.UseTransaction(args.Transaction);

            var log = ensureSyncLog(args.Context, syncLogContext);

            log.ServerChangesSelected = args.ChangesSelected != null ? JsonConvert.SerializeObject(args.ChangesSelected) : null;

            syncLogContext.SaveChanges();
        });

        remoteOrchestrator.OnTableChangesSelecting(args =>
        {
            using var syncLogContext = new SyncLogsContext(remoteOrchestrator.Provider.ConnectionString);
            //syncLogContext.Database.UseTransaction(args.Transaction);

            var logTable = ensureSyncLogTable(args.Context, args.SchemaTable.GetFullName(), syncLogContext);

            logTable.State = "TableChangesSelecting";
            logTable.Command = args.Command.CommandText;
            syncLogContext.SaveChanges();
        });

        remoteOrchestrator.OnTableChangesSelected(args =>
        {
            using var syncLogContext = new SyncLogsContext(remoteOrchestrator.Provider.ConnectionString);
            //syncLogContext.Database.UseTransaction(args.Transaction);

            var logTable = ensureSyncLogTable(args.Context, args.SchemaTable.GetFullName(), syncLogContext);

            logTable.State = "TableChangesSelected";
            logTable.TableChangesSelected = args.TableChangesSelected != null ? JsonConvert.SerializeObject(args.TableChangesSelected) : null;
            syncLogContext.SaveChanges();

        });


        remoteOrchestrator.OnTableChangesApplied(args =>
        {
            using var syncLogContext = new SyncLogsContext(remoteOrchestrator.Provider.ConnectionString);
            // syncLogContext.Database.UseTransaction(args.Transaction);

            var fullName = string.IsNullOrEmpty(args.TableChangesApplied.SchemaName) ? args.TableChangesApplied.TableName : $"{args.TableChangesApplied.SchemaName}.{args.TableChangesApplied.TableName}";

            var logTable = ensureSyncLogTable(args.Context, fullName, syncLogContext);

            logTable.State = "TableChangesApplied";

            if (args.TableChangesApplied.State == SyncRowState.Modified)
                logTable.TableChangesUpsertsApplied = args.TableChangesApplied != null ? JsonConvert.SerializeObject(args.TableChangesApplied) : null;
            else if (args.TableChangesApplied.State == SyncRowState.Deleted)
                logTable.TableChangesDeletesApplied = args.TableChangesApplied != null ? JsonConvert.SerializeObject(args.TableChangesApplied) : null;

            syncLogContext.SaveChanges();

        });


    }
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
