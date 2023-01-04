using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.MariaDB;
using Dotmim.Sync.MySql;
using Dotmim.Sync.PostgreSql;
using Dotmim.Sync.Serialization;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.SqlServer.Manager;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
#if NET5_0 || NET6_0 || NET7_0 || NETCOREAPP3_1
using MySqlConnector;
#elif NETCOREAPP2_1
using MySql.Data.MySqlClient;
#endif
using Newtonsoft.Json;
using Npgsql;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Dotmim.Sync.Tests
{
    //[TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    public abstract partial class TcpTests : IClassFixture<HelperProvider>, IDisposable
    {
        private Stopwatch stopwatch;

        /// <summary>
        /// Gets the sync tables involved in the tests
        /// </summary>
        public abstract string[] Tables { get; }

        /// <summary>
        /// Gets the clients type we want to tests
        /// </summary>
        public abstract List<ProviderType> ClientsType { get; }

        /// <summary>
        /// Gets the server type we want to test
        /// </summary>
        public abstract ProviderType ServerType { get; }

        /// <summary>
        /// Get the server rows count
        /// </summary>
        public abstract int GetServerDatabaseRowsCount((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) t);

        /// <summary>
        /// Create a provider
        /// </summary>
        public CoreProvider CreateProvider(ProviderType providerType, string dbName)
        {
            var cs = HelperDatabase.GetConnectionString(providerType, dbName);
            return providerType switch
            {
                ProviderType.MySql => new MySqlSyncProvider(cs),
                ProviderType.MariaDB => new MariaDBSyncProvider(cs),
                ProviderType.Sqlite => new SqliteSyncProvider(cs),
                ProviderType.Postgres => new NpgsqlSyncProvider(cs),
                _ => new SqlSyncProvider(cs),
            };
        }
        /// <summary>
        /// Create database, seed it, with or without schema
        /// </summary>
        /// <param name="t"></param>
        /// <param name="useSeeding"></param>
        /// <param name="useFallbackSchema"></param>
        public abstract Task EnsureDatabaseSchemaAndSeedAsync((string DatabaseName,
            ProviderType ProviderType, CoreProvider Provider) t, bool useSeeding = false, bool useFallbackSchema = false);

        /// <summary>
        /// Create an empty database
        /// </summary>
        public abstract Task CreateDatabaseAsync(ProviderType providerType, string dbName, bool recreateDb = true);

        // abstract fixture used to run the tests
        protected readonly HelperProvider fixture;

        // Current test running
        private XunitTest test;

        /// <summary>
        /// Gets the remote orchestrator and its database name
        /// </summary>
        public (string DatabaseName, ProviderType ProviderType, CoreProvider Provider) Server { get; private set; }

        /// <summary>
        /// Gets the dictionary of all local orchestrators with database name as key
        /// </summary>
        public List<(string DatabaseName, ProviderType ProviderType, CoreProvider Provider)> Clients { get; set; }

        /// <summary>
        /// Gets a bool indicating if we should generate the schema for tables
        /// </summary>
        public bool UseFallbackSchema => ServerType == ProviderType.Sql;

        public ITestOutputHelper Output { get; }

        /// <summary>
        /// Output to console and debug the current state
        /// </summary>
        public void OutputCurrentState(string subCategory = null)
        {
            var t = string.IsNullOrEmpty(subCategory) ? "" : $" - {subCategory}";
            t = $"{this.test.TestCase.Method.Name}{t}: {this.stopwatch.Elapsed.Minutes}:{this.stopwatch.Elapsed.Seconds}.{this.stopwatch.Elapsed.Milliseconds}";
            Console.WriteLine(t);
            Debug.WriteLine(t);
        }

        /// <summary>
        /// For each test, Create a server database and some clients databases, depending on ProviderType provided in concrete class
        /// </summary>
        public TcpTests(HelperProvider fixture, ITestOutputHelper output)
        {
            // Getting the test running
            this.Output = output;
            var type = output.GetType();
            var testMember = type.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
            this.test = (XunitTest)testMember.GetValue(output);
            this.stopwatch = Stopwatch.StartNew();

            this.fixture = fixture;

            // Since we are creating a lot of databases
            // each database will have its own pool
            // Droping database will not clear the pool associated
            // So clear the pools on every start of a new test
            SqlConnection.ClearAllPools();
            MySqlConnection.ClearAllPools();
            NpgsqlConnection.ClearAllPools();


            // get the server provider (and db created) without seed
            var serverDatabaseName = HelperDatabase.GetRandomName("tcp_sv_");

            // create remote orchestrator
            var serverProvider = this.CreateProvider(this.ServerType, serverDatabaseName);

            this.Server = (serverDatabaseName, this.ServerType, serverProvider);

            // Get all clients providers
            Clients = new List<(string DatabaseName, ProviderType ProviderType, CoreProvider provider)>(this.ClientsType.Count);

            // Generate Client database
            foreach (var clientType in this.ClientsType)
            {
                var dbCliName = HelperDatabase.GetRandomName("tcp_cli_");
                var localProvider = this.CreateProvider(clientType, dbCliName);
                this.Clients.Add((dbCliName, clientType, localProvider));
            }
        }

        /// <summary>
        /// Drop all databases used for the tests
        /// </summary>
        public void Dispose()
        {
            try
            {
                // Clear pool to be able to delete the database file
                SqliteConnection.ClearAllPools();
                GC.Collect();
                GC.WaitForPendingFinalizers();
                HelperDatabase.DropDatabase(this.ServerType, Server.DatabaseName);

                foreach (var client in Clients)
                    HelperDatabase.DropDatabase(client.ProviderType, client.DatabaseName);
            }
            catch (Exception) { }
            finally { }

            this.stopwatch.Stop();

            OutputCurrentState();

        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public virtual async Task Rows_Schema(SyncOptions options)
        {
            // create a server db and seed it
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // -----------------------------------
            // Checking Rows count
            // --------------------------------

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            var setup = new SyncSetup(this.Tables)
            { StoredProceduresPrefix = "cli", StoredProceduresSuffix = "", TrackingTablesPrefix = "tr" };

            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(rowsCount, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
            }

            OutputCurrentState("Total Rows");

            // -----------------------------------
            // Checking schema consistency
            // --------------------------------

            // Schema Check: Check we have a schema on client side that is equal (almost) to the server schema
            foreach (var client in Clients)
            {
                // Check we have the correct columns replicated
                using var clientConnection = client.Provider.CreateConnection();
                await clientConnection.OpenAsync();

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                // force to get schema from database by calling the GetSchemaAsync (that will not read the ScopInfo record, but will make a full read of the database schema)
                // The schema get here is not serialized / deserialiazed, like the remote schema (loaded from database)
                var clientSchema = await agent.LocalOrchestrator.GetSchemaAsync(setup);

                var serverScope = await agent.RemoteOrchestrator.GetScopeInfoAsync();
                var serverSchema = serverScope.Schema;

                foreach (var setupTable in setup.Tables)
                {
                    var clientTable = client.ProviderType == ProviderType.Sql ? clientSchema.Tables[setupTable.TableName, setupTable.SchemaName] : clientSchema.Tables[setupTable.TableName];
                    var serverTable = serverSchema.Tables[setupTable.TableName, setupTable.SchemaName];

                    Assert.Equal(clientTable.Columns.Count, serverTable.Columns.Count);

                    foreach (var serverColumn in serverTable.Columns)
                    {
                        var clientColumn = clientTable.Columns.FirstOrDefault(c => c.ColumnName == serverColumn.ColumnName);

                        Assert.NotNull(clientColumn);

                        if (this.ServerType == client.ProviderType && this.ServerType == ProviderType.Sql)
                        {
                            Assert.Equal(serverColumn.DataType, clientColumn.DataType);
                            Assert.Equal(serverColumn.IsUnicode, clientColumn.IsUnicode);
                            Assert.Equal(serverColumn.IsUnsigned, clientColumn.IsUnsigned);

                            var maxPrecision = Math.Min(SqlDbMetadata.PRECISION_MAX, serverColumn.Precision);
                            var maxScale = Math.Min(SqlDbMetadata.SCALE_MAX, serverColumn.Scale);

                            // dont assert max length since numeric reset this value
                            //Assert.Equal(serverColumn.MaxLength, clientColumn.MaxLength);

                            Assert.Equal(maxPrecision, clientColumn.Precision);
                            Assert.Equal(serverColumn.PrecisionIsSpecified, clientColumn.PrecisionIsSpecified);
                            Assert.Equal(maxScale, clientColumn.Scale);
                            Assert.Equal(serverColumn.ScaleIsSpecified, clientColumn.ScaleIsSpecified);

                            Assert.Equal(serverColumn.DefaultValue, clientColumn.DefaultValue);
                            Assert.Equal(serverColumn.ExtraProperty1, clientColumn.ExtraProperty1);
                            Assert.Equal(serverColumn.OriginalDbType, clientColumn.OriginalDbType);

                            // We don't replicate unique indexes
                            //Assert.Equal(serverColumn.IsUnique, clientColumn.IsUnique);

                            Assert.Equal(serverColumn.AutoIncrementSeed, clientColumn.AutoIncrementSeed);
                            Assert.Equal(serverColumn.AutoIncrementStep, clientColumn.AutoIncrementStep);
                            Assert.Equal(serverColumn.IsAutoIncrement, clientColumn.IsAutoIncrement);

                            //Assert.Equal(serverColumn.OriginalTypeName, clientColumn.OriginalTypeName);

                            // IsCompute is not replicated, because we are not able to replicate formulat
                            // Instead, we are allowing null for the column
                            //Assert.Equal(serverColumn.IsCompute, clientColumn.IsCompute);

                            // Readonly is not replicated, because we are not able to replicate formulat
                            // Instead, we are allowing null for the column
                            //Assert.Equal(serverColumn.IsReadOnly, clientColumn.IsReadOnly);

                            // Decimal is conflicting with Numeric
                            //Assert.Equal(serverColumn.DbType, clientColumn.DbType);

                            Assert.Equal(serverColumn.Ordinal, clientColumn.Ordinal);
                            Assert.Equal(serverColumn.AllowDBNull, clientColumn.AllowDBNull);
                        }

                        Assert.Equal(serverColumn.ColumnName, clientColumn.ColumnName);

                    }

                }
                clientConnection.Close();

            }

            OutputCurrentState("Schema");

            // Trying a multi scopes sync and check result count
            // Rows count applied is different from Rows count downloaded
            // as we have two tables that can't be updated since it's only primary keys
            // PricesListCategory & PostTag
            using var serverDbCtx = new AdventureWorksContext(this.Server);
            var pricesListCategoriesCount = serverDbCtx.PricesListCategory.Count();
            var postTagsCount = serverDbCtx.PostTag.Count();
            var notUpdatedOnClientsCount = pricesListCategoriesCount + postTagsCount;

            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync("v1", setup);
                var clientRowsCount = this.GetServerDatabaseRowsCount(client);
                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(rowsCount - notUpdatedOnClientsCount, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(rowsCount, clientRowsCount);

                var s2 = await agent.SynchronizeAsync("v2", setup);

                clientRowsCount = this.GetServerDatabaseRowsCount(client);
                Assert.Equal(rowsCount, s2.TotalChangesDownloadedFromServer);
                Assert.Equal(rowsCount - notUpdatedOnClientsCount, s2.TotalChangesAppliedOnClient);
                Assert.Equal(0, s2.TotalChangesUploadedToServer);
                Assert.Equal(rowsCount, clientRowsCount);

            }

            OutputCurrentState("Multi Scopes Rows Count");

            // -----------------------------------
            // Checking data consistency
            // --------------------------------

            foreach (var client in this.Clients)
            {
                using var ctxServer = new AdventureWorksContext(this.Server);
                using var ctxClient = new AdventureWorksContext(client, this.UseFallbackSchema);

                var serverSaleHeaders = ctxServer.SalesOrderHeader.AsNoTracking().ToList();
                var clientSaleHeaders = ctxClient.SalesOrderHeader.AsNoTracking().ToList();

                foreach (var clientSaleHeader in clientSaleHeaders)
                {
                    var serverSaleHeader = serverSaleHeaders.First(h => h.SalesOrderId == clientSaleHeader.SalesOrderId);

                    // decimal
                    Assert.Equal(clientSaleHeader.SubTotal, serverSaleHeader.SubTotal);
                    Assert.Equal(clientSaleHeader.Freight, serverSaleHeader.Freight);
                    Assert.Equal(clientSaleHeader.TaxAmt, serverSaleHeader.TaxAmt);
                    // string
                    Assert.Equal(clientSaleHeader.Comment, serverSaleHeader.Comment);
                    Assert.Equal(clientSaleHeader.AccountNumber, serverSaleHeader.AccountNumber);
                    Assert.Equal(clientSaleHeader.CreditCardApprovalCode, serverSaleHeader.CreditCardApprovalCode);
                    Assert.Equal(clientSaleHeader.PurchaseOrderNumber, serverSaleHeader.PurchaseOrderNumber);
                    Assert.Equal(clientSaleHeader.SalesOrderNumber, serverSaleHeader.SalesOrderNumber);
                    // int
                    Assert.Equal(clientSaleHeader.BillToAddressId, serverSaleHeader.BillToAddressId);
                    Assert.Equal(clientSaleHeader.SalesOrderId, serverSaleHeader.SalesOrderId);
                    Assert.Equal(clientSaleHeader.ShipToAddressId, serverSaleHeader.ShipToAddressId);
                    // guid
                    Assert.Equal(clientSaleHeader.CustomerId, serverSaleHeader.CustomerId);
                    Assert.Equal(clientSaleHeader.Rowguid, serverSaleHeader.Rowguid);
                    // bool
                    Assert.Equal(clientSaleHeader.OnlineOrderFlag, serverSaleHeader.OnlineOrderFlag);
                    // short
                    Assert.Equal(clientSaleHeader.RevisionNumber, serverSaleHeader.RevisionNumber);

                    // Check DateTime DateTimeOffset
                    Assert.Equal(clientSaleHeader.ShipDate, serverSaleHeader.ShipDate);
                    Assert.Equal(clientSaleHeader.OrderDate, serverSaleHeader.OrderDate);
                    Assert.Equal(clientSaleHeader.DueDate, serverSaleHeader.DueDate);
                    Assert.Equal(clientSaleHeader.ModifiedDate, serverSaleHeader.ModifiedDate);
                }

            }

            OutputCurrentState("Data Consistency");

            // -----------------------------------
            // Drop All 
            // --------------------------------

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // get clientScope for further check
                var clientScope = await localOrchestrator.GetScopeInfoAsync();

                // try to drop everything from local database
                await localOrchestrator.DropAllAsync();

                Assert.False(await localOrchestrator.ExistScopeInfoTableAsync());

                // get the db manager
                foreach (var setupTable in setup.Tables)
                {
                    Assert.False(await localOrchestrator.ExistTrackingTableAsync(clientScope, setupTable.TableName, setupTable.SchemaName));

                    Assert.False(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Delete));
                    Assert.False(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Insert));
                    Assert.False(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Update));

                    if (client.ProviderType == ProviderType.Sql)
                    {
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkDeleteRows));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkTableType));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkUpdateRows));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteMetadata));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteRow));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.Reset));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChanges));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChanges));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectRow));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.UpdateRow));

                        // No filters here
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChangesWithFilters));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChangesWithFilters));
                    }

                }

                // get clientScope for further check
                var serverScope = await remoteOrchestrator.GetScopeInfoAsync();

                // try to drop everything from local database
                await remoteOrchestrator.DropAllAsync();

                Assert.False(await remoteOrchestrator.ExistScopeInfoTableAsync());
                Assert.False(await remoteOrchestrator.ExistScopeInfoClientTableAsync());

                // get the db manager
                foreach (var setupTable in setup.Tables)
                {
                    Assert.False(await remoteOrchestrator.ExistTrackingTableAsync(serverScope, setupTable.TableName, setupTable.SchemaName));

                    Assert.False(await remoteOrchestrator.ExistTriggerAsync(serverScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Delete));
                    Assert.False(await remoteOrchestrator.ExistTriggerAsync(serverScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Insert));
                    Assert.False(await remoteOrchestrator.ExistTriggerAsync(serverScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Update));

                    if (client.ProviderType == ProviderType.Sql)
                    {
                        Assert.False(await remoteOrchestrator.ExistStoredProcedureAsync(serverScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkDeleteRows));
                        Assert.False(await remoteOrchestrator.ExistStoredProcedureAsync(serverScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkTableType));
                        Assert.False(await remoteOrchestrator.ExistStoredProcedureAsync(serverScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkUpdateRows));
                        Assert.False(await remoteOrchestrator.ExistStoredProcedureAsync(serverScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteMetadata));
                        Assert.False(await remoteOrchestrator.ExistStoredProcedureAsync(serverScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteRow));
                        Assert.False(await remoteOrchestrator.ExistStoredProcedureAsync(serverScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.Reset));
                        Assert.False(await remoteOrchestrator.ExistStoredProcedureAsync(serverScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChanges));
                        Assert.False(await remoteOrchestrator.ExistStoredProcedureAsync(serverScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChanges));
                        Assert.False(await remoteOrchestrator.ExistStoredProcedureAsync(serverScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectRow));
                        Assert.False(await remoteOrchestrator.ExistStoredProcedureAsync(serverScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.UpdateRow));

                        // No filters here
                        Assert.False(await remoteOrchestrator.ExistStoredProcedureAsync(serverScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChangesWithFilters));
                        Assert.False(await remoteOrchestrator.ExistStoredProcedureAsync(serverScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChangesWithFilters));
                    }

                }


            }

            OutputCurrentState("Drop All");

        }

        /// <summary>
        /// Check a bad connection should raise correct error
        /// </summary>
        [Fact]
        public async Task Bad_ConnectionString_ShouldRaiseError()
        {
            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            var beforeConnectionString = Server.Provider.ConnectionString;
            // change the remote orchestrator connection string
            Server.Provider.ConnectionString = $@"Server=unknown;Database=unknown;UID=sa;PWD=unknown";

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider);

                var onReconnect = new Action<ReConnectArgs>(args =>
                    Console.WriteLine($"[Retry Connection] Can't connect to database {args.Connection?.Database}. " +
                    $"Retry N°{args.Retry}. " +
                    $"Waiting {args.WaitingTimeSpan.Milliseconds}. Exception:{args.HandledException.Message}."));

                agent.LocalOrchestrator.OnReConnect(onReconnect);
                agent.RemoteOrchestrator.OnReConnect(onReconnect);

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync(this.Tables);
                });
            }

            OutputCurrentState("From Server");

            Server.Provider.ConnectionString = beforeConnectionString;

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var str = $"{test.TestCase.DisplayName}. Client {client.ProviderType}";
                Console.WriteLine(str);
                // change the local orchestrator connection string
                // Set a connection string that will faile everywhere (event Sqlite)
                client.Provider.ConnectionString = $@"Data Source=D;";

                var agent = new SyncAgent(client.Provider, Server.Provider);

                var onReconnect = new Action<ReConnectArgs>(args =>
                    Console.WriteLine($"[Retry Connection] Can't connect to database {args.Connection?.Database}. Retry N°{args.Retry}. Waiting {args.WaitingTimeSpan.Milliseconds}. Exception:{args.HandledException.Message}."));

                agent.LocalOrchestrator.OnReConnect(onReconnect);
                agent.RemoteOrchestrator.OnReConnect(onReconnect);


                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync(this.Tables);
                });
            }

            OutputCurrentState("From Client");

        }


        [Fact]
        public async Task Bad_Schema_ShouldRaiseError()
        {
            string tableTestCreationScript = "create table tabletest (testid int, testname varchar(50))";

            // create a server db without seed
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Create the table on the server
            await HelperDatabase.ExecuteScriptAsync(this.Server.ProviderType, this.Server.DatabaseName, tableTestCreationScript);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider);

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync(new string[] { "tabletest" });
                });

                Assert.Equal("MissingPrimaryKeyException", se.TypeName);

                await agent.LocalOrchestrator.DropAllAsync();
            }

            OutputCurrentState("Missing Primary Key");

            // Create setup
            var setup = new SyncSetup(Tables);

            // Add a malformatted column name
            setup.Tables["Employee"].Columns.AddRange(new string[] { "EmployeeID", "FirstName", "LastNam" });

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider);

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync(setup);
                });

                Assert.Equal("MissingColumnException", se.TypeName);

                await agent.LocalOrchestrator.DropAllAsync();
            }

            OutputCurrentState("Missing Column");

            // Create setup
            setup = new SyncSetup(Tables);

            // Add a fake table to setup tables
            setup.Tables.Add("WeirdTable");

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider);

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync("2", setup);
                });

                Assert.Equal("MissingTableException", se.TypeName);
            }

            OutputCurrentState("Missing Table");

        }


        /// <summary>
        /// Insert one row on server, should be correctly sync on all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Inserts(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var s = await agent.SynchronizeAsync(Tables);
            }
            // -----------------------------------
            // Insert one row on server side
            // --------------------------------
            // Create a new product on server
            var name = HelperDatabase.GetRandomName();
            var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);
            var product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };

            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                serverDbCtx.Product.Add(product);
                await serverDbCtx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.Equal(1, this.GetServerDatabaseRowsCount(client));
            }

            OutputCurrentState("Insert One Row On Server");

            // -----------------------------------
            // Insert one row in two tables on server side
            // --------------------------------

            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var productName = HelperDatabase.GetRandomName();
                productNumber = productName.ToUpperInvariant().Substring(0, 10);

                var productCategoryName = HelperDatabase.GetRandomName();
                var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);

                product = new Product { ProductId = Guid.NewGuid(), Name = productName, ProductNumber = productNumber };
                ctx.Add(product);

                await ctx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(2, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.Equal(this.GetServerDatabaseRowsCount(Server), this.GetServerDatabaseRowsCount(client));
            }

            OutputCurrentState("Insert One Row In Two Tables On Server");

            // -----------------------------------
            // Insert one row on server side, then update it before sync
            // --------------------------------
            // Create a new product on server
            name = HelperDatabase.GetRandomName().ToLowerInvariant();
            var nameUpdated = HelperDatabase.GetRandomName().ToLowerInvariant();
            productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);
            var productId = Guid.NewGuid();

            product = new Product { ProductId = productId, Name = name, ProductNumber = productNumber };

            // Add Product
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                serverDbCtx.Product.Add(product);
                await serverDbCtx.SaveChangesAsync();
            }

            // Then Update it
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var pc = await serverDbCtx.Product.SingleAsync(o => o.ProductId == productId);
                pc.Name = nameUpdated;
                await serverDbCtx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.Equal(this.GetServerDatabaseRowsCount(Server), this.GetServerDatabaseRowsCount(client));
            }

            OutputCurrentState("Insert Then Update One Row On Server");

            // -----------------------------------
            // Insert one row on client side
            // --------------------------------

            // Insert one line on each client
            foreach (var client in Clients)
            {
                var productClientname = HelperDatabase.GetRandomName();
                var productClientNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var productClient = new Product { ProductId = Guid.NewGuid(), Name = productClientname, ProductNumber = productClientNumber };

                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                ctx.Product.Add(productClient);
                await ctx.SaveChangesAsync();
            }

            // Sync all clients
            // First client  will upload one line and will download nothing
            // Second client will upload one line and will download one line
            // thrid client  will upload one line and will download two lines
            int download = 0;
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                Assert.Equal(this.GetServerDatabaseRowsCount(Server), this.GetServerDatabaseRowsCount(client));
            }

            OutputCurrentState("Insert One Row On Client");

            // -----------------------------------
            // Insert one row in two tables on client side
            // --------------------------------

            // Now sync again to be sure all clients have all lines
            foreach (var client in Clients)
                await new SyncAgent(client.Provider, Server.Provider, options).SynchronizeAsync();

            // Insert one line on each client
            foreach (var client in Clients)
            {
                var productCategoryName = HelperDatabase.GetRandomName();
                var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                productId = Guid.NewGuid();
                var productName = HelperDatabase.GetRandomName();
                productNumber = productName.ToUpperInvariant().Substring(0, 10);

                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);
                product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber, ProductCategoryId = productCategoryId };
                ctx.Add(product);
                await ctx.SaveChangesAsync();
            }

            // Sync all clients
            // First client  will upload two lines and will download nothing
            // Second client will upload two lines and will download two lines
            // thrid client  will upload two lines and will download four lines
            download = 0;
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download, s.TotalChangesDownloadedFromServer);
                Assert.Equal(2, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
                download += 2;
                Assert.Equal(this.GetServerDatabaseRowsCount(Server), this.GetServerDatabaseRowsCount(client));
            }

            // Now sync again to be sure all clients have all lines
            foreach (var client in Clients)
                await new SyncAgent(client.Provider, Server.Provider, options).SynchronizeAsync();

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var productRowCount = await ctx.Product.AsNoTracking().CountAsync();
                var productCategoryCount = await ctx.ProductCategory.AsNoTracking().CountAsync();

                foreach (var client in Clients)
                {
                    using var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
                    var pCount = await cliCtx.Product.AsNoTracking().CountAsync();
                    Assert.Equal(productRowCount, pCount);

                    var pcCount = await cliCtx.ProductCategory.AsNoTracking().CountAsync();
                    Assert.Equal(productCategoryCount, pcCount);
                }
            }

            OutputCurrentState("Insert One Row In Two Tables On Client");

            // -----------------------------------
            // Insert 1000 rows in on client side
            // --------------------------------

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
                await new SyncAgent(client.Provider, Server.Provider, options).SynchronizeAsync(Tables);

            // Insert one thousand lines on each client
            foreach (var client in Clients)
            {
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);

                for (var i = 0; i < 1000; i++)
                {
                    name = HelperDatabase.GetRandomName();
                    productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);
                    product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };
                    ctx.Product.Add(product);
                }
                await ctx.SaveChangesAsync();
            }

            // Sync all clients
            // First client  will upload 1000 lines and will download nothing
            // Second client will upload 1000 lines and will download 1000 lines
            // Third client  will upload 1000 line and will download 3000 lines
            download = 0;
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download * 1000, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1000, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                download++;

                var innerRowsCount = this.GetServerDatabaseRowsCount(this.Server);
                Assert.Equal(innerRowsCount, this.GetServerDatabaseRowsCount(client));
            }

            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);
            foreach (var client in Clients)
            {
                await new SyncAgent(client.Provider, Server.Provider, options).SynchronizeAsync();
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
            }

            OutputCurrentState("Insert 1000 Rows On Client");


            // -----------------------------------
            // Insert And Delete Rows on server
            // --------------------------------
            var productId1 = Guid.NewGuid();
            var productId2 = Guid.NewGuid();

            // Add 2 Products on server
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {

                // Create two products on server
                name = HelperDatabase.GetRandomName().ToLowerInvariant();
                productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                product = new Product { ProductId = productId1, Name = name, ProductNumber = productNumber };
                serverDbCtx.Product.Add(product);

                name = HelperDatabase.GetRandomName().ToLowerInvariant();
                productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var product2 = new Product { ProductId = productId2, Name = name, ProductNumber = productNumber };
                serverDbCtx.Product.Add(product2);
                await serverDbCtx.SaveChangesAsync();
            }

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(2, s.TotalChangesAppliedOnClient);
                Assert.Equal(this.GetServerDatabaseRowsCount(this.Server), this.GetServerDatabaseRowsCount(client));
            }

            // Add 2 Products and delete 2 products already synced
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                // Create two products on server
                name = HelperDatabase.GetRandomName().ToLowerInvariant();
                productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };
                serverDbCtx.Product.Add(product);

                name = HelperDatabase.GetRandomName().ToLowerInvariant();
                productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var product2 = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };
                serverDbCtx.Product.Add(product2);

                var p1 = await serverDbCtx.Product.SingleAsync(a => a.ProductId == productId1);
                var p2 = await serverDbCtx.Product.SingleAsync(a => a.ProductId == productId2);

                // remove them
                serverDbCtx.Product.Remove(p1);
                serverDbCtx.Product.Remove(p2);

                await serverDbCtx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(4, s.TotalChangesDownloadedFromServer);
                Assert.Equal(4, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.Equal(this.GetServerDatabaseRowsCount(this.Server), this.GetServerDatabaseRowsCount(client));
            }

            OutputCurrentState("Insert And Delete Rows On Server");

            // -----------------------------------
            // Insert And Delete Rows on server
            // --------------------------------

            // Create a new product on server with a big thumbnail photo
            name = HelperDatabase.GetRandomName();
            productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

            product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber, ThumbNailPhoto = new byte[20000] };

            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                serverDbCtx.Product.Add(product);
                await serverDbCtx.SaveChangesAsync();
            }

            // Create a new product on client with a big thumbnail photo
            foreach (var client in this.Clients)
            {
                var clientName = HelperDatabase.GetRandomName();
                var clientProductNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var clientProduct = new Product { ProductId = Guid.NewGuid(), Name = clientName, ProductNumber = clientProductNumber, ThumbNailPhoto = new byte[20000] };

                using (var clientDbCtx = new AdventureWorksContext(client, UseFallbackSchema))
                {
                    clientDbCtx.Product.Add(product);
                    await clientDbCtx.SaveChangesAsync();
                }
            }
            // Two sync to be sure all clients have all rows from all
            foreach (var client in this.Clients)
                await new SyncAgent(client.Provider, Server.Provider).SynchronizeAsync();
            foreach (var client in this.Clients)
                await new SyncAgent(client.Provider, Server.Provider).SynchronizeAsync();

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var products = await ctx.Product.AsNoTracking().ToListAsync();
                foreach (var p in products)
                    Assert.Equal(20000, p.ThumbNailPhoto.Length);
            }

            foreach (var client in Clients)
            {
                using var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema);

                var products = await cliCtx.Product.AsNoTracking().ToListAsync();
                foreach (var p in products)
                    Assert.Equal(20000, p.ThumbNailPhoto.Length);
            }
            OutputCurrentState("Check Blob Consistency on Insert");
        }


        /// <summary>
        /// Update one row on server, should be correctly sync on all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Updates(SyncOptions options)
        {
            // Generate a random city name
            var cityName = HelperDatabase.GetRandomName("City");
            var addressLine = HelperDatabase.GetRandomName("Address");

            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // get rows count
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                await agent.SynchronizeAsync(Tables);
            }

            // -----------------------------------
            // Update one row on server side
            // --------------------------------

            int addressId;
            using (var ctx = new AdventureWorksContext(Clients[0], this.UseFallbackSchema))
            {
                addressId = ctx.Address.First().AddressId;
            }

            // Update one address on server
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var address = serverDbCtx.Address.Single(a => a.AddressId == addressId);

                // Update at least two properties
                address.City = cityName;
                address.AddressLine1 = addressLine;

                await serverDbCtx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                // check row updated values
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var cliAddress = await ctx.Address.AsNoTracking().SingleAsync(a => a.AddressId == 1);
                Assert.Equal(cityName, cliAddress.City);
                Assert.Equal(addressLine, cliAddress.AddressLine1);
            }

            // get rows count
            rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            foreach (var client in Clients)
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));

            OutputCurrentState("Update One Row On Server");

            // -----------------------------------
            // Update one row on client side
            // -----------------------------------

            // Update one address on each client
            // To avoid conflicts, each client will update differents lines
            // each address id is generated from the foreach index
            addressId = 0;
            foreach (var client in Clients)
            {
                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    var addresses = ctx.Address.OrderBy(a => a.AddressId).Take(Clients.Count).ToList();
                    var address = addresses[addressId];

                    // Update at least two properties
                    address.City = HelperDatabase.GetRandomName("City");
                    address.AddressLine1 = HelperDatabase.GetRandomName("Address");

                    await ctx.SaveChangesAsync();
                }
                addressId++;
            }
            // Execute a sync on all clients and check results
            // Each client will download the "upload from previous client"
            int download = 0;
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Now sync again to be sure all clients have all lines
            foreach (var client in Clients)
                await new SyncAgent(client.Provider, Server.Provider, options).SynchronizeAsync();

            // get rows count
            rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                // get all addresses
                var serverAddresses = await ctx.Address.AsNoTracking().ToListAsync();

                foreach (var client in Clients)
                {
                    Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));

                    using var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
                    // get all addresses
                    var clientAddresses = await cliCtx.Address.AsNoTracking().ToListAsync();

                    // check row count
                    Assert.Equal(serverAddresses.Count, clientAddresses.Count);

                    foreach (var clientAddress in clientAddresses)
                    {
                        var serverAddress = serverAddresses.First(a => a.AddressId == clientAddress.AddressId);

                        // check column value
                        Assert.Equal(serverAddress.StateProvince, clientAddress.StateProvince);
                        Assert.Equal(serverAddress.AddressLine1, clientAddress.AddressLine1);
                        Assert.Equal(serverAddress.AddressLine2, clientAddress.AddressLine2);
                    }
                }
            }
            OutputCurrentState("Update One Row On Client");


            // -----------------------------------
            // Update one row on client side to Null
            // -----------------------------------

            // Update one address on each client, with null value on addressline2 (which is not null when seed)
            // To avoid conflicts, each client will update differents lines
            addressId = 1;
            foreach (var client in Clients)
            {
                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    var address = await ctx.Address.SingleAsync(a => a.AddressId == addressId);

                    // update to null value
                    address.AddressLine2 = null;

                    await ctx.SaveChangesAsync();
                }
                addressId++;
            }

            // Execute a sync on all clients and check results
            // Each client will download the "upload from previous client"
            download = 0;

            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Now sync again to be sure all clients have all lines
            foreach (var client in Clients)
                await new SyncAgent(client.Provider, Server.Provider, options).SynchronizeAsync();

            rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                // get all addresses
                var serverAddresses = await ctx.Address.AsNoTracking().ToListAsync();

                foreach (var client in Clients)
                {
                    Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));

                    using var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
                    // get all addresses
                    var clientAddresses = await cliCtx.Address.AsNoTracking().ToListAsync();

                    // check row count
                    Assert.Equal(serverAddresses.Count, clientAddresses.Count);

                    foreach (var clientAddress in clientAddresses)
                    {
                        var serverAddress = serverAddresses.First(a => a.AddressId == clientAddress.AddressId);

                        // check column value
                        Assert.Equal(serverAddress.AddressLine2, clientAddress.AddressLine2);
                        Assert.Null(serverAddress.AddressLine2);
                    }
                }
            }
            OutputCurrentState("Update One Row On Client To Null");

            // -----------------------------------
            // Update one row on side side to Null
            // -----------------------------------

            // Update one address on server with a null value which was not null before
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);

                // set null to a previous value which was not null
                address.AddressLine2 = null;

                await serverDbCtx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                // check row updated values
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var cliAddress = await ctx.Address.AsNoTracking().SingleAsync(a => a.AddressId == 1);
                Assert.Null(cliAddress.AddressLine2);
            }

            // Update one address on server with a non null value (on a value which was null before)
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);
                // set not null value to a previous value which was null
                address.AddressLine2 = "NoT a null value !";
                await serverDbCtx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                // check row updated values
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var cliAddress = await ctx.Address.AsNoTracking().SingleAsync(a => a.AddressId == 1);
                Assert.Equal("NoT a null value !", cliAddress.AddressLine2);
            }

            rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            foreach (var client in Clients)
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));

            OutputCurrentState("Update One Row On Server To Null");

        }


        /// <summary>
        /// Delete rows on server, should be correctly sync on all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Deletes(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // -----------------------------------
            // Delete one row on server side
            // -----------------------------------

            // part of the filter
            var employeeId = 1;
            // will be defined when address is inserted
            var addressId = 0;

            // Insert one address row and one addressemployee row
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                // Insert a new address for employee 1
                var city = "Paris " + HelperDatabase.GetRandomName();
                var addressline1 = "Rue Monthieu " + HelperDatabase.GetRandomName();
                var stateProvince = "Ile de France";
                var countryRegion = "France";
                var postalCode = "75001";

                var address = new Address
                {
                    AddressLine1 = addressline1,
                    City = city,
                    StateProvince = stateProvince,
                    CountryRegion = countryRegion,
                    PostalCode = postalCode

                };

                serverDbCtx.Add(address);
                await serverDbCtx.SaveChangesAsync();
                addressId = address.AddressId;

                var employeeAddress = new EmployeeAddress
                {
                    EmployeeId = employeeId,
                    AddressId = address.AddressId,
                    AddressType = "SERVER"
                };

                var ea = serverDbCtx.EmployeeAddress.Add(employeeAddress);
                await serverDbCtx.SaveChangesAsync();

            }


            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var s = await agent.SynchronizeAsync(Tables);
            }

            // Delete those lines from server
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                // Get the addresses query
                var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == addressId);
                var empAddress = await serverDbCtx.EmployeeAddress.SingleAsync(a => a.AddressId == addressId && a.EmployeeId == employeeId);

                // remove them
                serverDbCtx.EmployeeAddress.Remove(empAddress);
                serverDbCtx.Address.Remove(address);

                // Execute query
                await serverDbCtx.SaveChangesAsync();
            }

            // Sync and check we have delete these lines on each server
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(2, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                // check row deleted on client values
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var finalAddressesCount = await ctx.Address.AsNoTracking().CountAsync(a => a.AddressId == addressId);
                var finalEmployeeAddressesCount = await ctx.EmployeeAddress.AsNoTracking().CountAsync(a => a.AddressId == addressId && a.EmployeeId == employeeId);
                Assert.Equal(0, finalAddressesCount);
                Assert.Equal(0, finalEmployeeAddressesCount);
            }

            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            foreach (var client in Clients)
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));

            OutputCurrentState("Delete One Row On Server");

            // -----------------------------------
            // Delete one row on client side
            // -----------------------------------


            // Insert a new product category on each client
            foreach (var client in Clients)
            {
                // Insert product category on each client
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var productCategoryId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
                var productCategoryNameClient = HelperDatabase.GetRandomName("CLI_");

                ctx.Add(new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryNameClient });
                await ctx.SaveChangesAsync();

            }
            foreach (var client in Clients)
                await new SyncAgent(client.Provider, Server.Provider, options).SynchronizeAsync();

            // Execute a second sync on all clients to be sure all clients have download all others clients product
            foreach (var client in Clients)
                await new SyncAgent(client.Provider, Server.Provider, options).SynchronizeAsync();


            // Now delete rows on each client
            foreach (var client in Clients)
            {
                // Then delete all product category items
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                foreach (var pc in ctx.ProductCategory.Where(pc => pc.Name.StartsWith("CLI_")))
                    ctx.ProductCategory.Remove(pc);
                await ctx.SaveChangesAsync();
            }

            var cpt = 0; // first client won't have any conflicts, but others will upload their deleted rows that are ALREADY deleted
            foreach (var client in Clients)
            {
                var s = await new SyncAgent(client.Provider, Server.Provider, options).SynchronizeAsync();

                // we may download deleted rows from server
                Assert.Equal(cpt, s.TotalChangesDownloadedFromServer);
                // but we should not have any rows applied locally
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                // anyway we are always uploading our deleted rows
                Assert.Equal(Clients.Count, s.TotalChangesUploadedToServer);
                // w may have resolved conflicts locally
                Assert.Equal(cpt, s.TotalResolvedConflicts);

                cpt = Clients.Count;
            }

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var serverPC = await ctx.ProductCategory.AsNoTracking().CountAsync();
                foreach (var client in Clients)
                {
                    using var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
                    var clientPC = await cliCtx.ProductCategory.AsNoTracking().CountAsync();
                    Assert.Equal(serverPC, clientPC);
                }
            }

            OutputCurrentState("Delete One Row On Client");

        }

        /// <summary>
        /// </summary>
        [Fact]
        public async Task Using_ExistingClientDatabase_ProvisionDeprovision()
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // generate a sync conf to host the schema
            var setup = new SyncSetup(this.Tables);

            // options
            var options = new SyncOptions();

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                // create a client schema without seeding
                await this.EnsureDatabaseSchemaAndSeedAsync(client, false, UseFallbackSchema);

                var localOrchestrator = new LocalOrchestrator(client.Provider, options);
                var provision = SyncProvision.ScopeInfo | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

                // just check interceptor
                var onTableCreatedCount = 0;
                localOrchestrator.OnTableCreated(args => onTableCreatedCount++);

                var remoteOrchestrator = new RemoteOrchestrator(this.Server.Provider);
                var schema = await remoteOrchestrator.GetSchemaAsync(setup);

                // Read client scope
                var clientScope = await localOrchestrator.GetScopeInfoAsync();

                var serverScope = new ScopeInfo
                {
                    Name = clientScope.Name,
                    Schema = schema,
                    Setup = setup,
                    Version = clientScope.Version
                };

                // Provision the database with all tracking tables, stored procedures, triggers and scope
                clientScope = await localOrchestrator.ProvisionAsync(serverScope, provision);

                //--------------------------
                // ASSERTION
                //--------------------------

                // check if scope table is correctly created
                var scopeInfoTableExists = await localOrchestrator.ExistScopeInfoTableAsync();
                Assert.True(scopeInfoTableExists);

                // get the db manager
                foreach (var setupTable in setup.Tables)
                {
                    Assert.True(await localOrchestrator.ExistTrackingTableAsync(clientScope, setupTable.TableName, setupTable.SchemaName));

                    Assert.True(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Delete));
                    Assert.True(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Insert));
                    Assert.True(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Update));

                    if (client.ProviderType == ProviderType.Sql)
                    {
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkTableType));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkDeleteRows));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkUpdateRows));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteMetadata));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteRow));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.Reset));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChanges));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChanges));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectRow));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.UpdateRow));

                        // No filters here
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChangesWithFilters));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChangesWithFilters));
                    }

                }

                //localOrchestrator.OnTableProvisioned(null);

                //// Deprovision the database with all tracking tables, stored procedures, triggers and scope

                await localOrchestrator.DeprovisionAsync(provision);

                // check if scope table is correctly created
                scopeInfoTableExists = await localOrchestrator.ExistScopeInfoTableAsync();
                Assert.False(scopeInfoTableExists);

                // get the db manager
                foreach (var setupTable in setup.Tables)
                {
                    Assert.False(await localOrchestrator.ExistTrackingTableAsync(clientScope, setupTable.TableName, setupTable.SchemaName));

                    Assert.False(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Delete));
                    Assert.False(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Insert));
                    Assert.False(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Update));

                    if (client.ProviderType == ProviderType.Sql)
                    {
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkDeleteRows));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkTableType));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkUpdateRows));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteMetadata));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteRow));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.Reset));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChanges));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChanges));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectRow));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.UpdateRow));

                        // No filters here
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChangesWithFilters));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChangesWithFilters));
                    }

                }


            }
        }

        /// <summary>
        /// Check foreign keys existence
        /// </summary>
        [Fact]
        public async Task Check_Composite_ForeignKey_Existence()
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider);

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var clientScope = await agent.LocalOrchestrator.GetScopeInfoAsync();

                using var connection = client.Provider.CreateConnection();
                await connection.OpenAsync();
                using var transaction = connection.BeginTransaction();


                var tablePricesListCategory = agent.LocalOrchestrator.GetTableBuilder(clientScope.Schema.Tables["PricesListCategory"], clientScope);
                Assert.NotNull(tablePricesListCategory);

                var relations = (await tablePricesListCategory.GetRelationsAsync(connection, transaction)).ToList();
                Assert.Single(relations);

                if (client.ProviderType != ProviderType.Sqlite)
                    Assert.StartsWith("FK_PricesListCategory_PricesList_PriceListId", relations[0].ForeignKey);

                Assert.Single(relations[0].Columns);

                var tablePricesListDetail = agent.LocalOrchestrator.GetTableBuilder(clientScope.Schema.Tables["PricesListDetail"], clientScope);

                Assert.NotNull(tablePricesListDetail);

                var relations2 = (await tablePricesListDetail.GetRelationsAsync(connection, transaction)).ToArray();
                Assert.Single(relations2);

                if (client.ProviderType != ProviderType.Sqlite)
                    Assert.StartsWith("FK_PricesListDetail_PricesListCategory_PriceListId", relations2[0].ForeignKey);

                Assert.Equal(2, relations2[0].Columns.Count);

                var tableEmployeeAddress = agent.LocalOrchestrator.GetTableBuilder(clientScope.Schema.Tables["EmployeeAddress"], clientScope);
                Assert.NotNull(tableEmployeeAddress);

                var relations3 = (await tableEmployeeAddress.GetRelationsAsync(connection, transaction)).ToArray();
                Assert.Equal(2, relations3.Count());

                if (client.ProviderType != ProviderType.Sqlite)
                {
                    Assert.StartsWith("FK_EmployeeAddress_Address_AddressID", relations3[0].ForeignKey);
                    Assert.StartsWith("FK_EmployeeAddress_Employee_EmployeeID", relations3[1].ForeignKey);

                }
                Assert.Single(relations3[0].Columns);
                Assert.Single(relations3[1].Columns);

                transaction.Commit();
                connection.Close();
            }
        }

        /// <summary>
        /// Be sure we continue to trakc correctly rows even during a sync process
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Insert_Record_Then_Insert_During_GetChanges(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Insert one line on each client
            foreach (var client in Clients)
            {
                var productCategoryName = HelperDatabase.GetRandomName();
                var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                var productId = Guid.NewGuid();
                var productName = HelperDatabase.GetRandomName();
                var productNumber = productName.ToUpperInvariant().Substring(0, 10);

                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);
                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber, ProductCategoryId = productCategoryId };
                ctx.Add(product);
                var priceList = new PriceList { PriceListId = Guid.NewGuid(), Description = HelperDatabase.GetRandomName() };
                ctx.Add(priceList);
                await ctx.SaveChangesAsync();
            }

            // Sync all clients
            // First client  will upload 3 lines and will download nothing
            // Second client will upload 3 lines and will download 3 lines
            // thrid client  will upload 3 lines and will download 6 lines
            int download = 0;
            foreach (var client in Clients)
            {

                // Sleep during a selecting changes on first sync
                void tableChangesSelected(TableChangesSelectedArgs changes)
                {
                    if (changes.TableChangesSelected.TableName != "PricesList")
                        return;

                    // Insert on same connection as current sync.
                    // Using same connection to avoid lock, especially on SQlite
                    var command = changes.Connection.CreateCommand();

                    // As Column names are case-sensitive in postgresql
                    if (client.ProviderType == ProviderType.Postgres)
                        command.CommandText = "INSERT INTO \"PricesList\" (\"PriceListId\", \"Description\") Values (@PriceListId, @Description);";
                    else
                        command.CommandText = "INSERT INTO PricesList (PriceListId, Description) Values (@PriceListId, @Description);";
                    var p = command.CreateParameter();
                    p.ParameterName = "@PriceListId";
                    p.Value = Guid.NewGuid();
                    command.Parameters.Add(p);

                    p = command.CreateParameter();
                    p.ParameterName = "@Description";
                    p.Value = HelperDatabase.GetRandomName();
                    command.Parameters.Add(p);

                    command.Transaction = changes.Transaction;
                    try
                    {
                        var inserted = command.ExecuteNonQuery();
                        Debug.WriteLine($"Execution result: {inserted}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        throw;
                    }
                    return;
                };

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                // Intercept TableChangesSelected
                agent.LocalOrchestrator.OnTableChangesSelected(tableChangesSelected);

                var s = await agent.SynchronizeAsync(Tables);

                agent.LocalOrchestrator.ClearInterceptors();

                Assert.Equal(download, s.TotalChangesDownloadedFromServer);
                Assert.Equal(3, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
                download += 3;

            }

            // CLI1 (6 rows) : CLI1 will upload 1 row and download 3 rows from CLI2 and 3 rows from CLI3
            // CLI2 (4 rows) : CLI2 will upload 1 row and download 3 rows from CLI3 and 1 row from CLI1
            // CLI3 (2 rows) : CLI3 will upload 1 row and download 1 row from CLI1 and 1 row from CLI2
            download = 3 * (Clients.Count - 1);
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
                download -= 2;
            }


            // CLI1 (6) : CLI1 will download 1 row from CLI3 and 1 rows from CLI2
            // CLI2 (4) : CLI2 will download 1 row from CLI3
            // CLI3 (2) : CLI3 will download nothing
            download = Clients.Count - 1;
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download--, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var productRowCount = await ctx.Product.AsNoTracking().CountAsync();
                var productCategoryCount = await ctx.ProductCategory.AsNoTracking().CountAsync();
                var priceListCount = await ctx.PricesList.AsNoTracking().CountAsync();
                var rowsCount = this.GetServerDatabaseRowsCount(this.Server);
                foreach (var client in Clients)
                {
                    Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));

                    using var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
                    var pCount = await cliCtx.Product.AsNoTracking().CountAsync();
                    Assert.Equal(productRowCount, pCount);

                    var pcCount = await cliCtx.ProductCategory.AsNoTracking().CountAsync();
                    Assert.Equal(productCategoryCount, pcCount);

                    var plCount = await cliCtx.PricesList.AsNoTracking().CountAsync();
                    Assert.Equal(priceListCount, plCount);
                }
            }
        }


        /// <summary>
        /// Force failing constraints.
        /// But since we set the correct options, shoudl work correctly
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Force_Failing_Constraints_ButWorks_WithOptions(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Disable check constraints
            // "true" is the default value, but set for information purpose
            options.DisableConstraintsOnApplyChanges = true;

            // product category and product items
            var productCategoryName = HelperDatabase.GetRandomName();
            var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

            var productId = Guid.NewGuid();
            var productName = HelperDatabase.GetRandomName();
            var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);


            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
                await new SyncAgent(client.Provider, Server.Provider, options).SynchronizeAsync(Tables);


            // Add a product and its product category
            using (var ctx = new AdventureWorksContext(this.Server))
            {

                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.ProductCategory.Add(pc);

                // Create a product and affect ProductCategory
                var product = new Product
                {
                    ProductId = productId,
                    Name = productName,
                    ProductNumber = productNumber,
                    ProductCategory = pc
                };

                ctx.Product.Add(product);

                await ctx.SaveChangesAsync();
            }

            // Sync all clients 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.Equal(2, this.GetServerDatabaseRowsCount(client));
            }

            // Creating the fail constraint 
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                // Get the ProductCategory and the Product
                var pc = ctx.ProductCategory.Find(productCategoryId);
                var p = ctx.Product.Find(productId);

                // Update Product to remove foreign key to Product Category
                p.ProductCategory = null;
                // Delete the ProductCategory row
                ctx.ProductCategory.Remove(pc);

                // Save
                await ctx.SaveChangesAsync();

            }

            // Sync all clients. Should not raise an error, because we disable constraint check
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                // we removed a product category
                Assert.Equal(1, this.GetServerDatabaseRowsCount(client));
            }
        }


        /// <summary>
        /// Force failing constraints.
        /// Try to solve with interceptors
        /// </summary>
        /// <param name="options"></param>
        /// <returns></returns>
        [Fact]
        public async Task Force_Failing_Constraints_ButWorks_WithInterceptors()
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Enable check constraints
            var options = new SyncOptions
            {
                DisableConstraintsOnApplyChanges = false
            };

            // product category and product items
            var productCategoryName = HelperDatabase.GetRandomName();
            var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

            var productId = Guid.NewGuid();
            var productName = HelperDatabase.GetRandomName();
            var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);


            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
                await new SyncAgent(client.Provider, Server.Provider, options).SynchronizeAsync(Tables);


            // Add a product and its product category
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.ProductCategory.Add(pc);


                // Create a product and affect ProductCategory
                var product = new Product
                {
                    ProductId = productId,
                    Name = productName,
                    ProductNumber = productNumber,
                    ProductCategory = pc
                };

                ctx.Product.Add(product);

                await ctx.SaveChangesAsync();
            }

            // Sync all clients 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }


            // Creating the fail constraint 
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                // Get the ProductCategory and the Product
                var pc = ctx.ProductCategory.Find(productCategoryId);
                var p = ctx.Product.Find(productId);

                // Update Product to remove foreign key to Product Category
                p.ProductCategory = null;
                // Delete the ProductCategory row
                ctx.ProductCategory.Remove(pc);

                // Save
                await ctx.SaveChangesAsync();

            }

            // Sync all clients. Should raise an error
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var clientScope = await agent.LocalOrchestrator.GetScopeInfoAsync();

                //await Assert.ThrowsAsync<SyncException>(async () =>
                //{
                //    var res = await agent.SynchronizeAsync();
                //});

                // NOTE : Using interceptors to disable constraint
                // but options.DisableConstraintsOnApplyChanges = true; DOES THE SAME 
                // Using interceptors just to test if it's possible "on the fly"

                // Setting PRAGMA only works outside a transaction
                // So setting the pragma on the connection opening
                // No need to reaffect PRAGMA at the end, since the modification leaves
                // only during the connection open time
                agent.LocalOrchestrator.OnConnectionOpen(coa =>
                {
                    if (client.ProviderType != ProviderType.Sqlite)
                        return;

                    var cmd = coa.Connection.CreateCommand();
                    cmd.Connection = coa.Connection;

                    cmd.CommandText = "PRAGMA foreign_keys = OFF;";
                    object res = cmd.ExecuteScalar();

                });

                agent.LocalOrchestrator.OnDatabaseChangesApplying(tca =>
                {
                    if (client.ProviderType == ProviderType.Sqlite)
                        return;

                    if (client.ProviderType == ProviderType.MySql || client.ProviderType == ProviderType.MariaDB)
                    {
                        var cmd = tca.Connection.CreateCommand();
                        cmd.CommandText = "SET FOREIGN_KEY_CHECKS = 0;";
                        cmd.Connection = tca.Connection;
                        cmd.Transaction = tca.Transaction;
                        cmd.ExecuteNonQuery();

                        return;
                    }
                    if (client.ProviderType == ProviderType.Sql)
                    {
                        foreach (var table in clientScope.Schema.Tables.Where(t => t.TableName == "Product" || t.TableName == "ProductCategory"))
                        {
                            var cmd = tca.Connection.CreateCommand();
                            var tableAndSchemaName = ParserName.Parse(table).Schema().Quoted().ToString();
                            var tableName = ParserName.Parse(table).Schema().Quoted().ToString();
                            cmd.CommandText = $"ALTER TABLE {tableAndSchemaName} NOCHECK CONSTRAINT ALL";
                            cmd.Connection = tca.Connection;
                            cmd.Transaction = tca.Transaction;
                            cmd.ExecuteNonQuery();

                        }
                    }
                });

                agent.LocalOrchestrator.OnDatabaseChangesApplied(tca =>
                {

                    if (client.ProviderType == ProviderType.Sqlite)
                        return;


                    if (client.ProviderType == ProviderType.MySql || client.ProviderType == ProviderType.MariaDB)
                    {
                        var cmd = tca.Connection.CreateCommand();
                        cmd.CommandText = "SET FOREIGN_KEY_CHECKS = 1;";
                        cmd.Connection = tca.Connection;
                        cmd.Transaction = tca.Transaction;
                        cmd.ExecuteNonQuery();

                        return;
                    }

                    if (client.ProviderType == ProviderType.Sql)
                    {
                        foreach (var table in clientScope.Schema.Tables.Where(t => t.TableName == "Product" || t.TableName == "ProductCategory"))
                        {
                            var cmd = tca.Connection.CreateCommand();
                            var tableAndSchemaName = ParserName.Parse(table).Schema().Quoted().ToString();
                            var tableName = ParserName.Parse(table).Schema().Quoted().ToString();
                            cmd.CommandText = $"ALTER TABLE {tableAndSchemaName} CHECK CONSTRAINT ALL";
                            cmd.Connection = tca.Connection;
                            cmd.Transaction = tca.Transaction;
                            cmd.ExecuteNonQuery();
                        }
                    }

                });



                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                agent.LocalOrchestrator.ClearInterceptors();
            }

            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);
            foreach (var client in Clients)
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
        }


        /// <summary>
        /// Insert one row on each client, should be sync on server and clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Reinitialize(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // ----------------------------
            // Reinitialize
            // ----------------------------

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
            }

            // Insert one line on each client
            foreach (var client in Clients)
            {
                var productCategoryName = HelperDatabase.GetRandomName();
                var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                var productId = Guid.NewGuid();
                var productName = HelperDatabase.GetRandomName();
                var productNumber = productName.ToUpperInvariant().Substring(0, 10);

                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);
                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber, ProductCategoryId = productCategoryId };
                ctx.Add(product);
                await ctx.SaveChangesAsync();
            }

            // Sync all clients
            // inserted rows will be deleted 
            foreach (var client in Clients)
            {
                // coz of ProductCategory Parent Id Foreign Key Constraints
                // on Reset table in MySql
                options.DisableConstraintsOnApplyChanges = true;

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                if (options.TransactionMode != TransactionMode.AllOrNothing && (client.ProviderType == ProviderType.MySql || client.ProviderType == ProviderType.MariaDB))
                {
                    agent.LocalOrchestrator.OnGetCommand(async args =>
                    {
                        if (args.CommandType == DbCommandType.Reset)
                        {
                            var scopeInfo = await agent.LocalOrchestrator.GetScopeInfoAsync(args.Connection, args.Transaction);
                            await agent.LocalOrchestrator.DisableConstraintsAsync(scopeInfo, args.Table.TableName, args.Table.SchemaName, args.Connection, args.Transaction);
                        }
                    });
                }

                var s = await agent.SynchronizeAsync(SyncType.Reinitialize);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
            }

            OutputCurrentState("Reinitialize");

            // ------------------------------
            // Reinitialize with upload
            // ------------------------------

            // Get count of rows
            rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var s = await agent.SynchronizeAsync(Tables);
            }

            // Insert one line on each client
            foreach (var client in Clients)
            {
                var productCategoryName = HelperDatabase.GetRandomName();
                var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                var productId = Guid.NewGuid();
                var productName = HelperDatabase.GetRandomName();
                var productNumber = productName.ToUpperInvariant().Substring(0, 10);

                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);
                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber, ProductCategoryId = productCategoryId };
                ctx.Add(product);
                await ctx.SaveChangesAsync();
            }

            // Sync all clients
            // client  will upload two lines and will download all + its two lines
            int download = 2;
            foreach (var client in Clients)
            {
                // coz of ProductCategory Parent Id Foreign Key Constraints
                // on Reset table in MySql
                options.DisableConstraintsOnApplyChanges = true;

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                if (options.TransactionMode != TransactionMode.AllOrNothing)
                {
                    agent.LocalOrchestrator.OnGetCommand(async args =>
                    {
                        if (args.CommandType == DbCommandType.Reset)
                        {
                            var scopeInfo = await agent.LocalOrchestrator.GetScopeInfoAsync(args.Connection, args.Transaction);
                            await agent.LocalOrchestrator.DisableConstraintsAsync(scopeInfo, args.Table.TableName, args.Table.SchemaName, args.Connection, args.Transaction);
                        }
                    });
                }

                var s = await agent.SynchronizeAsync(SyncType.ReinitializeWithUpload);

                Assert.Equal(rowsCount + download, s.TotalChangesDownloadedFromServer);
                Assert.Equal(2, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
                download += 2;
            }

            rowsCount = this.GetServerDatabaseRowsCount(this.Server);
            foreach (var client in Clients)
            {
                await new SyncAgent(client.Provider, Server.Provider, options).SynchronizeAsync().ConfigureAwait(false);
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
            }

            OutputCurrentState("Reinitialize With Upload");

        }


        /// <summary>
        /// Configuring tables to be upload only
        /// Server should receive lines but will not send back its own lines
        /// </summary>
        [Fact]
        public async Task UploadOnly_DownloadOnly()
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // -----------------------------------
            // Upload Only
            // -----------------------------------

            // Set Employee, Address, EmployeeAddress to Upload only
            // All others are Bidirectional by default.
            var setup = new SyncSetup(Tables);
            setup.Tables["Employee"].SyncDirection = SyncDirection.UploadOnly;
            setup.Tables["Address"].SyncDirection = SyncDirection.UploadOnly;
            setup.Tables["EmployeeAddress"].SyncDirection = SyncDirection.UploadOnly;


            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
                await new SyncAgent(client.Provider, Server.Provider).SynchronizeAsync(setup);

            // Insert one employee address address_employee on each client
            int index = 10;
            foreach (var client in Clients)
            {
                // Insert one employee, address, employeeaddress
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);

                ctx.Database.OpenConnection();

                // Insert an employee
                var employee = new Employee { EmployeeId = index, FirstName = "John", LastName = "Doe" };
                ctx.Add(employee);

                if (client.ProviderType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Employee ON;");

                await ctx.SaveChangesAsync();

                if (client.ProviderType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Employee OFF");

                // Insert a new address for employee 
                var city = "Paris " + HelperDatabase.GetRandomName();
                var addressline1 = "Rue Monthieu " + HelperDatabase.GetRandomName();
                var stateProvince = "Ile de France";
                var countryRegion = "France";
                var postalCode = "75001";

                var address = new Address { AddressId = index, AddressLine1 = addressline1, City = city, StateProvince = stateProvince, CountryRegion = countryRegion, PostalCode = postalCode };

                ctx.Add(address);
                if (client.ProviderType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Address ON;");

                await ctx.SaveChangesAsync();

                if (client.ProviderType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Address OFF");

                var employeeAddress = new EmployeeAddress { EmployeeId = employee.EmployeeId, AddressId = address.AddressId, AddressType = "CLIENT" };

                ctx.EmployeeAddress.Add(employeeAddress);
                await ctx.SaveChangesAsync();

                ctx.Database.CloseConnection();
                index++;
            }

            // Insert one ProductCategory, Employee, Address, EmployeeAddress on server
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                ctx.Database.OpenConnection();

                var productId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
                var productCategoryNameServer = HelperDatabase.GetRandomName("SRV");

                // ProductCategory
                ctx.Add(new ProductCategory { ProductCategoryId = productId, Name = productCategoryNameServer });
                await ctx.SaveChangesAsync();

                // Insert an employee
                var employee = new Employee { EmployeeId = 1000, FirstName = "John", LastName = "Doe" };
                ctx.Add(employee);

                if (this.ServerType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Employee ON;");

                await ctx.SaveChangesAsync();

                if (this.ServerType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Employee OFF");

                // Insert a new address for employee 
                var address = new Address { AddressId = 1000, AddressLine1 = HelperDatabase.GetRandomName(), City = "Lyon " + HelperDatabase.GetRandomName(), StateProvince = "Rhones", CountryRegion = "France", PostalCode = "69001" };

                ctx.Add(address);
                if (this.ServerType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Address ON;");

                await ctx.SaveChangesAsync();

                if (this.ServerType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Address OFF");

                var employeeAddress = new EmployeeAddress { EmployeeId = employee.EmployeeId, AddressId = address.AddressId, AddressType = "SERVER" };

                ctx.EmployeeAddress.Add(employeeAddress);
                await ctx.SaveChangesAsync();

                ctx.Database.CloseConnection();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider);

                var s = await agent.SynchronizeAsync();

                // Server shoud not sent back lines, so download equals 1 (just product category)
                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(3, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                // get all product categories
                var serverPC = await ctx.ProductCategory.AsNoTracking().ToListAsync();
                Assert.Single(serverPC);

                // get all employees
                var employees = await ctx.Employee.AsNoTracking().ToListAsync();
                Assert.Equal(Clients.Count + 1, employees.Count);
                // get all employees address
                var employeesAddresses = await ctx.EmployeeAddress.AsNoTracking().ToListAsync();
                Assert.Equal(Clients.Count + 1, employeesAddresses.Count);
                // get all addresses
                var addresses = await ctx.Address.AsNoTracking().ToListAsync();
                Assert.Equal(Clients.Count + 1, addresses.Count);

            }
            foreach (var client in Clients)
            {
                using var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
                // get all product categories
                var clientPC = await cliCtx.ProductCategory.AsNoTracking().ToListAsync();
                Assert.Single(clientPC);

                // get all employees
                var employees = await cliCtx.Employee.AsNoTracking().ToListAsync();
                Assert.Single(employees);
                // get all employees address
                var employeesAddresses = await cliCtx.EmployeeAddress.AsNoTracking().ToListAsync();
                Assert.Single(employeesAddresses);
                // get all addresses
                var addresses = await cliCtx.Address.AsNoTracking().ToListAsync();
                Assert.Single(addresses);
            }

            OutputCurrentState("Upload Only");

            // -----------------------------------
            // Download Only
            // -----------------------------------

            // Drop all to start from scratch
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, new SyncOptions());
                await agent.LocalOrchestrator.DropAllAsync();
                await agent.RemoteOrchestrator.DropAllAsync();
            }

            // Set Employee, Address, EmployeeAddress to Upload only
            // All others are Bidirectional by default.
            setup = new SyncSetup(Tables);
            setup.Tables["Employee"].SyncDirection = SyncDirection.DownloadOnly;
            setup.Tables["Address"].SyncDirection = SyncDirection.DownloadOnly;
            setup.Tables["EmployeeAddress"].SyncDirection = SyncDirection.DownloadOnly;

            // Insert one line on each client
            index = 50;
            foreach (var client in Clients)
            {
                // Insert one employee, address, employeeaddress
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);

                ctx.Database.OpenConnection();

                // Insert an employee
                var employee = new Employee { EmployeeId = index, FirstName = "John", LastName = "Doe" };

                ctx.Add(employee);

                if (client.ProviderType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Employee ON;");

                await ctx.SaveChangesAsync();

                if (client.ProviderType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Employee OFF");

                // Insert a new address for employee 
                var city = "Paris " + HelperDatabase.GetRandomName();
                var addressline1 = "Rue Monthieu " + HelperDatabase.GetRandomName();
                var stateProvince = "Ile de France";
                var countryRegion = "France";
                var postalCode = "75001";

                var address = new Address { AddressId = index, AddressLine1 = addressline1, City = city, StateProvince = stateProvince, CountryRegion = countryRegion, PostalCode = postalCode };

                ctx.Add(address);
                if (client.ProviderType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Address ON;");

                await ctx.SaveChangesAsync();

                if (client.ProviderType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Address OFF");

                var employeeAddress = new EmployeeAddress { EmployeeId = employee.EmployeeId, AddressId = address.AddressId, AddressType = "CLIENT" };

                ctx.EmployeeAddress.Add(employeeAddress);
                await ctx.SaveChangesAsync();

                ctx.Database.CloseConnection();
                index++;
            }

            // Insert one ProductCategory, Employee, Address, EmployeeAddress on server
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                ctx.Database.OpenConnection();

                var productId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
                var productCategoryNameServer = HelperDatabase.GetRandomName("SRV");

                // ProductCategory
                ctx.Add(new ProductCategory { ProductCategoryId = productId, Name = productCategoryNameServer });
                await ctx.SaveChangesAsync();

                // Insert an employee
                var employee = new Employee { EmployeeId = 9000, FirstName = "John", LastName = "Doe" };

                ctx.Add(employee);

                if (this.ServerType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Employee ON;");

                await ctx.SaveChangesAsync();

                if (this.ServerType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Employee OFF");

                // Insert a new address for employee 
                var city = "Lyon " + HelperDatabase.GetRandomName();
                var addressline1 = HelperDatabase.GetRandomName();
                var stateProvince = "Rhones";
                var countryRegion = "France";
                var postalCode = "69001";

                var address = new Address { AddressId = 9000, AddressLine1 = addressline1, City = city, StateProvince = stateProvince, CountryRegion = countryRegion, PostalCode = postalCode };

                ctx.Add(address);
                if (this.ServerType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Address ON;");

                await ctx.SaveChangesAsync();

                if (this.ServerType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Address OFF");

                var employeeAddress = new EmployeeAddress { EmployeeId = employee.EmployeeId, AddressId = address.AddressId, AddressType = "SERVER" };

                ctx.EmployeeAddress.Add(employeeAddress);
                await ctx.SaveChangesAsync();

                ctx.Database.CloseConnection();
            }

            var rowsCount = this.GetServerDatabaseRowsCount(Server);

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider);
                var s = await agent.SynchronizeAsync(setup);

                // Server send lines, but clients don't
                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(rowsCount, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            OutputCurrentState("Download Only");
        }


        /// <summary>
        /// Insert one row in two tables on server, should be correctly sync on all clients
        /// </summary>
        [Fact]
        public async Task Snapshots()
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            var setup = new SyncSetup(Tables);

            // snapshot directory
            var snapshotDirctory = HelperDatabase.GetRandomName();
            var directory = Path.Combine(Environment.CurrentDirectory, snapshotDirctory);

            var options = new SyncOptions
            {
                SnapshotsDirectory = directory,
                BatchSize = 3000,
            };

            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider, options);
            Server.Provider.SupportsMultipleActiveResultSets = false;

            // ----------------------------------
            // Create a snapshot
            // ----------------------------------
            await remoteOrchestrator.CreateSnapshotAsync(setup);

            // ----------------------------------
            // Add rows on server AFTER snapshot
            // ----------------------------------
            var productId = Guid.NewGuid();
            var productName = HelperDatabase.GetRandomName();
            var productNumber = productName.ToUpperInvariant().Substring(0, 10);

            var productCategoryName = HelperDatabase.GetRandomName();
            var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);

                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber };
                ctx.Add(product);

                await ctx.SaveChangesAsync();
            }

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
            }


            OutputCurrentState("Snapshot Initialize");

            // -----------------------------------
            // Snapshot Initialize Then Client Upload Sync Then Reinitialize
            // --------------------------------

            // Add rows on client

            foreach (var client in Clients)
            {
                var name = HelperDatabase.GetRandomName();
                var pn = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = pn };

                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                ctx.Product.Add(product);
                await ctx.SaveChangesAsync();
            }

            // Sync all clients
            // First client  will upload one line and will download nothing
            // Second client will upload one line and will download one line
            // thrid client  will upload one line and will download two lines
            int download = 0;
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Get count of rows
            rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // ----------------------------------
            // Now Reinitialize
            // ----------------------------------

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                options.DisableConstraintsOnApplyChanges = true;
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                if (options.TransactionMode != TransactionMode.AllOrNothing && (client.ProviderType == ProviderType.MySql || client.ProviderType == ProviderType.MariaDB))
                {
                    agent.LocalOrchestrator.OnGetCommand(async args =>
                    {
                        if (args.CommandType == DbCommandType.Reset)
                        {
                            var scopeInfo = await agent.LocalOrchestrator.GetScopeInfoAsync(args.Connection, args.Transaction);
                            await agent.LocalOrchestrator.DisableConstraintsAsync(scopeInfo, args.Table.TableName, args.Table.SchemaName, args.Connection, args.Transaction);
                        }
                    });
                }

                var s = await agent.SynchronizeAsync(SyncType.Reinitialize);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
            }

            OutputCurrentState("Snapshot Initialize Then Client Upload Sync Then Reinitialize");


            // ----------------------------------
            // Create a snapshot and after delete rows on server
            // ----------------------------------

            productCategoryName = HelperDatabase.GetRandomName();
            productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);

                await ctx.SaveChangesAsync();
            }


            // ----------------------------------
            // Create a snapshot
            // ----------------------------------
            await remoteOrchestrator.CreateSnapshotAsync(setup);

            // ----------------------------------
            // Delete added rows on server BEFORE Sync
            // ----------------------------------

            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var pc = ctx.ProductCategory.Where(pc => pc.ProductCategoryId == productCategoryId).First();
                ctx.ProductCategory.Remove(pc);
                await ctx.SaveChangesAsync();
            }

            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                await agent.LocalOrchestrator.DropAllAsync();
            }

            // Get count of rows
            rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            var clientTotalChangesApplied = rowsCount + 2; // rows count + 1 insert (before delete, contained in snapshot) + 1 delete (after delete)

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.ChangesAppliedOnClient.TotalAppliedChanges);
                Assert.Equal(rowsCount - 1, s.SnapshotChangesAppliedOnClient.TotalAppliedChanges);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));

            }
            OutputCurrentState("Snapshot Initialize Then Delete Server Rows Before Sync");

        }


        [Fact]
        public async Task Serialize_And_Deserialize()
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            var scopeName = "scopesnap1";
            var setup = new SyncSetup(Tables);
            //var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            // Defining options with Batchsize to enable serialization on disk
            var options = new SyncOptions { BatchSize = 1000 };

            var myRijndael = new RijndaelManaged();
            myRijndael.GenerateKey();
            myRijndael.GenerateIV();

            var writringRowsTables = new ConcurrentDictionary<string, int>();
            var readingRowsTables = new ConcurrentDictionary<string, int>();

            var serializingRowsAction = new Func<SerializingRowArgs, Task>((args) =>
            {
                // Assertion
                writringRowsTables.AddOrUpdate(args.SchemaTable.GetFullName(), 1, (key, oldValue) => oldValue + 1);

                var strSet = JsonConvert.SerializeObject(args.RowArray);
                using var encryptor = myRijndael.CreateEncryptor(myRijndael.Key, myRijndael.IV);
                using var msEncrypt = new MemoryStream();
                using var csEncrypt = new CryptoStream(msEncrypt, encryptor, CryptoStreamMode.Write);
                using (var swEncrypt = new StreamWriter(csEncrypt))
                    swEncrypt.Write(strSet);

                args.Result = Convert.ToBase64String(msEncrypt.ToArray());

                return Task.CompletedTask;
            });

            var deserializingRowsAction = new Func<DeserializingRowArgs, Task>((args) =>
            {
                // Assertion
                readingRowsTables.AddOrUpdate(args.SchemaTable.GetFullName(), 1, (key, oldValue) => oldValue + 1);

                string value;
                var byteArray = Convert.FromBase64String(args.RowString);
                using var decryptor = myRijndael.CreateDecryptor(myRijndael.Key, myRijndael.IV);
                using var msDecrypt = new MemoryStream(byteArray);
                using var csDecrypt = new CryptoStream(msDecrypt, decryptor, CryptoStreamMode.Read);
                using (var swDecrypt = new StreamReader(csDecrypt))
                    value = swDecrypt.ReadToEnd();

                var array = JsonConvert.DeserializeObject<object[]>(value);

                args.Result = array;
                return Task.CompletedTask;

            });

            foreach (var client in this.Clients)
            {
                writringRowsTables.Clear();
                readingRowsTables.Clear();

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                // Get the orchestrators
                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;


                localOrchestrator.OnSerializingSyncRow(serializingRowsAction);
                remoteOrchestrator.OnSerializingSyncRow(serializingRowsAction);

                localOrchestrator.OnDeserializingSyncRow(deserializingRowsAction);
                remoteOrchestrator.OnDeserializingSyncRow(deserializingRowsAction);

                // Making a first sync, will initialize everything we need
                var result = await agent.SynchronizeAsync(scopeName, setup);

                foreach (var table in result.ChangesAppliedOnClient.TableChangesApplied)
                {
                    var fullName = string.IsNullOrEmpty(table.SchemaName) ? table.TableName : $"{table.SchemaName}.{table.TableName}";
                    writringRowsTables.TryGetValue(fullName, out int writedRows);
                    Assert.Equal(table.Applied, writedRows);

                }

                foreach (var table in result.ServerChangesSelected.TableChangesSelected)
                {
                    var fullName = string.IsNullOrEmpty(table.SchemaName) ? table.TableName : $"{table.SchemaName}.{table.TableName}";
                    readingRowsTables.TryGetValue(fullName, out int readRows);
                    Assert.Equal(table.TotalChanges, readRows);
                }


                Assert.Equal(GetServerDatabaseRowsCount(this.Server), result.TotalChangesDownloadedFromServer);



            }
        }


        [Fact]
        public async Task IsOutdated_ShouldWork_If_Correct_Action()
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            var scopeName = "scopesnap1";


            foreach (var client in this.Clients)
            {
                // Defining options with Batchsize to enable serialization on disk
                var options = new SyncOptions { BatchSize = 1000 };

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                // Making a first sync, will initialize everything we need
                var s = await agent.SynchronizeAsync(scopeName, Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            foreach (var client in this.Clients)
            {

                // Defining options with Batchsize to enable serialization on disk
                var options = new SyncOptions { BatchSize = 1000 };

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                // Call a server delete metadata to update the last valid timestamp value in scope_info_server table
                var dmc = await agent.RemoteOrchestrator.DeleteMetadatasAsync();

                // Client side : Create a product category and a product
                var productId = Guid.NewGuid();
                var productName = HelperDatabase.GetRandomName();
                var productNumber = productName.ToUpperInvariant().Substring(0, 10);
                var productCategoryName = HelperDatabase.GetRandomName();
                var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                    ctx.Add(pc);

                    var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber };
                    ctx.Add(product);

                    await ctx.SaveChangesAsync();
                }

                // Generate an outdated situation
                await HelperDatabase.ExecuteScriptAsync(client.ProviderType, client.DatabaseName,
                                    $"Update scope_info_client set scope_last_server_sync_timestamp=-1");

                // Making a first sync, will initialize everything we need
                var se = await Assert.ThrowsAsync<SyncException>(async () =>
                {
                    var tmpR = await agent.SynchronizeAsync(scopeName);
                });

                Assert.Equal("OutOfDateException", se.TypeName);

                // Intercept outdated event, and make a reinitialize with upload action
                agent.LocalOrchestrator.OnOutdated(oa =>
                {
                    oa.Action = OutdatedAction.ReinitializeWithUpload;
                });

                var r = await agent.SynchronizeAsync(scopeName);
                var c = GetServerDatabaseRowsCount(this.Server);
                Assert.Equal(c, r.TotalChangesDownloadedFromServer);
                Assert.Equal(2, r.TotalChangesUploadedToServer);

                Assert.Equal(c, this.GetServerDatabaseRowsCount(client));


            }
        }


        /// <summary>
        /// Configuring tables to be upload only
        /// Server should receive lines but will not send back its own lines
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Change_Bidirectional_To_UploadOnly_ShouldWork(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            var setup = new SyncSetup(this.Tables) { StoredProceduresPrefix = "cli", StoredProceduresSuffix = "", TrackingTablesPrefix = "tr" };

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            int addressesCount = 0;
            int employeesAddressesCount = 0;
            int employeesCount = 0;
            int productCategoriesCount = 0;

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                addressesCount = ctx.Address.AsNoTracking().Count();
                employeesAddressesCount = ctx.EmployeeAddress.AsNoTracking().Count();
                employeesCount = ctx.Employee.AsNoTracking().Count();
                productCategoriesCount = ctx.ProductCategory.AsNoTracking().Count();
            }


            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
            }



            // Insert one line on each client
            int index = 10;
            foreach (var client in Clients)
            {
                // Insert one employee, address, employeeaddress
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);

                ctx.Database.OpenConnection();

                // Insert an employee
                var employee = new Employee
                {
                    EmployeeId = index,
                    FirstName = "John",
                    LastName = "Doe"
                };

                ctx.Add(employee);

                if (client.ProviderType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Employee ON;");

                await ctx.SaveChangesAsync();

                if (client.ProviderType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Employee OFF");

                // Insert a new address for employee 
                var city = "Paris " + HelperDatabase.GetRandomName();
                var addressline1 = "Rue Monthieu " + HelperDatabase.GetRandomName();
                var stateProvince = "Ile de France";
                var countryRegion = "France";
                var postalCode = "75001";

                var address = new Address
                {
                    AddressId = index,
                    AddressLine1 = addressline1,
                    City = city,
                    StateProvince = stateProvince,
                    CountryRegion = countryRegion,
                    PostalCode = postalCode

                };

                ctx.Add(address);
                if (client.ProviderType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Address ON;");

                await ctx.SaveChangesAsync();

                if (client.ProviderType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Address OFF");


                var employeeAddress = new EmployeeAddress
                {
                    EmployeeId = employee.EmployeeId,
                    AddressId = address.AddressId,
                    AddressType = "CLIENT"
                };

                ctx.EmployeeAddress.Add(employeeAddress);
                await ctx.SaveChangesAsync();


                ctx.Database.CloseConnection();

                index++;


            }

            // Insert one ProductCategory, Employee, Address, EmployeeAddress on server
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                ctx.Database.OpenConnection();

                var productId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
                var productCategoryNameServer = HelperDatabase.GetRandomName("SRV");


                // ProductCategory
                ctx.Add(new ProductCategory
                {
                    ProductCategoryId = productId,
                    Name = productCategoryNameServer
                });
                await ctx.SaveChangesAsync();

                // Insert an employee
                var employee = new Employee
                {
                    EmployeeId = 1000,
                    FirstName = "John",
                    LastName = "Doe"
                };

                ctx.Add(employee);

                if (this.ServerType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Employee ON;");

                await ctx.SaveChangesAsync();

                if (this.ServerType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Employee OFF");

                // Insert a new address for employee 
                var city = "Lyon " + HelperDatabase.GetRandomName();
                var addressline1 = HelperDatabase.GetRandomName();
                var stateProvince = "Rhones";
                var countryRegion = "France";
                var postalCode = "69001";

                var address = new Address
                {
                    AddressId = 1000,
                    AddressLine1 = addressline1,
                    City = city,
                    StateProvince = stateProvince,
                    CountryRegion = countryRegion,
                    PostalCode = postalCode

                };

                ctx.Add(address);
                if (this.ServerType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Address ON;");

                await ctx.SaveChangesAsync();

                if (this.ServerType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Address OFF");

                var employeeAddress = new EmployeeAddress
                {
                    EmployeeId = employee.EmployeeId,
                    AddressId = address.AddressId,
                    AddressType = "SERVER"
                };

                ctx.EmployeeAddress.Add(employeeAddress);
                await ctx.SaveChangesAsync();

                ctx.Database.CloseConnection();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var localScope = await agent.LocalOrchestrator.GetScopeInfoAsync();
                localScope.Setup.Tables["Employee"].SyncDirection = SyncDirection.UploadOnly;
                localScope.Setup.Tables["Address"].SyncDirection = SyncDirection.UploadOnly;
                localScope.Setup.Tables["EmployeeAddress"].SyncDirection = SyncDirection.UploadOnly;
                await agent.LocalOrchestrator.SaveScopeInfoAsync(localScope);


                var remoteScope = await agent.RemoteOrchestrator.GetScopeInfoAsync();
                remoteScope.Setup.Tables["Employee"].SyncDirection = SyncDirection.UploadOnly;
                remoteScope.Setup.Tables["Address"].SyncDirection = SyncDirection.UploadOnly;
                remoteScope.Setup.Tables["EmployeeAddress"].SyncDirection = SyncDirection.UploadOnly;
                await agent.RemoteOrchestrator.SaveScopeInfoAsync(remoteScope);


                var s = await agent.SynchronizeAsync();

                // Server shoud not sent back lines, so download equals 1 (just product category)
                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(3, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }


            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                // get all product categories
                var serverPC = ctx.ProductCategory.AsNoTracking().Count();
                Assert.Equal(productCategoriesCount + 1, serverPC);

                // get all employees
                var employees = ctx.Employee.AsNoTracking().Count();
                Assert.Equal(employeesCount + Clients.Count + 1, employees);
                // get all employees address
                var employeesAddresses = ctx.EmployeeAddress.AsNoTracking().Count();
                Assert.Equal(employeesAddressesCount + Clients.Count + 1, employeesAddresses);
                // get all addresses
                var addresses = ctx.Address.AsNoTracking().Count();
                Assert.Equal(addressesCount + Clients.Count + 1, addresses);

            }

            foreach (var client in Clients)
            {
                using var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
                // get all product categories
                var clientPC = cliCtx.ProductCategory.AsNoTracking().Count();
                Assert.Equal(productCategoriesCount + 1, clientPC);

                // get all employees
                var employees = cliCtx.Employee.AsNoTracking().Count();
                Assert.Equal(employeesCount + 1, employees);

                // get all employees address
                var employeesAddresses = cliCtx.EmployeeAddress.AsNoTracking().Count();
                Assert.Equal(employeesAddressesCount + 1, employeesAddresses);

                // get all addresses
                var addresses = cliCtx.Address.AsNoTracking().Count();
                Assert.Equal(addressesCount + 1, addresses);
            }

        }



        /// <summary>
        /// Create a snapshot, then remove a row from server, then sync
        /// </summary>
        [Fact]
        public async Task Snapshot_Initialize_ThenDeleteServerRows_ThenSync()
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            var setup = new SyncSetup(Tables);

            // snapshot directory
            var snapshotDirctory = HelperDatabase.GetRandomName();
            var directory = Path.Combine(Environment.CurrentDirectory, snapshotDirctory);

            var options = new SyncOptions
            {
                SnapshotsDirectory = directory,
                BatchSize = 3000
            };

            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider, options);


        }


        /// <summary>
        /// Insert one row on server, should be correctly sync on all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Parallel_Sync_For_TwentyClients(SyncOptions options)
        {
            // create a server database
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Provision server, to be sure no clients will try to do something that could break server
            var remoteOrchestrator = new RemoteOrchestrator(this.Server.Provider, options);

            // Ensure schema is ready on server side. Will create everything we need (triggers, tracking, stored proc, scopes)
            var setup = new SyncSetup(Tables);
            var serverScope = await remoteOrchestrator.GetScopeInfoAsync(SyncOptions.DefaultScopeName, setup);
            await remoteOrchestrator.ProvisionAsync(serverScope);

            var providers = this.Clients.Select(c => c.ProviderType).Distinct();

            var clientProviders = new List<CoreProvider>();
            var createdDatabases = new List<(ProviderType ProviderType, string DatabaseName)>();
            foreach (var provider in providers)
            {
                for (int i = 0; i < 10; i++)
                {
                    // Create the provider
                    var dbCliName = HelperDatabase.GetRandomName("tcp_cli_");
                    var localProvider = this.CreateProvider(provider, dbCliName);

                    clientProviders.Add(localProvider);

                    // Create the database
                    await this.CreateDatabaseAsync(provider, dbCliName, true);

                    createdDatabases.Add((provider, dbCliName));
                }
            }

            var allTasks = new List<Task<SyncResult>>();

            // Execute a sync on all clients and add the task to a list of tasks
            foreach (var clientProvider in clientProviders)
            {
                var agent = new SyncAgent(clientProvider, Server.Provider, options);
                allTasks.Add(agent.SynchronizeAsync());
            }

            // Await all tasks
            await Task.WhenAll(allTasks);

            foreach (var s in allTasks)
            {
                Assert.Equal(rowsCount, s.Result.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.Result.TotalChangesUploadedToServer);
                Assert.Equal(0, s.Result.TotalResolvedConflicts);
            }

            // Create a new product on server 
            var name = HelperDatabase.GetRandomName();
            var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

            var product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };

            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                serverDbCtx.Product.Add(product);
                await serverDbCtx.SaveChangesAsync();
            }

            allTasks = new List<Task<SyncResult>>();

            // Execute a sync on all clients to get the new server row
            foreach (var clientProvider in clientProviders)
            {
                var agent = new SyncAgent(clientProvider, Server.Provider, options);
                allTasks.Add(agent.SynchronizeAsync());
            }

            // Await all tasks
            await Task.WhenAll(allTasks);

            foreach (var s in allTasks)
            {
                Assert.Equal(1, s.Result.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.Result.TotalChangesUploadedToServer);
                Assert.Equal(0, s.Result.TotalResolvedConflicts);
            }

            foreach (var db in createdDatabases)
            {
                try
                {
                    HelperDatabase.DropDatabase(db.ProviderType, db.DatabaseName);
                }
                catch (Exception) { }
            }
        }


        /// <summary>
        /// Testing an insert / update on a table where a column is not part of the sync setup, and should stay alive after a sync
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async virtual Task OneColumn_NotInSetup_Row_IsUploaded_ToServer_ButValue_RemainsTheSame(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases with schema
            foreach (var client in this.Clients)
                await this.EnsureDatabaseSchemaAndSeedAsync(client, false, UseFallbackSchema);


            // this Guid will be updated on the client
            var clientGuid = Guid.NewGuid();

            // Get server Guid value, that should not change
            Guid? serverGuid;
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);
                serverGuid = address.Rowguid;
            }


            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var setup = new SyncSetup(new string[] { "Address" });

                // Add all columns to address except Rowguid and ModifiedDate
                setup.Tables["Address"].Columns.AddRange(new string[] {
                    "AddressId", "AddressLine1", "AddressLine2", "City", "StateProvince",
                    "CountryRegion", "PostalCode" });

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync(setup);

                // Editing Rowguid on client. This column is not part of the setup
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);

                var cliAddress = await ctx.Address.SingleAsync(a => a.AddressId == 1);

                // Now Update on client this address with a rowGuid
                cliAddress.Rowguid = clientGuid;

                await ctx.SaveChangesAsync();
            }

            // each client (except first one) will downloaded row from previous client sync
            var cliDownload = 0;
            // but it will raise a conflict
            var cliConflict = 0;

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var s = await agent.SynchronizeAsync();

                Assert.Equal(cliDownload, s.TotalChangesDownloadedFromServer);

                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(cliConflict, s.TotalResolvedConflicts);

                // check row on client should not have been updated 
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var cliAddress = await ctx.Address.AsNoTracking().SingleAsync(a => a.AddressId == 1);

                Assert.Equal(clientGuid, cliAddress.Rowguid);

                cliDownload = 1;
                cliConflict = 1;
            }


            // Check on server guid has not been uploaded
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);
                Assert.Equal(serverGuid, address.Rowguid);
            }
        }

        /// <summary>
        /// Testing an insert / update on a table where a column is not part of the sync setup, and should stay alive after a sync
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async virtual Task OneColumn_NotInSetup_AfterCleanMetadata_IsTracked_ButNotUpdated(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases with schema
            foreach (var client in this.Clients)
                await this.EnsureDatabaseSchemaAndSeedAsync(client, false, UseFallbackSchema);

            // this Guid will be updated on the client
            var clientGuid = Guid.NewGuid();

            // Get server Guid value, that should not change
            Guid? serverGuid;
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);
                serverGuid = address.Rowguid;
            }

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var setup = new SyncSetup(new string[] { "Address" });

                // Add all columns to address except Rowguid and ModifiedDate
                setup.Tables["Address"].Columns.AddRange(
                    new string[] { "AddressId", "AddressLine1", "AddressLine2",
                        "City", "StateProvince", "CountryRegion", "PostalCode" });

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync(setup);

                // call CleanMetadata to be sure we don't have row in tracking
                var ts = await agent.LocalOrchestrator.GetLocalTimestampAsync();

                // be sure we are deleting ALL rows from tracking table
                var dc = await agent.LocalOrchestrator.DeleteMetadatasAsync(ts + 1);

                // checking if there is no rows in tracking table for address
                var connection = client.Provider.CreateConnection();
                var command = connection.CreateCommand();

                // As Column names are case-sensitive in postgresql
                if (client.ProviderType == ProviderType.Postgres)
                    command.CommandText = "SELECT COUNT(*) FROM \"Address_tracking\"";
                else
                    command.CommandText = "SELECT COUNT(*) FROM Address_tracking";

                command.Connection = connection;
                await connection.OpenAsync();
                var count = await command.ExecuteScalarAsync();
                var countRows = Convert.ToInt32(count);
                Assert.Equal(0, countRows);
                connection.Close();

                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);

                // Editing Rowguid on client. This column is not part of the setup
                // So far, it should be uploaded to server but without the column value
                var cliAddress = await ctx.Address.SingleAsync(a => a.AddressId == 1);

                // Now Update on client this address with a rowGuid
                cliAddress.Rowguid = clientGuid;

                await ctx.SaveChangesAsync();

                await connection.OpenAsync();
                count = await command.ExecuteScalarAsync();
                countRows = Convert.ToInt32(count);
                Assert.Equal(1, countRows);
                connection.Close();


            }

            // each client (except first one) will downloaded row from previous client sync
            var cliDownload = 0;
            // but it will raise a conflict
            var cliConflict = 0;

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(cliDownload, s.TotalChangesDownloadedFromServer);

                // 1 upload since Rowguid is modified, but the column value is not part of the upload
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(cliConflict, s.TotalResolvedConflicts);

                // check row on client should not have been updated 
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var cliAddress = await ctx.Address.AsNoTracking().SingleAsync(a => a.AddressId == 1);

                Assert.Equal(clientGuid, cliAddress.Rowguid);
                cliConflict = 1;
                cliDownload = 1;
            }


            // Check on server guid has not been uploaded
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);
                Assert.Equal(serverGuid, address.Rowguid);
            }
        }


        /// <summary>
        /// Testing that an upate from the server does not replace, but just update the local row, so that columns that are not included in the sync are not owervritten/cleared
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async virtual Task OneColumn_NotInSetup_IfServerSendsChanges_UpdatesLocalRow_AndDoesNotClear_OneColumn(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases with schema
            foreach (var client in this.Clients)
                await this.EnsureDatabaseSchemaAndSeedAsync(client, false, UseFallbackSchema);


            // this Guid will be updated on the client
            var clientGuid = Guid.NewGuid();

            // Get server Guid value, that should not change
            Guid? serverGuid;
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);
                serverGuid = address.Rowguid;
            }


            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var setup = new SyncSetup(new string[] { "Address" });

                // Add all columns to address except Rowguid and ModifiedDate
                setup.Tables["Address"].Columns.AddRange(new string[]
                {   "AddressId", "AddressLine1",
                    "AddressLine2", "City", "StateProvince",
                    "CountryRegion", "PostalCode"
                });

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync(setup);

                // Editing Rowguid on client. This column is not part of the setup
                // The row will be uploaded to the server but the column will not be overriden
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);

                var cliAddress = await ctx.Address.SingleAsync(a => a.AddressId == 1);

                // Now Update on client this address with a rowGuid
                cliAddress.Rowguid = clientGuid;

                await ctx.SaveChangesAsync();
            }

            // Act
            // Change row on server and make sure that client rows are just UPDATED and not REPLACED
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);
                address.City = "Mimecity";
                await serverDbCtx.SaveChangesAsync();
            }

            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var s = await agent.SynchronizeAsync();

                // "Mimecity" change should be received from server
                Assert.Equal(1, s.TotalChangesDownloadedFromServer);

                // One upload
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                // We have resolved a conflict here
                Assert.Equal(1, s.TotalResolvedConflicts);

                // check row on client should not have been updated 
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var cliAddress = await ctx.Address.AsNoTracking().SingleAsync(a => a.AddressId == 1);

                Assert.Equal(clientGuid, cliAddress.Rowguid);
                Assert.Equal("Mimecity", cliAddress.City);
            }


            // Check on server guid has not been uploaded
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);
                Assert.Equal(serverGuid, address.Rowguid);
            }
        }


        /// <summary>
        /// </summary>
        [Fact]
        public virtual async Task Using_ExistingClientDatabase_UpdateUntrackedRowsAsync()
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // generate a sync conf to host the schema
            var setup = new SyncSetup(this.Tables);

            // options
            var options = new SyncOptions();

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                // Get count of rows
                var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

                // create a client schema without seeding
                await this.EnsureDatabaseSchemaAndSeedAsync(client, false, UseFallbackSchema);

                var name = HelperDatabase.GetRandomName();
                var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };

                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                ctx.Product.Add(product);
                await ctx.SaveChangesAsync();

                // create an agent
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                // mark local row as tracked
                await agent.LocalOrchestrator.UpdateUntrackedRowsAsync();
                s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                Assert.Equal(this.GetServerDatabaseRowsCount(Server), this.GetServerDatabaseRowsCount(client));
            }
        }


    }
}
