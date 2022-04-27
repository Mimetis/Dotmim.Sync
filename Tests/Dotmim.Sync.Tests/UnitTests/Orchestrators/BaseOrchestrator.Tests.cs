using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests.UnitTests
{
    public partial class BaseOrchestratorTests : IDisposable
    {
        public string[] Tables => new string[]
        {
            "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product", "Employee", "Customer", "Address", "CustomerAddress", "EmployeeAddress",
            "SalesLT.SalesOrderHeader", "SalesLT.SalesOrderDetail",  "Posts", "Tags", "PostTag",
            "PricesList", "PricesListCategory", "PricesListDetail"
        };

        // Current test running
        private ITest test;
        private Stopwatch stopwatch;
        public ITestOutputHelper Output { get; }


        public BaseOrchestratorTests(ITestOutputHelper output)
        {

            // Getting the test running
            this.Output = output;
            var type = output.GetType();
            var testMember = type.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
            this.test = (ITest)testMember.GetValue(output);
            this.stopwatch = Stopwatch.StartNew();
        }

        public void Dispose()
        {

            this.stopwatch.Stop();

            var str = $"{test.TestCase.DisplayName} : {this.stopwatch.Elapsed.Minutes}:{this.stopwatch.Elapsed.Seconds}.{this.stopwatch.Elapsed.Milliseconds}";
            Console.WriteLine(str);
            Debug.WriteLine(str);

        }
        [Fact]
        public void BaseOrchestrator_Constructor()
        {
            var provider = new SqlSyncProvider();
            var options = new SyncOptions();
            var orchestrator = new LocalOrchestrator(provider, options);

            Assert.NotNull(orchestrator.Options);
            Assert.Same(options, orchestrator.Options);

            Assert.NotNull(orchestrator.Provider);
            Assert.Same(provider, orchestrator.Provider);

            Assert.NotNull(provider.Orchestrator);
            Assert.Same(provider.Orchestrator, orchestrator);

        }

        [Fact]
        public void BaseOrchestrator_ShouldFail_When_Args_AreNull()
        {
            var provider = new SqlSyncProvider();
            var options = new SyncOptions();
            var setup = new SyncSetup();

            Assert.Throws<ArgumentNullException>(() => new LocalOrchestrator(null, null));
            Assert.Throws<ArgumentNullException>(() => new LocalOrchestrator(provider, null));
            Assert.Throws<ArgumentNullException>(() => new LocalOrchestrator(null, options));
        }

        [Fact]
        public void BaseOrchestrator_GetContext_ShouldBeInitialized()
        {
            var options = new SyncOptions();
            var setup = new SyncSetup();
            var provider = new SqlSyncProvider();

            var localOrchestrator = new LocalOrchestrator(provider, options);

            var ctx = localOrchestrator.GetContext("scope1");

            Assert.Equal(SyncStage.None, ctx.SyncStage);
            Assert.Equal("scope1", ctx.ScopeName);
            Assert.Equal(SyncType.Normal, ctx.SyncType);
            Assert.Equal(SyncWay.None, ctx.SyncWay);
            Assert.Null(ctx.Parameters);
        }

        [Fact]
        public async Task BaseOrchestrator_GetServerScope_ShouldFail_If_SetupIsEmpty()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);

            var options = new SyncOptions();
            var setup = new SyncSetup();
            var provider = new SqlSyncProvider(cs);

            var orchestrator = new RemoteOrchestrator(provider, options);

            var se = await Assert.ThrowsAsync<SyncException>(
                async () => await orchestrator.GetServerScopeAsync("scope1", setup));

            Assert.Equal(SyncStage.ScopeLoading, se.SyncStage);
            Assert.Equal(SyncSide.ServerSide, se.Side);
            Assert.Equal("MissingTablesException", se.TypeName);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);

        }

        internal static void AssertConnectionAndTransaction(BaseOrchestrator orchestrator, string scopeName, SyncStage stage)
        {
            orchestrator.OnConnectionOpen(args =>
            {
                Assert.IsType<ConnectionOpenedArgs>(args);
                Assert.Equal(stage, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.Null(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
            });
            orchestrator.OnTransactionOpen(args =>
            {
                Assert.IsType<TransactionOpenedArgs>(args);
                Assert.Equal(stage, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.NotNull(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
                Assert.Same(args.Connection, args.Transaction.Connection);
            });
            orchestrator.OnTransactionCommit(args =>
            {
                Assert.IsType<TransactionCommitArgs>(args);
                Assert.Equal(stage, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.NotNull(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
                Assert.Same(args.Connection, args.Transaction.Connection);
            });
            orchestrator.OnConnectionClose(args =>
            {
                Assert.IsType<ConnectionClosedArgs>(args);
                Assert.Equal(stage, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.Null(args.Transaction);
                Assert.Equal(ConnectionState.Closed, args.Connection.State);
            });

        }

        [Fact]
        public async Task BaseOrchestrator_GetSchema_ShouldReturnSchema()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();
            var scopeName = "scope";

            var options = new SyncOptions();
            var setup = new SyncSetup(this.Tables);

            var onSchemaRead = false;
            var onSchemaReading = false;

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            localOrchestrator.OnSchemaLoading(args =>
            {
                Assert.Equal(scopeName, args.Context.ScopeName);
                onSchemaReading = true;
            });

            localOrchestrator.OnSchemaLoaded(args =>
            {
                Assert.IsType<SchemaLoadedArgs>(args);
                Assert.Equal(SyncStage.ScopeLoading, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.Null(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
                Assert.Equal(16, args.Schema.Tables.Count);
                onSchemaRead = true;

            });

            AssertConnectionAndTransaction(localOrchestrator, scopeName, SyncStage.ScopeLoading);

            var scopeInfo = await localOrchestrator.GetClientScopeAsync(scopeName, setup);

            Assert.NotNull(scopeInfo.Schema);
            Assert.NotNull(scopeInfo.Setup);
            Assert.Equal(SyncStage.ScopeLoading, localOrchestrator.GetContext(scopeName).SyncStage);
            Assert.Equal(16, scopeInfo.Schema.Tables.Count);
            Assert.True(onSchemaRead);
            Assert.True(onSchemaReading);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task BaseOrchestrator_GetSchema_CancellationToken_ShouldInterrupt()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();
            var setup = new SyncSetup(this.Tables);

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);
            using var cts = new CancellationTokenSource();

            localOrchestrator.OnConnectionOpen(args =>
            {
                Assert.Equal(SyncStage.ScopeLoading, args.Context.SyncStage);
                Assert.IsType<ConnectionOpenedArgs>(args);
                Assert.NotNull(args.Connection);
                Assert.Null(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
                cts.Cancel();
            });

            var se = await Assert.ThrowsAsync<SyncException>(async () =>
            {
                var scopeInfo = await localOrchestrator.GetClientScopeAsync(cancellationToken: cts.Token);
            });

            Assert.Equal(SyncStage.ScopeLoading, se.SyncStage);
            Assert.Equal(SyncSide.ClientSide, se.Side);
            Assert.Equal("OperationCanceledException", se.TypeName);


            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task BaseOrchestrator_GetSchema_SetupColumnsDefined_ShouldReturn_SchemaWithSetupColumnsOnly()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();
            // Create a bad setup with a non existing table

            var tables = new string[] { "Customer", "Address", "CustomerAddress" };
            var setup = new SyncSetup(tables);
            setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "FirstName", "LastName", "CompanyName" });

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var scopeInfo = await localOrchestrator.GetClientScopeAsync(setup);

            Assert.Equal(SyncStage.ScopeLoading, localOrchestrator.GetContext(SyncOptions.DefaultScopeName).SyncStage);
            Assert.Equal(3, scopeInfo.Schema.Tables.Count);

            // Only 4 columns shoud be part of Customer table
            Assert.Equal(4, scopeInfo.Schema.Tables["Customer"].Columns.Count);

            Assert.Equal(9, scopeInfo.Schema.Tables["Address"].Columns.Count);
            Assert.Equal(5, scopeInfo.Schema.Tables["CustomerAddress"].Columns.Count);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task BaseOrchestrator_GetSchema_NoPrimaryKeysColumn_ShouldFail()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();
            // Create a bad setup with a non existing table

            var tables = new string[] { "Customer", "Address", "CustomerAddress" };
            var setup = new SyncSetup(tables);
            setup.Tables["Customer"].Columns.AddRange(new string[] { "FirstName", "LastName", "CompanyName" });

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var se = await Assert.ThrowsAsync<SyncException>(async () =>
            {
                var scopeInfo = await localOrchestrator.GetClientScopeAsync(setup);
            });

            Assert.Equal(SyncStage.ScopeLoading, se.SyncStage);
            Assert.Equal(SyncSide.ClientSide, se.Side);
            Assert.Equal("MissingPrimaryKeyColumnException", se.TypeName);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task BaseOrchestrator_GetSchema_NonExistingColumns_ShouldFail()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();
            // Create a bad setup with a non existing table

            var tables = new string[] { "Customer", "Address", "CustomerAddress" };
            var setup = new SyncSetup(tables);
            setup.Tables["Customer"].Columns.AddRange(new string[] { "FirstName", "LastName", "CompanyName", "BADCOLUMN" });

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var se = await Assert.ThrowsAsync<SyncException>(async () =>
            {
                await localOrchestrator.GetClientScopeAsync(setup);
            });

            Assert.Equal(SyncStage.ScopeLoading, se.SyncStage);
            Assert.Equal(SyncSide.ClientSide, se.Side);
            Assert.Equal("MissingColumnException", se.TypeName);


            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task BaseOrchestrator_GetSchema_NonExistingTables_ShouldFail()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();
            // Create a bad setup with a non existing table

            var tables = new string[]
            {
                "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product", "Employee", "Customer", "Address", "CustomerAddress", "EmployeeAddress",
                "SalesLT.SalesOrderHeader", "SalesLT.SalesOrderDetail", "Posts", "Tags", "PostTag",
                "PricesList", "PricesListCategory", "PricesListDetail", "WRONGTABLE"
            };
            var setup = new SyncSetup(tables);

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var se = await Assert.ThrowsAsync<SyncException>(async () =>
            {
                await localOrchestrator.GetClientScopeAsync(setup);
            });

            Assert.Equal(SyncStage.ScopeLoading, se.SyncStage);
            Assert.Equal(SyncSide.ClientSide, se.Side);
            Assert.Equal("MissingTableException", se.TypeName);


            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }


        [Fact]
        public async Task BaseOrchestrator_Provision_ShouldFail_If_SetupIsEmpty()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();
            var scopeName = "scope";

            var options = new SyncOptions();
            var setup = new SyncSetup();

            var orchestrator = new LocalOrchestrator(sqlProvider, options);

            var provision = SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

            var se = await Assert.ThrowsAsync<SyncException>(
                async () => await orchestrator.ProvisionAsync(scopeName, setup, provision));

            Assert.Equal(SyncStage.Provisioning, se.SyncStage);
            Assert.Equal(SyncSide.ClientSide, se.Side);
            Assert.Equal("MissingTablesException", se.TypeName);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task BaseOrchestrator_Provision_SchemaCreated_If_SetupHasTables()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();
            var scopeName = "scope";

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var provision = SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

            var clientScopeInfo = await localOrchestrator.ProvisionAsync(scopeName, setup, provision);

            var context = localOrchestrator.GetContext(scopeName);

            Assert.Equal(SyncStage.Provisioning, context.SyncStage);
            Assert.Single(clientScopeInfo.Schema.Tables);
            Assert.Equal("SalesLT.Product", clientScopeInfo.Schema.Tables[0].GetFullName());
            Assert.Equal(17, clientScopeInfo.Schema.Tables[0].Columns.Count);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }


        [Fact]
        public async Task BaseOrchestrator_Provision_SchemaNotCreated_If_SetupHasTables_AndDbIsEmpty()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var scopeName = "scope";

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var provision = SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

            var se = await Assert.ThrowsAsync<SyncException>(
                async () => await localOrchestrator.ProvisionAsync(scopeName, setup, provision));

            Assert.Equal(SyncStage.Provisioning, se.SyncStage);
            Assert.Equal(SyncSide.ClientSide, se.Side);
            Assert.Equal("MissingTableException", se.TypeName);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task BaseOrchestrator_Provision_SchemaCreated_If_SchemaHasColumnsDefinition()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var scopeName = "scope";

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            var schema = new SyncSet();
            var table = new SyncTable("Product", "SalesLT");
            var colID = new SyncColumn("ID", typeof(Guid));
            var colName = new SyncColumn("Name", typeof(string));

            table.Columns.Add(colID);
            table.Columns.Add(colName);
            table.Columns.Add("Number", typeof(int));
            table.PrimaryKeys.Add("ID");

            schema.Tables.Add(table);

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var scopeInfo = await localOrchestrator.GetClientScopeAsync(scopeName);
            scopeInfo.Schema = schema;
            scopeInfo.Setup = setup;

            var provision = SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

            await localOrchestrator.ProvisionAsync(scopeInfo, provision);

            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);

                var tbl = await SqlManagementUtils.GetTableAsync(c, null, "Product", "SalesLT");

                var tblName = tbl.Rows[0]["TableName"].ToString();
                var schName = tbl.Rows[0]["SchemaName"].ToString();

                Assert.Equal(table.TableName, tblName);
                Assert.Equal(table.SchemaName, schName);

                var cols = await SqlManagementUtils.GetColumnsForTableAsync(c, null, "Product", "SalesLT");

                Assert.Equal(3, cols.Rows.Count);

                c.Close();
            }

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task BaseOrchestrator_Provision_ShouldCreate_TrackingTable()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var scopeName = "scope";

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" })
            { TrackingTablesSuffix = "sync", TrackingTablesPrefix = "trck" };

            var schema = new SyncSet();
            var table = new SyncTable("Product", "SalesLT");
            var colID = new SyncColumn("ID", typeof(Guid));
            var colName = new SyncColumn("Name", typeof(string));

            table.Columns.Add(colID);
            table.Columns.Add(colName);
            table.Columns.Add("Number", typeof(int));
            table.PrimaryKeys.Add("ID");

            schema.Tables.Add(table);

            // trackign table name is composed with prefix and suffix from setup
            var trackingTableName = $"{setup.TrackingTablesPrefix}{table.TableName}{setup.TrackingTablesSuffix}";

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var scopeInfo = await localOrchestrator.GetClientScopeAsync(scopeName);
            scopeInfo.Setup = setup;
            scopeInfo.Schema = schema;

            var provision = SyncProvision.TrackingTable;

            await localOrchestrator.ProvisionAsync(scopeInfo, provision);

            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);

                var tbl = await SqlManagementUtils.GetTableAsync(c, null, trackingTableName, "SalesLT");

                var tblName = tbl.Rows[0]["TableName"].ToString();
                var schName = tbl.Rows[0]["SchemaName"].ToString();

                Assert.Equal(trackingTableName, tblName);
                Assert.Equal(table.SchemaName, schName);

                c.Close();
            }

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }


        [Fact]
        public async Task BaseOrchestrator_Provision_ShouldCreate_StoredProcedures_WithSpecificScopeName()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);
            // Create default table
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var scopeName = "scope";

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" })
            {
                StoredProceduresPrefix = "s",
                StoredProceduresSuffix = "proc"
            };

            // trackign table name is composed with prefix and suffix from setup
            var bulkDelete = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_{scopeName}_bulkdelete";
            var bulkUpdate = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_{scopeName}_bulkupdate";
            var changes = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_{scopeName}_changes";
            var delete = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_{scopeName}_delete";
            var deletemetadata = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_{scopeName}_deletemetadata";
            var initialize = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_{scopeName}_initialize";
            var reset = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_{scopeName}_reset";
            var selectrow = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_{scopeName}_selectrow";
            var update = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_{scopeName}_update";

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            // Needs the tracking table to be able to create stored procedures
            var provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures;

            var scopeInfo = await localOrchestrator.GetClientScopeAsync(scopeName, setup);

            await localOrchestrator.ProvisionAsync(scopeInfo, provision);

            using (var connection = new SqlConnection(cs))
            {
                await connection.OpenAsync().ConfigureAwait(false);

                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, bulkDelete));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, bulkUpdate));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, changes));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, delete));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, deletemetadata));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, initialize));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, reset));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, selectrow));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, update));

                connection.Close();
            }

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }
        [Fact]
        public async Task BaseOrchestrator_Provision_ShouldCreate_StoredProcedures_WithDefaultScopeName()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);
            // Create default table
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" })
            {
                StoredProceduresPrefix = "s",
                StoredProceduresSuffix = "proc"
            };

            // trackign table name is composed with prefix and suffix from setup
            var bulkDelete = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_bulkdelete";
            var bulkUpdate = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_bulkupdate";
            var changes = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_changes";
            var delete = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_delete";
            var deletemetadata = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_deletemetadata";
            var initialize = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_initialize";
            var reset = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_reset";
            var selectrow = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_selectrow";
            var update = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_update";

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            // Needs the tracking table to be able to create stored procedures
            var provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures;

            var scopeInfo = await localOrchestrator.GetClientScopeAsync(setup);

            await localOrchestrator.ProvisionAsync(scopeInfo, provision);

            using (var connection = new SqlConnection(cs))
            {
                await connection.OpenAsync().ConfigureAwait(false);

                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, bulkDelete));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, bulkUpdate));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, changes));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, delete));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, deletemetadata));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, initialize));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, reset));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, selectrow));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, update));

                connection.Close();
            }

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }


        [Fact]
        public async Task BaseOrchestrator_Provision_ShouldCreate_Table()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var scopeName = "scope";

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            var schema = new SyncSet();
            var table = new SyncTable("Product", "SalesLT");
            var colID = new SyncColumn("ID", typeof(Guid));
            var colName = new SyncColumn("Name", typeof(string));

            table.Columns.Add(colID);
            table.Columns.Add(colName);
            table.Columns.Add("Number", typeof(int));
            table.PrimaryKeys.Add("ID");

            schema.Tables.Add(table);

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var scopeInfo = await localOrchestrator.GetClientScopeAsync(scopeName);
            scopeInfo.Schema = schema;
            scopeInfo.Setup = setup;

            var provision = SyncProvision.Table;

            await localOrchestrator.ProvisionAsync(scopeInfo, provision);

            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);

                var tbl = await SqlManagementUtils.GetTableAsync(c, null, "Product", "SalesLT");

                var tblName = tbl.Rows[0]["TableName"].ToString();
                var schName = tbl.Rows[0]["SchemaName"].ToString();

                Assert.Equal(table.TableName, tblName);
                Assert.Equal(table.SchemaName, schName);

                c.Close();
            }

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }



        [Fact]
        public async Task BaseOrchestrator_Provision_SchemaFail_If_SchemaHasColumnsDefinitionButNoPrimaryKey()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var scopeName = "scope";

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            var schema = new SyncSet();
            var table = new SyncTable("Product", "SalesLT");
            var colID = new SyncColumn("ID", typeof(Guid));
            var colName = new SyncColumn("Name", typeof(string));

            table.Columns.Add(colID);
            table.Columns.Add(colName);
            table.Columns.Add("Number", typeof(int));

            schema.Tables.Add(table);

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var scopeInfo = await localOrchestrator.GetClientScopeAsync(scopeName);
            scopeInfo.Schema = schema;
            scopeInfo.Setup = setup;

            var provision = SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

            var se = await Assert.ThrowsAsync<SyncException>(
                async () => await localOrchestrator.ProvisionAsync(scopeInfo, provision));

            Assert.Equal(SyncStage.Provisioning, se.SyncStage);
            Assert.Equal(SyncSide.ClientSide, se.Side);
            Assert.Equal("MissingPrimaryKeyException", se.TypeName);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }


        [Fact]
        public async Task BaseOrchestrator_Provision_ShouldFails_If_SetupTable_DoesNotExist()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();
            var scopeName = "scope";

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.badTable" });

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var se = await Assert.ThrowsAsync<SyncException>(async () =>
                await localOrchestrator.GetClientScopeAsync(scopeName, setup));

            Assert.Equal(SyncStage.ScopeLoading, se.SyncStage);
            Assert.Equal(SyncSide.ClientSide, se.Side);
            Assert.Equal("MissingTableException", se.TypeName);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }



    }
}
