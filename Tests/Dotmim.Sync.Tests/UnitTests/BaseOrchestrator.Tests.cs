using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Tests.UnitTests
{
    public partial class BaseOrchestratorTests
    {

        public string[] Tables => new string[]
        {
            "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product", "Employee", "Customer", "Address", "CustomerAddress", "EmployeeAddress",
            "SalesLT.SalesOrderHeader", "SalesLT.SalesOrderDetail", "dbo.Sql", "Posts", "Tags", "PostTag",
            "PricesList", "PricesListCategory", "PricesListDetail"
        };


        [Fact]
        public void BaseOrchestrator_Constructor()
        {
            var provider = new MockProvider();
            var options = new SyncOptions();
            var setup = new SyncSetup();
            var orchestrator = new MockBaseClientOrchestrator(provider, options, setup);

            Assert.NotNull(orchestrator.Options);
            Assert.Same(options, orchestrator.Options);

            Assert.NotNull(orchestrator.Provider);
            Assert.Same(provider, orchestrator.Provider);

            Assert.NotNull(orchestrator.Setup);
            Assert.Same(setup, orchestrator.Setup);

            Assert.NotNull(provider.Orchestrator);
            Assert.Same(provider.Orchestrator, orchestrator);

        }

        [Fact]
        public void BaseOrchestrator_ShouldFail_When_Args_AreNull()
        {
            var provider = new MockProvider();
            var options = new SyncOptions();
            var setup = new SyncSetup();

            Assert.Throws<ArgumentNullException>(() => new MockBaseClientOrchestrator(null, null, null));
            Assert.Throws<ArgumentNullException>(() => new MockBaseClientOrchestrator(provider, null, null));
            Assert.Throws<ArgumentNullException>(() => new MockBaseClientOrchestrator(provider, options, null));
            Assert.Throws<ArgumentNullException>(() => new MockBaseClientOrchestrator(null, options, setup));
            Assert.Throws<ArgumentNullException>(() => new MockBaseClientOrchestrator(null, null, setup));
        }

        [Fact]
        public void BaseOrchestrator_GetContext_ShouldBeInitialized()
        {
            var options = new SyncOptions();
            var setup = new SyncSetup();
            var provider = new SqlSyncProvider();

            var localOrchestrator = new MockBaseClientOrchestrator(provider, options, setup, "scope1");

            var ctx = localOrchestrator.GetContext();

            Assert.Equal(SyncStage.None, ctx.SyncStage);
            Assert.Equal(localOrchestrator.ScopeName, ctx.ScopeName);
            Assert.Equal(SyncType.Normal, ctx.SyncType);
            Assert.Equal(SyncWay.None, ctx.SyncWay);
            Assert.Null(ctx.Parameters);
        }

        [Fact]
        public async Task BaseOrchestrator_GetSchema_ShouldFail_If_SetupIsEmpty()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);

            var options = new SyncOptions();
            var setup = new SyncSetup();
            var provider = new SqlSyncProvider(cs);

            var orchestrator = new MockBaseServerOrchestrator(provider, options, setup);

            var se = await Assert.ThrowsAsync<SyncException>(async () => await orchestrator.GetSchemaAsync());

            Assert.Equal(SyncStage.SchemaReading, se.SyncStage);
            Assert.Equal(SyncSide.ServerSide, se.Side);
            Assert.Equal("MissingTablesException", se.TypeName);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);

        }
         
        internal static void AssertConnectionAndTransaction(BaseOrchestrator orchestrator, SyncStage beginStage, SyncStage endStage)
        {
            orchestrator.OnConnectionOpen(args =>
            {
                Assert.IsType<ConnectionOpenedArgs>(args);
                Assert.Equal(beginStage, args.Context.SyncStage);
                Assert.Equal(orchestrator.ScopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.Null(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
            });
            orchestrator.OnTransactionOpen(args =>
            {
                Assert.IsType<TransactionOpenedArgs>(args);
                Assert.Equal(beginStage, args.Context.SyncStage);
                Assert.Equal(orchestrator.ScopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.NotNull(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
                Assert.Same(args.Connection, args.Transaction.Connection);
            });
            orchestrator.OnTransactionCommit(args =>
            {
                Assert.IsType<TransactionCommitArgs>(args);
                Assert.Equal(beginStage, args.Context.SyncStage);
                Assert.Equal(orchestrator.ScopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.NotNull(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
                Assert.Same(args.Connection, args.Transaction.Connection);
            });
            orchestrator.OnConnectionClose(args =>
            {
                Assert.IsType<ConnectionClosedArgs>(args);
                Assert.Equal(endStage, args.Context.SyncStage);
                Assert.Equal(orchestrator.ScopeName, args.Context.ScopeName);
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

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup, scopeName);

 
            localOrchestrator.OnSchemaRead(args =>
            {
                Assert.IsType<SchemaArgs>(args);
                Assert.Equal(SyncStage.SchemaRead, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.Null(args.Transaction);
                Assert.Equal(ConnectionState.Closed, args.Connection.State);
                Assert.Equal(17, args.Schema.Tables.Count);
                onSchemaRead = true;

            });

            AssertConnectionAndTransaction(localOrchestrator, SyncStage.SchemaReading, SyncStage.SchemaRead);

            var schema = await localOrchestrator.GetSchemaAsync();

            Assert.NotNull(schema);
            Assert.Equal(SyncStage.SchemaRead, localOrchestrator.GetContext().SyncStage);
            Assert.Equal(17, schema.Tables.Count);
            Assert.True(onSchemaRead);

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

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup);
            var cts = new CancellationTokenSource();

            localOrchestrator.OnConnectionOpen(args =>
            {
                Assert.Equal(SyncStage.SchemaReading, args.Context.SyncStage);
                Assert.IsType<ConnectionOpenedArgs>(args);
                Assert.NotNull(args.Connection);
                Assert.Null(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
                cts.Cancel();
            });

            var se = await Assert.ThrowsAsync<SyncException>(async () =>
            {
                var schema = await localOrchestrator.GetSchemaAsync(cts.Token);
            });

            Assert.Equal(SyncStage.SchemaReading, se.SyncStage);
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

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup);

            var schema = await localOrchestrator.GetSchemaAsync();

            Assert.Equal(SyncStage.SchemaRead, localOrchestrator.GetContext().SyncStage);
            Assert.Equal(3, schema.Tables.Count);

            // Only 4 columns shoud be part of Customer table
            Assert.Equal(4, schema.Tables["Customer"].Columns.Count);

            Assert.Equal(9, schema.Tables["Address"].Columns.Count);
            Assert.Equal(5, schema.Tables["CustomerAddress"].Columns.Count);

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

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup);

            var se = await Assert.ThrowsAsync<SyncException>(async () =>
            {
                var schema = await localOrchestrator.GetSchemaAsync();
            });

            Assert.Equal(SyncStage.SchemaReading, se.SyncStage);
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

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup);

            var se = await Assert.ThrowsAsync<SyncException>(async () =>
            {
                var schema = await localOrchestrator.GetSchemaAsync();
            });

            Assert.Equal(SyncStage.SchemaReading, se.SyncStage);
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
                "SalesLT.SalesOrderHeader", "SalesLT.SalesOrderDetail", "dbo.Sql", "Posts", "Tags", "PostTag",
                "PricesList", "PricesListCategory", "PricesListDetail", "WRONGTABLE"
            };
            var setup = new SyncSetup(tables);

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup);

            var se = await Assert.ThrowsAsync<SyncException>(async () =>
            {
                var schema = await localOrchestrator.GetSchemaAsync();
            });

            Assert.Equal(SyncStage.SchemaReading, se.SyncStage);
            Assert.Equal(SyncSide.ClientSide, se.Side);
            Assert.Equal("MissingTableException", se.TypeName);


            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task BaseOrchestrator_Provision_Check_Interceptors()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();
            var scopeName = "scope";

            var options = new SyncOptions();
            // Options set here and we must check if TableBuilder has correct version
            options.UseBulkOperations = true;

            var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            var onTableProvisioned = false;
            var onTableProvisioning = false;
            var onDatabaseProvisioned = false;
            var onDatabaseProvisioning = false;

            var provision = SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup, scopeName);

            // Assuming GetSchemaAsync() is correct
            // make the GetSchema call, before registrating interceptors
            var schema = await localOrchestrator.GetSchemaAsync();


            localOrchestrator.OnDatabaseProvisioning(args =>
            {
                Assert.IsType<DatabaseProvisioningArgs>(args);
                Assert.Equal(SyncStage.Provisioning, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.NotNull(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
                Assert.Equal(provision, args.Provision);
                Assert.Equal(schema, args.Schema);

                onDatabaseProvisioning = true;
            });

            localOrchestrator.OnDatabaseProvisioned(args =>
            {
                Assert.IsType<DatabaseProvisionedArgs>(args);
                Assert.Equal(SyncStage.Provisioned, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.Null(args.Transaction);
                Assert.Equal(ConnectionState.Closed, args.Connection.State);
                Assert.Equal(provision, args.Provision);
                Assert.Equal(schema, args.Schema);

                onDatabaseProvisioned = true;
            });

            localOrchestrator.OnTableProvisioning(args =>
            {
                Assert.IsType<TableProvisioningArgs>(args);
                Assert.Equal(SyncStage.Provisioning, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.NotNull(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
                Assert.True(args.TableBuilder.UseBulkProcedures);
                Assert.False(args.TableBuilder.UseChangeTracking);
                Assert.NotNull(args.TableBuilder.TableDescription);
                Assert.Equal("SalesLT.Product", args.TableBuilder.TableDescription.GetFullName());

                onTableProvisioning = true;
            });
            localOrchestrator.OnTableProvisioned(args =>
            {
                Assert.IsType<TableProvisionedArgs>(args);

                // We are still provisioning the tables
                Assert.Equal(SyncStage.Provisioning, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.NotNull(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
                Assert.NotNull(args.SchemaTable);
                Assert.Equal("SalesLT.Product", args.SchemaTable.GetFullName());

                onTableProvisioned = true;
            });

            AssertConnectionAndTransaction(localOrchestrator, SyncStage.Provisioning, SyncStage.Provisioned);

            await localOrchestrator.ProvisionAsync(schema, provision);

            Assert.Equal(SyncStage.Provisioned, localOrchestrator.GetContext().SyncStage);

            Assert.True(onTableProvisioned);
            Assert.True(onTableProvisioning);
            Assert.True(onDatabaseProvisioned);
            Assert.True(onDatabaseProvisioning);

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

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup, scopeName);

            var provision = SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

            var se = await Assert.ThrowsAsync<SyncException>(async () => await localOrchestrator.ProvisionAsync(provision));

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

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup, scopeName);

            var provision = SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

            var schema = await localOrchestrator.ProvisionAsync(provision);

            var context = localOrchestrator.GetContext();

            Assert.Equal(SyncStage.Provisioned, context.SyncStage);
            Assert.Single(schema.Tables);
            Assert.Equal("SalesLT.Product", schema.Tables[0].GetFullName());
            Assert.Equal(17, schema.Tables[0].Columns.Count);

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

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup, scopeName);

            var provision = SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

            var se = await Assert.ThrowsAsync<SyncException>(async () => await localOrchestrator.ProvisionAsync(provision));

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
            var setup = new SyncSetup();

            var schema = new SyncSet();
            var table = new SyncTable("Product", "SalesLT");
            var colID = new SyncColumn("ID", typeof(Guid));
            var colName = new SyncColumn("Name", typeof(string));

            table.Columns.Add(colID);
            table.Columns.Add(colName);
            table.Columns.Add("Number", typeof(int));
            table.PrimaryKeys.Add("ID");

            schema.Tables.Add(table);

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup, scopeName);

            var provision = SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

            await localOrchestrator.ProvisionAsync(schema, provision);

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
            var setup = new SyncSetup();

            var schema = new SyncSet();
            var table = new SyncTable("Product", "SalesLT");
            var colID = new SyncColumn("ID", typeof(Guid));
            var colName = new SyncColumn("Name", typeof(string));

            table.Columns.Add(colID);
            table.Columns.Add(colName);
            table.Columns.Add("Number", typeof(int));
            table.PrimaryKeys.Add("ID");

            schema.TrackingTablesSuffix = "sync";
            schema.TrackingTablesPrefix = "trck";

            schema.Tables.Add(table);

            // trackign table name is composed with prefix and suffix from setup
            var trackingTableName = $"{schema.TrackingTablesPrefix}{table.TableName}{schema.TrackingTablesSuffix}";

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup, scopeName);

            var provision = SyncProvision.TrackingTable;

            await localOrchestrator.ProvisionAsync(schema, provision);

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
        public async Task BaseOrchestrator_Provision_ShouldCreate_Triggers()
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
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            setup.TrackingTablesSuffix = "sync";
            setup.TrackingTablesPrefix = "trck";

            setup.TriggersPrefix = "trg_";
            setup.TriggersSuffix = "_trg";


            // trackign table name is composed with prefix and suffix from setup
            var triggerDelete = $"{setup.TriggersPrefix}Product{setup.TriggersSuffix}_delete_trigger";
            var triggerInsert = $"{setup.TriggersPrefix}Product{setup.TriggersSuffix}_insert_trigger";
            var triggerUpdate = $"{setup.TriggersPrefix}Product{setup.TriggersSuffix}_update_trigger";

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup, scopeName);

            // Needs the tracking table to be able to create triggers
            var provision = SyncProvision.TrackingTable | SyncProvision.Triggers;

            await localOrchestrator.ProvisionAsync(provision);

            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);

                var trigDel = await SqlManagementUtils.GetTriggerAsync(c, null, triggerDelete, "SalesLT");
                Assert.Equal(triggerDelete, trigDel.Rows[0]["Name"].ToString());

                var trigIns = await SqlManagementUtils.GetTriggerAsync(c, null, triggerInsert, "SalesLT");
                Assert.Equal(triggerInsert, trigIns.Rows[0]["Name"].ToString());

                var trigUdate = await SqlManagementUtils.GetTriggerAsync(c, null, triggerUpdate, "SalesLT");
                Assert.Equal(triggerUpdate, trigUdate.Rows[0]["Name"].ToString());

                c.Close();
            }

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task BaseOrchestrator_Provision_ShouldCreate_StoredProcedures()
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
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            setup.StoredProceduresPrefix = "s";
            setup.StoredProceduresSuffix = "proc";

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

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup, scopeName);

            // Needs the tracking table to be able to create stored procedures
            var provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures;

            await localOrchestrator.ProvisionAsync(provision);

            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);

                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(c, null, bulkDelete));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(c, null, bulkUpdate));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(c, null, changes));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(c, null, delete));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(c, null, deletemetadata));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(c, null, initialize));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(c, null, reset));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(c, null, selectrow));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(c, null, update));

                c.Close();
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
            var setup = new SyncSetup();

            var schema = new SyncSet();
            var table = new SyncTable("Product", "SalesLT");
            var colID = new SyncColumn("ID", typeof(Guid));
            var colName = new SyncColumn("Name", typeof(string));

            table.Columns.Add(colID);
            table.Columns.Add(colName);
            table.Columns.Add("Number", typeof(int));
            table.PrimaryKeys.Add("ID");

            schema.Tables.Add(table);

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup, scopeName);

            var provision = SyncProvision.Table;

            await localOrchestrator.ProvisionAsync(schema, provision);

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
            var setup = new SyncSetup();

            var schema = new SyncSet();
            var table = new SyncTable("Product", "SalesLT");
            var colID = new SyncColumn("ID", typeof(Guid));
            var colName = new SyncColumn("Name", typeof(string));

            table.Columns.Add(colID);
            table.Columns.Add(colName);
            table.Columns.Add("Number", typeof(int));

            schema.Tables.Add(table);

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup, scopeName);

            var provision = SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

            

            var se = await Assert.ThrowsAsync<SyncException>(async () => await localOrchestrator.ProvisionAsync(schema, provision));

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

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup, scopeName);

            var provision = SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

            var se = await Assert.ThrowsAsync<SyncException>(async () => await localOrchestrator.ProvisionAsync(provision));

            Assert.Equal(SyncStage.Provisioning, se.SyncStage);
            Assert.Equal(SyncSide.ClientSide, se.Side);
            Assert.Equal("MissingTableException", se.TypeName);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

     

    }
}
