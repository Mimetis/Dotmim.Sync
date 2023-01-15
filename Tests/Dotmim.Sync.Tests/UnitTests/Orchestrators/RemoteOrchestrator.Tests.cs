using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Fixtures;
using Dotmim.Sync.Tests.Misc;
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
    public abstract partial class RemoteOrchestratorTests : DatabaseTest, IClassFixture<DatabaseServerFixture>, IDisposable
    {
        private CoreProvider serverProvider;
        private CoreProvider clientProvider;
        private IEnumerable<CoreProvider> clientsProvider;
        private SyncSetup setup;
        private SyncOptions options;

        public RemoteOrchestratorTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
            serverProvider = GetServerProvider();
            clientsProvider = GetClientProviders();
            clientProvider = clientsProvider.First();
            setup = GetSetup();
            options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

        }

       
        [Fact]
        public void RemoteOrchestrator_Constructor()
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
        public void RemoteOrchestrator_ShouldFail_When_Args_AreNull()
        {
            var provider = new SqlSyncProvider();
            var options = new SyncOptions();
            var setup = new SyncSetup();

            var ex1 = Assert.Throws<SyncException>(() => 
                new RemoteOrchestrator(null, null));
            Assert.Equal("ArgumentNullException", ex1.TypeName);

            var ex2 = Assert.Throws<SyncException>(() => 
            new RemoteOrchestrator(provider, null));

            Assert.Equal("ArgumentNullException", ex2.TypeName);

            var orc = new RemoteOrchestrator(null, options);
            Assert.NotNull(orc);
            Assert.Null(orc.Provider);
            Assert.NotNull(orc.Options);
        }


        [Fact]
        public async Task RemoteOrchestrator_GetServerScopeInfo_ShouldNotFail_If_SetupIsEmpty()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);

            var options = new SyncOptions();
            var setup = new SyncSetup();
            var provider = new SqlSyncProvider(cs);

            var orchestrator = new RemoteOrchestrator(provider, options);

            var sScopeInfo = await orchestrator.GetScopeInfoAsync("scope1", setup);

            Assert.NotNull(sScopeInfo);
            Assert.Null(sScopeInfo.Schema);
            Assert.Null(sScopeInfo.Setup);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);

        }

        internal static void AssertConnectionAndTransaction(BaseOrchestrator orchestrator, string scopeName)
        {
            orchestrator.OnConnectionOpen(args =>
            {
                Assert.IsType<ConnectionOpenedArgs>(args);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.Null(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
            });
            orchestrator.OnTransactionOpen(args =>
            {
                Assert.IsType<TransactionOpenedArgs>(args);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.NotNull(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
                Assert.Same(args.Connection, args.Transaction.Connection);
            });
            orchestrator.OnTransactionCommit(args =>
            {
                Assert.IsType<TransactionCommitArgs>(args);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.NotNull(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
                Assert.Same(args.Connection, args.Transaction.Connection);
            });
            orchestrator.OnConnectionClose(args =>
            {
                Assert.IsType<ConnectionClosedArgs>(args);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.Null(args.Transaction);
                Assert.Equal(ConnectionState.Closed, args.Connection.State);
            });

        }

        [Fact]
        public async Task RemoteOrchestrator_GetServerScopeInfo_ShouldReturnSchema()
        {
            var scopeName = "scope";
            var onSchemaRead = false;
            var onSchemaReading = false;

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            remoteOrchestrator.OnSchemaLoading(args =>
            {
                Assert.Equal(scopeName, args.Context.ScopeName);
                onSchemaReading = true;
            });

            remoteOrchestrator.OnSchemaLoaded(args =>
            {
                Assert.IsType<SchemaLoadedArgs>(args);
                Assert.Equal(SyncStage.Provisioning, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.Null(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
                Assert.Equal(17, args.Schema.Tables.Count);
                onSchemaRead = true;

            });

            AssertConnectionAndTransaction(remoteOrchestrator, scopeName);

            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(scopeName, setup);

            Assert.NotNull(scopeInfo.Schema);
            Assert.NotNull(scopeInfo.Setup);
            Assert.Equal(17, scopeInfo.Schema.Tables.Count);
            Assert.True(onSchemaRead);
            Assert.True(onSchemaReading);

            var schema = await remoteOrchestrator.GetSchemaAsync(scopeName, setup);

            Assert.NotNull(schema);
            Assert.Equal(17, schema.Tables.Count);
        }

        [Fact]
        public async Task RemoteOrchestrator_GetServerScopeInfo_SetupColumnsDefined_ShouldReturn_SchemaWithSetupColumnsOnly()
        {
            var tables = new string[] { "Customer", "Address", "CustomerAddress" };
            var setup = new SyncSetup(tables);
            setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "FirstName", "LastName", "CompanyName" });

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);
            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
            Assert.Equal(3, scopeInfo.Schema.Tables.Count);
            // Only 4 columns shoud be part of Customer table
            Assert.Equal(4, scopeInfo.Schema.Tables["Customer"].Columns.Count);

            Assert.Equal(9, scopeInfo.Schema.Tables["Address"].Columns.Count);
            Assert.Equal(5, scopeInfo.Schema.Tables["CustomerAddress"].Columns.Count);
        }

        [Fact]
        public async Task RemoteOrchestrator_GetServerScopeInfo_NoPrimaryKeysColumn_ShouldFail()
        {
            var tables = new string[] { "Customer", "Address", "CustomerAddress" };
            var setup = new SyncSetup(tables);
            setup.Tables["Customer"].Columns.AddRange(new string[] { "FirstName", "LastName", "CompanyName" });

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var se = await Assert.ThrowsAsync<SyncException>(async () =>
            {
                var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
            });

            Assert.Equal(SyncStage.Provisioning, se.SyncStage);
            Assert.Equal("MissingPrimaryKeyColumnException", se.TypeName);
        }

        [Fact]
        public async Task RemoteOrchestrator_GetServerScopeInfo_NonExistingColumns_ShouldFail()
        {
            var tables = new string[] { "Customer", "Address", "CustomerAddress" };
            var setup = new SyncSetup(tables);
            setup.Tables["Customer"].Columns.AddRange(new string[] { "FirstName", "LastName", "CompanyName", "BADCOLUMN" });

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var se = await Assert.ThrowsAsync<SyncException>(async () =>
            {
                await remoteOrchestrator.GetScopeInfoAsync(setup);
            });

            Assert.Equal(SyncStage.Provisioning, se.SyncStage);
            Assert.Equal("MissingColumnException", se.TypeName);
        }

        [Fact]
        public async Task RemoteOrchestrator_GetServerScopeInfo_NonExistingTables_ShouldFail()
        {

            setup.Tables.Add("WrongTable");

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var se = await Assert.ThrowsAsync<SyncException>(async () =>
            {
                await remoteOrchestrator.GetScopeInfoAsync(setup);
            });

            Assert.Equal(SyncStage.Provisioning, se.SyncStage);
            Assert.Equal("MissingTableException", se.TypeName);
        }


        [Fact]
        public async Task RemoteOrchestrator_Provision_SchemaCreated_If_SetupHasTables()
        {
            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });
            var scopeName = "scope";

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var provision = SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

            var clientScopeInfo = await remoteOrchestrator.ProvisionAsync(scopeName, setup, provision);

            Assert.Single(clientScopeInfo.Schema.Tables);
            Assert.Equal("SalesLT.Product", clientScopeInfo.Schema.Tables[0].GetFullName());
            Assert.Equal(17, clientScopeInfo.Schema.Tables[0].Columns.Count);
        }


        [Fact]
        public async Task RemoteOrchestrator_Provision_ShouldCreate_StoredProcedures_WithSpecificScopeName()
        {
            var scopeName = "scope";

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
            var initialize = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_{scopeName}_initialize";
            var update = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_{scopeName}_update";

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            // Needs the tracking table to be able to create stored procedures
            var provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures;

            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(scopeName, setup);

            await remoteOrchestrator.ProvisionAsync(scopeInfo, provision);

            using (var connection = new SqlConnection(serverProvider.ConnectionString))
            {
                await connection.OpenAsync().ConfigureAwait(false);

                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, bulkDelete));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, bulkUpdate));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, changes));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, update));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, delete));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, initialize));

                connection.Close();
            }
        }
        [Fact]
        public async Task RemoteOrchestrator_Provision_ShouldCreate_StoredProcedures_WithDefaultScopeName()
        {
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
            var initialize = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_initialize";
            var update = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_update";

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            // Needs the tracking table to be able to create stored procedures
            var provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures;

            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);

            await remoteOrchestrator.ProvisionAsync(scopeInfo, provision);

            using (var connection = new SqlConnection(serverProvider.ConnectionString))
            {
                await connection.OpenAsync().ConfigureAwait(false);

                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, bulkDelete));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, bulkUpdate));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, changes));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, initialize));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, update));
                Assert.True(await SqlManagementUtils.ProcedureExistsAsync(connection, null, delete));

                connection.Close();
            }
        }

        [Fact]
        public async Task RemoteOrchestrator_Provision_SchemaFail_If_SchemaHasColumnsDefinitionButNoPrimaryKey()
        {
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

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(scopeName, setup);

            // Overriding scope info to introduce a bad table with no primary key
            scopeInfo.Schema = schema;
            scopeInfo.Setup = setup;

            var provision = SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

            var se = await Assert.ThrowsAsync<SyncException>(
                async () => await remoteOrchestrator.ProvisionAsync(scopeInfo, provision));

            Assert.Equal(SyncStage.Provisioning, se.SyncStage);
            Assert.Equal("MissingPrimaryKeyException", se.TypeName);
        }


        [Fact]
        public async Task RemoteOrchestrator_Provision_ShouldFails_If_SetupTable_DoesNotExist()
        {
            var scopeName = "scope";
            var setup = new SyncSetup(new string[] { "SalesLT.badTable" });

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var se = await Assert.ThrowsAsync<SyncException>(async () =>
                await remoteOrchestrator.GetScopeInfoAsync(scopeName, setup));

            Assert.Equal(SyncStage.Provisioning, se.SyncStage);
            Assert.Equal("MissingTableException", se.TypeName);

        }
    }
}
