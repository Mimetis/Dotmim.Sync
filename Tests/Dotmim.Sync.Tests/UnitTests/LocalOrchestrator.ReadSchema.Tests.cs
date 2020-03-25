using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Tests.UnitTests
{
    public partial class LocalOrchestratorTests
    {

     
        [Fact]
        public async Task LocalOrchestrator_ReadSchema_Should_Fail_If_NoTables_In_Setup()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);

            var options = new SyncOptions();
            var setup = new SyncSetup();
            var provider = new SqlSyncProvider(cs);

            var localOrchestrator = new LocalOrchestrator(provider, options, setup);

            var se = await Assert.ThrowsAsync<SyncException>(async () =>
            {
                var schema = await localOrchestrator.ReadSchemaAsync();
            });

            Assert.Equal(SyncStage.SchemaReading, se.SyncStage);
            Assert.Equal(SyncExceptionSide.ClientSide, se.Side);
            Assert.Equal("MissingTablesException", se.TypeName);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);

        }

        [Fact]
        public async Task LocalOrchestrator_Should_ReadSchema()
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

            var onConnectionOpen = false;
            var onTransactionOpen = false;
            var onTransactionCommit = false;
            var onConnectionClose = false;
            var onSchema = false;

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup, scopeName);

            localOrchestrator.OnConnectionOpen(args =>
            {
                Assert.IsType<ConnectionOpenArgs>(args);
                Assert.Equal(SyncStage.SchemaReading, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.Null(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
                onConnectionOpen = true;
            });

            localOrchestrator.OnConnectionClose(args =>
            {
                Assert.IsType<ConnectionCloseArgs>(args);
                Assert.Equal(SyncStage.SchemaRead, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.NotNull(args.Transaction);
                Assert.Equal(ConnectionState.Closed, args.Connection.State);
                onConnectionClose = true;
            });

            localOrchestrator.OnTransactionOpen(args =>
            {
                Assert.IsType<TransactionOpenArgs>(args);
                Assert.Equal(SyncStage.SchemaReading, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.NotNull(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
                Assert.Same(args.Connection, args.Transaction.Connection);
                onTransactionOpen = true;
            });

            localOrchestrator.OnTransactionCommit(args =>
            {
                Assert.IsType<TransactionCommitArgs>(args);
                Assert.Equal(SyncStage.SchemaRead, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.NotNull(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
                Assert.Same(args.Connection, args.Transaction.Connection);
                onTransactionCommit = true;
            });

            localOrchestrator.OnSchema(args =>
            {
                Assert.IsType<SchemaArgs>(args);
                Assert.Equal(SyncStage.SchemaRead, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.NotNull(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
                Assert.Same(args.Connection, args.Transaction.Connection);
                Assert.Equal(17, args.Schema.Tables.Count);
                onSchema = true;

            });

            var schema = await localOrchestrator.ReadSchemaAsync();

            Assert.NotNull(schema);
            Assert.Equal(17, schema.Tables.Count);
            Assert.True(onConnectionOpen);
            Assert.True(onConnectionClose);
            Assert.True(onTransactionOpen);
            Assert.True(onTransactionCommit);
            Assert.True(onSchema);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }


        [Fact]
        public async Task LocalOrchestrator_CancellationToken_ShouldInterrupt_ReadingSchema()
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
                Assert.IsType<ConnectionOpenArgs>(args);
                Assert.NotNull(args.Connection);
                Assert.Null(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
                cts.Cancel();
            });

            var se = await Assert.ThrowsAsync<SyncException>(async () =>
            {
                var schema = await localOrchestrator.ReadSchemaAsync(cts.Token);
            });

            Assert.Equal(SyncStage.SchemaReading, se.SyncStage);
            Assert.Equal(SyncExceptionSide.ClientSide, se.Side);
            Assert.Equal("OperationCanceledException", se.TypeName);



            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }
    }
}
