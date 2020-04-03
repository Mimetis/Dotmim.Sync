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
        public async Task LocalOrchestrator_EnsureScope_ShouldNot_Fail_If_NoTables_In_Setup()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);

            var options = new SyncOptions();
            var setup = new SyncSetup();
            var provider = new SqlSyncProvider(cs);

            var localOrchestrator = new LocalOrchestrator(provider, options, setup);

            var scope = await localOrchestrator.EnsureScopeAsync();

            Assert.NotNull(scope);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);

        }

        [Fact]
        public async Task LocalOrchestrator_EnsureScope_CheckInterceptors()
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
            var onScopeLoading = false;
            var onScopeLoaded = false;

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup, scopeName);

            localOrchestrator.OnScopeLoading(args =>
            {
                Assert.Equal(SyncStage.ScopeLoading, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.Equal(scopeName, args.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.NotNull(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
                Assert.Same(args.Connection, args.Transaction.Connection);
                onScopeLoading = true;
            });

            localOrchestrator.OnScopeLoaded(args =>
            {
                Assert.Equal(SyncStage.ScopeLoaded, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.ScopeInfo);
                Assert.Equal(scopeName, args.ScopeInfo.Name);
                Assert.NotNull(args.Connection);
                Assert.Null(args.Transaction);
                Assert.Equal(ConnectionState.Closed, args.Connection.State);
                onScopeLoaded = true;
            });

            // Check connection and transaction interceptors
            BaseOrchestratorTests.AssertConnectionAndTransaction(localOrchestrator, SyncStage.ScopeLoading, SyncStage.ScopeLoaded);

            var localScopeInfo = await localOrchestrator.EnsureScopeAsync();

            Assert.True(onScopeLoaded);
            Assert.True(onScopeLoading);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }


        [Fact]
        public async Task LocalOrchestrator_EnsureScope_NewScope()
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

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup, scopeName);

            var localScopeInfo = await localOrchestrator.EnsureScopeAsync();

            Assert.NotNull(localScopeInfo);
            Assert.Equal(scopeName, localScopeInfo.Name);
            Assert.True(localScopeInfo.IsNewScope);
            Assert.NotEqual(Guid.Empty, localScopeInfo.Id);
            Assert.Equal(0, localScopeInfo.LastServerSyncTimestamp);
            Assert.Null(localScopeInfo.LastSync);
            Assert.Equal(0, localScopeInfo.LastSyncDuration);
            Assert.Equal(0, localScopeInfo.LastSyncTimestamp);
            Assert.Null(localScopeInfo.Schema);
            Assert.Null(localScopeInfo.Version);

            // Check context
            SyncContext syncContext = localOrchestrator.GetContext();
            Assert.Equal(scopeName, syncContext.ScopeName);
            Assert.NotEqual(Guid.Empty, syncContext.SessionId);
            Assert.Null(syncContext.Parameters);
            Assert.Equal(SyncStage.ScopeLoaded, syncContext.SyncStage);
            Assert.Equal(SyncType.Normal, syncContext.SyncType);
            Assert.Equal(SyncWay.None, syncContext.SyncWay);


            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }


        [Fact]
        public async Task LocalOrchestrator_CancellationToken_ShouldInterrupt_EnsureScope_OnConnectionOpened()
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

            localOrchestrator.OnConnectionOpen(args => cts.Cancel());

            var se = await Assert.ThrowsAsync<SyncException>(async () => await localOrchestrator.EnsureScopeAsync(cts.Token));

            Assert.Equal(SyncSide.ClientSide, se.Side);
            Assert.Equal("OperationCanceledException", se.TypeName);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task LocalOrchestrator_CancellationToken_ShouldInterrupt_EnsureScope_OnTransactionCommit()
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

            localOrchestrator.OnTransactionCommit(args => cts.Cancel());
            var se = await Assert.ThrowsAsync<SyncException>(async () => await localOrchestrator.EnsureScopeAsync(cts.Token));
            Assert.Equal(SyncSide.ClientSide, se.Side);
            Assert.Equal("OperationCanceledException", se.TypeName);
            
            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }
    }
}
