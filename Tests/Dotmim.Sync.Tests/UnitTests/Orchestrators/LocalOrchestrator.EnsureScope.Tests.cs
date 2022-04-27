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

            var localOrchestrator = new LocalOrchestrator(provider, options);

            var scope = await localOrchestrator.GetClientScopeAsync(setup);

            Assert.NotNull(scope);

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

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var localScopeInfo = await localOrchestrator.GetClientScopeAsync(scopeName, setup);

            Assert.NotNull(localScopeInfo);
            Assert.Equal(scopeName, localScopeInfo.Name);
            Assert.True(localScopeInfo.IsNewScope);
            Assert.NotEqual(Guid.Empty, localScopeInfo.Id);
            Assert.Null(localScopeInfo.LastServerSyncTimestamp);
            Assert.Null(localScopeInfo.LastSync);
            Assert.Equal(0, localScopeInfo.LastSyncDuration);
            Assert.Null(localScopeInfo.LastSyncTimestamp);
            Assert.Null(localScopeInfo.Schema);
            Assert.Equal(SyncVersion.Current, new Version(localScopeInfo.Version));

            // Check context
            SyncContext syncContext = localOrchestrator.GetContext(scopeName);
            Assert.Equal(scopeName, syncContext.ScopeName);
            Assert.NotEqual(Guid.Empty, syncContext.SessionId);
            Assert.Null(syncContext.Parameters);
            Assert.Equal(SyncStage.ScopeLoading, syncContext.SyncStage);
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

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);
            using var cts = new CancellationTokenSource();

            localOrchestrator.OnConnectionOpen(args => cts.Cancel());

            var se = await Assert.ThrowsAsync<SyncException>(
                async () => await localOrchestrator.GetClientScopeAsync(setup, default, default, cts.Token));

            Assert.Equal(SyncSide.ClientSide, se.Side);
            Assert.Equal("OperationCanceledException", se.TypeName);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }
    }
}
