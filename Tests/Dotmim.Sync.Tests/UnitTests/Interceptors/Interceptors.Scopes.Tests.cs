using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
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
    public partial class InterceptorsTests
    {
        [Fact]
        public async Task LocalOrchestrator_Scope()
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


            var scopeTableCreating = 0;
            var scopeTableCreated = 0;
            var scopeLoading = 0;
            var scopeLoaded = 0;
            var scopeSaving = 0;
            var scopeSaved = 0;

            localOrchestrator.OnScopeSaving(ssa =>
            {
                Assert.NotNull(ssa.Command);
                scopeSaving++;
            });

            localOrchestrator.OnScopeSaved(ssa => scopeSaved++);

            localOrchestrator.OnScopeTableCreating(stca =>
            {
                Assert.NotNull(stca.Command);
                scopeTableCreating++;
            });

            localOrchestrator.OnScopeTableCreated(stca =>
            {
                scopeTableCreated++;
            });

            localOrchestrator.OnScopeLoading(args =>
            {
                Assert.NotNull(args.Command);
                Assert.Equal(SyncStage.ScopeLoading, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.Equal(scopeName, args.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.NotNull(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
                Assert.Same(args.Connection, args.Transaction.Connection);
                scopeLoading++;
            });

            localOrchestrator.OnScopeLoaded(args =>
            {
                Assert.Equal(SyncStage.ScopeLoading, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.ScopeInfo);
                Assert.Equal(scopeName, args.ScopeInfo.Name);
                Assert.NotNull(args.Connection);
                Assert.NotNull(args.Transaction);
                scopeLoaded++;
            });

            var localScopeInfo = await localOrchestrator.GetClientScopeAsync();

            localScopeInfo.Version = "2.0";

            await localOrchestrator.SaveClientScopeAsync(localScopeInfo);

            Assert.Equal(1, scopeTableCreating);
            Assert.Equal(1, scopeTableCreated);
            Assert.Equal(1, scopeLoading);
            Assert.Equal(1, scopeLoaded);
            Assert.Equal(1, scopeSaving);
            Assert.Equal(1, scopeSaved);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }
        [Fact]
        public async Task RemoteOrchestrator_Scope()
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

            var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options, setup, scopeName);

            var scopeTableCreating = 0;
            var scopeTableCreated = 0;
            var scopeLoading = 0;
            var scopeLoaded = 0;
            var scopeSaving = 0;
            var scopeSaved = 0;

            remoteOrchestrator.OnScopeSaving(ssa =>
            {
                Assert.NotNull(ssa.Command);
                scopeSaving++;
            });

            remoteOrchestrator.OnScopeSaved(ssa => scopeSaved++);

            remoteOrchestrator.OnScopeTableCreating(stca =>
            {
                Assert.NotNull(stca.Command);
                scopeTableCreating++;
            });

            remoteOrchestrator.OnScopeTableCreated(stca =>
            {
                scopeTableCreated++;
            });

            remoteOrchestrator.OnServerScopeLoading(args =>
            {
                Assert.NotNull(args.Command);
                Assert.Equal(SyncStage.ScopeLoading, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.Equal(scopeName, args.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.NotNull(args.Transaction);
                Assert.Equal(ConnectionState.Open, args.Connection.State);
                Assert.Same(args.Connection, args.Transaction.Connection);
                scopeLoading++;
            });

            remoteOrchestrator.OnServerScopeLoaded(args =>
            {
                Assert.Equal(SyncStage.ScopeLoading, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.ScopeInfo);
                Assert.Equal(scopeName, args.ScopeInfo.Name);
                Assert.NotNull(args.Connection);
                Assert.NotNull(args.Transaction);
                scopeLoaded++;
            });

            var serverScopeInfo = await remoteOrchestrator.GetServerScopeAsync();

            serverScopeInfo.Version = "2.0";

            await remoteOrchestrator.SaveServerScopeAsync(serverScopeInfo);

            Assert.Equal(2, scopeTableCreating);
            Assert.Equal(2, scopeTableCreated);
            Assert.Equal(1, scopeLoading);
            Assert.Equal(1, scopeLoaded);
            Assert.Equal(2, scopeSaving);
            Assert.Equal(2, scopeSaved);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }
    }
}
