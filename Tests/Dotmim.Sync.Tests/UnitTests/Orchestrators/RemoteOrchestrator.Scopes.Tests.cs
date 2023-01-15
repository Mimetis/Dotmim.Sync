using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
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
    public partial class RemoteOrchestratorTests
    {


        [Fact]
        public async Task RemoteOrchestrator_Scope_Should_NotFail_If_NoTables_In_Setup()
        {
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);
            var sScopeInfo = await remoteOrchestrator.GetScopeInfoAsync(new SyncSetup());

            Assert.NotNull(sScopeInfo);
            Assert.Null(sScopeInfo.Schema);
            Assert.Null(sScopeInfo.Setup);
        }

        [Fact]
        public async Task RemoteOrchestrator_Scope_NewScope()
        {
            var scopeName = "scope";
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var remoteScopeInfo = await remoteOrchestrator.GetScopeInfoAsync(scopeName, setup);

            Assert.NotNull(remoteScopeInfo);
            Assert.Equal(scopeName, remoteScopeInfo.Name);
            Assert.Null(remoteScopeInfo.LastCleanupTimestamp);
            Assert.NotNull(remoteScopeInfo.Schema);
            Assert.NotNull(remoteScopeInfo.Setup);

            Assert.Equal(SyncVersion.Current, new Version(remoteScopeInfo.Version));
        }

        [Fact]
        public async Task RemoteOrchestrator_Scope_IsNotNewScope_OnceSaved()
        {
            var scopeName = "scope";
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);
            var remoteScopeInfo = await remoteOrchestrator.GetScopeInfoAsync(scopeName, setup);
            Assert.Equal(SyncVersion.Current, new Version(remoteScopeInfo.Version));
        }

        [Fact]
        public async Task RemoteOrchestrator_Scopes_Multiples_Check_Schema_Setup_AreNotEmpty()
        {
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);
            var remoteScopeInfo1 = await remoteOrchestrator.GetScopeInfoAsync(setup);
            var remoteScopeInfo2 = await remoteOrchestrator.GetScopeInfoAsync("A", setup);

            Assert.Equal(SyncOptions.DefaultScopeName, remoteScopeInfo1.Name);
            Assert.Equal("A", remoteScopeInfo2.Name);

            Assert.NotNull(remoteScopeInfo1);
            Assert.NotNull(remoteScopeInfo2);

            Assert.NotNull(remoteScopeInfo1.Schema);
            Assert.NotNull(remoteScopeInfo2.Schema);

            Assert.NotNull(remoteScopeInfo1.Setup);
            Assert.NotNull(remoteScopeInfo2.Setup);
        }
    }
}
