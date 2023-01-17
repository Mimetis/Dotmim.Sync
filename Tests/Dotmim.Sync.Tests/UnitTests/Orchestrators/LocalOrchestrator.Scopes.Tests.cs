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
    public partial class LocalOrchestratorTests
    {


        [Fact]
        public async Task LocalOrchestrator_EnsureScope_ShouldNot_Fail_If_NoTables_In_Setup()
        {
            var localOrchestrator = new LocalOrchestrator(clientProvider, options);
            var scope = await localOrchestrator.GetScopeInfoAsync();
            Assert.NotNull(scope);
        }


        [Fact]
        public async Task LocalOrchestrator_EnsureScope_NewScope()
        {
            var scopeName = "scope";
            var localOrchestrator = new LocalOrchestrator(clientProvider, options);

            var localScopeInfo = await localOrchestrator.GetScopeInfoAsync(scopeName);

            Assert.NotNull(localScopeInfo);
            Assert.Equal(scopeName, localScopeInfo.Name);
            Assert.Null(localScopeInfo.Schema);
            Assert.Null(localScopeInfo.Setup);
            Assert.Equal(SyncVersion.Current, new Version(localScopeInfo.Version));
        }

        [Fact]
        public async Task LocalOrchestrator_EnsureScope_NewScope_WithoutSetup_ShouldBeEmpty()
        {
            var scopeName = "scope";
            var localOrchestrator = new LocalOrchestrator(clientProvider, options);

            var localScopeInfo = await localOrchestrator.GetScopeInfoAsync(scopeName);

            Assert.NotNull(localScopeInfo);
            Assert.Equal(scopeName, localScopeInfo.Name);
            Assert.Null(localScopeInfo.Schema);
            Assert.Null(localScopeInfo.Setup);
            Assert.Equal(SyncVersion.Current, new Version(localScopeInfo.Version));
        }
    }
}
