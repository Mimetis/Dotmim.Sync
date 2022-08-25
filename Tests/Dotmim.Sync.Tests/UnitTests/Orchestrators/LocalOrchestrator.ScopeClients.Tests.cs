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
        public async Task LocalOrchestrator_MultipleScopeClients_ShouldHave_SameClientId()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var options = new SyncOptions();

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var localScopeInfo1 = await localOrchestrator.GetScopeInfoClientAsync();
            var localScopeInfo2 = await localOrchestrator.GetScopeInfoClientAsync("A");
            var localScopeInfo3 = await localOrchestrator.GetScopeInfoClientAsync("B");

            Assert.Equal(localScopeInfo1.Id, localScopeInfo2.Id);
            Assert.Equal(localScopeInfo2.Id, localScopeInfo3.Id);


            // Check we get the 3 scopes
            var allScopes = await localOrchestrator.GetAllScopeInfoClientsAsync();

            Assert.Equal(3, allScopes.Count);

            // Check the scope id, read from database, is good
            foreach (var scope in allScopes)
            {
                Assert.Equal(scope.Id, localScopeInfo1.Id);
            }


            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

    }
}
