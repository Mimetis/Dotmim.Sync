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
        public async Task LocalOrchestrator_GetEstimatedChanges()
        {
            var scopeName = "scopesnap1";

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider, options);

            // Making a first sync, will initialize everything we need
            var s = await agent.SynchronizeAsync(scopeName, setup);

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;
            var remoteOrchestrator = agent.RemoteOrchestrator;

            // Client side : Create a product category and a product
            // Create a productcategory item
            // Create a new product on server
            await clientProvider.AddProductCategoryAsync();
            await clientProvider.AddProductAsync();

            var onSelecting = 0;
            var onSelected = 0;
            var onDatabaseSelecting = 0;
            var onDatabaseSelected = 0;

            localOrchestrator.OnTableChangesSelecting(action =>
            {
                Assert.NotNull(action.Command);
                onSelecting++;
            });

            localOrchestrator.OnTableChangesSelected(action =>
            {
                Assert.Null(action.BatchPartInfos);
                onSelected++;
            });
            localOrchestrator.OnDatabaseChangesSelecting(dcs =>
            {
                onDatabaseSelecting++;
            });

            localOrchestrator.OnDatabaseChangesSelected(dcs =>
            {
                Assert.Null(dcs.BatchInfo);
                Assert.Equal(2, dcs.ChangesSelected.TableChangesSelected.Count);
                onDatabaseSelected++;
            });

            // Get changes to be populated to the server
            var scopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName);
            var changes = await localOrchestrator.GetEstimatedChangesCountAsync(scopeInfoClient);

            Assert.Equal(setup.Tables.Count, onSelecting);
            Assert.Equal(setup.Tables.Count, onSelected);
            Assert.Equal(1, onDatabaseSelected);
            Assert.Equal(1, onDatabaseSelecting);
        }

        [Fact]
        public async Task RemoteOrchestrator_GetEstimatedChanges()
        {
            var scopeName = "scopesnap1";

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider, options);

            // Making a first sync, will initialize everything we need
            var s = await agent.SynchronizeAsync(scopeName, setup);

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;
            var remoteOrchestrator = agent.RemoteOrchestrator;

            // Server side : Create a product category and a product
            // Create a productcategory item
            // Create a new product on server
            await serverProvider.AddProductCategoryAsync();
            await serverProvider.AddProductAsync();

            var onSelecting = 0;
            var onSelected = 0;
            var onDatabaseSelecting = 0;
            var onDatabaseSelected = 0;

            remoteOrchestrator.OnDatabaseChangesSelecting(dcs =>
            {
                onDatabaseSelecting++;
            });

            remoteOrchestrator.OnDatabaseChangesSelected(dcs =>
            {
                Assert.Null(dcs.BatchInfo);
                Assert.Equal(2, dcs.ChangesSelected.TableChangesSelected.Count);
                onDatabaseSelected++;
            });

            remoteOrchestrator.OnTableChangesSelecting(action =>
            {
                Assert.NotNull(action.Command);
                onSelecting++;
            });

            remoteOrchestrator.OnTableChangesSelected(action =>
            {
                Assert.Null(action.BatchPartInfos);
                onSelected++;
            });

            var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName);

            // Get changes to be populated to be sent to the client
            var changes = await remoteOrchestrator.GetEstimatedChangesCountAsync(cScopeInfoClient);

            Assert.Equal(setup.Tables.Count, onSelecting);
            Assert.Equal(setup.Tables.Count, onSelected);
            Assert.Equal(1, onDatabaseSelected);
            Assert.Equal(1, onDatabaseSelecting);
        }
    }
}
