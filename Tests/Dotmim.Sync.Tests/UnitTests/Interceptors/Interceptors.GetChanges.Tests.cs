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
        public async Task LocalOrchestrator_GetChanges()
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

            var onDatabaseSelecting = 0;
            var onDatabaseSelected = 0;
            var onSelecting = 0;
            var onSelected = 0;

            localOrchestrator.OnDatabaseChangesSelecting(dcs =>
            {
                onDatabaseSelecting++;
            });

            localOrchestrator.OnDatabaseChangesSelected(dcs =>
            {
                Assert.NotNull(dcs.BatchInfo);
                Assert.Equal(2, dcs.ChangesSelected.TableChangesSelected.Count);
                onDatabaseSelected++;
            });

            localOrchestrator.OnTableChangesSelecting(action =>
            {
                Assert.NotNull(action.Command);
                onSelecting++;
            });

            localOrchestrator.OnTableChangesSelected(action =>
            {
                Assert.NotNull(action.BatchPartInfos);
                onSelected++;
            });

            // Get changes to be populated to the server
            var scopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName);

            var changes = await localOrchestrator.GetChangesAsync(scopeInfoClient);

            Assert.Equal(setup.Tables.Count, onSelecting);
            Assert.Equal(setup.Tables.Count, onSelected);
            Assert.Equal(1, onDatabaseSelected);
            Assert.Equal(1, onDatabaseSelecting);
        }

        [Fact]
        public async Task LocalOrchestrator_GetBatchChanges()
        {
            var scopeName = "scopesnap1";

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider, options);

            // Making a first sync, will initialize everything we need
            var s = await agent.SynchronizeAsync(scopeName, setup);

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;
            var remoteOrchestrator = agent.RemoteOrchestrator;


            await clientProvider.AddProductCategoryAsync();
            await clientProvider.AddProductAsync();

            var onDatabaseSelecting = 0;
            var onDatabaseSelected = 0;
            var onSelecting = 0;
            var onSelected = 0;

            localOrchestrator.OnDatabaseChangesSelecting(dcs =>
            {
                onDatabaseSelecting++;
            });

            localOrchestrator.OnDatabaseChangesSelected(dcs =>
            {
                Assert.NotNull(dcs.BatchInfo);
                Assert.Equal(2, dcs.ChangesSelected.TableChangesSelected.Count);
                onDatabaseSelected++;
            });

            localOrchestrator.OnTableChangesSelecting(action =>
            {
                Assert.NotNull(action.Command);
                onSelecting++;
            });

            localOrchestrator.OnTableChangesSelected(action =>
            {
                Assert.NotNull(action.BatchPartInfos);
                onSelected++;
            });

            // Get changes to be populated to the server
            var scopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName);
            var changes = await localOrchestrator.GetChangesAsync(scopeInfoClient);

            Assert.Equal(setup.Tables.Count, onSelecting);
            Assert.Equal(setup.Tables.Count, onSelected);
            Assert.Equal(1, onDatabaseSelected);
            Assert.Equal(1, onDatabaseSelecting);
        }



        [Fact]
        public async Task RemoteOrchestrator_GetChanges()
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
            await serverProvider.AddProductCategoryAsync();
            await serverProvider.AddProductAsync();

            var onSelecting = 0;
            var onSelected = 0;
            var onDatabaseSelecting = 0;
            var onDatabaseSelected = 0;

            remoteOrchestrator.OnTableChangesSelecting(action =>
            {
                Assert.NotNull(action.Command);
                onSelecting++;
            });

            remoteOrchestrator.OnTableChangesSelected(action =>
            {
                Assert.NotNull(action.BatchPartInfos);
                onSelected++;
            });
            remoteOrchestrator.OnDatabaseChangesSelecting(dcs =>
            {
                onDatabaseSelecting++;
            });

            remoteOrchestrator.OnDatabaseChangesSelected(dcs =>
            {
                Assert.NotNull(dcs.BatchInfo);
                Assert.Equal(2, dcs.ChangesSelected.TableChangesSelected.Count);
                onDatabaseSelected++;
            });

            var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName);

            // Get changes to be populated to be sent to the client
            var changes = await remoteOrchestrator.GetChangesAsync(cScopeInfoClient);

            Assert.Equal(setup.Tables.Count, onSelecting);
            Assert.Equal(setup.Tables.Count, onSelected);
            Assert.Equal(1, onDatabaseSelected);
            Assert.Equal(1, onDatabaseSelecting);
        }

    }
}
