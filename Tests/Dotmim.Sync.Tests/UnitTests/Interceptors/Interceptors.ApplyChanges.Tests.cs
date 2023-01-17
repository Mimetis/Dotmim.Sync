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
        public async Task LocalOrchestrator_ApplyChanges()
        {
            var scopeName = "scopesnap1";
          
            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider, options);

            // Making a first sync, will initialize everything we need
            var s = await agent.SynchronizeAsync(scopeName, setup);

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;
            var remoteOrchestrator = agent.RemoteOrchestrator;

            // Create a productcategory item
            // Create a new product on server
            await serverProvider.AddProductCategoryAsync();
            await serverProvider.AddProductAsync();

            var onDatabaseApplying = 0;
            var onDatabaseApplied = 0;
            var onApplying = 0;
            var onApplied = 0;

            localOrchestrator.OnDatabaseChangesApplying(dcs =>
            {
                onDatabaseApplying++;
            });

            localOrchestrator.OnDatabaseChangesApplied(dcs =>
            {
                Assert.NotNull(dcs.ChangesApplied);
                Assert.Equal(2, dcs.ChangesApplied.TableChangesApplied.Count);
                onDatabaseApplied++;
            });


            localOrchestrator.OnTableChangesApplying(action =>
            {
                Assert.NotNull(action.SchemaTable);
                onApplying++;
            });

            localOrchestrator.OnTableChangesApplied(action =>
            {
                Assert.Equal(1, action.TableChangesApplied.Applied);
                onApplied++;
            });

            // Making a first sync, will initialize everything we need
            var s2 = await agent.SynchronizeAsync(scopeName);

            Assert.Equal(1, onDatabaseApplying);
            Assert.Equal(1, onDatabaseApplied);
            Assert.Equal(4, onApplying); // Deletes + Modified state = Table count * 2
            Assert.Equal(2, onApplied); // Two tables applied

        }

        [Fact]
        public async Task RemoteOrchestrator_ApplyChanges()
        {
            var scopeName = "scopesnap1";
         
            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider, options);

            // Making a first sync, will initialize everything we need
            var s = await agent.SynchronizeAsync(scopeName, setup);

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;
            var remoteOrchestrator = agent.RemoteOrchestrator;

            // Create a productcategory item
            // Create a new product on server
            await clientProvider.AddProductCategoryAsync();
            await clientProvider.AddProductAsync();

            var onDatabaseApplying = 0;
            var onDatabaseApplied = 0;
            var onApplying = 0;

            remoteOrchestrator.OnDatabaseChangesApplying(dcs =>
            {
                onDatabaseApplying++;
            });

            remoteOrchestrator.OnDatabaseChangesApplied(dcs =>
            {
                Assert.NotNull(dcs.ChangesApplied);
                Assert.Equal(2, dcs.ChangesApplied.TableChangesApplied.Count);
                onDatabaseApplied++;
            });

            remoteOrchestrator.OnTableChangesApplying(action =>
            {
                Assert.NotNull(action.BatchPartInfos);
                onApplying++;
            });

  

            // Making a first sync, will initialize everything we need
            var s2 = await agent.SynchronizeAsync(scopeName);

            Assert.Equal(4, onApplying);

            Assert.Equal(1, onDatabaseApplying);
            Assert.Equal(1, onDatabaseApplied);
        }


    }
}
