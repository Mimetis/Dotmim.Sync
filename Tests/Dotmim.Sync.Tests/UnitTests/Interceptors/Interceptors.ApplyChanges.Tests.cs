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
            var dbNameSrv = HelperDatabase.GetRandomName("tcp_lo_srv");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbNameSrv, true);

            var dbNameCli = HelperDatabase.GetRandomName("tcp_lo_cli");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbNameCli, true);

            var csServer = HelperDatabase.GetConnectionString(ProviderType.Sql, dbNameSrv);
            var serverProvider = new SqlSyncProvider(csServer);

            var csClient = HelperDatabase.GetConnectionString(ProviderType.Sql, dbNameCli);
            var clientProvider = new SqlSyncProvider(csClient);

            await new AdventureWorksContext((dbNameSrv, ProviderType.Sql, serverProvider), true, false).Database.EnsureCreatedAsync();
            await new AdventureWorksContext((dbNameCli, ProviderType.Sql, clientProvider), true, false).Database.EnsureCreatedAsync();

            var scopeName = "scopesnap1";
            var syncOptions = new SyncOptions();
            var setup = new SyncSetup();

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider, this.Tables, scopeName);

            // Making a first sync, will initialize everything we need
            var s = await agent.SynchronizeAsync();

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;
            var remoteOrchestrator = agent.RemoteOrchestrator;

            // Client side : Create a product category and a product
            // Create a productcategory item
            // Create a new product on server
            var productId = Guid.NewGuid();
            var productName = HelperDatabase.GetRandomName();
            var productNumber = productName.ToUpperInvariant().Substring(0, 10);

            var productCategoryName = HelperDatabase.GetRandomName();
            var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

            using (var ctx = new AdventureWorksContext((dbNameSrv, ProviderType.Sql, serverProvider)))
            {
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);

                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber };
                ctx.Add(product);

                await ctx.SaveChangesAsync();
            }

            var onDatabaseApplying = 0;
            var onDatabaseApplied = 0;
            var onBatchApplied = 0;
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

            localOrchestrator.OnTableChangesBatchApplied(action =>
            {
                Assert.Equal(1, action.TableChangesApplied.Applied);
                onBatchApplied++;
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
            var s2 = await agent.SynchronizeAsync();

            Assert.Equal(2, onBatchApplied);
            Assert.Equal(1, onDatabaseApplying);
            Assert.Equal(1, onDatabaseApplied);
            Assert.Equal(4, onApplying); // Deletes + Modified state = Table count * 2
            Assert.Equal(2, onApplied); // Two tables applied

            HelperDatabase.DropDatabase(ProviderType.Sql, dbNameSrv);
            HelperDatabase.DropDatabase(ProviderType.Sql, dbNameCli);
        }

        [Fact]
        public async Task RemoteOrchestrator_ApplyChanges()
        {
            var dbNameSrv = HelperDatabase.GetRandomName("tcp_lo_srv");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbNameSrv, true);

            var dbNameCli = HelperDatabase.GetRandomName("tcp_lo_cli");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbNameCli, true);

            var csServer = HelperDatabase.GetConnectionString(ProviderType.Sql, dbNameSrv);
            var serverProvider = new SqlSyncProvider(csServer);

            var csClient = HelperDatabase.GetConnectionString(ProviderType.Sql, dbNameCli);
            var clientProvider = new SqlSyncProvider(csClient);

            await new AdventureWorksContext((dbNameSrv, ProviderType.Sql, serverProvider), true, false).Database.EnsureCreatedAsync();
            await new AdventureWorksContext((dbNameCli, ProviderType.Sql, clientProvider), true, false).Database.EnsureCreatedAsync();

            var scopeName = "scopesnap1";
            var syncOptions = new SyncOptions();
            var setup = new SyncSetup();

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider, this.Tables, scopeName);

            // Making a first sync, will initialize everything we need
            var s = await agent.SynchronizeAsync();

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;
            var remoteOrchestrator = agent.RemoteOrchestrator;

            // Client side : Create a product category and a product
            // Create a productcategory item
            // Create a new product on server
            var productId = Guid.NewGuid();
            var productName = HelperDatabase.GetRandomName();
            var productNumber = productName.ToUpperInvariant().Substring(0, 10);

            var productCategoryName = HelperDatabase.GetRandomName();
            var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

            using (var ctx = new AdventureWorksContext((dbNameCli, ProviderType.Sql, clientProvider)))
            {
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);

                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber };
                ctx.Add(product);

                await ctx.SaveChangesAsync();
            }

            var onDatabaseApplying = 0;
            var onDatabaseApplied = 0;
            var onApplying = 0;
            var onApplied = 0;

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

            remoteOrchestrator.OnTableChangesBatchApplied(action =>
            {
                Assert.Equal(1, action.TableChangesApplied.Applied);
                onApplied++;
            });

            // Making a first sync, will initialize everything we need
            var s2 = await agent.SynchronizeAsync();

            Assert.Equal(4, onApplying);
            Assert.Equal(2, onApplied);
            Assert.Equal(1, onDatabaseApplying);
            Assert.Equal(1, onDatabaseApplied);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbNameSrv);
            HelperDatabase.DropDatabase(ProviderType.Sql, dbNameCli);
        }


    }
}
