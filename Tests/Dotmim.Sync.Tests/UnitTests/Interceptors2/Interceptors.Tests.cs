using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Fixtures;
using Dotmim.Sync.Tests.Models;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
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
using Xunit.Sdk;

namespace Dotmim.Sync.Tests.UnitTests
{

    public class SqlServerInterceptorTests : InterceptorsTests2<SqlServerFixtureType>
    {
        public SqlServerInterceptorTests(ITestOutputHelper output, DatabaseServerFixture<SqlServerFixtureType> fixture) : base(output, fixture)
        {
        }
    }
    public class MySqlInterceptorTests : InterceptorsTests2<MySqlFixtureType>
    {
        public MySqlInterceptorTests(ITestOutputHelper output, DatabaseServerFixture<MySqlFixtureType> fixture) : base(output, fixture)
        {
        }
    }

    public class MariaDBInterceptorTests : InterceptorsTests2<MariaDBFixtureType>
    {
        public MariaDBInterceptorTests(ITestOutputHelper output, DatabaseServerFixture<MariaDBFixtureType> fixture) : base(output, fixture)
        {
        }
    }

    public class PostgreSqlInterceptorTests : InterceptorsTests2<PostgresFixtureType>
    {
        public PostgreSqlInterceptorTests(ITestOutputHelper output, DatabaseServerFixture<PostgresFixtureType> fixture) : base(output, fixture)
        {
        }
    }

    public abstract class InterceptorsTests2<T> : DatabaseTest<T>, IDisposable where T : RelationalFixture
    {

        public InterceptorsTests2(ITestOutputHelper output, DatabaseServerFixture<T> fixture) : base(output, fixture)
        {
        }   

        [Fact]
        public async Task LocalOrchestrator_ApplyChanges()
        {
            var scopeName = "scopesnap1";
            var serverProvider = Fixture.GetServerProvider();

            // Make a first sync to be sure everything is in place
            foreach (var clientProvider in Fixture.GetClientProviders())
            {
                var agent = new SyncAgent(clientProvider, serverProvider);
                await agent.LocalOrchestrator.DropAllAsync();
                await agent.SynchronizeAsync(scopeName, this.Fixture.Tables);
            }

            // Create a productcategory item
            // Create a new product on server
            await Fixture.AddProductCategoryAsync(serverProvider);
            await Fixture.AddProductAsync(serverProvider);

            foreach (var clientProvider in Fixture.GetClientProviders())
            {
                // Make a first sync to be sure everything is in place
                var agent = new SyncAgent(clientProvider, serverProvider);

                // Get the orchestrators
                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

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
        }

        [Fact]
        public async Task RemoteOrchestrator_ApplyChanges()
        {
            var scopeName = "scopesnap1";
            var serverProvider = Fixture.GetServerProvider();

            // Make a first sync to be sure everything is in place
            foreach (var clientProvider in Fixture.GetClientProviders())
                await new SyncAgent(clientProvider, serverProvider).SynchronizeAsync(scopeName, this.Fixture.Tables);

            foreach (var clientProvider in Fixture.GetClientProviders())
            {
                // Make a first sync to be sure everything is in place
                var agent = new SyncAgent(clientProvider, serverProvider);

                // Get the orchestrators
                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // Client side : Create a product category and a product
                await Fixture.AddProductCategoryAsync(clientProvider);
                await Fixture.AddProductAsync(clientProvider);

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


        [Fact]
        public async Task RemoteOrchestrator_ApplyChanges_OnRowsApplied_ContinueOnError()
        {
            var serverProvider = Fixture.GetServerProvider();
            var scopeName = "scopesnap1";

            // Make a first sync to be sure everything is in place
            foreach (var clientProvider in Fixture.GetClientProviders())
                await new SyncAgent(clientProvider, serverProvider).SynchronizeAsync(scopeName, this.Fixture.Tables);

            // Add a product category we can easily retrieve during sync
            var pc = await Fixture.AddProductCategoryAsync(serverProvider);

            foreach (var clientProvider in Fixture.GetClientProviders())
            {
                // disable bulk mode to prevent fk constraint to be resolved in one batch
                clientProvider.UseBulkOperations = false;
               
                var agent = new SyncAgent(clientProvider, serverProvider);

                // Get the orchestrators
                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                var onRowsChangesAppliedHappened = 0;
                var onRowsErrorOccuredHappened = 0;

                // Generate the foreignkey error
                localOrchestrator.OnRowsChangesApplying(args =>
                {
                    if (args.SyncRows == null || args.SyncRows.Count <= 0)
                        return;
                    var row = args.SyncRows[0];
                 
                    // Generate the foreign key constraint
                    if (row["ProductCategoryId"].ToString()  == pc.ProductCategoryId && row["ParentProductCategoryId"] == null)
                        row["ParentProductCategoryId"] = "BBBBB";
                });

                localOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    args.Resolution = ErrorResolution.ContinueOnError;
                    onRowsErrorOccuredHappened++;
                });

                localOrchestrator.OnRowsChangesApplied(args =>
                {
                    Assert.NotNull(args.SyncRows);
                    Assert.Single(args.SyncRows);

                    onRowsChangesAppliedHappened++;
                });

                // Making a first sync, will initialize everything we need
                var s = await agent.SynchronizeAsync(scopeName, this.Fixture.Tables);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(1, s.TotalChangesFailedToApplyOnClient);

                Assert.Equal(1, onRowsChangesAppliedHappened);
                Assert.Equal(1, onRowsErrorOccuredHappened);
            }

            // Sync to reinit data on all databases
            foreach (var clientProvider in Fixture.GetClientProviders())
                await new SyncAgent(clientProvider, serverProvider).SynchronizeAsync(scopeName, this.Fixture.Tables);
        }

        [Fact]
        public async Task RemoteOrchestrator_ApplyChanges_OnRowsApplied_ErrorResolved()
        {
            var serverProvider = Fixture.GetServerProvider();
            var scopeName = "scopesnap1";

            // Make a first sync to be sure everything is in place
            foreach (var clientProvider in Fixture.GetClientProviders())
                await new SyncAgent(clientProvider, serverProvider).SynchronizeAsync(scopeName, this.Fixture.Tables);

            // Add a product category we can easily retrieve during sync
            var pc = await Fixture.AddProductCategoryAsync(serverProvider);

            foreach (var clientProvider in Fixture.GetClientProviders())
            {
                // disable bulk mode to prevent fk constraint to be resolved in one batch
                clientProvider.UseBulkOperations = false;

                // Make a first sync to be sure everything is in place
                var agent = new SyncAgent(clientProvider, serverProvider);

                // Get the orchestrators
                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                var onRowsChangesAppliedHappened = 0;
                var onRowsErrorOccuredHappened = 0;

                // Generate the foreignkey error
                localOrchestrator.OnRowsChangesApplying(args =>
                {
                    if (args.SyncRows == null || args.SyncRows.Count <= 0)
                        return;
                    var row = args.SyncRows[0];

                    // Generate the foreign key constraint
                    if (row["ProductCategoryId"].ToString() == pc.ProductCategoryId && row["ParentProductCategoryId"] == null)
                        row["ParentProductCategoryId"] = "BBBBB";
                });

                localOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    args.Resolution = ErrorResolution.Resolved;
                    onRowsErrorOccuredHappened++;
                });

                localOrchestrator.OnRowsChangesApplied(args =>
                {
                    Assert.NotNull(args.SyncRows);
                    Assert.Single(args.SyncRows);
                    onRowsChangesAppliedHappened++;
                });

                // Making a first sync, will initialize everything we need
                var s = await agent.SynchronizeAsync(scopeName, this.Fixture.Tables);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);

                Assert.Equal(1, onRowsChangesAppliedHappened);
                Assert.Equal(1, onRowsErrorOccuredHappened);
            }

            // Sync to reinit data on all databases
            foreach (var clientProvider in Fixture.GetClientProviders())
                await new SyncAgent(clientProvider, serverProvider).SynchronizeAsync(scopeName, this.Fixture.Tables);
        }


        [Fact]
        public async Task RemoteOrchestrator_ApplyChanges_OnRowsApplied_ErrorRetryOneMoreTime()
        {
            var serverProvider = Fixture.GetServerProvider();
            var scopeName = "scopesnap1";

            // Make a first sync to be sure everything is in place
            foreach (var clientProvider in Fixture.GetClientProviders())
                await new SyncAgent(clientProvider, serverProvider).SynchronizeAsync(scopeName, this.Fixture.Tables);

            // Add a product category we can easily retrieve during sync
            var pc = await Fixture.AddProductCategoryAsync(serverProvider);

            foreach (var clientProvider in Fixture.GetClientProviders())
            {
                // disable bulk mode to prevent fk constraint to be resolved in one batch
                clientProvider.UseBulkOperations = false;

                // Make a first sync to be sure everything is in place
                var agent = new SyncAgent(clientProvider, serverProvider);

                // Get the orchestrators
                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                var onRowsChangesAppliedHappened = 0;
                var onRowsErrorOccuredHappened = 0;

                // Generate the foreignkey error
                localOrchestrator.OnRowsChangesApplying(args =>
                {
                    if (args.SyncRows == null || args.SyncRows.Count <= 0)
                        return;
                    var row = args.SyncRows[0];

                    // Generate the foreign key constraint
                    if (row["ProductCategoryId"].ToString() == pc.ProductCategoryId && row["ParentProductCategoryId"] == null)
                        row["ParentProductCategoryId"] = "BBBBB";
                });

                localOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    args.Resolution = ErrorResolution.RetryOneMoreTimeAndContinueOnError;
                    onRowsErrorOccuredHappened++;
                });

                localOrchestrator.OnRowsChangesApplied(args =>
                {
                    Assert.NotNull(args.SyncRows);
                    Assert.Single(args.SyncRows);
                    onRowsChangesAppliedHappened++;
                });

                // Making a first sync, will initialize everything we need
                var s = await agent.SynchronizeAsync(scopeName, this.Fixture.Tables);
           
                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(1, s.TotalChangesFailedToApplyOnClient);

                Assert.Equal(2, onRowsChangesAppliedHappened);
                Assert.Equal(1, onRowsErrorOccuredHappened);
            }
        }
    }
}
