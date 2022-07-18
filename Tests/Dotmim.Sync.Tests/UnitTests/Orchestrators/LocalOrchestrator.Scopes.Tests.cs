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
            var provider = new SqlSyncProvider(cs);

            var localOrchestrator = new LocalOrchestrator(provider, options);

            var scope = await localOrchestrator.GetClientScopeInfoAsync();

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

            var localScopeInfo = await localOrchestrator.GetClientScopeInfoAsync(scopeName);

            Assert.NotNull(localScopeInfo);
            Assert.Equal(scopeName, localScopeInfo.Name);
            Assert.True(localScopeInfo.IsNewScope);
            Assert.NotEqual(Guid.Empty, localScopeInfo.Id);
            Assert.Null(localScopeInfo.LastServerSyncTimestamp);
            Assert.Null(localScopeInfo.LastSync);
            Assert.Equal(0, localScopeInfo.LastSyncDuration);
            Assert.Null(localScopeInfo.LastSyncTimestamp);
            Assert.Null(localScopeInfo.Schema);
            Assert.Null(localScopeInfo.Setup);
            Assert.Equal(SyncVersion.Current, new Version(localScopeInfo.Version));

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task LocalOrchestrator_EnsureScope_NewScope_WithoutSetup_ShouldBeEmpty()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var scopeName = "scope";

            var options = new SyncOptions();

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var localScopeInfo = await localOrchestrator.GetClientScopeInfoAsync(scopeName);

            Assert.NotNull(localScopeInfo);
            Assert.Equal(scopeName, localScopeInfo.Name);
            Assert.True(localScopeInfo.IsNewScope);
            Assert.NotEqual(Guid.Empty, localScopeInfo.Id);
            Assert.Null(localScopeInfo.LastServerSyncTimestamp);
            Assert.Null(localScopeInfo.LastSync);
            Assert.Equal(0, localScopeInfo.LastSyncDuration);
            Assert.Null(localScopeInfo.LastSyncTimestamp);
            Assert.Null(localScopeInfo.Schema);
            Assert.Null(localScopeInfo.Setup);
            Assert.Equal(SyncVersion.Current, new Version(localScopeInfo.Version));

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task LocalOrchestrator_MultipleScopes_ShouldHave_SameClientId()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var options = new SyncOptions();

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var localScopeInfo1 = await localOrchestrator.GetClientScopeInfoAsync();
            var localScopeInfo2 = await localOrchestrator.GetClientScopeInfoAsync("A");
            var localScopeInfo3 = await localOrchestrator.GetClientScopeInfoAsync("B");

            Assert.Equal(localScopeInfo1.Id, localScopeInfo2.Id);
            Assert.Equal(localScopeInfo2.Id, localScopeInfo3.Id);


            // Check we get the 3 scopes
            var allScopes = await localOrchestrator.GetAllClientScopesInfoAsync();

            Assert.Equal(3, allScopes.Count);

            // Check the scope id, read from database, is good
            foreach (var scope in allScopes)
            {
                Assert.Equal(scope.Id, localScopeInfo1.Id);
            }


            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task LocalOrchestrator_MultipleScopes_Check_Metadatas_Are_Created()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var setup = new SyncSetup(this.Tables);

            var setup2 = new SyncSetup(this.Tables);
            setup2.Filters.Add("Customer", "EmployeeID");

            var schema = await localOrchestrator.GetSchemaAsync(setup);
            var schema2 = await localOrchestrator.GetSchemaAsync(setup2);

            var localScopeInfo1 = await localOrchestrator.GetClientScopeInfoAsync();
            var localScopeInfo2 = await localOrchestrator.GetClientScopeInfoAsync("A");

            var serverScope1 = new ServerScopeInfo
            {
                Name = localScopeInfo1.Name,
                Schema = schema,
                Setup = setup,
                Version = localScopeInfo1.Version
            };

            var serverScope2 = new ServerScopeInfo
            {
                Name = localScopeInfo2.Name,
                Schema = schema2,
                Setup = setup2,
                Version = localScopeInfo2.Version
            };

            localScopeInfo1 = await localOrchestrator.ProvisionAsync(serverScope1);
            localScopeInfo2 = await localOrchestrator.ProvisionAsync(serverScope2);

            foreach (var table in localScopeInfo1.Setup.Tables)
            {
                var tableName = table.TableName;
                var schemaName = table.SchemaName;

                foreach (var objectSpType in Enum.GetValues(typeof(Builders.DbStoredProcedureType)))
                {
                    var spType = (Builders.DbStoredProcedureType)objectSpType;

                    var exists1 = await localOrchestrator.ExistStoredProcedureAsync(
                        localScopeInfo1, tableName, schemaName, spType);

                    var exists2 = await localOrchestrator.ExistStoredProcedureAsync(
                        localScopeInfo2, tableName, schemaName, spType);


                    if (spType == Builders.DbStoredProcedureType.SelectChangesWithFilters ||
                        spType == Builders.DbStoredProcedureType.SelectInitializedChangesWithFilters)
                    {
                        Assert.False(exists1);

                        if (tableName == "Customer")
                            Assert.True(exists2);
                        else
                            Assert.False(exists2);

                    }
                    else
                    {
                        Assert.True(exists1);
                        Assert.True(exists2);

                    }


                }

                foreach (var objectSpType in Enum.GetValues(typeof(Builders.DbTriggerType)))
                {
                    var trigType = (Builders.DbTriggerType)objectSpType;

                    var existsTrig1 = await localOrchestrator.ExistTriggerAsync(localScopeInfo1, tableName, schemaName, trigType);
                    var existsTrig2 = await localOrchestrator.ExistTriggerAsync(localScopeInfo2, tableName, schemaName, trigType);

                    Assert.True(existsTrig1);
                    Assert.True(existsTrig2);

                }

                var trackTableExists1 = await localOrchestrator.ExistTrackingTableAsync(localScopeInfo1, tableName, schemaName);
                var trackTableExists2 = await localOrchestrator.ExistTrackingTableAsync(localScopeInfo2, tableName, schemaName);

                Assert.True(trackTableExists1);
                Assert.True(trackTableExists2);

            }




            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }


        [Fact]
        public async Task LocalOrchestrator_MultipleScopes_Check_Metadatas_Are_Deleted()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var setup = new SyncSetup(this.Tables);

            var setup2 = new SyncSetup(this.Tables);
            setup2.Filters.Add("Customer", "EmployeeID");

            var schema = await localOrchestrator.GetSchemaAsync(setup);

            var localScopeInfo1 = await localOrchestrator.GetClientScopeInfoAsync();
            var localScopeInfo2 = await localOrchestrator.GetClientScopeInfoAsync("A");

            var serverScope1 = new ServerScopeInfo
            {
                Name = localScopeInfo1.Name,
                Schema = schema,
                Setup = setup,
                Version = localScopeInfo1.Version
            };

            var serverScope2 = new ServerScopeInfo
            {
                Name = localScopeInfo2.Name,
                Schema = schema,
                Setup = setup2,
                Version = localScopeInfo2.Version
            };

            // Provision two scopes (already tested in previous test)
            localScopeInfo1 = await localOrchestrator.ProvisionAsync(serverScope1);
            localScopeInfo2 = await localOrchestrator.ProvisionAsync(serverScope2);

            Assert.NotNull(localScopeInfo1.Setup);
            Assert.NotNull(localScopeInfo1.Schema);

            Assert.NotNull(localScopeInfo2.Setup);
            Assert.NotNull(localScopeInfo2.Schema);


            // Deprovision
            await localOrchestrator.DeprovisionAsync("A");

            foreach (var table in localScopeInfo1.Setup.Tables)
            {
                var tableName = table.TableName;
                var schemaName = table.SchemaName;

                foreach (var objectSpType in Enum.GetValues(typeof(Builders.DbStoredProcedureType)))
                {
                    var spType = (Builders.DbStoredProcedureType)objectSpType;

                    var exists1 = await localOrchestrator.ExistStoredProcedureAsync(
                        localScopeInfo1, tableName, schemaName, spType);

                    var exists2 = await localOrchestrator.ExistStoredProcedureAsync(
                        localScopeInfo2, tableName, schemaName, spType);


                    if (spType == Builders.DbStoredProcedureType.SelectChangesWithFilters ||
                        spType == Builders.DbStoredProcedureType.SelectInitializedChangesWithFilters)
                    {
                        Assert.False(exists1);
                    }
                    else
                    {
                        Assert.True(exists1);
                    }

                    Assert.False(exists2);

                }

                foreach (var objectSpType in Enum.GetValues(typeof(Builders.DbTriggerType)))
                {
                    var trigType = (Builders.DbTriggerType)objectSpType;

                    var existsTrig1 = await localOrchestrator.ExistTriggerAsync(localScopeInfo1, tableName, schemaName, trigType);
                    var existsTrig2 = await localOrchestrator.ExistTriggerAsync(localScopeInfo2, tableName, schemaName, trigType);

                    Assert.False(existsTrig1);
                    Assert.False(existsTrig2);

                }

                var trackTableExists1 = await localOrchestrator.ExistTrackingTableAsync(localScopeInfo1, tableName, schemaName);
                var trackTableExists2 = await localOrchestrator.ExistTrackingTableAsync(localScopeInfo2, tableName, schemaName);

                // Tracking table are still existing for others scopes
                Assert.True(trackTableExists1);
                Assert.True(trackTableExists2);

            }

            // Deprovision
            await localOrchestrator.DeprovisionAsync();

            foreach (var table in localScopeInfo1.Setup.Tables)
            {
                var tableName = table.TableName;
                var schemaName = table.SchemaName;

                foreach (var objectSpType in Enum.GetValues(typeof(Builders.DbStoredProcedureType)))
                {
                    var spType = (Builders.DbStoredProcedureType)objectSpType;

                    var exists1 = await localOrchestrator.ExistStoredProcedureAsync(
                        localScopeInfo1, tableName, schemaName, spType);

                    var exists2 = await localOrchestrator.ExistStoredProcedureAsync(
                        localScopeInfo2, tableName, schemaName, spType);

                    Assert.False(exists1);
                    Assert.False(exists2);

                }
            }


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
                async () => await localOrchestrator.GetClientScopeInfoAsync(cancellationToken: cts.Token));

            Assert.Equal(SyncSide.ClientSide, se.Side);
            Assert.Equal("OperationCanceledException", se.TypeName);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }
    }
}
