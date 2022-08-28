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
    public partial class RemoteOrchestratorTests
    {


        [Fact]
        public async Task RemoteOrchestrator_Scope_Should_NotFail_If_NoTables_In_Setup()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_ro_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);

            var options = new SyncOptions();
            var setup = new SyncSetup();
            var provider = new SqlSyncProvider(cs);

            var remoteOrchestrator = new RemoteOrchestrator(provider, options);

            var sScopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);

            Assert.NotNull(sScopeInfo);
            Assert.Null(sScopeInfo.Schema);
            Assert.Null(sScopeInfo.Setup);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);

        }




        [Fact]
        public async Task RemoteOrchestrator_Scope_NewScope()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_ro_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();
            var scopeName = "scope";

            var options = new SyncOptions();
            var setup = new SyncSetup(this.Tables);

            var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);

            var remoteScopeInfo = await remoteOrchestrator.GetScopeInfoAsync(scopeName, setup);

            Assert.NotNull(remoteScopeInfo);
            Assert.Equal(scopeName, remoteScopeInfo.Name);
            Assert.Null(remoteScopeInfo.LastCleanupTimestamp);
            Assert.NotNull(remoteScopeInfo.Schema);
            Assert.NotNull(remoteScopeInfo.Setup);

            Assert.Equal(SyncVersion.Current, new Version(remoteScopeInfo.Version));

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task RemoteOrchestrator_Scope_IsNotNewScope_OnceSaved()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_ro_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();
            var scopeName = "scope";

            var options = new SyncOptions();
            var setup = new SyncSetup(this.Tables);

            var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);

            var remoteScopeInfo = await remoteOrchestrator.GetScopeInfoAsync(scopeName, setup);

            Assert.Equal(SyncVersion.Current, new Version(remoteScopeInfo.Version));

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task RemoteOrchestrator_Scopes_Multiples_Check_Schema_Setup_AreNotEmpty()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_ro_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();

            var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);

            var setup = new SyncSetup(this.Tables);

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


            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }



        [Fact]
        public async Task RemoteOrchestrator_Scopes_Multiple_Check_Metadatas_Are_Created()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_ro_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();

            var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);

            var setup = new SyncSetup(this.Tables);

            var setup2 = new SyncSetup(this.Tables);
            setup2.Filters.Add("Customer", "EmployeeID");

            var remoteScopeInfo1 = await remoteOrchestrator.GetScopeInfoAsync(setup);
            var remoteScopeInfo2 = await remoteOrchestrator.GetScopeInfoAsync("A", setup2);

            await remoteOrchestrator.ProvisionAsync(setup);
            await remoteOrchestrator.ProvisionAsync("A", setup2);

            foreach (var table in remoteScopeInfo1.Setup.Tables)
            {
                var tableName = table.TableName;
                var schemaName = table.SchemaName;

                foreach (var objectSpType in Enum.GetValues(typeof(Builders.DbStoredProcedureType)))
                {
                    var spType = (Builders.DbStoredProcedureType)objectSpType;

                    var exists1 = await remoteOrchestrator.ExistStoredProcedureAsync(
                        remoteScopeInfo1, tableName, schemaName, spType);

                    var exists2 = await remoteOrchestrator.ExistStoredProcedureAsync(
                        remoteScopeInfo2, tableName, schemaName, spType);


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

                    var existsTrig1 = await remoteOrchestrator.ExistTriggerAsync(remoteScopeInfo1, tableName, schemaName, trigType);
                    var existsTrig2 = await remoteOrchestrator.ExistTriggerAsync(remoteScopeInfo2, tableName, schemaName, trigType);

                    Assert.True(existsTrig1);
                    Assert.True(existsTrig2);

                }

                var trackTableExists1 = await remoteOrchestrator.ExistTrackingTableAsync(remoteScopeInfo1, tableName, schemaName);
                var trackTableExists2 = await remoteOrchestrator.ExistTrackingTableAsync(remoteScopeInfo2, tableName, schemaName);

                Assert.True(trackTableExists1);
                Assert.True(trackTableExists2);

            }
            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }


        [Fact]
        public async Task RemoteOrchestrator_Scopes_Multiple_Check_Metadatas_Are_Deleted()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_ro_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();

            var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);

            var setup = new SyncSetup(this.Tables);

            var setup2 = new SyncSetup(this.Tables);
            setup2.Filters.Add("Customer", "EmployeeID");

            var remoteScopeInfo1 = await remoteOrchestrator.GetScopeInfoAsync(setup);
            var remoteScopeInfo2 = await remoteOrchestrator.GetScopeInfoAsync("A", setup2);

            Assert.NotNull(remoteScopeInfo1.Setup);
            Assert.NotNull(remoteScopeInfo1.Schema);

            Assert.NotNull(remoteScopeInfo2.Setup);
            Assert.NotNull(remoteScopeInfo2.Schema);

            // Provision two scopes (already tested in previous test)
            remoteScopeInfo1 = await remoteOrchestrator.ProvisionAsync(setup);
            remoteScopeInfo2 = await remoteOrchestrator.ProvisionAsync("A", setup2);

            // Deprovision
            await remoteOrchestrator.DeprovisionAsync("A");

            foreach (var table in remoteScopeInfo1.Setup.Tables)
            {
                var tableName = table.TableName;
                var schemaName = table.SchemaName;

                foreach (var objectSpType in Enum.GetValues(typeof(Builders.DbStoredProcedureType)))
                {
                    var spType = (Builders.DbStoredProcedureType)objectSpType;

                    var exists1 = await remoteOrchestrator.ExistStoredProcedureAsync(
                        remoteScopeInfo1, tableName, schemaName, spType);

                    var exists2 = await remoteOrchestrator.ExistStoredProcedureAsync(
                        remoteScopeInfo2, tableName, schemaName, spType);


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

                    var existsTrig1 = await remoteOrchestrator.ExistTriggerAsync(remoteScopeInfo1, tableName, schemaName, trigType);
                    var existsTrig2 = await remoteOrchestrator.ExistTriggerAsync(remoteScopeInfo2, tableName, schemaName, trigType);

                    Assert.False(existsTrig1);
                    Assert.False(existsTrig2);

                }

                var trackTableExists1 = await remoteOrchestrator.ExistTrackingTableAsync(remoteScopeInfo1, tableName, schemaName);
                var trackTableExists2 = await remoteOrchestrator.ExistTrackingTableAsync(remoteScopeInfo2, tableName, schemaName);

                Assert.True(trackTableExists1);
                Assert.True(trackTableExists2);

            }

            // Deprovision
            await remoteOrchestrator.DeprovisionAsync();

            foreach (var table in remoteScopeInfo1.Setup.Tables)
            {
                var tableName = table.TableName;
                var schemaName = table.SchemaName;

                foreach (var objectSpType in Enum.GetValues(typeof(Builders.DbStoredProcedureType)))
                {
                    var spType = (Builders.DbStoredProcedureType)objectSpType;

                    var exists1 = await remoteOrchestrator.ExistStoredProcedureAsync(
                        remoteScopeInfo1, tableName, schemaName, spType);

                    var exists2 = await remoteOrchestrator.ExistStoredProcedureAsync(
                        remoteScopeInfo2, tableName, schemaName, spType);

                    Assert.False(exists1);
                    Assert.False(exists2);

                }
            }


            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }
    }
}
