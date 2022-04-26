using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Microsoft.Data.SqlClient;
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
        public async Task StoredProcedure_Create_One()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            // Create default table
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var scopeInfo = await localOrchestrator.GetClientScopeAsync(setup);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            localOrchestrator.OnStoredProcedureCreating(tca => onCreating++);
            localOrchestrator.OnStoredProcedureCreated(tca => onCreated++);
            localOrchestrator.OnStoredProcedureDropping(tca => onDropping++);
            localOrchestrator.OnStoredProcedureDropped(tca => onDropped++);

            var isCreated = await localOrchestrator.CreateStoredProcedureAsync(scopeInfo,
                "Product", "SalesLT", DbStoredProcedureType.SelectChanges);

            Assert.True(isCreated);
            Assert.Equal(1, onCreating);
            Assert.Equal(1, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            // Check 
            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);
                var check = await SqlManagementUtils.ProcedureExistsAsync(c, null, "SalesLT.Product_changes").ConfigureAwait(false);
                Assert.True(check);
                c.Close();
            }

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task StoredProcedure_Exists()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            // Create default table
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var scopeInfo = await localOrchestrator.GetClientScopeAsync(setup);

            await localOrchestrator.CreateStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges);

            var exists = await localOrchestrator.ExistStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges);
            Assert.True(exists);

            exists = await localOrchestrator.ExistStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChangesWithFilters);
            Assert.False(exists);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }


        [Fact]
        public async Task StoredProcedure_Create_All()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            // Create default table
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            localOrchestrator.OnStoredProcedureCreating(tca => onCreating++);
            localOrchestrator.OnStoredProcedureCreated(tca => onCreated++);
            localOrchestrator.OnStoredProcedureDropping(tca => onDropping++);
            localOrchestrator.OnStoredProcedureDropped(tca => onDropped++);

            var scopeInfo = await localOrchestrator.GetClientScopeAsync(setup);

            var isCreated = await localOrchestrator.CreateStoredProceduresAsync(scopeInfo, "Product", "SalesLT");

            Assert.True(isCreated);
            Assert.Equal(10, onCreating);
            Assert.Equal(10, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            // Check 
            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);
                var check = await SqlManagementUtils.ProcedureExistsAsync(c, null, "SalesLT.Product_bulkdelete").ConfigureAwait(false);
                Assert.True(check);
                check = await SqlManagementUtils.ProcedureExistsAsync(c, null, "SalesLT.Product_bulkupdate").ConfigureAwait(false);
                Assert.True(check);
                check = await SqlManagementUtils.ProcedureExistsAsync(c, null, "SalesLT.Product_changes").ConfigureAwait(false);
                Assert.True(check);
                check = await SqlManagementUtils.ProcedureExistsAsync(c, null, "SalesLT.Product_delete").ConfigureAwait(false);
                Assert.True(check);
                check = await SqlManagementUtils.ProcedureExistsAsync(c, null, "SalesLT.Product_deletemetadata").ConfigureAwait(false);
                Assert.True(check);
                check = await SqlManagementUtils.ProcedureExistsAsync(c, null, "SalesLT.Product_initialize").ConfigureAwait(false);
                Assert.True(check);
                check = await SqlManagementUtils.ProcedureExistsAsync(c, null, "SalesLT.Product_reset").ConfigureAwait(false);
                Assert.True(check);
                check = await SqlManagementUtils.ProcedureExistsAsync(c, null, "SalesLT.Product_selectrow").ConfigureAwait(false);
                Assert.True(check);
                check = await SqlManagementUtils.ProcedureExistsAsync(c, null, "SalesLT.Product_update").ConfigureAwait(false);
                Assert.True(check);
                c.Close();
            }


            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task StoredProcedure_Create_All_Overwrite()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            // Create default table
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var scopeInfo = await localOrchestrator.GetClientScopeAsync(setup);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            localOrchestrator.OnStoredProcedureCreating(tca => onCreating++);
            localOrchestrator.OnStoredProcedureCreated(tca => onCreated++);
            localOrchestrator.OnStoredProcedureDropping(tca => onDropping++);
            localOrchestrator.OnStoredProcedureDropped(tca => onDropped++);

            var isCreated = await localOrchestrator.CreateStoredProceduresAsync(scopeInfo, "Product", "SalesLT");

            Assert.True(isCreated);
            Assert.Equal(10, onCreating);
            Assert.Equal(10, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            onCreating = 0;
            onCreated = 0;
            onDropping = 0;
            onDropped = 0;

            isCreated = await localOrchestrator.CreateStoredProceduresAsync(scopeInfo, "Product", "SalesLT");

            Assert.False(isCreated);
            Assert.Equal(0, onCreating);
            Assert.Equal(0, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            onCreating = 0;
            onCreated = 0;
            onDropping = 0;
            onDropped = 0;

            isCreated = await localOrchestrator.CreateStoredProceduresAsync(scopeInfo, "Product", "SalesLT", true);

            Assert.True(isCreated);
            Assert.Equal(10, onCreating);
            Assert.Equal(10, onCreated);
            Assert.Equal(10, onDropping);
            Assert.Equal(10, onDropped);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }



        [Fact]
        public async Task StoredProcedure_Drop_One()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            // Create default table
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var scopeInfo = await localOrchestrator.GetClientScopeAsync(setup);

            var isCreated = await localOrchestrator.CreateStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges);
            Assert.True(isCreated);

            // Ensuring we have a clean new instance
            localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            localOrchestrator.OnStoredProcedureCreating(tca => onCreating++);
            localOrchestrator.OnStoredProcedureCreated(tca => onCreated++);
            localOrchestrator.OnStoredProcedureDropping(tca => onDropping++);
            localOrchestrator.OnStoredProcedureDropped(tca => onDropped++);

            var isDropped = await localOrchestrator.DropStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges);

            Assert.True(isDropped);
            Assert.Equal(0, onCreating);
            Assert.Equal(0, onCreated);
            Assert.Equal(1, onDropping);
            Assert.Equal(1, onDropped);

            // Check 
            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);
                var check = await SqlManagementUtils.ProcedureExistsAsync(c, null, "SalesLT.Product_changes").ConfigureAwait(false);
                Assert.False(check);
                c.Close();
            }

            // try to delete again a non existing sp type
            isDropped = await localOrchestrator.DropStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChangesWithFilters);

            Assert.False(isDropped);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task StoredProcedure_Drop_One_Cancel()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            // Create default table
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var scopeInfo = await localOrchestrator.GetClientScopeAsync(setup);

            var isCreated = await localOrchestrator.CreateStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges);
            Assert.True(isCreated);

            // Ensuring we have a clean new instance
            localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            localOrchestrator.OnStoredProcedureCreating(tca => onCreating++);

            localOrchestrator.OnStoredProcedureCreated(tca => onCreated++);
            localOrchestrator.OnStoredProcedureDropping(tca =>
            {
                tca.Cancel = true;
                onDropping++;
            });
            localOrchestrator.OnStoredProcedureDropped(tca => onDropped++);

            var isDropped = await localOrchestrator.DropStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges);

            Assert.False(isDropped);
            Assert.Equal(0, onCreating);
            Assert.Equal(0, onCreated);
            Assert.Equal(1, onDropping);
            Assert.Equal(0, onDropped);

            // Check 
            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);
                var check = await SqlManagementUtils.ProcedureExistsAsync(c, null, "SalesLT.Product_changes").ConfigureAwait(false);
                Assert.True(check);
                c.Close();
            }

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task StoredProcedure_Create_One_Overwrite()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            // Create default table
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var scopeInfo = await localOrchestrator.GetClientScopeAsync(setup);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            localOrchestrator.OnStoredProcedureCreating(tca => onCreating++);
            localOrchestrator.OnStoredProcedureCreated(tca => onCreated++);
            localOrchestrator.OnStoredProcedureDropping(tca => onDropping++);
            localOrchestrator.OnStoredProcedureDropped(tca => onDropped++);

            var isCreated = await localOrchestrator.CreateStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges);

            Assert.True(isCreated);
            Assert.Equal(1, onCreating);
            Assert.Equal(1, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            onCreating = 0;
            onCreated = 0;
            onDropping = 0;
            onDropped = 0;

            isCreated = await localOrchestrator.CreateStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges);

            Assert.False(isCreated);
            Assert.Equal(0, onCreating);
            Assert.Equal(0, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            isCreated = await localOrchestrator.CreateStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges, true);

            Assert.True(isCreated);
            Assert.Equal(1, onCreating);
            Assert.Equal(1, onCreated);
            Assert.Equal(1, onDropping);
            Assert.Equal(1, onDropped);


            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task StoredProcedure_Drop_All()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            // Create default table
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var scopeInfo = await localOrchestrator.GetClientScopeAsync(setup);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            localOrchestrator.OnStoredProcedureCreating(tca => onCreating++);
            localOrchestrator.OnStoredProcedureCreated(tca => onCreated++);
            localOrchestrator.OnStoredProcedureDropping(tca => onDropping++);
            localOrchestrator.OnStoredProcedureDropped(tca => onDropped++);

            var isCreated = await localOrchestrator.CreateStoredProceduresAsync(scopeInfo, "Product", "SalesLT");

            Assert.True(isCreated);
            Assert.Equal(10, onCreating);
            Assert.Equal(10, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);


            onCreating = 0;
            onCreated = 0;
            onDropping = 0;
            onDropped = 0;

            var isDropped = await localOrchestrator.DropStoredProceduresAsync(scopeInfo, "Product", "SalesLT");

            Assert.True(isCreated);
            Assert.Equal(0, onCreating);
            Assert.Equal(0, onCreated);
            Assert.Equal(10, onDropping);
            Assert.Equal(10, onDropped);


            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

    }
}
