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
        public async Task Trigger_Create_One()
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

            // 1) create a console logger
            //var loggerFactory = LoggerFactory.Create(builder => { builder.AddDebug().SetMinimumLevel(LogLevel.Debug); });
            //var logger = loggerFactory.CreateLogger("Dotmim.Sync");
            var logger = new SyncLogger().AddDebug().SetMinimumLevel(LogLevel.Debug);
            options.Logger = logger;

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var scopeInfo = await localOrchestrator.GetClientScopeInfoAsync(setup);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            localOrchestrator.OnTriggerCreating(tca => onCreating++);
            localOrchestrator.OnTriggerCreated(tca => onCreated++);
            localOrchestrator.OnTriggerDropping(tca => onDropping++);
            localOrchestrator.OnTriggerDropped(tca => onDropped++);

            var isCreated = await localOrchestrator.CreateTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert);

            Assert.True(isCreated);
            Assert.Equal(1, onCreating);
            Assert.Equal(1, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            // Check 
            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);
                var check = await SqlManagementUtils.GetTriggerAsync(c, null, "Product_insert_trigger", "SalesLT").ConfigureAwait(false);
                Assert.Single(check.Rows);
                c.Close();
            }

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task Trigger_Exists()
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

            var scopeInfo = await localOrchestrator.GetClientScopeInfoAsync(setup);

            await localOrchestrator.CreateTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert);

            var exists = await localOrchestrator.ExistTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert);
            Assert.True(exists);

            exists = await localOrchestrator.ExistTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Update);
            Assert.False(exists);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task Trigger_Create_All()
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

            var scopeInfo = await localOrchestrator.GetClientScopeInfoAsync(setup);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            localOrchestrator.OnTriggerCreating(tca => onCreating++);
            localOrchestrator.OnTriggerCreated(tca => onCreated++);
            localOrchestrator.OnTriggerDropping(tca => onDropping++);
            localOrchestrator.OnTriggerDropped(tca => onDropped++);

            var isCreated = await localOrchestrator.CreateTriggersAsync(scopeInfo, "Product", "SalesLT");

            Assert.True(isCreated);
            Assert.Equal(3, onCreating);
            Assert.Equal(3, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            // Check 
            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);
                var check = await SqlManagementUtils.GetTriggerAsync(c, null, "Product_insert_trigger", "SalesLT").ConfigureAwait(false);
                Assert.Single(check.Rows);
                check = await SqlManagementUtils.GetTriggerAsync(c, null, "Product_update_trigger", "SalesLT").ConfigureAwait(false);
                Assert.Single(check.Rows);
                check = await SqlManagementUtils.GetTriggerAsync(c, null, "Product_delete_trigger", "SalesLT").ConfigureAwait(false);
                Assert.Single(check.Rows);
                c.Close();
            }


            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task Trigger_Drop_One()
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

            var scopeInfo = await localOrchestrator.GetClientScopeInfoAsync(setup);

            var isCreated = await localOrchestrator.CreateTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert);
            Assert.True(isCreated);

            // Ensuring we have a clean new instance
            localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            localOrchestrator.OnTriggerCreating(tca => onCreating++);
            localOrchestrator.OnTriggerCreated(tca => onCreated++);
            localOrchestrator.OnTriggerDropping(tca => onDropping++);
            localOrchestrator.OnTriggerDropped(tca => onDropped++);

            var isDropped = await localOrchestrator.DropTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert);

            Assert.True(isDropped);
            Assert.Equal(0, onCreating);
            Assert.Equal(0, onCreated);
            Assert.Equal(1, onDropping);
            Assert.Equal(1, onDropped);

            // Check 
            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);
                var check = await SqlManagementUtils.GetTriggerAsync(c, null, "Product_insert_trigger", "SalesLT").ConfigureAwait(false);
                Assert.Empty(check.Rows);
                c.Close();
            }

            // try to delete a non existing one
            isDropped = await localOrchestrator.DropTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Update);

            Assert.False(isDropped);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task Trigger_Drop_One_Cancel()
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

            var scopeInfo = await localOrchestrator.GetClientScopeInfoAsync(setup);

            var isCreated = await localOrchestrator.CreateTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert);
            Assert.True(isCreated);

            // Ensuring we have a clean new instance
            localOrchestrator = new LocalOrchestrator(sqlProvider, options);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            localOrchestrator.OnTriggerCreating(tca => onCreating++);

            localOrchestrator.OnTriggerCreated(tca => onCreated++);
            localOrchestrator.OnTriggerDropping(tca =>
            {
                tca.Cancel = true;
                onDropping++;
            });
            localOrchestrator.OnTriggerDropped(tca => onDropped++);

            var isDropped = await localOrchestrator.DropTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert);

            Assert.False(isDropped);
            Assert.Equal(0, onCreating);
            Assert.Equal(0, onCreated);
            Assert.Equal(1, onDropping);
            Assert.Equal(0, onDropped);

            // Check 
            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);
                var check = await SqlManagementUtils.GetTriggerAsync(c, null, "Product_insert_trigger", "SalesLT").ConfigureAwait(false);
                Assert.Single(check.Rows);
                c.Close();
            }

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task Trigger_Create_One_Overwrite()
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

            var scopeInfo = await localOrchestrator.GetClientScopeInfoAsync(setup);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            localOrchestrator.OnTriggerCreating(tca => onCreating++);
            localOrchestrator.OnTriggerCreated(tca => onCreated++);
            localOrchestrator.OnTriggerDropping(tca => onDropping++);
            localOrchestrator.OnTriggerDropped(tca => onDropped++);

            var isCreated = await localOrchestrator.CreateTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert);

            Assert.True(isCreated);
            Assert.Equal(1, onCreating);
            Assert.Equal(1, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            onCreating = 0;
            onCreated = 0;
            onDropping = 0;
            onDropped = 0;

            isCreated = await localOrchestrator.CreateTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert);

            Assert.False(isCreated);
            Assert.Equal(0, onCreating);
            Assert.Equal(0, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            isCreated = await localOrchestrator.CreateTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert, true);

            Assert.True(isCreated);
            Assert.Equal(1, onCreating);
            Assert.Equal(1, onCreated);
            Assert.Equal(1, onDropping);
            Assert.Equal(1, onDropped);


            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task Trigger_Drop_All()
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

            var scopeInfo = await localOrchestrator.GetClientScopeInfoAsync(setup);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            localOrchestrator.OnTriggerCreating(tca => onCreating++);
            localOrchestrator.OnTriggerCreated(tca => onCreated++);
            localOrchestrator.OnTriggerDropping(tca => onDropping++);
            localOrchestrator.OnTriggerDropped(tca => onDropped++);

            var isCreated = await localOrchestrator.CreateTriggersAsync(scopeInfo, "Product", "SalesLT");

            Assert.True(isCreated);
            Assert.Equal(3, onCreating);
            Assert.Equal(3, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);


            onCreating = 0;
            onCreated = 0;
            onDropping = 0;
            onDropped = 0;

            var isDropped = await localOrchestrator.DropTriggersAsync(scopeInfo, "Product", "SalesLT");

            Assert.True(isCreated);
            Assert.Equal(0, onCreating);
            Assert.Equal(0, onCreated);
            Assert.Equal(3, onDropping);
            Assert.Equal(3, onDropped);


            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

    }
}
