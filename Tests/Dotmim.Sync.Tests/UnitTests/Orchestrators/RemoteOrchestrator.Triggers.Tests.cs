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
    public partial class RemoteOrchestratorTests
    {
        [Fact]
        public async Task RemoteOrchestrator_Provision_ShouldCreate_Triggers()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);
            // Create default table
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var scopeName = "scope";

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" })
            {
                TrackingTablesSuffix = "sync",
                TrackingTablesPrefix = "trck",
                TriggersPrefix = "trg_",
                TriggersSuffix = "_trg"
            };

            // trackign table name is composed with prefix and suffix from setup
            var triggerDelete = $"{setup.TriggersPrefix}Product{setup.TriggersSuffix}_delete_trigger";
            var triggerInsert = $"{setup.TriggersPrefix}Product{setup.TriggersSuffix}_insert_trigger";
            var triggerUpdate = $"{setup.TriggersPrefix}Product{setup.TriggersSuffix}_update_trigger";

            var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
            var scopeInfo = await remoteOrchestrator.GetServerScopeInfoAsync(scopeName, setup);

            // Needs the tracking table to be able to create triggers
            var provision = SyncProvision.TrackingTable | SyncProvision.Triggers;

            await remoteOrchestrator.ProvisionAsync(scopeInfo, provision);

            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);

                var trigDel = await SqlManagementUtils.GetTriggerAsync(c, null, triggerDelete, "SalesLT");
                Assert.Equal(triggerDelete, trigDel.Rows[0]["Name"].ToString());

                var trigIns = await SqlManagementUtils.GetTriggerAsync(c, null, triggerInsert, "SalesLT");
                Assert.Equal(triggerInsert, trigIns.Rows[0]["Name"].ToString());

                var trigUdate = await SqlManagementUtils.GetTriggerAsync(c, null, triggerUpdate, "SalesLT");
                Assert.Equal(triggerUpdate, trigUdate.Rows[0]["Name"].ToString());

                c.Close();
            }

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task RemoteOrchestrator_Trigger_ShouldCreate()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            // Create default table
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var scopeName = "scope";

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" })
            {
                TriggersPrefix = "trg_",
                TriggersSuffix = "_trg"
            };

            var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
            var scopeInfo = await remoteOrchestrator.GetServerScopeInfoAsync(scopeName, setup);

            var triggerInsert = $"{setup.TriggersPrefix}Product{setup.TriggersSuffix}_insert_trigger";
            await remoteOrchestrator.CreateTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert, false);

            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);

                var trigIns = await SqlManagementUtils.GetTriggerAsync(c, null, triggerInsert, "SalesLT");
                Assert.Equal(triggerInsert, trigIns.Rows[0]["Name"].ToString());

                c.Close();
            }

            var triggerUpdate = $"{setup.TriggersPrefix}Product{setup.TriggersSuffix}_update_trigger";
            await remoteOrchestrator.CreateTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Update, false);

            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);

                var trig = await SqlManagementUtils.GetTriggerAsync(c, null, triggerUpdate, "SalesLT");
                Assert.Equal(triggerUpdate, trig.Rows[0]["Name"].ToString());

                c.Close();
            }

            var triggerDelete = $"{setup.TriggersPrefix}Product{setup.TriggersSuffix}_delete_trigger";
            await remoteOrchestrator.CreateTriggerAsync(scopeInfo,"Product", "SalesLT", DbTriggerType.Delete, false);

            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);

                var trig = await SqlManagementUtils.GetTriggerAsync(c, null, triggerDelete, "SalesLT");
                Assert.Equal(triggerDelete, trig.Rows[0]["Name"].ToString());

                c.Close();
            }

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task RemoteOrchestrator_Trigger_ShouldOverwrite()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            // Create default table
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var scopeName = "scope";

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" })
            {
                TriggersPrefix = "trg_",
                TriggersSuffix = "_trg"
            };

            var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
            var scopeInfo = await remoteOrchestrator.GetServerScopeInfoAsync(scopeName, setup);

            var triggerInsert = $"{setup.TriggersPrefix}Product{setup.TriggersSuffix}_insert_trigger";
            await remoteOrchestrator.CreateTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert, false);

            var assertOverWritten = false;
            remoteOrchestrator.OnTriggerCreating(args =>
            {
               assertOverWritten = true;
            });

            await remoteOrchestrator.CreateTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert, true);

            Assert.True(assertOverWritten);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task RemoteOrchestrator_Trigger_ShouldNotOverwrite()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            // Create default table
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var scopeName = "scope";

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" })
            {
                TriggersPrefix = "trg_",
                TriggersSuffix = "_trg"
            };

            var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
            var scopeInfo = await remoteOrchestrator.GetServerScopeInfoAsync(scopeName, setup);


            var triggerInsert = $"{setup.TriggersPrefix}Product{setup.TriggersSuffix}_insert_trigger";
            await remoteOrchestrator.CreateTriggerAsync(scopeInfo,"Product", "SalesLT", DbTriggerType.Insert, false);


            var assertOverWritten = false;
            remoteOrchestrator.OnTriggerCreating(args =>
            {
                assertOverWritten = true;
            });

            await remoteOrchestrator.CreateTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert, false);

            Assert.False(assertOverWritten);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task RemoteOrchestrator_Trigger_Exists()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            // Create default table
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var scopeName = "scope";

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" })
            {
                TriggersPrefix = "trg_",
                TriggersSuffix = "_trg"
            };

            var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
            var scopeInfo = await remoteOrchestrator.GetServerScopeInfoAsync(scopeName, setup);

            await remoteOrchestrator.CreateTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert, false);

            var insertExists = await remoteOrchestrator.ExistTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert);
            var updateExists = await remoteOrchestrator.ExistTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Update);

            Assert.True(insertExists);
            Assert.False(updateExists);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task RemoteOrchestrator_Triggers_ShouldCreate()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            // Create default table
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var scopeName = "scope";

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            setup.TriggersPrefix = "trg_";
            setup.TriggersSuffix = "_trg";

            var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
            var scopeInfo = await remoteOrchestrator.GetServerScopeInfoAsync(scopeName, setup);

            var triggerInsert = $"{setup.TriggersPrefix}Product{setup.TriggersSuffix}_insert_trigger";
            var triggerUpdate = $"{setup.TriggersPrefix}Product{setup.TriggersSuffix}_update_trigger";
            var triggerDelete = $"{setup.TriggersPrefix}Product{setup.TriggersSuffix}_delete_trigger";

            await remoteOrchestrator.CreateTriggersAsync(scopeInfo, "Product", "SalesLT");

            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);

                var trigIns = await SqlManagementUtils.GetTriggerAsync(c, null, triggerInsert, "SalesLT");
                Assert.Equal(triggerInsert, trigIns.Rows[0]["Name"].ToString());

                c.Close();
            }

            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);

                var trig = await SqlManagementUtils.GetTriggerAsync(c, null, triggerUpdate, "SalesLT");
                Assert.Equal(triggerUpdate, trig.Rows[0]["Name"].ToString());

                c.Close();
            }

            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);

                var trig = await SqlManagementUtils.GetTriggerAsync(c, null, triggerDelete, "SalesLT");
                Assert.Equal(triggerDelete, trig.Rows[0]["Name"].ToString());

                c.Close();
            }

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }



    }
}
