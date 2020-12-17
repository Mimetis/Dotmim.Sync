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
    public partial class BaseOrchestratorTests
    {
       
        [Fact]
        public async Task BaseOrchestrator_StoredProcedure_ShouldCreate()
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

            setup.StoredProceduresPrefix = "sp_";
            setup.StoredProceduresSuffix = "_sp";

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup, scopeName);

            var storedProcedureSelectChanges = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_changes";
            
            await localOrchestrator.CreateStoredProcedureAsync(setup.Tables["Product", "SalesLT"], DbStoredProcedureType.SelectChanges, false);

            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);

                var trigIns = await SqlManagementUtils.ProcedureExistsAsync(c, null, storedProcedureSelectChanges);
                Assert.True(trigIns);

                c.Close();
            }


            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }


        [Fact]
        public async Task BaseOrchestrator_StoredProcedure_ShouldOverwrite()
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

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup, scopeName);

            var triggerInsert = $"{setup.TriggersPrefix}Product{setup.TriggersSuffix}_insert_trigger";
            await localOrchestrator.CreateTriggerAsync(setup.Tables["Product", "SalesLT"], DbTriggerType.Insert, false);

            var assertOverWritten = false;
            localOrchestrator.On(new Action<TriggerCreatingArgs>(args =>
            {
               assertOverWritten = true;
            }));

            await localOrchestrator.CreateTriggerAsync(setup.Tables["Product", "SalesLT"], DbTriggerType.Insert, true);

            Assert.True(assertOverWritten);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }


        [Fact]
        public async Task BaseOrchestrator_StoredProcedure_ShouldNotOverwrite()
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

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup, scopeName);

            var triggerInsert = $"{setup.TriggersPrefix}Product{setup.TriggersSuffix}_insert_trigger";
            await localOrchestrator.CreateTriggerAsync(setup.Tables["Product", "SalesLT"], DbTriggerType.Insert, false);


            var assertOverWritten = false;
            localOrchestrator.On(new Action<TriggerCreatingArgs>(args =>
            {
                assertOverWritten = true;
            }));

            await localOrchestrator.CreateTriggerAsync(setup.Tables["Product", "SalesLT"], DbTriggerType.Insert, false);

            Assert.False(assertOverWritten);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task BaseOrchestrator_StoredProcedure_Exists()
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

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup, scopeName);

            var productTable = setup.Tables["Product", "SalesLT"];

            await localOrchestrator.CreateTriggerAsync(productTable, DbTriggerType.Insert, false);

            var insertExists = await localOrchestrator.ExistTriggerAsync(productTable, DbTriggerType.Insert);
            var updateExists = await localOrchestrator.ExistTriggerAsync(productTable, DbTriggerType.Update);

            Assert.True(insertExists);
            Assert.False(updateExists);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task BaseOrchestrator_StoredProcedures_ShouldCreate()
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

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup, scopeName);

            var triggerInsert = $"{setup.TriggersPrefix}Product{setup.TriggersSuffix}_insert_trigger";
            var triggerUpdate = $"{setup.TriggersPrefix}Product{setup.TriggersSuffix}_update_trigger";
            var triggerDelete = $"{setup.TriggersPrefix}Product{setup.TriggersSuffix}_delete_trigger";

            await localOrchestrator.CreateTriggersAsync(setup.Tables["Product", "SalesLT"]);

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
