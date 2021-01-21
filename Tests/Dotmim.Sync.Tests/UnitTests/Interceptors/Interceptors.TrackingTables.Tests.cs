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
        public async Task TrackingTable_Create_One()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });
            setup.TrackingTablesPrefix = "t_";
            setup.TrackingTablesSuffix = "_t";

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup);

            var onCreating = false;
            var onCreated = false;

            localOrchestrator.OnTrackingTableCreating(ttca =>
            {
                var addingID = $" ALTER TABLE {ttca.TrackingTableName.Schema().Quoted()} ADD internal_id int identity(1,1)";
                ttca.Command.CommandText += addingID;
                onCreating = true;
            });

            localOrchestrator.OnTrackingTableCreated(ttca =>
            {
                onCreated = true;
            });

            await localOrchestrator.CreateTrackingTableAsync(setup.Tables[0]);

            Assert.True(onCreating);
            Assert.True(onCreated);


            // Check we have a new column in tracking table
            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);
                var cols = await SqlManagementUtils.GetColumnsForTableAsync(c, null, "t_Product_t", "SalesLT").ConfigureAwait(false);
                Assert.Equal(7, cols.Rows.Count);
                Assert.NotNull(cols.Rows.FirstOrDefault(r => r["name"].ToString() == "internal_id"));
                c.Close();
            }

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task TrackingTable_Exists()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product", "SalesLT.ProductCategory" });
            setup.TrackingTablesPrefix = "t_";
            setup.TrackingTablesSuffix = "_t";

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup);

            await localOrchestrator.CreateTrackingTableAsync(setup.Tables[0]);

            var exists = await localOrchestrator.ExistTrackingTableAsync(setup.Tables[0]);
            Assert.True(exists);
            exists = await localOrchestrator.ExistTrackingTableAsync(setup.Tables[1]);
            Assert.False(exists);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }


        [Fact]
        public async Task TrackingTable_Create_All()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product", "dbo.Sql", "Posts" });
            setup.TrackingTablesPrefix = "t_";
            setup.TrackingTablesSuffix = "_t";

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            localOrchestrator.OnTrackingTableCreating(ttca => onCreating++);
            localOrchestrator.OnTrackingTableCreated(ttca => onCreated++);
            localOrchestrator.OnTrackingTableDropping(ttca => onDropping++);
            localOrchestrator.OnTrackingTableDropped(ttca => onDropped++);

            await localOrchestrator.CreateTrackingTablesAsync();

            Assert.Equal(5, onCreating);
            Assert.Equal(5, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            onCreating = 0;
            onCreated = 0;
            onDropping = 0;
            onDropped = 0;

            await localOrchestrator.CreateTrackingTablesAsync();

            Assert.Equal(0, onCreating);
            Assert.Equal(0, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            onCreating = 0;
            onCreated = 0;
            onDropping = 0;
            onDropped = 0;

            await localOrchestrator.CreateTrackingTablesAsync(true);

            Assert.Equal(5, onCreating);
            Assert.Equal(5, onCreated);
            Assert.Equal(5, onDropping);
            Assert.Equal(5, onDropped);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task TrackingTable_Drop_One()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });
            setup.TrackingTablesPrefix = "t_";
            setup.TrackingTablesSuffix = "_t";

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup);

            var onDropping = false;
            var onDropped = false;

            localOrchestrator.OnTrackingTableDropping(ttca =>
            {
                onDropping = true;
            });

            localOrchestrator.OnTrackingTableDropped(ttca =>
            {
                onDropped = true;
            });

            await localOrchestrator.CreateTrackingTableAsync(setup.Tables[0]);
            await localOrchestrator.DropTrackingTableAsync(setup.Tables[0]);

            Assert.True(onDropping);
            Assert.True(onDropped);


            // Check we have a new column in tracking table
            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);

                var table = await SqlManagementUtils.GetTableAsync(c, null, "t_Product_t", "SalesLT").ConfigureAwait(false);

                Assert.Empty(table.Rows);

                c.Close();
            }

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task TrackingTable_Drop_One_Cancel()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });
            setup.TrackingTablesPrefix = "t_";
            setup.TrackingTablesSuffix = "_t";

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup);

            var onDropping = false;
            var onDropped = false;

            localOrchestrator.OnTrackingTableDropping(ttca =>
            {
                ttca.Cancel = true;
                onDropping = true;
            });

            localOrchestrator.OnTrackingTableDropped(ttca =>
            {
                onDropped = true;
            });

            await localOrchestrator.CreateTrackingTableAsync(setup.Tables[0]);
            await localOrchestrator.DropTrackingTableAsync(setup.Tables[0]);

            Assert.True(onDropping);
            Assert.False(onDropped);

            // Check we have a new column in tracking table
            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);

                var table = await SqlManagementUtils.GetTableAsync(c, null, "t_Product_t", "SalesLT").ConfigureAwait(false);

                Assert.NotEmpty(table.Rows);

                c.Close();
            }

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task TrackingTable_Drop_All()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product", "dbo.Sql", "Posts" });
            setup.TrackingTablesPrefix = "t_";
            setup.TrackingTablesSuffix = "_t";

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup);

            var onDropping = 0;
            var onDropped = 0;

            localOrchestrator.OnTrackingTableDropping(ttca => onDropping++);
            localOrchestrator.OnTrackingTableDropped(ttca => onDropped++);

            await localOrchestrator.CreateTrackingTablesAsync();
            await localOrchestrator.DropTrackingTablesAsync();


            Assert.Equal(5, onDropping);
            Assert.Equal(5, onDropped);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

    }
}
