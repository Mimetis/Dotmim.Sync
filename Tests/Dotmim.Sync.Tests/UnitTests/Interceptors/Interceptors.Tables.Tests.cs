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
        public async Task Table_Create_One()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            var table = new SyncTable("Product", "SalesLT");
            var colID = new SyncColumn("ID", typeof(Guid));
            var colName = new SyncColumn("Name", typeof(string));

            table.Columns.Add(colID);
            table.Columns.Add(colName);
            table.Columns.Add("Number", typeof(int));
            table.PrimaryKeys.Add("ID");

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup);

            var onCreating = false;
            var onCreated = false;

            localOrchestrator.OnTableCreating(ttca =>
            {
                var addingID = Environment.NewLine + $"ALTER TABLE {ttca.TableName.Schema().Quoted()} ADD internal_id int identity(1,1)";
                ttca.Command.CommandText += addingID;
                onCreating = true;
            });

            localOrchestrator.OnTableCreated(ttca =>
            {
                onCreated = true;
            });

            var isCreated = await localOrchestrator.CreateTableAsync(table);

            Assert.True(isCreated);
            Assert.True(onCreating);
            Assert.True(onCreated);


            // Check we have a new column in tracking table
            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);
                var cols = await SqlManagementUtils.GetColumnsForTableAsync(c, null, "Product", "SalesLT").ConfigureAwait(false);
                Assert.Equal(4, cols.Rows.Count);
                Assert.NotNull(cols.Rows.FirstOrDefault(r => r["name"].ToString() == "internal_id"));
                c.Close();
            }

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task Table_Exists()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product", "SalesLT.ProductCategory" });

            var table = new SyncTable("Product", "SalesLT");
            var colID = new SyncColumn("ID", typeof(Guid));
            var colName = new SyncColumn("Name", typeof(string));

            table.Columns.Add(colID);
            table.Columns.Add(colName);
            table.Columns.Add("Number", typeof(int));
            table.PrimaryKeys.Add("ID");

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup);

            await localOrchestrator.CreateTableAsync(table);

            var exists = await localOrchestrator.ExistTableAsync(setup.Tables[0]).ConfigureAwait(false);
            Assert.True(exists);
            exists = await localOrchestrator.ExistTableAsync(setup.Tables[1]).ConfigureAwait(false);
            Assert.False(exists);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task Table_Create_One_Overwrite()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            // Overwrite existing table with this new one
            var table = new SyncTable("Product", "SalesLT");
            var colID = new SyncColumn("ID", typeof(Guid));
            var colName = new SyncColumn("Name", typeof(string));

            table.Columns.Add(colID);
            table.Columns.Add(colName);
            table.Columns.Add("Number", typeof(int));
            table.PrimaryKeys.Add("ID");

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup);

            // Call create a first time to have an existing table
            var isCreated = await localOrchestrator.CreateTableAsync(table);

            // Ensuring we have a clean new instance
            localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup);

            var onCreating = false;
            var onCreated = false;
            var onDropping = false;
            var onDropped = false;

            localOrchestrator.OnTableCreating(ttca => onCreating = true);
            localOrchestrator.OnTableCreated(ttca => onCreated = true);
            localOrchestrator.OnTableDropping(ttca => onDropping = true);
            localOrchestrator.OnTableDropped(ttca => onDropped = true);

            isCreated = await localOrchestrator.CreateTableAsync(table);

            Assert.False(isCreated);
            Assert.False(onDropping);
            Assert.False(onDropped);
            Assert.False(onCreating);
            Assert.False(onCreated);

            onCreating = false;
            onCreated = false;
            onDropping = false;
            onDropped = false;

            isCreated = await localOrchestrator.CreateTableAsync(table, true);

            Assert.True(isCreated);
            Assert.True(onDropping);
            Assert.True(onDropped);
            Assert.True(onCreating);
            Assert.True(onCreated);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task Table_Create_All()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product", "Posts" });

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup);

            var schema = await localOrchestrator.GetSchemaAsync();

            // new empty db
            dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            sqlProvider = new SqlSyncProvider(cs);

            localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup);


            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            localOrchestrator.OnTableCreating(ttca => onCreating++);
            localOrchestrator.OnTableCreated(ttca => onCreated++);
            localOrchestrator.OnTableDropping(ttca => onDropping++);
            localOrchestrator.OnTableDropped(ttca => onDropped++);

            await localOrchestrator.CreateTablesAsync(schema);

            Assert.Equal(4, onCreating);
            Assert.Equal(4, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            onCreating = 0;
            onCreated = 0;
            onDropping = 0;
            onDropped = 0;

            await localOrchestrator.CreateTablesAsync(schema);

            Assert.Equal(0, onCreating);
            Assert.Equal(0, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            await localOrchestrator.CreateTablesAsync(schema, true);

            Assert.Equal(4, onCreating);
            Assert.Equal(4, onCreated);
            Assert.Equal(4, onDropping);
            Assert.Equal(4, onDropped);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task Table_Drop_One()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            // Overwrite existing table with this new one
            var table = new SyncTable("Product", "SalesLT");
            var colID = new SyncColumn("ID", typeof(Guid));
            var colName = new SyncColumn("Name", typeof(string));

            table.Columns.Add(colID);
            table.Columns.Add(colName);
            table.Columns.Add("Number", typeof(int));
            table.PrimaryKeys.Add("ID");

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup);

            // Call create a first time to have an existing table
            var isCreated = await localOrchestrator.CreateTableAsync(table);

            Assert.True(isCreated);

            // Ensuring we have a clean new instance
            localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup);

            var onCreating = false;
            var onCreated = false;
            var onDropping = false;
            var onDropped = false;

            localOrchestrator.OnTableCreating(ttca => onCreating = true);
            localOrchestrator.OnTableCreated(ttca => onCreated = true);
            localOrchestrator.OnTableDropping(ttca => onDropping = true);
            localOrchestrator.OnTableDropped(ttca => onDropped = true);

            var isDropped = await localOrchestrator.DropTableAsync(setup.Tables[0]);

            Assert.True(isDropped);
            Assert.True(onDropping);
            Assert.True(onDropped);
            Assert.False(onCreating);
            Assert.False(onCreated);

            // Check we have the correct table ovewritten
            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);
                var stable = await SqlManagementUtils.GetTableAsync(c, null, "Product", "SalesLT").ConfigureAwait(false);
                Assert.Empty(stable.Rows);
                c.Close();
            }

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task Table_Drop_One_Cancel()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            // Overwrite existing table with this new one
            var table = new SyncTable("Product", "SalesLT");
            var colID = new SyncColumn("ID", typeof(Guid));
            var colName = new SyncColumn("Name", typeof(string));

            table.Columns.Add(colID);
            table.Columns.Add(colName);
            table.Columns.Add("Number", typeof(int));
            table.PrimaryKeys.Add("ID");

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup);

            // Call create a first time to have an existing table
            var isCreated = await localOrchestrator.CreateTableAsync(table);

            Assert.True(isCreated);

            // Ensuring we have a clean new instance
            localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup);

            var onCreating = false;
            var onCreated = false;
            var onDropping = false;
            var onDropped = false;

            localOrchestrator.OnTableCreating(ttca => onCreating = true);
            localOrchestrator.OnTableCreated(ttca => onCreated = true);
            localOrchestrator.OnTableDropped(ttca => onDropped = true);

            localOrchestrator.OnTableDropping(ttca =>
            {
                ttca.Cancel = true;
                onDropping = true;
            });

            var isDropped = await localOrchestrator.DropTableAsync(setup.Tables[0]);

            Assert.True(onDropping);

            Assert.False(isDropped);
            Assert.False(onDropped);
            Assert.False(onCreating);
            Assert.False(onCreated);

            using (var c = new SqlConnection(cs))
            {
                await c.OpenAsync().ConfigureAwait(false);
                var stable = await SqlManagementUtils.GetTableAsync(c, null, "Product", "SalesLT").ConfigureAwait(false);
                Assert.Single(stable.Rows);
                c.Close();
            }

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }

        [Fact]
        public async Task Table_Drop_All()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);

            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);

            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, false);
            await ctx.Database.EnsureCreatedAsync();

            var options = new SyncOptions();
            var setup = new SyncSetup(this.Tables);

            var localOrchestrator = new LocalOrchestrator(sqlProvider, options, setup);

            var onDropping = 0;
            var onDropped = 0;

            localOrchestrator.OnTableDropping(ttca => onDropping++);
            localOrchestrator.OnTableDropped(ttca => onDropped++);

            await localOrchestrator.DropTablesAsync();

            Assert.Equal(this.Tables.Length, onDropping);
            Assert.Equal(this.Tables.Length, onDropped);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }
    }
}
