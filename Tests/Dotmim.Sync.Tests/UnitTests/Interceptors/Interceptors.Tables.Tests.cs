using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
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
            // Create a new empty client database
            var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(clientProviderType, dbName, true);
            clientProvider = HelperDatabase.GetSyncProvider(clientProviderType, dbName, clientProvider.UseFallbackSchema());

            var localOrchestrator = new LocalOrchestrator(clientProvider, options);
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            var table = new SyncTable("Product", "SalesLT");
            var colID = new SyncColumn("ID", typeof(Guid));
            var colName = new SyncColumn("Name", typeof(string));

            table.Columns.Add(colID);
            table.Columns.Add(colName);
            table.Columns.Add("Number", typeof(int));
            table.PrimaryKeys.Add("ID");

            var schema = new SyncSet();
            schema.Tables.Add(table);

            var scopeInfo = await localOrchestrator.GetScopeInfoAsync();
            scopeInfo.Setup = setup;
            scopeInfo.Schema = schema;
            await localOrchestrator.SaveScopeInfoAsync(scopeInfo);

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


            var isCreated = await localOrchestrator.CreateTableAsync(scopeInfo, table.TableName, table.SchemaName);

            Assert.True(isCreated);
            Assert.True(onCreating);
            Assert.True(onCreated);


            // Check we have a new column in tracking table
            using (var c = new SqlConnection(clientProvider.ConnectionString))
            {
                await c.OpenAsync().ConfigureAwait(false);
                var cols = await SqlManagementUtils.GetColumnsForTableAsync("Product", "SalesLT", c, null).ConfigureAwait(false);
                Assert.Equal(4, cols.Rows.Count);
                Assert.NotNull(cols.Rows.FirstOrDefault(r => r["name"].ToString() == "internal_id"));
                c.Close();
            }
        }

        [Fact]
        public async Task Table_Exists()
        {
            var localOrchestrator = new LocalOrchestrator(clientProvider, options);
            var setup = new SyncSetup(new string[] { "SalesLT.Product", "SalesLT.ProductCategory" });

            var table = new SyncTable("Product", "SalesLT");
            var colID = new SyncColumn("ID", typeof(Guid));
            var colName = new SyncColumn("Name", typeof(string));

            table.Columns.Add(colID);
            table.Columns.Add(colName);
            table.Columns.Add("Number", typeof(int));
            table.PrimaryKeys.Add("ID");

            var schema = new SyncSet();
            schema.Tables.Add(table);   

            var scopeInfo = await localOrchestrator.GetScopeInfoAsync();
            scopeInfo.Setup = setup;
            scopeInfo.Schema = schema;
            await localOrchestrator.SaveScopeInfoAsync(scopeInfo);

            await localOrchestrator.CreateTableAsync(scopeInfo, "Product", "SalesLT");

            var exists = await localOrchestrator.ExistTableAsync(scopeInfo, table.TableName, table.SchemaName).ConfigureAwait(false);
            Assert.True(exists);

            exists = await localOrchestrator.ExistTableAsync(scopeInfo, "ProductCategory", "SalesLT").ConfigureAwait(false);
            Assert.False(exists);
        }

        [Fact]
        public async Task Table_Create_One_Overwrite()
        {
            // Create a new empty client database
            var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(clientProviderType, dbName, true);
            clientProvider = HelperDatabase.GetSyncProvider(clientProviderType, dbName, clientProvider.UseFallbackSchema());

            var localOrchestrator = new LocalOrchestrator(clientProvider, options);
            var setup = new SyncSetup(new string[] { "SalesLT.Product" });

            // Overwrite existing table with this new one
            var table = new SyncTable("Product", "SalesLT");
            var colID = new SyncColumn("ID", typeof(Guid));
            var colName = new SyncColumn("Name", typeof(string));

            table.Columns.Add(colID);
            table.Columns.Add(colName);
            table.Columns.Add("Number", typeof(int));
            table.PrimaryKeys.Add("ID");

            var schema = new SyncSet();
            schema.Tables.Add(table);

            var scopeInfo = await localOrchestrator.GetScopeInfoAsync();
            scopeInfo.Setup = setup;
            scopeInfo.Schema = schema;
            await localOrchestrator.SaveScopeInfoAsync(scopeInfo);

            // Call create a first time to have an existing table
            var isCreated = await localOrchestrator.CreateTableAsync(scopeInfo, table.TableName, table.SchemaName);

            // Ensuring we have a clean new instance
            localOrchestrator = new LocalOrchestrator(clientProvider, options);

            var onCreating = false;
            var onCreated = false;
            var onDropping = false;
            var onDropped = false;

            localOrchestrator.OnTableCreating(ttca => onCreating = true);
            localOrchestrator.OnTableCreated(ttca => onCreated = true);
            localOrchestrator.OnTableDropping(ttca => onDropping = true);
            localOrchestrator.OnTableDropped(ttca => onDropped = true);

            // get scope info again
            scopeInfo = await localOrchestrator.GetScopeInfoAsync();

            isCreated = await localOrchestrator.CreateTableAsync(scopeInfo, table.TableName, table.SchemaName);

            Assert.False(isCreated);
            Assert.False(onDropping);
            Assert.False(onDropped);
            Assert.False(onCreating);
            Assert.False(onCreated);

            onCreating = false;
            onCreated = false;
            onDropping = false;
            onDropped = false;

            isCreated = await localOrchestrator.CreateTableAsync(scopeInfo, table.TableName, table.SchemaName, true);

            Assert.True(isCreated);
            Assert.True(onDropping);
            Assert.True(onDropped);
            Assert.True(onCreating);
            Assert.True(onCreated);
        }

        [Fact]
        public async Task Table_Create_All()
        {
            var localOrchestrator = new LocalOrchestrator(clientProvider, options);
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var setup = new SyncSetup(new string[] { "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product", "Posts" });

            var serverScope = await remoteOrchestrator.GetScopeInfoAsync(setup);

            // new empty db
            var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);

            // Create a new empty client database
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(clientProviderType, dbName, true);
            clientProvider = HelperDatabase.GetSyncProvider(clientProviderType, dbName, clientProvider.UseFallbackSchema());

            localOrchestrator = new LocalOrchestrator(clientProvider, options);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            localOrchestrator.OnTableCreating(ttca => onCreating++);
            localOrchestrator.OnTableCreated(ttca => onCreated++);
            localOrchestrator.OnTableDropping(ttca => onDropping++);
            localOrchestrator.OnTableDropped(ttca => onDropped++);

            var scopeInfo = await localOrchestrator.GetScopeInfoAsync();
            scopeInfo.Setup = serverScope.Setup;
            scopeInfo.Schema = serverScope.Schema;
            await localOrchestrator.SaveScopeInfoAsync(scopeInfo);

            await localOrchestrator.CreateTablesAsync(scopeInfo);

            Assert.Equal(4, onCreating);
            Assert.Equal(4, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            onCreating = 0;
            onCreated = 0;
            onDropping = 0;
            onDropped = 0;

            await localOrchestrator.CreateTablesAsync(scopeInfo);

            Assert.Equal(0, onCreating);
            Assert.Equal(0, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            await localOrchestrator.CreateTablesAsync(scopeInfo, true);

            Assert.Equal(4, onCreating);
            Assert.Equal(4, onCreated);
            Assert.Equal(4, onDropping);
            Assert.Equal(4, onDropped);

            HelperDatabase.DropDatabase(clientProviderType, dbName);
        }
    }
}
