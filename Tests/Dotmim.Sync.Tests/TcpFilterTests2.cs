using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.MariaDB;
using Dotmim.Sync.MySql;
using Dotmim.Sync.PostgreSql;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Fixtures;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
#if NET5_0 || NET6_0 || NET7_0 || NETCOREAPP3_1
using MySqlConnector;
using Npgsql;
#elif NETCOREAPP2_1
using MySql.Data.MySqlClient;
#endif

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Dotmim.Sync.Tests.IntegrationTests2
{

    public class SqlServerTcpFilterTests : TcpFilterTests2<SqlServerFixtureType>
    {
        public SqlServerTcpFilterTests(ITestOutputHelper output, DatabaseFilterServerFixture<SqlServerFixtureType> fixture) : base(output, fixture)
        {
        }
    }
    public class PostgresTcpFilterTests : TcpFilterTests2<PostgresFixtureType>
    {
        public PostgresTcpFilterTests(ITestOutputHelper output, DatabaseFilterServerFixture<PostgresFixtureType> fixture) : base(output, fixture)
        {
        }
    }

    public class MySqlTcpFilterTests : TcpFilterTests2<MySqlFixtureType>
    {
        public MySqlTcpFilterTests(ITestOutputHelper output, DatabaseFilterServerFixture<MySqlFixtureType> fixture) : base(output, fixture)
        {
        }
    }

    public abstract partial class TcpFilterTests2<T> : DatabaseTest<T>, IClassFixture<DatabaseFilterServerFixture<T>>, IDisposable where T : RelationalFixture
    {
        private CoreProvider serverProvider;
        private IEnumerable<CoreProvider> clientsProvider;
        private SyncSetup setup;
        private SyncParameters parameters;

        public new DatabaseFilterServerFixture<T> Fixture { get; }

        public TcpFilterTests2(ITestOutputHelper output, DatabaseFilterServerFixture<T> fixture) : base(output, fixture)
        {
            Fixture = fixture;
            serverProvider = Fixture.GetServerProvider();
            clientsProvider = Fixture.GetClientProviders();
            setup = Fixture.GetSyncSetup();
            parameters = Fixture.GetFilterParameters();
        }


        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public virtual async Task RowsCount(SyncOptions options)
        {
            // Deletes all tables in client
            foreach (var clientProvider in clientsProvider)
                await Fixture.DropAllTablesAsync(clientProvider, true);

            // Get count of rows
            var rowsCount = Fixture.GetDatabaseRowsCount(serverProvider);

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var s = await agent.SynchronizeAsync(setup, parameters);
                var clientRowsCount = Fixture.GetDatabaseRowsCount(clientProvider);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(rowsCount, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(rowsCount, clientRowsCount);

            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task InsertTwoRowsInTwoTablesOnServerSide(SyncOptions options)
        {
            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup, parameters);

            // These 4 lines are part of the filter
            var address1 = await Fixture.AddAddressAsync(serverProvider);
            var address2 = await Fixture.AddAddressAsync(serverProvider);
            var customerAddress1 = await Fixture.AddCustomerAddressAsync(serverProvider, address1.AddressId, AdventureWorksContext.CustomerId1ForFilter);
            var customerAddress2 = await Fixture.AddCustomerAddressAsync(serverProvider, address2.AddressId, AdventureWorksContext.CustomerId1ForFilter);

            // these 2 lines are out of filter, so should not be synced
            var address3 = await Fixture.AddAddressAsync(serverProvider);
            var customerAddress3 = await Fixture.AddCustomerAddressAsync(serverProvider, address3.AddressId, AdventureWorksContext.CustomerId2ForFilter);

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync(setup, parameters);

                Assert.Equal(4, s.TotalChangesDownloadedFromServer);
                Assert.Equal(4, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var clientAddress1 = await Fixture.GetAddressAsync(clientProvider, address1.AddressId);
                Assert.NotNull(clientAddress1);
                var clientAddress2 = await Fixture.GetAddressAsync(clientProvider, address2.AddressId);
                Assert.NotNull(clientAddress2);
                var clientAddress3 = await Fixture.GetAddressAsync(clientProvider, address3.AddressId);
                Assert.Null(clientAddress3);

                var clientCustomerAddress1 = await Fixture.GetCustomerAddressAsync(clientProvider, customerAddress1.AddressId, customerAddress1.CustomerId);
                Assert.NotNull(clientCustomerAddress1);
                var clientCustomerAddress2 = await Fixture.GetCustomerAddressAsync(clientProvider, customerAddress2.AddressId, customerAddress2.CustomerId);
                Assert.NotNull(clientCustomerAddress2);
                var clientCustomerAddress3 = await Fixture.GetCustomerAddressAsync(clientProvider, customerAddress3.AddressId, customerAddress3.CustomerId);
                Assert.Null(clientCustomerAddress3);


            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task InsertTwoRowsInTwoTablesOnClientSide(SyncOptions options)
        {
            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup, parameters);

            // Add one row in each client
            var addressId = 100;
            foreach (var clientProvider in clientsProvider)
            {
                var address1 = await Fixture.AddAddressAsync(clientProvider, addressId++);
                var address2 = await Fixture.AddAddressAsync(clientProvider, addressId++);
                await Fixture.AddCustomerAddressAsync(clientProvider, address1.AddressId, AdventureWorksContext.CustomerId1ForFilter);
                await Fixture.AddCustomerAddressAsync(clientProvider, address2.AddressId, AdventureWorksContext.CustomerId1ForFilter);
            }

            // these 2 lines are out of filter, so should not be synced
            var address3 = await Fixture.AddAddressAsync(serverProvider);
            await Fixture.AddCustomerAddressAsync(serverProvider, address3.AddressId, AdventureWorksContext.CustomerId2ForFilter);


            int download = 0;
            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync(parameters);

                Assert.Equal(download, s.TotalChangesDownloadedFromServer);
                Assert.Equal(4, s.TotalChangesUploadedToServer);
                Assert.Equal(4, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
                download += 4;
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task DeleteTwoRowsInTwoTablesOnClientSide(SyncOptions options)
        {
            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup, parameters);

            // these 2 lines are out of filter, so should not be synced
            var address3 = await Fixture.AddAddressAsync(serverProvider);
            await Fixture.AddCustomerAddressAsync(serverProvider, address3.AddressId, AdventureWorksContext.CustomerId2ForFilter);

            // Add rows in each client
            var sohId = 5000;
            foreach (var clientProvider in clientsProvider)
            {
                var product = await Fixture.AddProductAsync(clientProvider, productCategoryId: "A_ACCESS");
                var soh1 = await Fixture.AddSalesOrderHeaderAsync(clientProvider, AdventureWorksContext.CustomerId1ForFilter, sohId++);
                var soh2 = await Fixture.AddSalesOrderHeaderAsync(clientProvider, AdventureWorksContext.CustomerId1ForFilter, sohId++);
                var sod1 = await Fixture.AddSalesOrderDetailAsync(clientProvider, soh1.SalesOrderId, product.ProductId, sohId++, 10);
                var sod2 = await Fixture.AddSalesOrderDetailAsync(clientProvider, soh2.SalesOrderId, product.ProductId, sohId++, 100);

                // Execute Sync to send these 5 lines
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var s = await agent.SynchronizeAsync(parameters);

                Assert.Equal(5, s.TotalChangesUploadedToServer);
                Assert.Equal(5, s.TotalChangesAppliedOnServer);

                // Delete these rows on client
                await Fixture.DeleteSalesOrderDetailAsync(clientProvider, sod1.SalesOrderDetailId);
                await Fixture.DeleteSalesOrderDetailAsync(clientProvider, sod2.SalesOrderDetailId);
                await Fixture.DeleteSalesOrderHeaderAsync(clientProvider, soh1.SalesOrderId);
                await Fixture.DeleteSalesOrderHeaderAsync(clientProvider, soh2.SalesOrderId);

                s = await agent.SynchronizeAsync(parameters);

                Assert.Equal(4, s.TotalChangesUploadedToServer);
                Assert.Equal(4, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.Equal(2, s.ChangesAppliedOnServer.TableChangesApplied.Count);
                Assert.Equal(SyncRowState.Deleted, s.ChangesAppliedOnServer.TableChangesApplied[0].State);
                Assert.Equal(SyncRowState.Deleted, s.ChangesAppliedOnServer.TableChangesApplied[1].State);
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Snapshots(SyncOptions options)
        {
            // snapshot directory
            var snapshotDirctory = HelperDatabase.GetRandomName();
            var directory = Path.Combine(Environment.CurrentDirectory, snapshotDirctory);

            // Settings the options to enable snapshot
            options.SnapshotsDirectory = directory;
            options.BatchSize = 3000;
            // Disable constraints
            options.DisableConstraintsOnApplyChanges = true;

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            // Adding a row that I will delete after creating snapshot
            var productCategoryTodelete = await Fixture.AddProductCategoryAsync(serverProvider, parentProductCategoryId: "A_ACCESS");

            // Create a snapshot
            await remoteOrchestrator.CreateSnapshotAsync(setup, parameters);

            // Add rows after creating snapshot
            var pc1 = await Fixture.AddProductCategoryAsync(serverProvider, parentProductCategoryId: "A_ACCESS");
            var pc2 = await Fixture.AddProductCategoryAsync(serverProvider);
            var p2 = await Fixture.AddPriceListAsync(serverProvider);
            // no ProductCategoryId, so not synced
            var p1 = await Fixture.AddProductAsync(serverProvider);

            // Delete a row
            await Fixture.DeleteProductCategoryAsync(serverProvider, productCategoryTodelete.ProductCategoryId);

            // Get count of rows
            var rowsCount = Fixture.GetDatabaseRowsCount(serverProvider);

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var s = await agent.SynchronizeAsync(setup, parameters);

                // + 2 because
                // * 1 for the product category to delete, part of snapshot
                // * 1 for the product category to delete, actually deleted
                Assert.Equal(rowsCount + 2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(rowsCount + 2, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.Equal(rowsCount - 4 + 2, s.SnapshotChangesAppliedOnClient.TotalAppliedChanges);
                Assert.Equal(4, s.ChangesAppliedOnClient.TotalAppliedChanges);
                Assert.Equal(4, s.ServerChangesSelected.TotalChangesSelected);

                Assert.Equal(rowsCount, Fixture.GetDatabaseRowsCount(clientProvider));

                // Check rows added or deleted
                var clipc = await Fixture.GetProductCategoryAsync(clientProvider, productCategoryTodelete.ProductCategoryId);
                Assert.Null(clipc);
                var cliPC1 = await Fixture.GetProductCategoryAsync(clientProvider, pc1.ProductCategoryId);
                Assert.NotNull(cliPC1);
                var cliPC2 = await Fixture.GetProductCategoryAsync(clientProvider, pc2.ProductCategoryId);
                Assert.NotNull(cliPC2);
                var cliP1 = await Fixture.GetProductAsync(clientProvider, p1.ProductId);
                Assert.Null(cliP1);
                var cliP2 = await Fixture.GetPriceListAsync(clientProvider, p2.PriceListId);
                Assert.NotNull(cliP2);
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task SnapshotsThenReinitialize(SyncOptions options)
        {
            // snapshot directory
            var snapshotDirctory = HelperDatabase.GetRandomName();
            var directory = Path.Combine(Environment.CurrentDirectory, snapshotDirctory);

            // Settings the options to enable snapshot
            options.SnapshotsDirectory = directory;
            options.BatchSize = 3000;
            // Disable constraints
            options.DisableConstraintsOnApplyChanges = true;

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            // Adding a row that I will delete after creating snapshot
            var productCategoryTodelete = await Fixture.AddProductCategoryAsync(serverProvider);

            // Create a snapshot
            await remoteOrchestrator.CreateSnapshotAsync(setup, parameters);

            // Add rows after creating snapshot
            var pc1 = await Fixture.AddProductCategoryAsync(serverProvider);
            var pc2 = await Fixture.AddProductCategoryAsync(serverProvider);
            var p2 = await Fixture.AddPriceListAsync(serverProvider);
            // not synced as no ProductCategoryId
            var p1 = await Fixture.AddProductAsync(serverProvider);
            // Delete a row
            await Fixture.DeleteProductCategoryAsync(serverProvider, productCategoryTodelete.ProductCategoryId);

            // Execute a sync on all clients
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                await agent.SynchronizeAsync(setup, parameters);

                // Check rows added or deleted
                var clipc = await Fixture.GetProductCategoryAsync(clientProvider, productCategoryTodelete.ProductCategoryId);
                Assert.Null(clipc);
                var cliPC1 = await Fixture.GetProductCategoryAsync(clientProvider, pc1.ProductCategoryId);
                Assert.NotNull(cliPC1);
                var cliPC2 = await Fixture.GetProductCategoryAsync(clientProvider, pc2.ProductCategoryId);
                Assert.NotNull(cliPC2);
                var cliP1 = await Fixture.GetProductAsync(clientProvider, p1.ProductId);
                Assert.Null(cliP1);
                var cliP2 = await Fixture.GetPriceListAsync(clientProvider, p2.PriceListId);
                Assert.NotNull(cliP2);
            }

            // Add one row in each client then ReinitializeWithUpload
            foreach (var clientProvider in clientsProvider)
            {
                var productCategory = await Fixture.AddProductCategoryAsync(clientProvider);

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var s = await agent.SynchronizeAsync(setup, SyncType.ReinitializeWithUpload, parameters);

                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnServer);

                // Check rows added or deleted
                var pc = await Fixture.GetProductCategoryAsync(clientProvider, productCategory.ProductCategoryId);
                Assert.NotNull(pc);
                var clipc = await Fixture.GetProductCategoryAsync(clientProvider, productCategoryTodelete.ProductCategoryId);
                Assert.Null(clipc);
                var cliPC1 = await Fixture.GetProductCategoryAsync(clientProvider, pc1.ProductCategoryId);
                Assert.NotNull(cliPC1);
                var cliPC2 = await Fixture.GetProductCategoryAsync(clientProvider, pc2.ProductCategoryId);
                Assert.NotNull(cliPC2);
                var cliP1 = await Fixture.GetProductAsync(clientProvider, p1.ProductId);
                Assert.Null(cliP1);
                var cliP2 = await Fixture.GetPriceListAsync(clientProvider, p2.PriceListId);
                Assert.NotNull(cliP2);
            }

            // Execute a sync on all clients to be sure all clients have all rows
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync();

            // Get count of rows
            var rowsCount = Fixture.GetDatabaseRowsCount(serverProvider);

            // Execute a sync on all clients to be sure all clients have all rows
            foreach (var clientProvider in clientsProvider)
                Assert.Equal(rowsCount, Fixture.GetDatabaseRowsCount(clientProvider));
        }

        [Fact]
        public async Task ProvisionAndDeprovision()
        {
            foreach (var clientProvider in clientsProvider)
            {
                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);
                var localOrchestrator = new LocalOrchestrator(clientProvider);
                var provision = SyncProvision.ScopeInfo | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;
                var remoteOrchestrator = new RemoteOrchestrator(serverProvider);
                var schema = await remoteOrchestrator.GetSchemaAsync(setup);

                // Read client scope
                var clientScope = await localOrchestrator.GetScopeInfoAsync();

                var serverScope = new ScopeInfo
                {
                    Name = clientScope.Name,
                    Schema = schema,
                    Setup = setup,
                    Version = clientScope.Version
                };

                // Provision the database with all tracking tables, stored procedures, triggers and scope
                clientScope = await localOrchestrator.ProvisionAsync(serverScope, provision);

                //--------------------------
                // ASSERTION
                //--------------------------

                // check if scope table is correctly created
                var scopeInfoTableExists = await localOrchestrator.ExistScopeInfoTableAsync();
                Assert.True(scopeInfoTableExists);

                // get the db manager
                foreach (var setupTable in setup.Tables)
                {
                    Assert.True(await localOrchestrator.ExistTrackingTableAsync(clientScope, setupTable.TableName, setupTable.SchemaName));

                    Assert.True(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Delete));
                    Assert.True(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Insert));
                    Assert.True(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Update));

                    if (clientProviderType == ProviderType.Sql)
                    {
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkTableType));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkDeleteRows));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkUpdateRows));
                    }
                    if (clientProviderType == ProviderType.Sql || clientProviderType == ProviderType.MySql || clientProviderType == ProviderType.MariaDB)
                    {
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteMetadata));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteRow));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.Reset));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChanges));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChanges));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectRow));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.UpdateRow));

                        // We have filters here
                        if (setupTable.TableName == "Address" || setupTable.TableName == "Customer" || setupTable.TableName == "CustomerAddress" ||
                            setupTable.TableName == "SalesOrderDetail" || setupTable.TableName == "SalesOrderHeader" || setupTable.TableName == "Product")
                        {

                            Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChangesWithFilters));
                            Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChangesWithFilters));
                        }
                    }

                }

                // Deprovision the database with all tracking tables, stored procedures, triggers and scope
                await localOrchestrator.DeprovisionAsync(provision);

                // check if scope table is correctly created
                scopeInfoTableExists = await localOrchestrator.ExistScopeInfoTableAsync();
                Assert.False(scopeInfoTableExists);

                // get the db manager
                foreach (var setupTable in setup.Tables)
                {
                    Assert.False(await localOrchestrator.ExistTrackingTableAsync(clientScope, setupTable.TableName, setupTable.SchemaName));

                    Assert.False(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Delete));
                    Assert.False(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Insert));
                    Assert.False(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Update));

                    if (clientProviderType == ProviderType.Sql)
                    {
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkDeleteRows));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkTableType));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkUpdateRows));
                    }
                    if (clientProviderType == ProviderType.Sql || clientProviderType == ProviderType.MySql || clientProviderType == ProviderType.MariaDB)
                    {
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteMetadata));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteRow));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.Reset));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChanges));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChanges));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectRow));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.UpdateRow));

                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChangesWithFilters));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChangesWithFilters));
                    }
                }
            }
        }

        [Fact]
        public async Task SnapshotsShouldNotDeleteFolders()
        {
            // snapshot directory
            var snapshotDirctory = HelperDatabase.GetRandomName();
            var directory = Path.Combine(Environment.CurrentDirectory, snapshotDirctory);

            // Settings the options to enable snapshot
            SyncOptions options = new SyncOptions
            {
                SnapshotsDirectory = directory,
                BatchSize = 3000,
                DisableConstraintsOnApplyChanges = true
            };

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            // getting snapshot directory names
            var (rootDirectory, nameDirectory)
                = await remoteOrchestrator.GetSnapshotDirectoryAsync(parameters).ConfigureAwait(false);

            Assert.False(Directory.Exists(rootDirectory));
            Assert.False(Directory.Exists(Path.Combine(rootDirectory, nameDirectory)));

            // Create a snapshot
            await remoteOrchestrator.CreateSnapshotAsync(setup, parameters);

            Assert.True(Directory.Exists(rootDirectory));
            Assert.True(Directory.Exists(Path.Combine(rootDirectory, nameDirectory)));

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                await agent.SynchronizeAsync(setup, parameters);

                Assert.True(Directory.Exists(rootDirectory));
                Assert.True(Directory.Exists(Path.Combine(rootDirectory, nameDirectory)));
            }
        }

        [Fact]
        public async Task SnapshotsThenAddRowsOnClientsThenReinitialize()
        {
            // snapshot directory
            var snapshotDirctory = HelperDatabase.GetRandomName();
            var directory = Path.Combine(Environment.CurrentDirectory, snapshotDirctory);

            // Settings the options to enable snapshot
            SyncOptions options = new SyncOptions
            {
                SnapshotsDirectory = directory,
                BatchSize = 3000,
                DisableConstraintsOnApplyChanges = true
            };

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            // Create a snapshot
            await remoteOrchestrator.CreateSnapshotAsync(setup, parameters);

            // Execute a sync on all clients
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                await agent.SynchronizeAsync(setup, parameters);
            }

            // Add one row in each client then Sync
            foreach (var clientProvider in clientsProvider)
            {
                await Fixture.AddProductCategoryAsync(clientProvider);
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var s = await agent.SynchronizeAsync(setup, parameters);

                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnServer);
            }

            // Get count of rows
            var rowsCount = Fixture.GetDatabaseRowsCount(serverProvider);

            // Execute a sync on all clients to be sure all clients have all rows
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var s = await agent.SynchronizeAsync(setup, SyncType.Reinitialize, parameters);

                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnServer);
                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(rowsCount, s.TotalChangesAppliedOnClient);

                Assert.Equal(rowsCount, Fixture.GetDatabaseRowsCount(clientProvider));
            }
        }

        [Fact]
        public async Task SynchronizeThenDeprovisionThenAddPrefixes()
        {
            // Deletes all tables in client
            foreach (var clientProvider in clientsProvider)
                await Fixture.DropAllTablesAsync(clientProvider, true);

            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };
            setup = new SyncSetup(new string[] { "Customer" });

            // Filtered columns. 
            setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName" });
            setup.Filters.Add("Customer", "EmployeeID");

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var p = new SyncParameters(("EmployeeID", 1));
                var s = await agent.SynchronizeAsync(setup, p);

                Assert.Equal(2, s.ChangesAppliedOnClient.TotalAppliedChanges);
            }
            
            foreach (var clientProvider in clientsProvider)
            {
                // Deprovision everything
                var localOrchestrator = new LocalOrchestrator(clientProvider, options);
                var clientScope = await localOrchestrator.GetScopeInfoAsync();

                await localOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures
                    | SyncProvision.Triggers
                    | SyncProvision.TrackingTable);

                await localOrchestrator.DeleteScopeInfoAsync(clientScope);
            }

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);
            var serverScope = await remoteOrchestrator.GetScopeInfoAsync();

            await remoteOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures
                | SyncProvision.Triggers
                | SyncProvision.TrackingTable);

            await remoteOrchestrator.DeleteScopeInfoAsync(serverScope);

            // Adding a new table
            setup.Tables.Add("Employee");

            // Adding prefixes
            setup.StoredProceduresPrefix = "sync";
            setup.StoredProceduresSuffix = "sp";
            setup.TrackingTablesPrefix = "track";
            setup.TrackingTablesSuffix = "tbl";
            setup.TriggersPrefix = "trg";
            setup.TriggersSuffix = "tbl";

            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var p = new SyncParameters(("EmployeeID", 1));

                var s = await agent.SynchronizeAsync(setup, SyncType.Reinitialize, p);
                Assert.Equal(5, s.ChangesAppliedOnClient.TotalAppliedChanges);
            }
        }


        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public virtual async Task MultiFiltersParameters(SyncOptions options)
        {
            // Get count of rows for parameter 1
            var rowsCount = Fixture.GetDatabaseRowsCount(serverProvider);

            // Get count of rows for parameter 2
            var rowsCount2 = Fixture.GetDatabaseRowsCount(serverProvider, AdventureWorksContext.CustomerId2ForFilter);

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var s = await agent.SynchronizeAsync(setup, parameters);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(rowsCount, Fixture.GetDatabaseRowsCount(clientProvider));

                // create agent with filtered tables and second parameter
                var parameters2 = new SyncParameters(("CustomerID", AdventureWorksContext.CustomerId2ForFilter));
                agent = new SyncAgent(clientProvider, serverProvider, options);
                s = await agent.SynchronizeAsync(setup, parameters2);

                Assert.Equal(rowsCount2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(rowsCount2, Fixture.GetDatabaseRowsCount(clientProvider, AdventureWorksContext.CustomerId2ForFilter));


            }
        }

    }
}
