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
#if NET6_0 || NET8_0 
using MySqlConnector;
using Npgsql;
#elif NETCOREAPP3_1
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

namespace Dotmim.Sync.Tests.IntegrationTests
{

    public abstract partial class TcpFilterTests : DatabaseTest, IClassFixture<DatabaseServerFixture>, IDisposable
    {
        internal CoreProvider serverProvider;
        internal IEnumerable<CoreProvider> clientsProvider;
        internal SyncSetup setup;
        internal SyncParameters parameters;

        public new DatabaseServerFixture Fixture { get; }

        public TcpFilterTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
            serverProvider = GetServerProvider();
            clientsProvider = GetClientProviders();
            setup = GetFilteredSetup();
            parameters = GetFilterParameters();
        }


        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public virtual async Task RowsCount(SyncOptions options)
        {
            // Deletes all tables in client
            foreach (var clientProvider in clientsProvider)
                await clientProvider.DropAllTablesAsync(true);

            // Get count of rows
            var rowsCount = serverProvider.GetDatabaseFilteredRowsCount();

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var s = await agent.SynchronizeAsync(setup, parameters);
                var clientRowsCount = clientProvider.GetDatabaseFilteredRowsCount();

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
            var address1 = await serverProvider.AddAddressAsync();
            var address2 = await serverProvider.AddAddressAsync();
            var customerAddress1 = await serverProvider.AddCustomerAddressAsync(address1.AddressId, AdventureWorksContext.CustomerId1ForFilter);
            var customerAddress2 = await serverProvider.AddCustomerAddressAsync(address2.AddressId, AdventureWorksContext.CustomerId1ForFilter);

            // these 2 lines are out of filter, so should not be synced
            var address3 = await serverProvider.AddAddressAsync();
            var customerAddress3 = await serverProvider.AddCustomerAddressAsync(address3.AddressId, AdventureWorksContext.CustomerId2ForFilter);

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

                var clientAddress1 = await clientProvider.GetAddressAsync(address1.AddressId);
                Assert.NotNull(clientAddress1);
                var clientAddress2 = await clientProvider.GetAddressAsync(address2.AddressId);
                Assert.NotNull(clientAddress2);
                var clientAddress3 = await clientProvider.GetAddressAsync(address3.AddressId);
                Assert.Null(clientAddress3);

                var clientCustomerAddress1 = await clientProvider.GetCustomerAddressAsync(customerAddress1.AddressId, customerAddress1.CustomerId);
                Assert.NotNull(clientCustomerAddress1);
                var clientCustomerAddress2 = await clientProvider.GetCustomerAddressAsync(customerAddress2.AddressId, customerAddress2.CustomerId);
                Assert.NotNull(clientCustomerAddress2);
                var clientCustomerAddress3 = await clientProvider.GetCustomerAddressAsync(customerAddress3.AddressId, customerAddress3.CustomerId);
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
                var address1 = await clientProvider.AddAddressAsync(addressId++);
                var address2 = await clientProvider.AddAddressAsync(addressId++);
                await clientProvider.AddCustomerAddressAsync(address1.AddressId, AdventureWorksContext.CustomerId1ForFilter);
                await clientProvider.AddCustomerAddressAsync(address2.AddressId, AdventureWorksContext.CustomerId1ForFilter);
            }

            // these 2 lines are out of filter, so should not be synced
            var address3 = await serverProvider.AddAddressAsync();
            await serverProvider.AddCustomerAddressAsync(address3.AddressId, AdventureWorksContext.CustomerId2ForFilter);


            var download = 0;
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
            var address3 = await serverProvider.AddAddressAsync();
            await serverProvider.AddCustomerAddressAsync(address3.AddressId, AdventureWorksContext.CustomerId2ForFilter);

            // Add rows in each client
            var sohId = 5000;
            foreach (var clientProvider in clientsProvider)
            {
                var product = await clientProvider.AddProductAsync(productCategoryId: "A_ACCESS");
                var soh1 = await clientProvider.AddSalesOrderHeaderAsync(AdventureWorksContext.CustomerId1ForFilter, sohId++);
                var soh2 = await clientProvider.AddSalesOrderHeaderAsync(AdventureWorksContext.CustomerId1ForFilter, sohId++);
                var sod1 = await clientProvider.AddSalesOrderDetailAsync(soh1.SalesOrderId, product.ProductId, sohId++, 10);
                var sod2 = await clientProvider.AddSalesOrderDetailAsync(soh2.SalesOrderId, product.ProductId, sohId++, 100);

                // Execute Sync to send these 5 lines
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var s = await agent.SynchronizeAsync(parameters);

                Assert.Equal(5, s.TotalChangesUploadedToServer);
                Assert.Equal(5, s.TotalChangesAppliedOnServer);

                // Delete these rows on client
                await clientProvider.DeleteSalesOrderDetailAsync(sod1.SalesOrderDetailId);
                await clientProvider.DeleteSalesOrderDetailAsync(sod2.SalesOrderDetailId);
                await clientProvider.DeleteSalesOrderHeaderAsync(soh1.SalesOrderId);
                await clientProvider.DeleteSalesOrderHeaderAsync(soh2.SalesOrderId);

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
        public async Task DeleteOneRowInOneTableOnServerSide(SyncOptions options)
        {
            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup, parameters);

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                // init everything
                await agent.SynchronizeAsync(setup, parameters);

                // Add product category
                var productCategory = await serverProvider.AddProductCategoryAsync();
                // add product part of the filter
                var product = await serverProvider.AddProductAsync(productCategoryId: productCategory.ProductCategoryId);
                // sync this category on each client to be able to delete it after

                // send it to client
                var s = await agent.SynchronizeAsync(setup, parameters);
                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(2, s.TotalChangesAppliedOnClient);

                // delete product on server
                await serverProvider.DeleteProductAsync(product.ProductId);

                // Sync again and see if it's downloaded and deleted
                s = await agent.SynchronizeAsync(setup, parameters);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Single(s.ChangesAppliedOnClient.TableChangesApplied);
                Assert.Equal(1, s.ChangesAppliedOnClient.TableChangesApplied[0].Applied);
                Assert.Equal(SyncRowState.Deleted, s.ChangesAppliedOnClient.TableChangesApplied[0].State);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
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
            var productCategoryTodelete = await serverProvider.AddProductCategoryAsync(parentProductCategoryId: "A_ACCESS");

            // Create a snapshot
            await remoteOrchestrator.CreateSnapshotAsync(setup, parameters);

            // Add rows after creating snapshot
            var pc1 = await serverProvider.AddProductCategoryAsync(parentProductCategoryId: "A_ACCESS");
            var pc2 = await serverProvider.AddProductCategoryAsync();
            var p2 = await serverProvider.AddPriceListAsync();
            // no ProductCategoryId, so not synced
            var p1 = await serverProvider.AddProductAsync();

            // Delete a row
            await serverProvider.DeleteProductCategoryAsync(productCategoryTodelete.ProductCategoryId);

            // Get count of rows
            var rowsCount = serverProvider.GetDatabaseFilteredRowsCount();

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

                Assert.Equal(rowsCount, clientProvider.GetDatabaseFilteredRowsCount());

                // Check rows added or deleted
                var clipc = await clientProvider.GetProductCategoryAsync(productCategoryTodelete.ProductCategoryId);
                Assert.Null(clipc);
                var cliPC1 = await clientProvider.GetProductCategoryAsync(pc1.ProductCategoryId);
                Assert.NotNull(cliPC1);
                var cliPC2 = await clientProvider.GetProductCategoryAsync(pc2.ProductCategoryId);
                Assert.NotNull(cliPC2);
                var cliP1 = await clientProvider.GetProductAsync(p1.ProductId);
                Assert.Null(cliP1);
                var cliP2 = await clientProvider.GetPriceListAsync(p2.PriceListId);
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
            var productCategoryTodelete = await serverProvider.AddProductCategoryAsync();

            // Create a snapshot
            await remoteOrchestrator.CreateSnapshotAsync(setup, parameters);

            // Add rows after creating snapshot
            var pc1 = await serverProvider.AddProductCategoryAsync();
            var pc2 = await serverProvider.AddProductCategoryAsync();
            var p2 = await serverProvider.AddPriceListAsync();
            // not synced as no ProductCategoryId
            var p1 = await serverProvider.AddProductAsync();
            // Delete a row
            await serverProvider.DeleteProductCategoryAsync(productCategoryTodelete.ProductCategoryId);

            // Execute a sync on all clients
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                await agent.SynchronizeAsync(setup, parameters);

                // Check rows added or deleted
                var clipc = await clientProvider.GetProductCategoryAsync(productCategoryTodelete.ProductCategoryId);
                Assert.Null(clipc);
                var cliPC1 = await clientProvider.GetProductCategoryAsync(pc1.ProductCategoryId);
                Assert.NotNull(cliPC1);
                var cliPC2 = await clientProvider.GetProductCategoryAsync(pc2.ProductCategoryId);
                Assert.NotNull(cliPC2);
                var cliP1 = await clientProvider.GetProductAsync(p1.ProductId);
                Assert.Null(cliP1);
                var cliP2 = await clientProvider.GetPriceListAsync(p2.PriceListId);
                Assert.NotNull(cliP2);
            }

            // Add one row in each client then ReinitializeWithUpload
            foreach (var clientProvider in clientsProvider)
            {
                var productCategory = await clientProvider.AddProductCategoryAsync();

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var s = await agent.SynchronizeAsync(setup, SyncType.ReinitializeWithUpload, parameters);

                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnServer);

                // Check rows added or deleted
                var pc = await clientProvider.GetProductCategoryAsync(productCategory.ProductCategoryId);
                Assert.NotNull(pc);
                var clipc = await clientProvider.GetProductCategoryAsync(productCategoryTodelete.ProductCategoryId);
                Assert.Null(clipc);
                var cliPC1 = await clientProvider.GetProductCategoryAsync(pc1.ProductCategoryId);
                Assert.NotNull(cliPC1);
                var cliPC2 = await clientProvider.GetProductCategoryAsync(pc2.ProductCategoryId);
                Assert.NotNull(cliPC2);
                var cliP1 = await clientProvider.GetProductAsync(p1.ProductId);
                Assert.Null(cliP1);
                var cliP2 = await clientProvider.GetPriceListAsync(p2.PriceListId);
                Assert.NotNull(cliP2);
            }

            // Execute a sync on all clients to be sure all clients have all rows
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync();

            // Get count of rows
            var rowsCount = serverProvider.GetDatabaseFilteredRowsCount();

            // Execute a sync on all clients to be sure all clients have all rows
            foreach (var clientProvider in clientsProvider)
                Assert.Equal(rowsCount, clientProvider.GetDatabaseFilteredRowsCount());
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
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChanges));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChanges));

                        // We have filters here
                        if (setupTable.TableName == "Address" || setupTable.TableName == "Customer" || setupTable.TableName == "CustomerAddress" ||
                            setupTable.TableName == "SalesOrderDetail" || setupTable.TableName == "SalesOrderHeader" || setupTable.TableName == "Product")
                        {

                            Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChangesWithFilters));
                            Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChangesWithFilters));
                        }
                    }

                    if (clientProviderType == ProviderType.Sql || clientProviderType == ProviderType.MySql || clientProviderType == ProviderType.MariaDB)
                    {
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteMetadata));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.Reset));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectRow));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.UpdateRow));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteRow));
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
            var options = new SyncOptions
            {
                SnapshotsDirectory = directory,
                BatchSize = 3000,
                DisableConstraintsOnApplyChanges = true
            };

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            // getting snapshot directory names
            var (rootDirectory, nameDirectory)
                = await remoteOrchestrator.GetSnapshotDirectoryAsync(parameters);

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
            var options = new SyncOptions
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
                await clientProvider.AddProductCategoryAsync();
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var s = await agent.SynchronizeAsync(setup, parameters);

                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnServer);
            }

            // Get count of rows
            var rowsCount = serverProvider.GetDatabaseFilteredRowsCount();

            // Execute a sync on all clients to be sure all clients have all rows
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var s = await agent.SynchronizeAsync(setup, SyncType.Reinitialize, parameters);

                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnServer);
                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(rowsCount, s.TotalChangesAppliedOnClient);

                Assert.Equal(rowsCount, clientProvider.GetDatabaseFilteredRowsCount());
            }
        }

        [Fact]
        public async Task SynchronizeThenDeprovisionThenAddPrefixes()
        {
            // Deletes all tables in client
            foreach (var clientProvider in clientsProvider)
                await clientProvider.DropAllTablesAsync(true);

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
            var rowsCount = serverProvider.GetDatabaseFilteredRowsCount();

            // Get count of rows for parameter 2
            var rowsCount2 = serverProvider.GetDatabaseFilteredRowsCount(AdventureWorksContext.CustomerId2ForFilter);

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var s = await agent.SynchronizeAsync(setup, parameters);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(rowsCount, clientProvider.GetDatabaseFilteredRowsCount());

                // create agent with filtered tables and second parameter
                var parameters2 = new SyncParameters(("CustomerID", AdventureWorksContext.CustomerId2ForFilter));
                agent = new SyncAgent(clientProvider, serverProvider, options);
                s = await agent.SynchronizeAsync(setup, parameters2);

                Assert.Equal(rowsCount2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(rowsCount2, clientProvider.GetDatabaseFilteredRowsCount(AdventureWorksContext.CustomerId2ForFilter));


            }
        }

    }
}
