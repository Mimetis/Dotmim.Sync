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

        private void ResetClientsAndServerByCreatingThemAgain()
        {
            HelperDatabase.DropDatabase(Fixture.ServerProviderType, Fixture.ServerDatabaseName);
            new AdventureWorksContext(Fixture.ServerDatabaseName, Fixture.ServerProviderType, Fixture.UseFallbackSchema, true).Database.EnsureCreated();

            // Drop DMS metadatas and truncate clients tables
            foreach (var (clientType, clientDatabaseName) in Fixture.ClientDatabaseNames)
            {
                HelperDatabase.DropDatabase(clientType, clientDatabaseName);
                new AdventureWorksContext(clientDatabaseName, clientType, Fixture.UseFallbackSchema, false).Database.EnsureCreated();
            }
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


        ///// <summary>
        ///// Insert one row in two tables on server, should be correctly sync on all clients
        ///// </summary>
        //[Fact]
        //public async Task Snapshot_Initialize()
        //{
        //    // create a server schema with seeding
        //    await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

        //    // create empty client databases
        //    foreach (var client in this.Clients)
        //        await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

        //    // ----------------------------------
        //    // Setting correct options for sync agent to be able to reach snapshot
        //    // ----------------------------------
        //    var snapshotDirctory = HelperDatabase.GetRandomName();
        //    var directory = Path.Combine(Environment.CurrentDirectory, snapshotDirctory);
        //    var options = new SyncOptions
        //    {
        //        SnapshotsDirectory = directory,
        //        BatchSize = 200
        //    };

        //    // ----------------------------------
        //    // Create a snapshot
        //    // ----------------------------------
        //    var remoteOrchestrator = new RemoteOrchestrator(Server.Provider, options);
        //    await remoteOrchestrator.CreateSnapshotAsync(this.FilterSetup, this.FilterParameters);

        //    // ----------------------------------
        //    // Add rows on server AFTER snapshot
        //    // ----------------------------------
        //    // Create a new address & customer address on server
        //    using (var serverDbCtx = new AdventureWorksContext(this.Server))
        //    {
        //        var addressLine1 = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

        //        var newAddress = new Address { AddressLine1 = addressLine1 };

        //        serverDbCtx.Address.Add(newAddress);
        //        await serverDbCtx.SaveChangesAsync();

        //        var newCustomerAddress = new CustomerAddress
        //        {
        //            AddressId = newAddress.AddressId,
        //            CustomerId = AdventureWorksContext.CustomerId1ForFilter,
        //            AddressType = "OTH"
        //        };

        //        serverDbCtx.CustomerAddress.Add(newCustomerAddress);
        //        await serverDbCtx.SaveChangesAsync();
        //    }

        //    // Get count of rows
        //    var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

        //    // Execute a sync on all clients and check results
        //    foreach (var client in Clients)
        //    {
        //        // create agent with filtered tables and parameter
        //        var agent = new SyncAgent(client.Provider, Server.Provider, options);

        //        var snapshotApplying = 0;
        //        var snapshotApplied = 0;

        //        agent.LocalOrchestrator.OnSnapshotApplying(saa => snapshotApplying++);
        //        agent.LocalOrchestrator.OnSnapshotApplied(saa => snapshotApplied++);
        //        var s = await agent.SynchronizeAsync(this.FilterSetup, this.FilterParameters);

        //        Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
        //        Assert.Equal(0, s.TotalChangesUploadedToServer);
        //        Assert.Equal(0, s.TotalResolvedConflicts);

        //        Assert.Equal(1, snapshotApplying);
        //        Assert.Equal(1, snapshotApplied);
        //    }
        //}

        ///// <summary>
        ///// Insert rows on server, and ensure DISTINCT is applied correctly 
        ///// </summary>
        //[Theory]
        //[ClassData(typeof(SyncOptionsData))]
        //public async Task Insert_TwoTables_EnsureDistinct(SyncOptions options)
        //{
        //    // create a server schema and seed
        //    await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

        //    // create empty client databases
        //    foreach (var client in this.Clients)
        //        await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

        //    // Get count of rows
        //    var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

        //    // Execute a sync on all clients to initialize client and server schema 
        //    foreach (var client in Clients)
        //    {
        //        // create agent with filtered tables and parameter
        //        var agent = new SyncAgent(client.Provider, Server.Provider, options);
        //        var s = await agent.SynchronizeAsync(this.FilterSetup, this.FilterParameters);

        //        Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
        //        Assert.Equal(0, s.TotalChangesUploadedToServer);
        //        Assert.Equal(0, s.TotalResolvedConflicts);
        //    }

        //    // Create a new address & customer address on server
        //    using (var serverDbCtx = new AdventureWorksContext(this.Server))
        //    {
        //        var addressLine1 = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);
        //        var newAddress = new Address { AddressLine1 = addressLine1 };
        //        serverDbCtx.Address.Add(newAddress);

        //        var addressLine2 = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);
        //        var newAddress2 = new Address { AddressLine1 = addressLine2 };
        //        serverDbCtx.Address.Add(newAddress2);

        //        await serverDbCtx.SaveChangesAsync();

        //        var newCustomerAddress = new CustomerAddress
        //        {
        //            AddressId = newAddress.AddressId,
        //            CustomerId = AdventureWorksContext.CustomerId1ForFilter,
        //            AddressType = "Secondary Home 1"
        //        };

        //        serverDbCtx.CustomerAddress.Add(newCustomerAddress);

        //        var newCustomerAddress2 = new CustomerAddress
        //        {
        //            AddressId = newAddress2.AddressId,
        //            CustomerId = AdventureWorksContext.CustomerId1ForFilter,
        //            AddressType = "Secondary Home 2"
        //        };

        //        serverDbCtx.CustomerAddress.Add(newCustomerAddress2);

        //        await serverDbCtx.SaveChangesAsync();

        //        // Update customer
        //        var customer = serverDbCtx.Customer.Find(AdventureWorksContext.CustomerId1ForFilter);
        //        customer.FirstName = "Orlanda";

        //        await serverDbCtx.SaveChangesAsync();
        //    }

        //    // Execute a sync on all clients and check results
        //    foreach (var client in Clients)
        //    {
        //        // create agent with filtered tables and parameter
        //        var agent = new SyncAgent(client.Provider, Server.Provider, options);
        //        var s = await agent.SynchronizeAsync(this.FilterSetup, this.FilterParameters);

        //        Assert.Equal(5, s.TotalChangesDownloadedFromServer);
        //        Assert.Equal(0, s.TotalChangesUploadedToServer);
        //        Assert.Equal(0, s.TotalResolvedConflicts);
        //    }
        //}




        ///// <summary>
        ///// </summary>
        //[Theory]
        //[ClassData(typeof(SyncOptionsData))]
        //public async Task Using_ExistingClientDatabase_Filter_With_NotSyncedColumn(SyncOptions options)
        //{

        //    if (this.Server.ProviderType != ProviderType.Sql)
        //        return;

        //    var clients = this.Clients.Where(c => c.ProviderType == ProviderType.Sql || c.ProviderType == ProviderType.Sqlite);

        //    var setup = new SyncSetup(new string[] { "Customer" });

        //    // Filter columns. We are not syncing EmployeeID, BUT this column will be part of the filter
        //    setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "NameStyle", "FirstName", "LastName" });

        //    // create a server schema and seed
        //    await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

        //    // create empty client databases WITH schema, and WITHOUT seeding
        //    foreach (var client in clients)
        //    {
        //        await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);
        //        await this.EnsureDatabaseSchemaAndSeedAsync(client, false, UseFallbackSchema);
        //    }


        //    var filter = new SetupFilter("Customer");
        //    filter.AddParameter("EmployeeID", DbType.Int32, true);
        //    filter.AddCustomWhere("EmployeeID = @EmployeeID or @EmployeeID is null");

        //    setup.Filters.Add(filter);

        //    // Execute a sync on all clients to initialize client and server schema 
        //    foreach (var client in clients)
        //    {
        //        // create agent with filtered tables and parameter
        //        var agent = new SyncAgent(client.Provider, Server.Provider, options);
        //        var p = new SyncParameters(("EmployeeID", 1));
        //        var s = await agent.SynchronizeAsync(setup, p);

        //        Assert.Equal(2, s.ChangesAppliedOnClient.TotalAppliedChanges);

        //    }
        //}

        ///// <summary>
        ///// </summary>
        //[Theory]
        //[ClassData(typeof(SyncOptionsData))]
        //public async Task Migration_Adding_Table(SyncOptions options)
        //{
        //    var setup = new SyncSetup(new string[] { "Customer" });

        //    // Filter columns. We are not syncing EmployeeID, BUT this column will be part of the filter
        //    setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName" });
        //    setup.Filters.Add("Customer", "EmployeeID");

        //    // create a server schema and seed
        //    await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

        //    // create empty client databases
        //    foreach (var client in this.Clients)
        //        await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);


        //    // Execute a sync on all clients to initialize client and server schema 
        //    foreach (var client in Clients)
        //    {
        //        // create agent with filtered tables and parameter
        //        var agent = new SyncAgent(client.Provider, Server.Provider, options);
        //        var p = new SyncParameters(("EmployeeID", 1));

        //        var s = await agent.SynchronizeAsync(setup, p);

        //        Assert.Equal(2, s.ChangesAppliedOnClient.TotalAppliedChanges);
        //    }

        //    var remoteOrchestrator = new RemoteOrchestrator(Server.Provider);

        //    // Adding a new scope on the server with a new table
        //    var setupv2 = new SyncSetup(new string[] { "Customer", "Employee" });
        //    setupv2.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName" });
        //    setupv2.Filters.Add("Customer", "EmployeeID");

        //    var sScopeInfo = await remoteOrchestrator.ProvisionAsync("v2", setupv2);

        //    // Execute a sync on all clients to initialize client and server schema 
        //    foreach (var client in Clients)
        //    {

        //        var parameters = new SyncParameters(("EmployeeID", 1));
        //        // Create the table on local database
        //        var localOrchestrator = new LocalOrchestrator(client.Provider);
        //        await localOrchestrator.CreateTableAsync(sScopeInfo, "Employee");

        //        // Once created we can provision the new scope, thanks to the serverScope instance we already have
        //        await localOrchestrator.ProvisionAsync(sScopeInfo);

        //        var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync("v2", parameters);

        //        // IF we launch synchronize on this new scope, it will get all the rows from the server
        //        // We are making a shadow copy of previous scope to get the last synchronization metadata
        //        var oldCScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(syncParameters: parameters);
        //        cScopeInfoClient.ShadowScope(oldCScopeInfoClient);
        //        await localOrchestrator.SaveScopeInfoClientAsync(cScopeInfoClient);


        //        // create agent with filtered tables and parameter
        //        var agent = new SyncAgent(client.Provider, Server.Provider, options);


        //        if (options.TransactionMode != TransactionMode.AllOrNothing && (client.ProviderType == ProviderType.MySql || client.ProviderType == ProviderType.MariaDB))
        //        {
        //            agent.LocalOrchestrator.OnGetCommand(async args =>
        //            {
        //                if (args.CommandType == DbCommandType.Reset)
        //                {
        //                    var scopeInfo = await agent.LocalOrchestrator.GetScopeInfoAsync(args.Connection, args.Transaction);
        //                    await agent.LocalOrchestrator.DisableConstraintsAsync(scopeInfo, args.Table.TableName, args.Table.SchemaName, args.Connection, args.Transaction);
        //                }
        //            });
        //        }

        //        var s = await agent.SynchronizeAsync("v2", SyncType.Reinitialize, parameters);

        //        Assert.Equal(5, s.ChangesAppliedOnClient.TotalAppliedChanges);
        //    }

        //}

        ///// <summary>
        ///// </summary>
        //[Theory]
        //[ClassData(typeof(SyncOptionsData))]
        //public async Task Migration_Modifying_Table(SyncOptions options)
        //{

        //    var setup = new SyncSetup(new string[] { "Customer" });

        //    // Filter columns. We are not syncing EmployeeID, BUT this column will be part of the filter
        //    setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName" });

        //    // create a server schema and seed
        //    await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

        //    // create empty client databases
        //    foreach (var client in this.Clients)
        //        await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

        //    setup.Filters.Add("Customer", "EmployeeID");

        //    // Execute a sync on all clients to initialize client and server schema 
        //    foreach (var client in Clients)
        //    {
        //        // create agent with filtered tables and parameter
        //        var agent = new SyncAgent(client.Provider, Server.Provider, options);
        //        var p = new SyncParameters(("EmployeeID", 1));

        //        var s = await agent.SynchronizeAsync(setup, p);

        //        Assert.Equal(2, s.ChangesAppliedOnClient.TotalAppliedChanges);
        //    }


        //    // Adding a new scope on the server with a the same table plus one column
        //    var setupv2 = new SyncSetup(new string[] { "Customer" });
        //    // Adding a new column to Customer
        //    setupv2.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName", "EmailAddress" });
        //    setupv2.Filters.Add("Customer", "EmployeeID");

        //    var remoteOrchestrator = new RemoteOrchestrator(Server.Provider);
        //    var serverScope = await remoteOrchestrator.ProvisionAsync("v2", setupv2);

        //    // Execute a sync on all clients to initialize client and server schema 
        //    foreach (var client in Clients)
        //    {
        //        var agent = new SyncAgent(client.Provider, Server.Provider, options);

        //        // Adding the column on client side
        //        var commandText = client.ProviderType switch
        //        {
        //            ProviderType.Sql => @"ALTER TABLE Customer ADD EmailAddress nvarchar(250) NULL;",
        //            ProviderType.Sqlite => @"ALTER TABLE Customer ADD EmailAddress text NULL;",
        //            ProviderType.MySql => @"ALTER TABLE `Customer` ADD `EmailAddress` nvarchar(250) NULL;",
        //            ProviderType.MariaDB => @"ALTER TABLE `Customer` ADD `EmailAddress` nvarchar(250) NULL;",
        //            ProviderType.Postgres => @"ALTER TABLE ""Customer"" ADD ""EmailAddress"" varchar(250) NULL;",
        //            _ => throw new NotImplementedException()
        //        };

        //        var connection = client.Provider.CreateConnection();
        //        connection.Open();
        //        var command = connection.CreateCommand();
        //        command.CommandText = commandText;
        //        command.Connection = connection;
        //        await command.ExecuteNonQueryAsync();
        //        connection.Close();

        //        // Once created we can provision the new scope, thanks to the serverScope instance we already have
        //        var clientScopeV2 = await agent.LocalOrchestrator.ProvisionAsync(serverScope);

        //        if (Server.ProviderType == ProviderType.MySql || Server.ProviderType == ProviderType.MariaDB)
        //        {
        //            agent.RemoteOrchestrator.OnConnectionOpen(coa =>
        //            {
        //                // tracking https://github.com/mysql-net/MySqlConnector/issues/924
        //                MySqlConnection.ClearPool(coa.Connection as MySqlConnection);
        //            });
        //        }

        //        if (client.ProviderType == ProviderType.MySql || client.ProviderType == ProviderType.MariaDB)
        //        {
        //            agent.LocalOrchestrator.OnConnectionOpen(coa =>
        //            {
        //                // tracking https://github.com/mysql-net/MySqlConnector/issues/924
        //                MySqlConnection.ClearPool(coa.Connection as MySqlConnection);
        //            });
        //        }

        //        // create agent with filtered tables and parameter
        //        var p = new SyncParameters(("EmployeeID", 1));

        //        var s = await agent.SynchronizeAsync("v2", SyncType.Reinitialize, p);

        //        Assert.Equal(2, s.ChangesAppliedOnClient.TotalAppliedChanges);
        //    }

        //}


        ///// <summary>
        ///// </summary>
        //[Theory]
        //[ClassData(typeof(SyncOptionsData))]
        //public async Task Migration_Removing_Table(SyncOptions options)
        //{
        //    var setup = new SyncSetup(new string[] { "Customer", "Employee" });

        //    // Filter columns. We are not syncing EmployeeID, BUT this column will be part of the filter
        //    setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName" });
        //    setup.Filters.Add("Customer", "EmployeeID");

        //    var parameters = new SyncParameters(("EmployeeID", 1));

        //    // create a server schema and seed
        //    await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

        //    // create empty client databases
        //    foreach (var client in this.Clients)
        //        await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);


        //    // Execute a sync on all clients to initialize client and server schema 
        //    foreach (var client in Clients)
        //    {
        //        // create agent with filtered tables and parameter
        //        var agent = new SyncAgent(client.Provider, Server.Provider, options);

        //        var s = await agent.SynchronizeAsync(setup, parameters);

        //        Assert.Equal(5, s.ChangesAppliedOnClient.TotalAppliedChanges);
        //    }


        //    // Adding a new scope on the server with a the same table plus one column
        //    var setupv2 = new SyncSetup(new string[] { "Employee" });
        //    var remoteOrchestrator = new RemoteOrchestrator(Server.Provider);
        //    var serverScopeV2 = await remoteOrchestrator.ProvisionAsync("v2", setupv2);

        //    // Execute a sync on all clients to initialize client and server schema 
        //    foreach (var client in Clients)
        //    {
        //        // create agent with filtered tables and parameter
        //        var agent = new SyncAgent(client.Provider, Server.Provider, options);

        //        // Once created we can provision the new scope, thanks to the serverScope instance we already have
        //        await agent.LocalOrchestrator.ProvisionAsync(serverScopeV2);
        //        var cScopeInfoClientV2 = await agent.LocalOrchestrator.GetScopeInfoClientAsync("v2");

        //        // IF we launch synchronize on this new scope, it will get all the rows from the server
        //        // We are making a shadow copy of previous scope client to get the last synchronization metadata
        //        var oldCScopeInfoClient = await agent.LocalOrchestrator.GetScopeInfoClientAsync(syncParameters: parameters);
        //        cScopeInfoClientV2.ShadowScope(oldCScopeInfoClient);
        //        await agent.LocalOrchestrator.SaveScopeInfoClientAsync(cScopeInfoClientV2);

        //        // Deprovision first scope
        //        await agent.LocalOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures);

        //        var s = await agent.SynchronizeAsync("v2");

        //        Assert.Equal(0, s.ChangesAppliedOnClient.TotalAppliedChanges);
        //    }

        //}


        ///// <summary>
        ///// </summary>
        //[Theory]
        //[ClassData(typeof(SyncOptionsData))]
        //public async Task Deprovision_Should_Remove_Filtered_StoredProcedures(SyncOptions options)
        //{
        //    var setup = new SyncSetup(new string[] { "Customer", "Employee" });

        //    // Filter columns. We are not syncing EmployeeID, BUT this column will be part of the filter
        //    setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName" });

        //    // create a server schema and seed
        //    await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

        //    // create empty client databases
        //    foreach (var client in this.Clients)
        //        await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

        //    setup.Filters.Add("Customer", "EmployeeID");

        //    // Execute a sync on all clients to initialize client and server schema 
        //    foreach (var client in Clients)
        //    {
        //        // create agent with filtered tables and parameter
        //        var agent = new SyncAgent(client.Provider, Server.Provider, options);
        //        var p = new SyncParameters(("EmployeeID", 1));

        //        var s = await agent.SynchronizeAsync(setup, p);

        //        Assert.Equal(5, s.ChangesAppliedOnClient.TotalAppliedChanges);

        //        var scopeInfo = await agent.LocalOrchestrator.GetScopeInfoAsync();

        //        await agent.LocalOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.ScopeInfo | SyncProvision.TrackingTable);

        //        foreach (var setupTable in setup.Tables)
        //        {
        //            Assert.False(await agent.LocalOrchestrator.ExistTriggerAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Delete));
        //            Assert.False(await agent.LocalOrchestrator.ExistTriggerAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Insert));
        //            Assert.False(await agent.LocalOrchestrator.ExistTriggerAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Update));

        //            if (client.ProviderType == ProviderType.Sql)
        //            {
        //                Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkDeleteRows));
        //                Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkTableType));
        //                Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkUpdateRows));
        //                Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteMetadata));
        //                Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteRow));
        //                Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.Reset));
        //                Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChanges));
        //                Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChanges));
        //                Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectRow));
        //                Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.UpdateRow));
        //                Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChangesWithFilters));
        //                Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChangesWithFilters));
        //            }
        //        }

        //    }
        //}

        ///// <summary>
        ///// Insert one row in two tables on server, should be correctly sync on all clients
        ///// </summary>
        //[Fact]
        //public async Task Snapshot_ShouldNot_Delete_Folders()
        //{
        //    // create a server schema with seeding
        //    await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

        //    // create empty client databases
        //    foreach (var client in this.Clients)
        //        await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

        //    // ----------------------------------
        //    // Setting correct options for sync agent to be able to reach snapshot
        //    // ----------------------------------
        //    var snapshotDirctory = HelperDatabase.GetRandomName();
        //    var directory = Path.Combine(Environment.CurrentDirectory, snapshotDirctory);
        //    var options = new SyncOptions
        //    {
        //        SnapshotsDirectory = directory,
        //        BatchSize = 200
        //    };
        //    // ----------------------------------
        //    // Create a snapshot
        //    // ----------------------------------
        //    var remoteOrchestrator = new RemoteOrchestrator(Server.Provider, options);

        //    // getting snapshot directory names
        //    var (rootDirectory, nameDirectory)
        //        = await remoteOrchestrator.GetSnapshotDirectoryAsync(SyncOptions.DefaultScopeName, this.FilterParameters).ConfigureAwait(false);

        //    Assert.False(Directory.Exists(rootDirectory));
        //    Assert.False(Directory.Exists(Path.Combine(rootDirectory, nameDirectory)));

        //    var setup = new SyncSetup(new string[] { "Customer" });

        //    await remoteOrchestrator.CreateSnapshotAsync(setup, this.FilterParameters);

        //    Assert.True(Directory.Exists(rootDirectory));
        //    Assert.True(Directory.Exists(Path.Combine(rootDirectory, nameDirectory)));


        //    // ----------------------------------
        //    // Add rows on server AFTER snapshot
        //    // ----------------------------------
        //    // Create a new address & customer address on server
        //    using (var serverDbCtx = new AdventureWorksContext(this.Server))
        //    {
        //        var addressLine1 = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

        //        var newAddress = new Address { AddressLine1 = addressLine1 };

        //        serverDbCtx.Address.Add(newAddress);
        //        await serverDbCtx.SaveChangesAsync();

        //        var newCustomerAddress = new CustomerAddress
        //        {
        //            AddressId = newAddress.AddressId,
        //            CustomerId = AdventureWorksContext.CustomerId1ForFilter,
        //            AddressType = "OTH"
        //        };

        //        serverDbCtx.CustomerAddress.Add(newCustomerAddress);
        //        await serverDbCtx.SaveChangesAsync();
        //    }

        //    // Execute a sync on all clients and check results
        //    foreach (var client in Clients)
        //    {
        //        // create agent with filtered tables and parameter
        //        var agent = new SyncAgent(client.Provider, Server.Provider, options);
        //        var s = await agent.SynchronizeAsync("v2", this.FilterSetup, this.FilterParameters);

        //        Assert.True(Directory.Exists(rootDirectory));
        //        Assert.True(Directory.Exists(Path.Combine(rootDirectory, nameDirectory)));
        //    }

        //}


        ///// <summary>
        ///// Insert one row in two tables on server, should be correctly sync on all clients
        ///// </summary>
        //[Fact]
        //public async Task Snapshot_Initialize_ThenClientUploadSync_ThenReinitialize()
        //{
        //    // create a server schema with seeding
        //    await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

        //    // create empty client databases
        //    foreach (var client in this.Clients)
        //        await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

        //    // snapshot directory
        //    var snapshotDirctory = HelperDatabase.GetRandomName();
        //    var directory = Path.Combine(Environment.CurrentDirectory, snapshotDirctory);

        //    var options = new SyncOptions
        //    {
        //        SnapshotsDirectory = directory,
        //        BatchSize = 200
        //    };

        //    var remoteOrchestrator = new RemoteOrchestrator(Server.Provider, options);

        //    await remoteOrchestrator.CreateSnapshotAsync(this.FilterSetup, this.FilterParameters);

        //    // ----------------------------------
        //    // Add rows on server AFTER snapshot
        //    // ----------------------------------
        //    var productId = Guid.NewGuid();
        //    var productName = HelperDatabase.GetRandomName();
        //    var productNumber = productName.ToUpperInvariant().Substring(0, 10);

        //    var productCategoryName = HelperDatabase.GetRandomName();
        //    var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

        //    using (var ctx = new AdventureWorksContext(this.Server))
        //    {
        //        var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
        //        ctx.Add(pc);

        //        var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber };
        //        ctx.Add(product);


        //        var addressLine1 = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

        //        var newAddress = new Address { AddressLine1 = addressLine1 };

        //        ctx.Address.Add(newAddress);
        //        await ctx.SaveChangesAsync();

        //        var newCustomerAddress = new CustomerAddress
        //        {
        //            AddressId = newAddress.AddressId,
        //            CustomerId = AdventureWorksContext.CustomerId1ForFilter,
        //            AddressType = "OTH"
        //        };

        //        ctx.CustomerAddress.Add(newCustomerAddress);

        //        await ctx.SaveChangesAsync();
        //    }

        //    // Get count of rows
        //    var rowsCount = this.GetServerDatabaseRowsCount(this.Server);


        //    // Execute a sync on all clients and check results
        //    foreach (var client in Clients)
        //    {
        //        // create agent with filtered tables and parameter
        //        var agent = new SyncAgent(client.Provider, Server.Provider, options);
        //        var s = await agent.SynchronizeAsync(this.FilterSetup, this.FilterParameters);

        //        Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
        //        Assert.Equal(0, s.TotalChangesUploadedToServer);
        //        Assert.Equal(0, s.TotalResolvedConflicts);
        //    }

        //    // ----------------------------------
        //    // Now add rows on client
        //    // ----------------------------------

        //    foreach (var client in Clients)
        //    {
        //        var name = HelperDatabase.GetRandomName();
        //        var pn = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

        //        var product = new Product { ProductId = Guid.NewGuid(), ProductCategoryId = "A_BIKES", Name = name, ProductNumber = pn };

        //        using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
        //        ctx.Product.Add(product);
        //        await ctx.SaveChangesAsync();
        //    }

        //    // Sync all clients
        //    // First client  will upload one line and will download nothing
        //    // Second client will upload one line and will download one line
        //    // thrid client  will upload one line and will download two lines
        //    int download = 0;
        //    foreach (var client in Clients)
        //    {
        //        // create agent with filtered tables and parameter
        //        var agent = new SyncAgent(client.Provider, Server.Provider, options);
        //        var s = await agent.SynchronizeAsync(this.FilterSetup, this.FilterParameters);

        //        Assert.Equal(download++, s.TotalChangesDownloadedFromServer);
        //        Assert.Equal(1, s.TotalChangesUploadedToServer);
        //        Assert.Equal(0, s.TotalResolvedConflicts);
        //    }

        //    // Get count of rows
        //    rowsCount = this.GetServerDatabaseRowsCount(this.Server);

        //    // ----------------------------------
        //    // Now Reinitialize
        //    // ----------------------------------

        //    // Execute a sync on all clients and check results
        //    foreach (var client in Clients)
        //    {
        //        // create agent with filtered tables and parameter
        //        var agent = new SyncAgent(client.Provider, Server.Provider, options);
        //        var s = await agent.SynchronizeAsync(this.FilterSetup, SyncType.Reinitialize, this.FilterParameters);

        //        Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
        //        Assert.Equal(0, s.TotalChangesUploadedToServer);
        //        Assert.Equal(0, s.TotalResolvedConflicts);
        //    }
        //}


        ///// <summary>
        ///// </summary>
        //[Fact]
        //public async Task Synchronize_ThenDeprovision_ThenAddPrefixes()
        //{
        //    var options = new SyncOptions();
        //    var setup = new SyncSetup(new string[] { "Customer" });

        //    // Filtered columns. 
        //    setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName" });

        //    // create a server schema and seed
        //    await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

        //    // create empty client databases
        //    foreach (var client in this.Clients)
        //        await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

        //    setup.Filters.Add("Customer", "EmployeeID");

        //    // Execute a sync on all clients to initialize client and server schema 
        //    foreach (var client in Clients)
        //    {
        //        // create agent with filtered tables and parameter
        //        var agent = new SyncAgent(client.Provider, Server.Provider, options);
        //        var p = new SyncParameters(("EmployeeID", 1));

        //        var s = await agent.SynchronizeAsync(setup, p);

        //        Assert.Equal(2, s.ChangesAppliedOnClient.TotalAppliedChanges);

        //    }
        //    foreach (var client in Clients)
        //    {
        //        // Deprovision everything
        //        var localOrchestrator = new LocalOrchestrator(client.Provider, options);
        //        var clientScope = await localOrchestrator.GetScopeInfoAsync();

        //        await localOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures
        //            | SyncProvision.Triggers
        //            | SyncProvision.TrackingTable);

        //        await localOrchestrator.DeleteScopeInfoAsync(clientScope);
        //    }

        //    var remoteOrchestrator = new RemoteOrchestrator(Server.Provider, options);
        //    var serverScope = await remoteOrchestrator.GetScopeInfoAsync();

        //    await remoteOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures
        //        | SyncProvision.Triggers
        //        | SyncProvision.TrackingTable);

        //    await remoteOrchestrator.DeleteScopeInfoAsync(serverScope);


        //    // Adding a new table
        //    setup.Tables.Add("Employee");

        //    // Adding prefixes
        //    setup.StoredProceduresPrefix = "sync";
        //    setup.StoredProceduresSuffix = "sp";
        //    setup.TrackingTablesPrefix = "track";
        //    setup.TrackingTablesSuffix = "tbl";
        //    setup.TriggersPrefix = "trg";
        //    setup.TriggersSuffix = "tbl";

        //    foreach (var client in Clients)
        //    {
        //        var agent = new SyncAgent(client.Provider, Server.Provider, options);
        //        var p = new SyncParameters(("EmployeeID", 1));


        //        var s = await agent.SynchronizeAsync(setup, SyncType.Reinitialize, p);
        //        Assert.Equal(5, s.ChangesAppliedOnClient.TotalAppliedChanges);
        //    }


        //}



        //[Theory]
        //[ClassData(typeof(SyncOptionsData))]
        //public virtual async Task MultiFiltersParameters(SyncOptions options)
        //{
        //    // create a server db and seed it
        //    await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

        //    // create empty client databases
        //    foreach (var client in this.Clients)
        //        await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

        //    // Get count of rows
        //    var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

        //    // Get count of rows for parameter 2
        //    var rowsCount2 = this.GetServerDatabaseRowsCount(this.Server, AdventureWorksContext.CustomerId2ForFilter);

        //    // Execute a sync on all clients and check results
        //    foreach (var client in this.Clients)
        //    {
        //        // create agent with filtered tables and parameter
        //        var agent = new SyncAgent(client.Provider, Server.Provider, options);
        //        var s = await agent.SynchronizeAsync(this.FilterSetup, this.FilterParameters);

        //        Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
        //        Assert.Equal(0, s.TotalChangesUploadedToServer);
        //        Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));

        //        // create agent with filtered tables and second parameter
        //        var parameters2 = new SyncParameters(("CustomerID", AdventureWorksContext.CustomerId2ForFilter));
        //        agent = new SyncAgent(client.Provider, Server.Provider, options);
        //        s = await agent.SynchronizeAsync(this.FilterSetup, parameters2);

        //        Assert.Equal(rowsCount2, s.TotalChangesDownloadedFromServer);
        //        Assert.Equal(0, s.TotalChangesUploadedToServer);
        //        Assert.Equal(rowsCount2, this.GetServerDatabaseRowsCount(client, AdventureWorksContext.CustomerId2ForFilter));


        //    }
        //}

    }
}
