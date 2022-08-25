using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests.UnitTests
{
    public partial class RemoteOrchestratorTests : IDisposable
    {

        public SyncSetup GetFilterSetup()
        {
            var setup = new SyncSetup(new string[] {
                            "SalesLT.ProductModel", "SalesLT.ProductCategory","SalesLT.Product",
                            "Customer","Address", "CustomerAddress", "Employee",
                            "SalesLT.SalesOrderHeader","SalesLT.SalesOrderDetail" });

            // Vertical Filter on columns
            setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName" });
            setup.Tables["Address"].Columns.AddRange(new string[] { "AddressID", "AddressLine1", "City", "PostalCode" });

            // Horizontal Filters on where clause

            // 1) EASY Way:
            setup.Filters.Add("CustomerAddress", "CustomerID");
            setup.Filters.Add("SalesOrderHeader", "CustomerID", "SalesLT");


            // 2) Same, but decomposed in 3 Steps

            var customerFilter = new SetupFilter("Customer");
            customerFilter.AddParameter("CustomerID", "Customer", true);
            customerFilter.AddWhere("CustomerID", "Customer", "CustomerID");
            setup.Filters.Add(customerFilter);

            // 3) Create your own filter

            // Create a filter on table Address
            var addressFilter = new SetupFilter("Address");
            addressFilter.AddParameter("CustomerID", "Customer");
            addressFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "AddressId", "Address", "AddressId");
            addressFilter.AddWhere("CustomerID", "CustomerAddress", "CustomerID");
            setup.Filters.Add(addressFilter);
            // ----------------------------------------------------

            // Create a filter on table SalesLT.SalesOrderDetail
            var salesOrderDetailFilter = new SetupFilter("SalesOrderDetail", "SalesLT");
            salesOrderDetailFilter.AddParameter("CustomerID", "Customer");
            salesOrderDetailFilter.AddJoin(Join.Left, "SalesLT.SalesOrderHeader").On("SalesLT.SalesOrderHeader", "SalesOrderId", "SalesLT.SalesOrderDetail", "SalesOrderId");
            salesOrderDetailFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerID", "SalesLT.SalesOrderHeader", "CustomerID");
            salesOrderDetailFilter.AddWhere("CustomerID", "CustomerAddress", "CustomerID");
            setup.Filters.Add(salesOrderDetailFilter);
            // ----------------------------------------------------

            // 4) Custom Wheres on Product.
            var productFilter = new SetupFilter("Product", "SalesLT");
            productFilter.AddCustomWhere("ProductCategoryID IS NOT NULL OR side.sync_row_is_tombstone = 1");
            setup.Filters.Add(productFilter);


            return setup;

        }

        public int GetFilterServerDatabaseRowsCount((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) t)
        {
            int totalCountRows = 0;

            using (var serverDbCtx = new AdventureWorksContext(t))
            {

                var addressesCount = serverDbCtx.Address.Where(a => a.CustomerAddress.Any(ca => ca.CustomerId == AdventureWorksContext.CustomerIdForFilter)).Count();
                var customersCount = serverDbCtx.Customer.Where(c => c.CustomerId == AdventureWorksContext.CustomerIdForFilter).Count();
                var customerAddressesCount = serverDbCtx.CustomerAddress.Where(c => c.CustomerId == AdventureWorksContext.CustomerIdForFilter).Count();
                var salesOrdersDetailsCount = serverDbCtx.SalesOrderDetail.Where(sod => sod.SalesOrder.CustomerId == AdventureWorksContext.CustomerIdForFilter).Count();
                var salesOrdersHeadersCount = serverDbCtx.SalesOrderHeader.Where(c => c.CustomerId == AdventureWorksContext.CustomerIdForFilter).Count();
                var employeesCount = serverDbCtx.Employee.Count();
                var productsCount = serverDbCtx.Product.Count();
                var productsCategoryCount = serverDbCtx.ProductCategory.Count();
                var productsModelCount = serverDbCtx.ProductModel.Count();

                totalCountRows = addressesCount + customersCount + customerAddressesCount + salesOrdersDetailsCount + salesOrdersHeadersCount +
                                 productsCount + productsCategoryCount + productsModelCount + employeesCount;
            }

            return totalCountRows;
        }


        /// <summary>
        /// RemoteOrchestrator.GetChanges() should return rows inserted on server, depending on the client scope sent
        /// </summary>
        [Fact]
        public async Task RemoteOrchestrator_GetChanges_WithFilters_ShouldReturnNewRowsInserted()
        {
            var dbNameSrv = HelperDatabase.GetRandomName("tcp_lo_srv");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbNameSrv, true);

            var dbNameCli = HelperDatabase.GetRandomName("tcp_lo_cli");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbNameCli, true);

            var csServer = HelperDatabase.GetConnectionString(ProviderType.Sql, dbNameSrv);
            var serverProvider = new SqlSyncProvider(csServer);

            var csClient = HelperDatabase.GetConnectionString(ProviderType.Sql, dbNameCli);
            var clientProvider = new SqlSyncProvider(csClient);

            await new AdventureWorksContext((dbNameSrv, ProviderType.Sql, serverProvider), true, true).Database.EnsureCreatedAsync();
            await new AdventureWorksContext((dbNameCli, ProviderType.Sql, clientProvider), true, false).Database.EnsureCreatedAsync();

            var scopeName = "scopesnap1";
            var setup = GetFilterSetup();
            var rowsCount = GetFilterServerDatabaseRowsCount((dbNameSrv, ProviderType.Sql, serverProvider));

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider);
            var parameters = new SyncParameters(("CustomerID", AdventureWorksContext.CustomerIdForFilter));

            // Making a first sync, will initialize everything we need
            var r = await agent.SynchronizeAsync(scopeName, setup, parameters);
            Assert.Equal(rowsCount, r.TotalChangesDownloaded);

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;
            var remoteOrchestrator = agent.RemoteOrchestrator;

            Guid otherCustomerId;

            // Server side : Create a sales order header + 3 sales order details linked to the filter
            // and create 1 sales order header not linked to filter
            using var ctxServer = new AdventureWorksContext((dbNameSrv, ProviderType.Sql, serverProvider), true);

            // get another customer than the filter one
            otherCustomerId = ctxServer.Customer.First(c => c.CustomerId != AdventureWorksContext.CustomerIdForFilter).CustomerId;

            var soh = new SalesOrderHeader
            {
                SalesOrderNumber = $"SO-99999",
                RevisionNumber = 1,
                Status = 5,
                OnlineOrderFlag = true,
                PurchaseOrderNumber = "PO348186287",
                AccountNumber = "10-4020-000609",
                CustomerId = AdventureWorksContext.CustomerIdForFilter,
                ShipToAddressId = 4,
                BillToAddressId = 5,
                ShipMethod = "CAR TRANSPORTATION",
                SubTotal = 6530.35M,
                TaxAmt = 70.4279M,
                Freight = 22.0087M,
                TotalDue = 6530.35M + 70.4279M + 22.0087M
            };

            var soh2 = new SalesOrderHeader
            {
                SalesOrderNumber = $"SO-99999",
                RevisionNumber = 1,
                Status = 5,
                OnlineOrderFlag = true,
                PurchaseOrderNumber = "PO348186287",
                AccountNumber = "10-4020-000609",
                CustomerId = otherCustomerId,
                ShipToAddressId = 4,
                BillToAddressId = 5,
                ShipMethod = "CAR TRANSPORTATION",
                SubTotal = 6530.35M,
                TaxAmt = 70.4279M,
                Freight = 22.0087M,
                TotalDue = 6530.35M + 70.4279M + 22.0087M
            };

            var productId = ctxServer.Product.First().ProductId;

            var sod1 = new SalesOrderDetail { OrderQty = 1, ProductId = productId, UnitPrice = 3578.2700M };
            var sod2 = new SalesOrderDetail { OrderQty = 2, ProductId = productId, UnitPrice = 44.5400M };
            var sod3 = new SalesOrderDetail { OrderQty = 2, ProductId = productId, UnitPrice = 1431.5000M };

            soh.SalesOrderDetail.Add(sod1);
            soh.SalesOrderDetail.Add(sod2);
            soh.SalesOrderDetail.Add(sod3);

            ctxServer.SalesOrderHeader.Add(soh);
            ctxServer.SalesOrderHeader.Add(soh2);
            await ctxServer.SaveChangesAsync();

            // Get changes from server
            var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName, parameters);
            var changes = await remoteOrchestrator.GetChangesAsync(cScopeInfoClient);

            Assert.NotNull(changes.ServerBatchInfo);
            Assert.NotNull(changes.ServerChangesSelected);
            Assert.Equal(2, changes.ServerChangesSelected.TableChangesSelected.Count);
            Assert.Contains("SalesOrderDetail", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
            Assert.Contains("SalesOrderHeader", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());

            var sodTable = await localOrchestrator.LoadTableFromBatchInfoAsync(changes.ServerBatchInfo, "SalesOrderDetail", "SalesLT");
            Assert.Equal(3, sodTable.Rows.Count);

            var sohTable = await localOrchestrator.LoadTableFromBatchInfoAsync(changes.ServerBatchInfo, "SalesOrderHeader", "SalesLT");
            Assert.Single(sohTable.Rows);

        }




        /// <summary>
        /// RemoteOrchestrator.GetChanges() should return rows inserted on server, depending on the client scope sent
        /// </summary>
        [Fact]
        public async Task RemoteOrchestrator_GetChanges_ShouldReturnNewRowsInserted()
        {
            var dbNameSrv = HelperDatabase.GetRandomName("tcp_lo_srv");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbNameSrv, true);

            var dbNameCli = HelperDatabase.GetRandomName("tcp_lo_cli");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbNameCli, true);

            var csServer = HelperDatabase.GetConnectionString(ProviderType.Sql, dbNameSrv);
            var serverProvider = new SqlSyncProvider(csServer);

            var csClient = HelperDatabase.GetConnectionString(ProviderType.Sql, dbNameCli);
            var clientProvider = new SqlSyncProvider(csClient);

            await new AdventureWorksContext((dbNameSrv, ProviderType.Sql, serverProvider), true, false).Database.EnsureCreatedAsync();
            await new AdventureWorksContext((dbNameCli, ProviderType.Sql, clientProvider), true, false).Database.EnsureCreatedAsync();

            var scopeName = "scopesnap1";
            var syncOptions = new SyncOptions();
            var setup = new SyncSetup();

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider);

            // Making a first sync, will initialize everything we need
            await agent.SynchronizeAsync(scopeName, this.Tables);

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;
            var remoteOrchestrator = agent.RemoteOrchestrator;

            // Server side : Create a product category and a product
            // Create a productcategory item
            // Create a new product on server
            var productId = Guid.NewGuid();
            var productName = HelperDatabase.GetRandomName();
            var productNumber = productName.ToUpperInvariant().Substring(0, 10);

            var productCategoryName = HelperDatabase.GetRandomName();
            var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

            using (var ctx = new AdventureWorksContext((dbNameSrv, ProviderType.Sql, serverProvider)))
            {
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);

                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber };
                ctx.Add(product);

                await ctx.SaveChangesAsync();
            }

            // Get client scope
            var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName);

            // Get changes to be populated to the server
            var serverSyncChanges = await remoteOrchestrator.GetChangesAsync(cScopeInfoClient);

            Assert.NotNull(serverSyncChanges.ServerBatchInfo);
            Assert.NotNull(serverSyncChanges.ServerChangesSelected);
            Assert.Equal(2, serverSyncChanges.ServerChangesSelected.TableChangesSelected.Count);
            Assert.Contains("Product", serverSyncChanges.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
            Assert.Contains("ProductCategory", serverSyncChanges.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());

            var productTable = await remoteOrchestrator.LoadTableFromBatchInfoAsync(serverSyncChanges.ServerBatchInfo, "Product", "SalesLT");
            var productRowName = productTable.Rows[0]["Name"];
            Assert.Equal(productName, productRowName);

            var productCategoryTable = await remoteOrchestrator.LoadTableFromBatchInfoAsync(serverSyncChanges.ServerBatchInfo, "ProductCategory", "SalesLT");
            var productCategoryRowName = productCategoryTable.Rows[0]["Name"];
            Assert.Equal(productCategoryName, productCategoryRowName);

        }


  
        /// <summary>
        /// RemoteOrchestrator.GetChanges() should return rows inserted on server, depending on the client scope sent
        /// </summary>
        [Fact]
        public async Task RemoteOrchestrator_HttpGetChanges_WithFilters_ShouldReturnNewRows()
        {
            var dbNameSrv = HelperDatabase.GetRandomName("tcp_lo_srv");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbNameSrv, true);

            var dbNameCli = HelperDatabase.GetRandomName("tcp_lo_cli");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbNameCli, true);

            var csServer = HelperDatabase.GetConnectionString(ProviderType.Sql, dbNameSrv);
            var serverProvider = new SqlSyncProvider(csServer);

            var csClient = HelperDatabase.GetConnectionString(ProviderType.Sql, dbNameCli);
            var clientProvider = new SqlSyncProvider(csClient);

            await new AdventureWorksContext((dbNameSrv, ProviderType.Sql, serverProvider), true, true).Database.EnsureCreatedAsync();
            await new AdventureWorksContext((dbNameCli, ProviderType.Sql, clientProvider), true, false).Database.EnsureCreatedAsync();

            var scopeName = "scopesnap1";
            var setup = GetFilterSetup();

            // Create a kestrell server
            var kestrell = new KestrellTestServer(false);

            // configure server orchestrator
            kestrell.AddSyncServer(serverProvider.GetType(), serverProvider.ConnectionString, scopeName, setup);

            var serviceUri = kestrell.Run();

            var rowsCount = GetFilterServerDatabaseRowsCount((dbNameSrv, ProviderType.Sql, serverProvider));

            var remoteOrchestrator = new WebRemoteOrchestrator(serviceUri);

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, remoteOrchestrator);
            var parameters = new SyncParameters(("CustomerID", AdventureWorksContext.CustomerIdForFilter));

            // Making a first sync, will initialize everything we need
            var r = await agent.SynchronizeAsync(scopeName, parameters);
            Assert.Equal(rowsCount, r.TotalChangesDownloaded);

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;

            Guid otherCustomerId;

            // Server side : Create a sales order header + 3 sales order details linked to the filter
            // and create 1 sales order header not linked to filter
            using var ctxServer = new AdventureWorksContext((dbNameSrv, ProviderType.Sql, serverProvider), true);

            // get another customer than the filter one
            otherCustomerId = ctxServer.Customer.First(c => c.CustomerId != AdventureWorksContext.CustomerIdForFilter).CustomerId;

            var soh = new SalesOrderHeader
            {
                SalesOrderNumber = $"SO-99999",
                RevisionNumber = 1,
                Status = 5,
                OnlineOrderFlag = true,
                PurchaseOrderNumber = "PO348186287",
                AccountNumber = "10-4020-000609",
                CustomerId = AdventureWorksContext.CustomerIdForFilter,
                ShipToAddressId = 4,
                BillToAddressId = 5,
                ShipMethod = "CAR TRANSPORTATION",
                SubTotal = 6530.35M,
                TaxAmt = 70.4279M,
                Freight = 22.0087M,
                TotalDue = 6530.35M + 70.4279M + 22.0087M
            };

            var soh2 = new SalesOrderHeader
            {
                SalesOrderNumber = $"SO-99999",
                RevisionNumber = 1,
                Status = 5,
                OnlineOrderFlag = true,
                PurchaseOrderNumber = "PO348186287",
                AccountNumber = "10-4020-000609",
                CustomerId = otherCustomerId,
                ShipToAddressId = 4,
                BillToAddressId = 5,
                ShipMethod = "CAR TRANSPORTATION",
                SubTotal = 6530.35M,
                TaxAmt = 70.4279M,
                Freight = 22.0087M,
                TotalDue = 6530.35M + 70.4279M + 22.0087M
            };

            var productId = ctxServer.Product.First().ProductId;

            var sod1 = new SalesOrderDetail { OrderQty = 1, ProductId = productId, UnitPrice = 3578.2700M };
            var sod2 = new SalesOrderDetail { OrderQty = 2, ProductId = productId, UnitPrice = 44.5400M };
            var sod3 = new SalesOrderDetail { OrderQty = 2, ProductId = productId, UnitPrice = 1431.5000M };

            soh.SalesOrderDetail.Add(sod1);
            soh.SalesOrderDetail.Add(sod2);
            soh.SalesOrderDetail.Add(sod3);

            ctxServer.SalesOrderHeader.Add(soh);
            ctxServer.SalesOrderHeader.Add(soh2);
            await ctxServer.SaveChangesAsync();

            // Get changes from server
            var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName);
            var changes = await remoteOrchestrator.GetChangesAsync(cScopeInfoClient);

            Assert.NotNull(changes.ServerBatchInfo);
            Assert.NotNull(changes.ServerChangesSelected);
            Assert.Equal(2, changes.ServerChangesSelected.TableChangesSelected.Count);
            Assert.Contains("SalesOrderDetail", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
            Assert.Contains("SalesOrderHeader", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());

            var sodTable = await localOrchestrator.LoadTableFromBatchInfoAsync(changes.ServerBatchInfo, "SalesOrderDetail", "SalesLT");
            Assert.Equal(3, sodTable.Rows.Count);

            var sohTable = await localOrchestrator.LoadTableFromBatchInfoAsync(changes.ServerBatchInfo, "SalesOrderHeader", "SalesLT");
            Assert.Single(sohTable.Rows);
        }


        [Fact]
        public async Task RemoteOrchestrator_HttpGetChanges_WithFilters_ShouldReturnDeletedRowsCount()
        {
            var dbNameSrv = HelperDatabase.GetRandomName("tcp_lo_srv");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbNameSrv, true);

            var dbNameCli = HelperDatabase.GetRandomName("tcp_lo_cli");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbNameCli, true);

            var csServer = HelperDatabase.GetConnectionString(ProviderType.Sql, dbNameSrv);
            var serverProvider = new SqlSyncProvider(csServer);

            var csClient = HelperDatabase.GetConnectionString(ProviderType.Sql, dbNameCli);
            var clientProvider = new SqlSyncProvider(csClient);

            await new AdventureWorksContext((dbNameSrv, ProviderType.Sql, serverProvider), true, true).Database.EnsureCreatedAsync();
            await new AdventureWorksContext((dbNameCli, ProviderType.Sql, clientProvider), true, false).Database.EnsureCreatedAsync();

            var scopeName = "scopesnap1";
            var setup = GetFilterSetup();

            // Create a kestrell server
            var kestrell = new KestrellTestServer(false);

            // configure server orchestrator
            kestrell.AddSyncServer(serverProvider.GetType(), serverProvider.ConnectionString, scopeName, setup);
            var serviceUri = kestrell.Run();

            var rowsCount = GetFilterServerDatabaseRowsCount((dbNameSrv, ProviderType.Sql, serverProvider));

            var remoteOrchestrator = new WebRemoteOrchestrator(serviceUri);

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, remoteOrchestrator);
            var parameters = new SyncParameters(("CustomerID", AdventureWorksContext.CustomerIdForFilter));

            // Making a first sync, will initialize everything we need
            var r = await agent.SynchronizeAsync(scopeName, parameters);
            Assert.Equal(rowsCount, r.TotalChangesDownloaded);

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;

            Guid otherCustomerId;

            // Server side : Create a sales order header + 3 sales order details linked to the filter
            // and create 1 sales order header not linked to filter
            using var ctxServer = new AdventureWorksContext((dbNameSrv, ProviderType.Sql, serverProvider), true);

            // get another customer than the filter one
            otherCustomerId = ctxServer.Customer.First(c => c.CustomerId != AdventureWorksContext.CustomerIdForFilter).CustomerId;

            var soh = new SalesOrderHeader
            {
                SalesOrderNumber = $"SO-99999",
                RevisionNumber = 1,
                Status = 5,
                OnlineOrderFlag = true,
                PurchaseOrderNumber = "PO348186287",
                AccountNumber = "10-4020-000609",
                CustomerId = AdventureWorksContext.CustomerIdForFilter,
                ShipToAddressId = 4,
                BillToAddressId = 5,
                ShipMethod = "CAR TRANSPORTATION",
                SubTotal = 6530.35M,
                TaxAmt = 70.4279M,
                Freight = 22.0087M,
                TotalDue = 6530.35M + 70.4279M + 22.0087M
            };

            var soh2 = new SalesOrderHeader
            {
                SalesOrderNumber = $"SO-99999",
                RevisionNumber = 1,
                Status = 5,
                OnlineOrderFlag = true,
                PurchaseOrderNumber = "PO348186287",
                AccountNumber = "10-4020-000609",
                CustomerId = otherCustomerId,
                ShipToAddressId = 4,
                BillToAddressId = 5,
                ShipMethod = "CAR TRANSPORTATION",
                SubTotal = 6530.35M,
                TaxAmt = 70.4279M,
                Freight = 22.0087M,
                TotalDue = 6530.35M + 70.4279M + 22.0087M
            };

            var productId = ctxServer.Product.First().ProductId;

            var sod1 = new SalesOrderDetail { OrderQty = 1, ProductId = productId, UnitPrice = 3578.2700M };
            var sod2 = new SalesOrderDetail { OrderQty = 2, ProductId = productId, UnitPrice = 44.5400M };
            var sod3 = new SalesOrderDetail { OrderQty = 2, ProductId = productId, UnitPrice = 1431.5000M };

            soh.SalesOrderDetail.Add(sod1);
            soh.SalesOrderDetail.Add(sod2);
            soh.SalesOrderDetail.Add(sod3);

            ctxServer.SalesOrderHeader.Add(soh);
            ctxServer.SalesOrderHeader.Add(soh2);
            await ctxServer.SaveChangesAsync();

            // Making a second sync, with these new rows
            r = await agent.SynchronizeAsync(scopeName, parameters);
            Assert.Equal(4, r.TotalChangesDownloaded);

            // now delete these lines on server
            ctxServer.SalesOrderDetail.Remove(sod1);
            ctxServer.SalesOrderDetail.Remove(sod2);
            ctxServer.SalesOrderDetail.Remove(sod3);
            ctxServer.SalesOrderHeader.Remove(soh);
            await ctxServer.SaveChangesAsync();


            // Get changes from server
            var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName);
            var changes = await remoteOrchestrator.GetChangesAsync(cScopeInfoClient);

            Assert.NotNull(changes.ServerBatchInfo);
            Assert.NotNull(changes.ServerChangesSelected);
            Assert.Equal(2, changes.ServerChangesSelected.TableChangesSelected.Count);
            Assert.Equal(4, changes.ServerChangesSelected.TableChangesSelected.Sum(tcs => tcs.Deletes));
            Assert.Equal(0, changes.ServerChangesSelected.TableChangesSelected.Sum(tcs => tcs.Upserts));
            Assert.Contains("SalesOrderDetail", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
            Assert.Contains("SalesOrderHeader", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());


            // testing with DataRowState
            var sodTable = await localOrchestrator.LoadTableFromBatchInfoAsync(changes.ServerBatchInfo, "SalesOrderDetail", "SalesLT", DataRowState.Deleted);
            Assert.Equal(3, sodTable.Rows.Count);
            foreach (var row in sodTable.Rows)
                Assert.Equal(DataRowState.Deleted, row.RowState);

            // testing with DataRowState
            var sodTable2 = await localOrchestrator.LoadTableFromBatchInfoAsync(changes.ServerBatchInfo, "SalesOrderDetail", "SalesLT", default);
            Assert.Equal(3, sodTable2.Rows.Count);
            foreach (var row in sodTable2.Rows)
                Assert.Equal(DataRowState.Deleted, row.RowState);

            // testing with DataRowState
            var sodTable3 = await localOrchestrator.LoadTableFromBatchInfoAsync(changes.ServerBatchInfo, "SalesOrderDetail", "SalesLT", DataRowState.Modified);
            Assert.Empty(sodTable3.Rows);

            // testing with DataRowState that is not valid
            var sodTable4 = await localOrchestrator.LoadTableFromBatchInfoAsync(changes.ServerBatchInfo, "SalesOrderDetail", "SalesLT", DataRowState.Unchanged);
            Assert.Empty(sodTable4.Rows);

            // testing without DataRowState
            var sohTable = await localOrchestrator.LoadTableFromBatchInfoAsync(changes.ServerBatchInfo, "SalesOrderHeader", "SalesLT");
            Assert.Single(sohTable.Rows);
            foreach (var row in sohTable.Rows)
                Assert.Equal(DataRowState.Deleted, row.RowState);

        }


    }
}
