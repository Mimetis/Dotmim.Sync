using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
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
    public partial class LocalOrchestratorTests : IDisposable
    {

        //public SyncSetup GetFilterSetup()
        //{
        //    var setup = new SyncSetup(new string[] {
        //                    "SalesLT.ProductModel", "SalesLT.ProductCategory","SalesLT.Product",
        //                    "Customer","Address", "CustomerAddress", "Employee",
        //                    "SalesLT.SalesOrderHeader","SalesLT.SalesOrderDetail" });

        //    // Vertical Filter on columns
        //    setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName" });
        //    setup.Tables["Address"].Columns.AddRange(new string[] { "AddressID", "AddressLine1", "City", "PostalCode" });

        //    // Horizontal Filters on where clause

        //    // 1) EASY Way:
        //    setup.Filters.Add("CustomerAddress", "CustomerID");
        //    setup.Filters.Add("SalesOrderHeader", "CustomerID", "SalesLT");


        //    // 2) Same, but decomposed in 3 Steps

        //    var customerFilter = new SetupFilter("Customer");
        //    customerFilter.AddParameter("CustomerID", "Customer", true);
        //    customerFilter.AddWhere("CustomerID", "Customer", "CustomerID");
        //    setup.Filters.Add(customerFilter);

        //    // 3) Create your own filter

        //    // Create a filter on table Address
        //    var addressFilter = new SetupFilter("Address");
        //    addressFilter.AddParameter("CustomerID", "Customer");
        //    addressFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "AddressId", "Address", "AddressId");
        //    addressFilter.AddWhere("CustomerID", "CustomerAddress", "CustomerID");
        //    setup.Filters.Add(addressFilter);
        //    // ----------------------------------------------------

        //    // Create a filter on table SalesLT.SalesOrderDetail
        //    var salesOrderDetailFilter = new SetupFilter("SalesOrderDetail", "SalesLT");
        //    salesOrderDetailFilter.AddParameter("CustomerID", "Customer");
        //    salesOrderDetailFilter.AddJoin(Join.Left, "SalesLT.SalesOrderHeader").On("SalesLT.SalesOrderHeader", "SalesOrderId", "SalesLT.SalesOrderDetail", "SalesOrderId");
        //    salesOrderDetailFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerID", "SalesLT.SalesOrderHeader", "CustomerID");
        //    salesOrderDetailFilter.AddWhere("CustomerID", "CustomerAddress", "CustomerID");
        //    setup.Filters.Add(salesOrderDetailFilter);
        //    // ----------------------------------------------------

        //    // 4) Custom Wheres on Product.
        //    var productFilter = new SetupFilter("Product", "SalesLT");
        //    productFilter.AddCustomWhere("ProductCategoryID IS NOT NULL OR side.sync_row_is_tombstone = 1");
        //    setup.Filters.Add(productFilter);


        //    return setup;

        //}

        //public int GetFilterServerDatabaseRowsCount((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) t, Guid? customerId = null)
        //{
        //    int totalCountRows = 0;

        //    if (!customerId.HasValue)
        //        customerId = AdventureWorksContext.CustomerId1ForFilter;

        //    using (var serverDbCtx = new AdventureWorksContext(t))
        //    {

        //        var addressesCount = serverDbCtx.Address.Where(a => a.CustomerAddress.Any(ca => ca.CustomerId == customerId)).Count();
        //        var customersCount = serverDbCtx.Customer.Where(c => c.CustomerId == customerId).Count();
        //        var customerAddressesCount = serverDbCtx.CustomerAddress.Where(c => c.CustomerId == customerId).Count();
        //        var salesOrdersDetailsCount = serverDbCtx.SalesOrderDetail.Where(sod => sod.SalesOrder.CustomerId == customerId).Count();
        //        var salesOrdersHeadersCount = serverDbCtx.SalesOrderHeader.Where(c => c.CustomerId == customerId).Count();
        //        var employeesCount = serverDbCtx.Employee.Count();
        //        var productsCount = serverDbCtx.Product.Count();
        //        var productsCategoryCount = serverDbCtx.ProductCategory.Count();
        //        var productsModelCount = serverDbCtx.ProductModel.Count();

        //        totalCountRows = addressesCount + customersCount + customerAddressesCount + salesOrdersDetailsCount + salesOrdersHeadersCount +
        //                         productsCount + productsCategoryCount + productsModelCount + employeesCount;
        //    }

        //    return totalCountRows;
        //}


        /// <summary>
        /// RemoteOrchestrator.GetChanges() should return rows inserted on server, depending on the client scope sent
        /// </summary>
        [Fact]
        public async Task LocalOrchestrator_GetChanges_WithFilters_ShouldReturnNewRowsInserted()
        {
            var localOrchestrator = new LocalOrchestrator(clientProvider, options);
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var scopeName = "scopesnap1";
            var setup = GetFilteredSetup();
            var rowsCount = serverProvider.GetDatabaseFilteredRowsCount();
            var parameters = GetFilterParameters();

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider, options);

            // Making a first sync, will initialize everything we need
            var r = await agent.SynchronizeAsync(scopeName, setup, parameters);
            Assert.Equal(rowsCount, r.TotalChangesDownloadedFromServer);

            // Client side : Create a sales order header + 3 sales order details linked to the filter
            var products = await clientProvider.GetProductsAsync();
            var soh = await clientProvider.AddSalesOrderHeaderAsync(AdventureWorksContext.CustomerId1ForFilter);
            await clientProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            await clientProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            await clientProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);

            // Get changes to be populated to the server
            var scopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName, parameters);
            var changes = await localOrchestrator.GetChangesAsync(scopeInfoClient);

            Assert.NotNull(changes.ClientBatchInfo);
            Assert.NotNull(changes.ClientChangesSelected);
            Assert.Equal(2, changes.ClientChangesSelected.TableChangesSelected.Count);
            Assert.Contains("SalesOrderDetail", changes.ClientChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
            Assert.Contains("SalesOrderHeader", changes.ClientChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());

            var sodTable = localOrchestrator.LoadTableFromBatchInfo(scopeName, changes.ClientBatchInfo, "SalesOrderDetail", "SalesLT");
            Assert.Equal(3, sodTable.Rows.Count);

            var sohTable = localOrchestrator.LoadTableFromBatchInfo(scopeName, changes.ClientBatchInfo, "SalesOrderHeader", "SalesLT");
            Assert.Single(sohTable.Rows);

        }

        /// <summary>
        /// RemoteOrchestrator.GetChanges() should return rows inserted on server, depending on the client scope sent
        /// </summary>
        [Fact]
        public async Task LocalOrchestrator_GetChanges_ShouldReturnNewRowsInserted()
        {
            var localOrchestrator = new LocalOrchestrator(clientProvider, options);
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);
            var scopeName = "scopesnap1";

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider);

            // Making a first sync, will initialize everything we need
            await agent.SynchronizeAsync(scopeName, setup);

            // Create a productcategory item
            // Create a new product on server
            var productCategory = await clientProvider.AddProductCategoryAsync();
            var product = await clientProvider.AddProductAsync();

            // Get changes to be populated to the server
            var scopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName);
            var changes = await localOrchestrator.GetChangesAsync(scopeInfoClient);

            Assert.NotNull(changes.ClientBatchInfo);
            Assert.NotNull(changes.ClientChangesSelected);
            Assert.Equal(2, changes.ClientChangesSelected.TableChangesSelected.Count);
            Assert.Contains("Product", changes.ClientChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
            Assert.Contains("ProductCategory", changes.ClientChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());

            var productTable = localOrchestrator.LoadTableFromBatchInfo(scopeName, changes.ClientBatchInfo, "Product", "SalesLT");
            var productRowName = productTable.Rows[0]["Name"];
            Assert.Equal(product.Name, productRowName);

            var productCategoryTable = localOrchestrator.LoadTableFromBatchInfo(scopeName, changes.ClientBatchInfo, "ProductCategory", "SalesLT");
            var productCategoryRowName = productCategoryTable.Rows[0]["Name"];
            Assert.Equal(productCategory.Name, productCategoryRowName);

        }


        /// <summary>
        /// RemoteOrchestrator.GetChanges() should return rows inserted on server, depending on the client scope sent
        /// </summary>
        [Fact]
        public async Task LocalOrchestrator_GetChanges_WithSerialize_Deserialize_ShouldReturnNewRowsInserted()
        {
            var localOrchestrator = new LocalOrchestrator(clientProvider, options);
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);
            var scopeName = "scopesnap1";

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider);

            // Making a first sync, will initialize everything we need
            await agent.SynchronizeAsync(scopeName, setup);

            var productCategory = await clientProvider.AddProductCategoryAsync();
            var product = await clientProvider.AddProductAsync();

            // Get changes to be populated to the server
            var scopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName);
            var changes = await localOrchestrator.GetChangesAsync(scopeInfoClient);

            Assert.NotNull(changes.ClientBatchInfo);
            Assert.NotNull(changes.ClientChangesSelected);
            Assert.Equal(2, changes.ClientChangesSelected.TableChangesSelected.Count);
            Assert.Contains("Product", changes.ClientChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
            Assert.Contains("ProductCategory", changes.ClientChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());

            var productTable =  localOrchestrator.LoadTableFromBatchInfo(scopeName, changes.ClientBatchInfo, "Product", "SalesLT");
            var productRowName = productTable.Rows[0]["Name"];
            Assert.Equal(product.Name, productRowName);

            var productCategoryTable =  localOrchestrator.LoadTableFromBatchInfo(scopeName, changes.ClientBatchInfo, "ProductCategory", "SalesLT");
            var productCategoryRowName = productCategoryTable.Rows[0]["Name"];
            Assert.Equal(productCategory.Name, productCategoryRowName);

        }

    }
}
