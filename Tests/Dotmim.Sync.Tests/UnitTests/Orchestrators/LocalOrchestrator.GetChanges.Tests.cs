using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
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

            using var sodTable = localOrchestrator.LoadTableFromBatchInfo(scopeName, changes.ClientBatchInfo, "SalesOrderDetail", "SalesLT");
            Assert.Equal(3, sodTable.Rows.Count);

            using var sohTable = localOrchestrator.LoadTableFromBatchInfo(scopeName, changes.ClientBatchInfo, "SalesOrderHeader", "SalesLT");
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

            using var productTable = localOrchestrator.LoadTableFromBatchInfo(scopeName, changes.ClientBatchInfo, "Product", "SalesLT");
            var productRowName = productTable.Rows[0]["Name"];
            Assert.Equal(product.Name, productRowName);

            using var productCategoryTable = localOrchestrator.LoadTableFromBatchInfo(scopeName, changes.ClientBatchInfo, "ProductCategory", "SalesLT");
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

            using var productTable =  localOrchestrator.LoadTableFromBatchInfo(scopeName, changes.ClientBatchInfo, "Product", "SalesLT");
            var productRowName = productTable.Rows[0]["Name"];
            Assert.Equal(product.Name, productRowName);

            using var productCategoryTable =  localOrchestrator.LoadTableFromBatchInfo(scopeName, changes.ClientBatchInfo, "ProductCategory", "SalesLT");
            var productCategoryRowName = productCategoryTable.Rows[0]["Name"];
            Assert.Equal(productCategory.Name, productCategoryRowName);

        }

    }
}
