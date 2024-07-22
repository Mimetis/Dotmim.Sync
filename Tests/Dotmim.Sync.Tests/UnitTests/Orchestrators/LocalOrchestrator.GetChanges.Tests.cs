using Dotmim.Sync.Tests.Models;
using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Tests.UnitTests
{
    public partial class LocalOrchestratorTests : IDisposable
    {

        /// <summary>
        /// RemoteOrchestrator.GetChanges() should return rows inserted on server, depending on the client scope sent.
        /// </summary>
        [Fact]
        public async Task LocalOrchestratorGetChangesWithFiltersShouldReturnNewRowsInserted()
        {
            var localOrchestrator = new LocalOrchestrator(this.clientProvider, this.options);
            var remoteOrchestrator = new RemoteOrchestrator(this.serverProvider, this.options);

            var scopeName = "scopesnap1";
            var setup = this.GetFilteredSetup();
            var rowsCount = this.serverProvider.GetDatabaseFilteredRowsCount();
            var parameters = this.GetFilterParameters();

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(this.clientProvider, this.serverProvider, this.options);

            // Making a first sync, will initialize everything we need
            var r = await agent.SynchronizeAsync(scopeName, setup, parameters);
            Assert.Equal(rowsCount, r.TotalChangesDownloadedFromServer);

            // Client side : Create a sales order header + 3 sales order details linked to the filter
            var products = await this.clientProvider.GetProductsAsync();
            var soh = await this.clientProvider.AddSalesOrderHeaderAsync(AdventureWorksContext.CustomerId1ForFilter);
            await this.clientProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            await this.clientProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            await this.clientProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);

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
        /// RemoteOrchestrator.GetChanges() should return rows inserted on server, depending on the client scope sent.
        /// </summary>
        [Fact]
        public async Task LocalOrchestratorGetChangesShouldReturnNewRowsInserted()
        {
            var localOrchestrator = new LocalOrchestrator(this.clientProvider, this.options);
            var remoteOrchestrator = new RemoteOrchestrator(this.serverProvider, this.options);
            var scopeName = "scopesnap1";

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(this.clientProvider, this.serverProvider);

            // Making a first sync, will initialize everything we need
            await agent.SynchronizeAsync(scopeName, this.setup);

            // Create a productcategory item
            // Create a new product on server
            var productCategory = await this.clientProvider.AddProductCategoryAsync();
            var product = await this.clientProvider.AddProductAsync();

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
        /// RemoteOrchestrator.GetChanges() should return rows inserted on server, depending on the client scope sent.
        /// </summary>
        [Fact]
        public async Task LocalOrchestratorGetChangesWithSerializeDeserializeShouldReturnNewRowsInserted()
        {
            var localOrchestrator = new LocalOrchestrator(this.clientProvider, this.options);
            var remoteOrchestrator = new RemoteOrchestrator(this.serverProvider, this.options);
            var scopeName = "scopesnap1";

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(this.clientProvider, this.serverProvider);

            // Making a first sync, will initialize everything we need
            await agent.SynchronizeAsync(scopeName, this.setup);

            var productCategory = await this.clientProvider.AddProductCategoryAsync();
            var product = await this.clientProvider.AddProductAsync();

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
    }
}