using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Web.Client;
using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Tests.UnitTests
{
    public partial class RemoteOrchestratorTests : IDisposable
    {

        /// <summary>
        /// RemoteOrchestrator.GetChanges() should return rows inserted on server, depending on the client scope sent.
        /// </summary>
        [Fact]
        public async Task RemoteOrchestratorGetChangesWithFiltersShouldReturnNewRowsInserted()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };
            var localOrchestrator = new LocalOrchestrator(this.clientProvider, options);
            var remoteOrchestrator = new RemoteOrchestrator(this.serverProvider, options);

            var scopeName = "scopesnap1";
            var setup = this.GetFilteredSetup();
            var rowsCount = this.serverProvider.GetDatabaseFilteredRowsCount();
            var parameters = this.GetFilterParameters();

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(this.clientProvider, this.serverProvider, options);

            // Making a first sync, will initialize everything we need
            var r = await agent.SynchronizeAsync(scopeName, setup, parameters);
            Assert.Equal(rowsCount, r.TotalChangesDownloadedFromServer);

            // Create a sales order header + 3 sales order details linked to the filter
            var products = await this.serverProvider.GetProductsAsync();
            var soh = await this.serverProvider.AddSalesOrderHeaderAsync(AdventureWorksContext.CustomerId1ForFilter);
            await this.serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            await this.serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            await this.serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);

            // Get changes from server
            var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName, parameters);
            var changes = await remoteOrchestrator.GetChangesAsync(cScopeInfoClient);

            Assert.NotNull(changes.ServerBatchInfo);
            Assert.NotNull(changes.ServerChangesSelected);
            Assert.Equal(2, changes.ServerChangesSelected.TableChangesSelected.Count);
            Assert.Contains("SalesOrderDetail", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
            Assert.Contains("SalesOrderHeader", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());

            var sodTable = localOrchestrator.LoadTableFromBatchInfo(scopeName, changes.ServerBatchInfo, "SalesOrderDetail", "SalesLT");
            Assert.Equal(3, sodTable.Rows.Count);

            var sohTable = localOrchestrator.LoadTableFromBatchInfo(scopeName, changes.ServerBatchInfo, "SalesOrderHeader", "SalesLT");
            Assert.Single(sohTable.Rows);
        }

        /// <summary>
        /// RemoteOrchestrator.GetChanges() should return rows inserted on server, depending on the client scope sent.
        /// </summary>
        [Fact]
        public async Task RemoteOrchestratorGetChangesShouldReturnNewRowsInserted()
        {
            var scopeName = "scopesnap1";

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(this.clientProvider, this.serverProvider);

            // Making a first sync, will initialize everything we need
            await agent.SynchronizeAsync(scopeName, this.setup);

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;
            var remoteOrchestrator = agent.RemoteOrchestrator;

            var productCategory = await this.serverProvider.AddProductCategoryAsync();
            var product = await this.serverProvider.AddProductAsync();

            // Get client scope
            var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName);

            // Get changes to be populated to the server
            var serverSyncChanges = await remoteOrchestrator.GetChangesAsync(cScopeInfoClient);

            Assert.NotNull(serverSyncChanges.ServerBatchInfo);
            Assert.NotNull(serverSyncChanges.ServerChangesSelected);
            Assert.Equal(2, serverSyncChanges.ServerChangesSelected.TableChangesSelected.Count);
            Assert.Contains("Product", serverSyncChanges.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
            Assert.Contains("ProductCategory", serverSyncChanges.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());

            var productTable = remoteOrchestrator.LoadTableFromBatchInfo(scopeName, serverSyncChanges.ServerBatchInfo, "Product", "SalesLT");
            var productRowName = productTable.Rows[0]["Name"];
            Assert.Equal(product.Name, productRowName);

            var productCategoryTable = remoteOrchestrator.LoadTableFromBatchInfo(scopeName, serverSyncChanges.ServerBatchInfo, "ProductCategory", "SalesLT");
            var productCategoryRowName = productCategoryTable.Rows[0]["Name"];
            Assert.Equal(productCategory.Name, productCategoryRowName);
        }

        /// <summary>
        /// RemoteOrchestrator.GetChanges() should return rows inserted on server, depending on the client scope sent.
        /// </summary>
        [Fact]
        public async Task RemoteOrchestratorHttpGetChangesWithFiltersShouldReturnNewRows()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };
            var scopeName = "scopesnap1";
            var setup = this.GetFilteredSetup();
            var rowsCount = this.serverProvider.GetDatabaseFilteredRowsCount();
            var parameters = this.GetFilterParameters();

            // Create a kestrel server
            var kestrel = new KestrelTestServer(false);

            // configure server orchestrator
            kestrel.AddSyncServer(this.serverProvider, setup, options, null, scopeName);

            var serviceUri = kestrel.Run();

            var remoteOrchestrator = new WebRemoteOrchestrator(serviceUri);

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(this.clientProvider, remoteOrchestrator, options);

            // Making a first sync, will initialize everything we need
            var r = await agent.SynchronizeAsync(scopeName, parameters);
            Assert.Equal(rowsCount, r.TotalChangesDownloadedFromServer);

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;

            // Create a sales order header + 3 sales order details linked to the filter
            var products = await this.serverProvider.GetProductsAsync();
            var soh = await this.serverProvider.AddSalesOrderHeaderAsync(AdventureWorksContext.CustomerId1ForFilter);
            await this.serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            await this.serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            await this.serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);

            // Get changes from server
            var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName, parameters);
            var changes = await remoteOrchestrator.GetChangesAsync(cScopeInfoClient);

            Assert.NotNull(changes.ServerBatchInfo);
            Assert.NotNull(changes.ServerChangesSelected);
            Assert.Equal(2, changes.ServerChangesSelected.TableChangesSelected.Count);
            Assert.Contains("SalesOrderDetail", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
            Assert.Contains("SalesOrderHeader", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());

            var sodTable = localOrchestrator.LoadTableFromBatchInfo(scopeName, changes.ServerBatchInfo, "SalesOrderDetail", "SalesLT");
            Assert.Equal(3, sodTable.Rows.Count);

            var sohTable = localOrchestrator.LoadTableFromBatchInfo(scopeName, changes.ServerBatchInfo, "SalesOrderHeader", "SalesLT");
            Assert.Single(sohTable.Rows);
        }

        [Fact]
        public async Task RemoteOrchestratorHttpGetChangesWithFiltersShouldReturnDeletedRowsCount()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };
            var scopeName = "scopesnap1";
            var setup = this.GetFilteredSetup();
            var rowsCount = this.serverProvider.GetDatabaseFilteredRowsCount();
            var parameters = this.GetFilterParameters();

            // Create a kestrel server
            var kestrel = new KestrelTestServer(false);

            // configure server orchestrator
            kestrel.AddSyncServer(this.serverProvider, setup, options, null, scopeName);
            var serviceUri = kestrel.Run();

            var remoteOrchestrator = new WebRemoteOrchestrator(serviceUri);

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(this.clientProvider, remoteOrchestrator, options);

            var r1 = await agent.SynchronizeAsync(scopeName, parameters);
            Assert.Equal(rowsCount, r1.TotalChangesDownloadedFromServer);

            // Making a first sync, will initialize everything we need

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;

            // Create a sales order header + 3 sales order details linked to the filter
            var products = await this.serverProvider.GetProductsAsync();
            var soh = await this.serverProvider.AddSalesOrderHeaderAsync(AdventureWorksContext.CustomerId1ForFilter);
            var sod1 = await this.serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            var sod2 = await this.serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            var sod3 = await this.serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);

            // Making a second sync, with these new rows
            var r = await agent.SynchronizeAsync(scopeName, parameters);
            Assert.Equal(4, r.TotalChangesDownloadedFromServer);

            // now delete these lines on server
            await this.serverProvider.DeleteSalesOrderDetailAsync(sod1.SalesOrderDetailId);
            await this.serverProvider.DeleteSalesOrderDetailAsync(sod2.SalesOrderDetailId);
            await this.serverProvider.DeleteSalesOrderDetailAsync(sod3.SalesOrderDetailId);
            await this.serverProvider.DeleteSalesOrderHeaderAsync(soh.SalesOrderId);

            // Get changes from server
            var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName, parameters);
            var changes = await remoteOrchestrator.GetChangesAsync(cScopeInfoClient);

            Assert.NotNull(changes.ServerBatchInfo);
            Assert.NotNull(changes.ServerChangesSelected);
            Assert.Equal(2, changes.ServerChangesSelected.TableChangesSelected.Count);
            Assert.Equal(4, changes.ServerChangesSelected.TableChangesSelected.Sum(tcs => tcs.Deletes));
            Assert.Equal(0, changes.ServerChangesSelected.TableChangesSelected.Sum(tcs => tcs.Upserts));
            Assert.Contains("SalesOrderDetail", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
            Assert.Contains("SalesOrderHeader", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());

            // testing with SyncRowState
            var sodTable = localOrchestrator.LoadTableFromBatchInfo(scopeName, changes.ServerBatchInfo, "SalesOrderDetail", "SalesLT", SyncRowState.Deleted);
            Assert.Equal(3, sodTable.Rows.Count);
            foreach (var row in sodTable.Rows)
                Assert.Equal(SyncRowState.Deleted, row.RowState);

            // testing with SyncRowState
            var sodTable2 = localOrchestrator.LoadTableFromBatchInfo(scopeName, changes.ServerBatchInfo, "SalesOrderDetail", "SalesLT", default);
            Assert.Equal(3, sodTable2.Rows.Count);
            foreach (var row in sodTable2.Rows)
                Assert.Equal(SyncRowState.Deleted, row.RowState);

            // testing with SyncRowState
            var sodTable3 = localOrchestrator.LoadTableFromBatchInfo(scopeName, changes.ServerBatchInfo, "SalesOrderDetail", "SalesLT", SyncRowState.Modified);
            Assert.Empty(sodTable3.Rows);

            // testing with SyncRowState that is not valid
            var sodTable4 = localOrchestrator.LoadTableFromBatchInfo(scopeName, changes.ServerBatchInfo, "SalesOrderDetail", "SalesLT", SyncRowState.None);
            Assert.Empty(sodTable4.Rows);

            // testing without SyncRowState
            var sohTable = localOrchestrator.LoadTableFromBatchInfo(scopeName, changes.ServerBatchInfo, "SalesOrderHeader", "SalesLT");
            Assert.Single(sohTable.Rows);
            foreach (var row in sohTable.Rows)
                Assert.Equal(SyncRowState.Deleted, row.RowState);
        }
    }
}