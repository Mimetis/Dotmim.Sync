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



        /// <summary>
        /// RemoteOrchestrator.GetChanges() should return rows inserted on server, depending on the client scope sent
        /// </summary>
        [Fact]
        public async Task RemoteOrchestrator_GetChanges_WithFilters_ShouldReturnNewRowsInserted()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };
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

            // Create a sales order header + 3 sales order details linked to the filter
            var products = await serverProvider.GetProductsAsync();
            var soh = await serverProvider.AddSalesOrderHeaderAsync(AdventureWorksContext.CustomerId1ForFilter);
            await serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            await serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            await serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);


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
        /// RemoteOrchestrator.GetChanges() should return rows inserted on server, depending on the client scope sent
        /// </summary>
        [Fact]
        public async Task RemoteOrchestrator_GetChanges_ShouldReturnNewRowsInserted()
        {
            var scopeName = "scopesnap1";

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider);

            // Making a first sync, will initialize everything we need
            await agent.SynchronizeAsync(scopeName, setup);

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;
            var remoteOrchestrator = agent.RemoteOrchestrator;

            var productCategory = await serverProvider.AddProductCategoryAsync();
            var product = await serverProvider.AddProductAsync();

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
        /// RemoteOrchestrator.GetChanges() should return rows inserted on server, depending on the client scope sent
        /// </summary>
        [Fact]
        public async Task RemoteOrchestrator_HttpGetChanges_WithFilters_ShouldReturnNewRows()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };
            var scopeName = "scopesnap1";
            var setup = GetFilteredSetup();
            var rowsCount = serverProvider.GetDatabaseFilteredRowsCount();
            var parameters = GetFilterParameters();

            // Create a kestrell server
            var kestrell = new KestrellTestServer(false);

            // configure server orchestrator
            kestrell.AddSyncServer(serverProvider.GetType(), serverProvider.ConnectionString, setup, options, null, scopeName);

            var serviceUri = kestrell.Run();

            var remoteOrchestrator = new WebRemoteOrchestrator(serviceUri);

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, remoteOrchestrator, options);

            // Making a first sync, will initialize everything we need
            var r = await agent.SynchronizeAsync(scopeName, parameters);
            Assert.Equal(rowsCount, r.TotalChangesDownloadedFromServer);

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;

            // Create a sales order header + 3 sales order details linked to the filter
            var products = await serverProvider.GetProductsAsync();
            var soh = await serverProvider.AddSalesOrderHeaderAsync(AdventureWorksContext.CustomerId1ForFilter);
            await serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            await serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            await serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);

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
        public async Task RemoteOrchestrator_HttpGetChanges_WithFilters_ShouldReturnDeletedRowsCount()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };
            var scopeName = "scopesnap1";
            var setup = GetFilteredSetup();
            var rowsCount = serverProvider.GetDatabaseFilteredRowsCount();
            var parameters = GetFilterParameters();

            // Create a kestrell server
            var kestrell = new KestrellTestServer(false);

            // configure server orchestrator
            kestrell.AddSyncServer(serverProvider.GetType(), serverProvider.ConnectionString, setup, options, null, scopeName);
            var serviceUri = kestrell.Run();

            var remoteOrchestrator = new WebRemoteOrchestrator(serviceUri);

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, remoteOrchestrator, options);

            var r1 = await agent.SynchronizeAsync(scopeName, parameters);
            Assert.Equal(rowsCount, r1.TotalChangesDownloadedFromServer);
            // Making a first sync, will initialize everything we need

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;

            // Create a sales order header + 3 sales order details linked to the filter
            var products = await serverProvider.GetProductsAsync();
            var soh = await serverProvider.AddSalesOrderHeaderAsync(AdventureWorksContext.CustomerId1ForFilter);
            var sod1 = await serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            var sod2 = await serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            var sod3 = await serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);

            // Making a second sync, with these new rows
            var r = await agent.SynchronizeAsync(scopeName, parameters);
            Assert.Equal(4, r.TotalChangesDownloadedFromServer);

            // now delete these lines on server
            await serverProvider.DeleteSalesOrderDetailAsync(sod1.SalesOrderDetailId);
            await serverProvider.DeleteSalesOrderDetailAsync(sod2.SalesOrderDetailId);
            await serverProvider.DeleteSalesOrderDetailAsync(sod3.SalesOrderDetailId);
            await serverProvider.DeleteSalesOrderHeaderAsync(soh.SalesOrderId);

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
