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
        public async Task RemoteOrchestrator_GetEstimatedChanges_WithFilters_ShouldReturnNewRowsCount()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };
            var scopeName = "scopesnap1";
            var setup = GetFilteredSetup();
            var rowsCount = serverProvider.GetDatabaseFilteredRowsCount();
            var parameters = GetFilterParameters();

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider, options);
            
            // Making a first sync, will initialize everything we need
            var r = await agent.SynchronizeAsync(scopeName, setup, parameters);
            Assert.Equal(rowsCount, r.TotalChangesDownloadedFromServer);

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;
            var remoteOrchestrator = agent.RemoteOrchestrator;

            var products = await serverProvider.GetProductsAsync();
            var soh = await serverProvider.AddSalesOrderHeaderAsync(AdventureWorksContext.CustomerId1ForFilter);
            var sod1 = await serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            var sod2 = await serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            var sod3 = await serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);

            // Get changes from server
            var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName, parameters);
            var changes = await remoteOrchestrator.GetEstimatedChangesCountAsync(cScopeInfoClient);

            Assert.Null(changes.ServerBatchInfo);
            Assert.NotNull(changes.ServerChangesSelected);
            Assert.Equal(2, changes.ServerChangesSelected.TableChangesSelected.Count);
            Assert.Contains("SalesOrderDetail", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
            Assert.Contains("SalesOrderHeader", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());

        }


        [Fact]
        public async Task RemoteOrchestrator_GetEstimatedChanges_AfterInitialize_ShouldReturnRowsCount()
        {
            var scopeName = "scopesnap1";

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider, options);

            // Making a first sync, will initialize everything we need
            await agent.SynchronizeAsync(scopeName, setup);

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;
            var remoteOrchestrator = agent.RemoteOrchestrator;

            var productCategory = await serverProvider.AddProductCategoryAsync();
            var product = await serverProvider.AddProductAsync();


            // Get client scope
            var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName);

            // Get the estimated changes count to be applied to the client
            var changes = await remoteOrchestrator.GetEstimatedChangesCountAsync(cScopeInfoClient);

            Assert.NotNull(changes.ServerChangesSelected);
            Assert.Equal(2, changes.ServerChangesSelected.TableChangesSelected.Count);
            Assert.Contains("Product", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
            Assert.Contains("ProductCategory", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
        }

        [Fact]

        public async Task RemoteOrchestrator_GetEstimatedChanges_BeforeInitialize_ShouldReturnRowsCount()
        {
            var scopeName = "scopesnap1";

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var productCategory = await serverProvider.AddProductCategoryAsync();
            var product = await serverProvider.AddProductAsync();

            var serverScope = await remoteOrchestrator.GetScopeInfoAsync(scopeName, setup);

            await remoteOrchestrator.ProvisionAsync(serverScope);

            // fake client scope
            var clientScopeInfo = new ScopeInfoClient()
            {
                Name = scopeName,
                IsNewScope = true,
                Id = Guid.NewGuid(),
                Hash = SyncParameters.DefaultScopeHash,
            };


            // Get estimated changes count to be sent to the client
            var changes = await remoteOrchestrator.GetEstimatedChangesCountAsync(clientScopeInfo);

            Assert.NotNull(changes.ServerChangesSelected);
            Assert.Equal(2, changes.ServerChangesSelected.TableChangesSelected.Count);
            Assert.Contains("Product", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
            Assert.Contains("ProductCategory", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
        }

        [Fact]
        public async Task RemoteOrchestrator_HttpGetEstimatedChanges_WithFilters_ShouldReturnNewRowsCount()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };
            var scopeName = "scopesnap1";
            var setup = GetFilteredSetup();
            var rowsCount = serverProvider.GetDatabaseFilteredRowsCount();
            var parameters = GetFilterParameters();

            // Create a kestrell server
            var kestrell = new KestrellTestServer(false);

            // configure server orchestrator
            kestrell.AddSyncServer(serverProvider.GetType(), serverProvider.ConnectionString, scopeName, setup);
            var serviceUri = kestrell.Run();


            var remoteOrchestrator = new WebRemoteOrchestrator(serviceUri);

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, remoteOrchestrator, options);

            // Making a first sync, will initialize everything we need
            var r = await agent.SynchronizeAsync(scopeName, parameters);
            Assert.Equal(rowsCount, r.TotalChangesDownloadedFromServer);

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;

            // Server side : Create a sales order header + 3 sales order details linked to the filter
            var products = await serverProvider.GetProductsAsync();
            var soh = await serverProvider.AddSalesOrderHeaderAsync(AdventureWorksContext.CustomerId1ForFilter);
            await serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            await serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            await serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);

            // Get changes from server
            var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName, parameters);
            var changes = await remoteOrchestrator.GetEstimatedChangesCountAsync(cScopeInfoClient);

            Assert.Null(changes.ServerBatchInfo);
            Assert.NotNull(changes.ServerChangesSelected);
            Assert.Equal(2, changes.ServerChangesSelected.TableChangesSelected.Count);
            Assert.Contains("SalesOrderDetail", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
            Assert.Contains("SalesOrderHeader", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());

        }

        [Fact]
        public async Task RemoteOrchestrator_HttpGetEstimatedChanges_WithFilters_ShouldReturnDeletedRowsCount()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };
            var scopeName = "scopesnap1";
            var setup = GetFilteredSetup();
            var rowsCount = serverProvider.GetDatabaseFilteredRowsCount();
            var parameters = GetFilterParameters();

            // Create a kestrell server
            var kestrell = new KestrellTestServer(false);

            // configure server orchestrator
            kestrell.AddSyncServer(serverProvider.GetType(), serverProvider.ConnectionString, scopeName, setup);
            var serviceUri = kestrell.Run();

            var remoteOrchestrator = new WebRemoteOrchestrator(serviceUri);

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, remoteOrchestrator, options);

            // Making a first sync, will initialize everything we need
            var r = await agent.SynchronizeAsync(scopeName, parameters);
            Assert.Equal(rowsCount, r.TotalChangesDownloadedFromServer);

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;

            var products = await serverProvider.GetProductsAsync();
            var soh = await serverProvider.AddSalesOrderHeaderAsync(AdventureWorksContext.CustomerId1ForFilter);
            var sod1 = await serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            var sod2 = await serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            var sod3 = await serverProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);

            // Making a second sync, with these new rows
            r = await agent.SynchronizeAsync(scopeName, parameters);
            Assert.Equal(4, r.TotalChangesDownloadedFromServer);

            // now delete these lines on server
            await serverProvider.DeleteSalesOrderDetailAsync(sod1.SalesOrderDetailId);
            await serverProvider.DeleteSalesOrderDetailAsync(sod2.SalesOrderDetailId);
            await serverProvider.DeleteSalesOrderDetailAsync(sod3.SalesOrderDetailId);
            await serverProvider.DeleteSalesOrderHeaderAsync(soh.SalesOrderId);

            // Get changes from server
            var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName, parameters);
            var changes = await remoteOrchestrator.GetEstimatedChangesCountAsync(cScopeInfoClient);

            Assert.Null(changes.ServerBatchInfo);
            Assert.NotNull(changes.ServerChangesSelected);
            Assert.Equal(2, changes.ServerChangesSelected.TableChangesSelected.Count);
            Assert.Equal(4, changes.ServerChangesSelected.TableChangesSelected.Sum(tcs => tcs.Deletes));
            Assert.Equal(0, changes.ServerChangesSelected.TableChangesSelected.Sum(tcs => tcs.Upserts));
            Assert.Contains("SalesOrderDetail", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
            Assert.Contains("SalesOrderHeader", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());

        }


    }
}
