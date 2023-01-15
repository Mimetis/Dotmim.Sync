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
    public partial class LocalOrchestratorTests : IDisposable
    {
        /// <summary>
        /// LocalOrchestrator.GetEstimatedChanges should return estimated rows to send back to the server
        /// </summary>
        [Fact]
        public async Task LocalOrchestrator_GetEstimatedChanges_AfterInitialized_ShouldReturnEstimatedRowsCount()
        {
            var scopeName = "scopesnap1";

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider, options);

            // Making a first sync, will initialize everything we need
            var s = await agent.SynchronizeAsync(scopeName, setup);

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;
            var remoteOrchestrator = agent.RemoteOrchestrator;

            // Client side : Create a product category and a product
            // Create a productcategory item
            // Create a new product on server
            var productCategory = await clientProvider.AddProductCategoryAsync();
            var product = await clientProvider.AddProductAsync();

            // Get changes to be populated to the server
            var scopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName);

            var changes = await localOrchestrator.GetEstimatedChangesCountAsync(scopeInfoClient);

            Assert.NotNull(changes.ClientChangesSelected);
            Assert.Equal(2, changes.ClientChangesSelected.TableChangesSelected.Count);
            Assert.Contains("Product", changes.ClientChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
            Assert.Contains("ProductCategory", changes.ClientChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
        }

        /// <summary>
        /// LocalOrchestrator.GetEstimatedChanges should return estimated rows to send back to the server
        /// </summary>
        [Fact]
        public async Task LocalOrchestrator_GetEstimatedChanges_BeforeInitialized_ShouldReturnNull_IfScopeInfoIsNotYetCreated()
        {
            var scopeName = "scopesnap1";

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider, options);

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;

            // Client side : Create a product category and a product
            // Create a productcategory item
            // Create a new product on server
            var productCategory = await clientProvider.AddProductCategoryAsync();
            var product = await clientProvider.AddProductAsync();

            // Get changes to be populated to the server
            // Since we are new and not yet initialized, no rows are marked to be sent
            var scopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName);

            var estimated = await localOrchestrator.GetEstimatedChangesCountAsync(scopeInfoClient);

            Assert.Null(estimated);
        }

        /// <summary>
        /// LocalOrchestrator.GetEstimatedChanges should return estimated rows to send back to the server
        /// </summary>
        [Fact]
        public async Task LocalOrchestrator_GetEstimatedChanges_WithFilters_ShouldReturnNewRowsInserted()
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

            // Get the orchestrators

            // Client side : Create a sales order header + 3 sales order details linked to the filter
            var products = await clientProvider.GetProductsAsync();
            var soh = await clientProvider.AddSalesOrderHeaderAsync(AdventureWorksContext.CustomerId1ForFilter);
            await clientProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            await clientProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);
            await clientProvider.AddSalesOrderDetailAsync(soh.SalesOrderId, products[0].ProductId);

            // Get changes to be populated to the server
            var scopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName, parameters);
            var changes = await localOrchestrator.GetEstimatedChangesCountAsync(scopeInfoClient);

            Assert.Null(changes.ClientBatchInfo);
            Assert.NotNull(changes.ClientChangesSelected);
            Assert.Equal(2, changes.ClientChangesSelected.TableChangesSelected.Count);
            Assert.Contains("SalesOrderDetail", changes.ClientChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
            Assert.Contains("SalesOrderHeader", changes.ClientChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
        }


    }
}
