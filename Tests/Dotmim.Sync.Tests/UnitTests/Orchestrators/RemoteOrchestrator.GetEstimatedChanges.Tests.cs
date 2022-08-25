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
            var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName);
            var changes = await remoteOrchestrator.GetEstimatedChangesCountAsync(cScopeInfoClient, parameters: parameters);

            Assert.Null(changes.ServerBatchInfo);
            Assert.NotNull(changes.ServerChangesSelected);
            Assert.Equal(2, changes.ServerChangesSelected.TableChangesSelected.Count);
            Assert.Contains("SalesOrderDetail", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
            Assert.Contains("SalesOrderHeader", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());

        }


        [Fact]
        public async Task RemoteOrchestrator_GetEstimatedChanges_AfterInitialize_ShouldReturnRowsCount()
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
            var dbNameSrv = HelperDatabase.GetRandomName("tcp_lo_srv");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbNameSrv, true);

            var csServer = HelperDatabase.GetConnectionString(ProviderType.Sql, dbNameSrv);
            var serverProvider = new SqlSyncProvider(csServer);

            await new AdventureWorksContext((dbNameSrv, ProviderType.Sql, serverProvider), true, false).Database.EnsureCreatedAsync();

            var scopeName = "scopesnap1";
            var setup = new SyncSetup(this.Tables);

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, new SyncOptions());

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
            var changes = await remoteOrchestrator.GetEstimatedChangesCountAsync(cScopeInfoClient, parameters);

            Assert.Null(changes.ServerBatchInfo);
            Assert.NotNull(changes.ServerChangesSelected);
            Assert.Equal(2, changes.ServerChangesSelected.TableChangesSelected.Count);
            Assert.Contains("SalesOrderDetail", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
            Assert.Contains("SalesOrderHeader", changes.ServerChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());

        }

        [Fact]
        public async Task RemoteOrchestrator_HttpGetEstimatedChanges_WithFilters_ShouldReturnDeletedRowsCount()
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
            var changes = await remoteOrchestrator.GetEstimatedChangesCountAsync(cScopeInfoClient, parameters);

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
