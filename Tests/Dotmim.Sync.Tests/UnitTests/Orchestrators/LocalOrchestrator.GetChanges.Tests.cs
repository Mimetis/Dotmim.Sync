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
        /// <summary>
        /// RemoteOrchestrator.GetChanges() should return rows inserted on server, depending on the client scope sent
        /// </summary>
        //[Fact]
        //public async Task LocalOrchestrator_GetChanges_ShouldReturnNewRowsInserted()
        //{
        //    var dbNameSrv = HelperDatabase.GetRandomName("tcp_lo_srv");
        //    await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbNameSrv, true);

        //    var dbNameCli = HelperDatabase.GetRandomName("tcp_lo_cli");
        //    await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbNameCli, true);

        //    var csServer = HelperDatabase.GetConnectionString(ProviderType.Sql, dbNameSrv);
        //    var serverProvider = new SqlSyncProvider(csServer);

        //    var csClient = HelperDatabase.GetConnectionString(ProviderType.Sql, dbNameCli);
        //    var clientProvider = new SqlSyncProvider(csClient);

        //    await new AdventureWorksContext((dbNameSrv, ProviderType.Sql, serverProvider), true, false).Database.EnsureCreatedAsync();
        //    await new AdventureWorksContext((dbNameCli, ProviderType.Sql, clientProvider), true, false).Database.EnsureCreatedAsync();

        //    var scopeName = "scopesnap1";
        //    var syncOptions = new SyncOptions();
        //    var setup = new SyncSetup();

        //    // Make a first sync to be sure everything is in place
        //    var agent = new SyncAgent(clientProvider, serverProvider, this.Tables, scopeName);

        //    // Making a first sync, will initialize everything we need
        //    await agent.SynchronizeAsync();

        //    // Get the orchestrators
        //    var localOrchestrator = agent.LocalOrchestrator;
        //    var remoteOrchestrator = agent.RemoteOrchestrator;

        //    // Server side : Create a product category and a product
        //    // Create a productcategory item
        //    // Create a new product on server
        //    var productId = Guid.NewGuid();
        //    var productName = HelperDatabase.GetRandomName();
        //    var productNumber = productName.ToUpperInvariant().Substring(0, 10);

        //    var productCategoryName = HelperDatabase.GetRandomName();
        //    var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

        //    using (var ctx = new AdventureWorksContext((dbNameCli, ProviderType.Sql, clientProvider)))
        //    {
        //        var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
        //        ctx.Add(pc);

        //        var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber };
        //        ctx.Add(product);

        //        await ctx.SaveChangesAsync();
        //    }

        //    // Get changes to be populated to the server
        //    var changes = await localOrchestrator.GetChangesAsync();

        //    Assert.NotNull(changes.ClientBatchInfo);
        //    Assert.NotNull(changes.ClientChangesSelected);
        //    Assert.Equal(2, changes.ClientChangesSelected.TableChangesSelected.Count);
        //    Assert.Contains("Product", changes.ClientChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());
        //    Assert.Contains("ProductCategory", changes.ClientChangesSelected.TableChangesSelected.Select(tcs => tcs.TableName).ToList());

        //    var productTable = changes.ClientBatchInfo.InMemoryData.Tables["Product", "SalesLT"];
        //    var productRowName = productTable.Rows[0]["Name"];

        //    Assert.Equal(productName, productRowName);

        //    var productCategoryTable = changes.ClientBatchInfo.InMemoryData.Tables["ProductCategory", "SalesLT"];
        //    var productCategoryRowName = productCategoryTable.Rows[0]["Name"];

        //    Assert.Equal(productCategoryName, productCategoryRowName);

        //}

    }
}
