using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests.UnitTests
{
    public partial class InterceptorsTests
    {
        [Fact]
        public async Task LocalOrchestrator_MetadataCleaning()
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
            var s = await agent.SynchronizeAsync(this.Tables, scopeName);

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

            var cleaning = 0;
            var cleaned = 0;

            localOrchestrator.OnMetadataCleaning(args =>
            {
                cleaning++;
            });

            localOrchestrator.OnMetadataCleaned(args =>
            {
                cleaned++;
                Assert.Equal(0, args.DatabaseMetadatasCleaned.RowsCleanedCount);
                Assert.Empty(args.DatabaseMetadatasCleaned.Tables);

            });

            // Making a first sync, will call cleaning, but nothing is cleaned (still interceptors are called)
            var s2 = await agent.SynchronizeAsync(scopeName);

            Assert.Equal(1, cleaning);
            Assert.Equal(1, cleaned);

            // Reset interceptors
            localOrchestrator.OnMetadataCleaning(null);
            localOrchestrator.OnMetadataCleaned(null);
            cleaning = 0;
            cleaned = 0;


            localOrchestrator.OnMetadataCleaning(args =>
            {
                cleaning++;
            });

            localOrchestrator.OnMetadataCleaned(args =>
            {
                cleaned++;
            });

            // Making a second empty sync.
            var s3 = await agent.SynchronizeAsync(scopeName);

            // If there is no changes on any tables, no metadata cleaning is called
            Assert.Equal(0, cleaning);
            Assert.Equal(0, cleaned);


            // Server side : Create a product category 
            productCategoryName = HelperDatabase.GetRandomName();
            productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

            using (var ctx = new AdventureWorksContext((dbNameSrv, ProviderType.Sql, serverProvider)))
            {
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);

                await ctx.SaveChangesAsync();
            }

            // Reset interceptors
            localOrchestrator.OnMetadataCleaning(null);
            localOrchestrator.OnMetadataCleaned(null);
            cleaning = 0;
            cleaned = 0;


            localOrchestrator.OnMetadataCleaning(args =>
            {
                cleaning++;
            });

            localOrchestrator.OnMetadataCleaned(args =>
            {
                cleaned++;
                Assert.Equal(1, args.DatabaseMetadatasCleaned.RowsCleanedCount);
                Assert.Single(args.DatabaseMetadatasCleaned.Tables);
                Assert.Equal("SalesLT", args.DatabaseMetadatasCleaned.Tables[0].SchemaName);
                Assert.Equal("ProductCategory", args.DatabaseMetadatasCleaned.Tables[0].TableName);
                Assert.Equal(1, args.DatabaseMetadatasCleaned.Tables[0].RowsCleanedCount);

            });
            var s4 = await agent.SynchronizeAsync(scopeName);

            Assert.Equal(1, cleaning);
            Assert.Equal(1, cleaned);


            // Server side : Create a product category and a product
            // Create a productcategory item
            // Create a new product on server
            productId = Guid.NewGuid();
            productName = HelperDatabase.GetRandomName();
            productNumber = productName.ToUpperInvariant().Substring(0, 10);

            productCategoryName = HelperDatabase.GetRandomName();
            productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

            using (var ctx = new AdventureWorksContext((dbNameSrv, ProviderType.Sql, serverProvider)))
            {
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);

                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber };
                ctx.Add(product);

                await ctx.SaveChangesAsync();
            }

            // Reset interceptors
            localOrchestrator.OnMetadataCleaning(null);
            localOrchestrator.OnMetadataCleaned(null);
            cleaning = 0;
            cleaned = 0;


            localOrchestrator.OnMetadataCleaning(args =>
            {
                cleaning++;
            });

            localOrchestrator.OnMetadataCleaned(args =>
            {
                cleaned++;
                Assert.Equal(0, args.DatabaseMetadatasCleaned.RowsCleanedCount);
            });

            var s5 = await agent.SynchronizeAsync(scopeName);

            // cleaning is always called on N-1 rows, so nothing here should be called
            Assert.Equal(1, cleaning);
            Assert.Equal(1, cleaned);


            HelperDatabase.DropDatabase(ProviderType.Sql, dbNameSrv);
            HelperDatabase.DropDatabase(ProviderType.Sql, dbNameCli);
        }
    }
}
