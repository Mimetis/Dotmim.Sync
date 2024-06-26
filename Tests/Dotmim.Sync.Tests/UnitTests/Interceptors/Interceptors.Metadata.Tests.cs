using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
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
            var scopeName = "scopesnap1";

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider, options);

            // Making a first sync, will initialize everything we need
            var s = await agent.SynchronizeAsync(scopeName, setup);

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;
            var remoteOrchestrator = agent.RemoteOrchestrator;

            // Server side : Create a product category and a product
            await serverProvider.AddProductCategoryAsync();
            await serverProvider.AddProductAsync();

            // get rows count
            var allRowsCount = serverProvider.GetDatabaseRowsCount();
            var allProductCategories = await serverProvider.GetProductCategoriesAsync();
            var allProducts = await serverProvider.GetProductsAsync();


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
            localOrchestrator.ClearInterceptors();

            // Server side : Create a product category 
            await serverProvider.AddProductCategoryAsync();

            cleaning = 0;
            cleaned = 0;

            // in this clean up, only initial rows are cleaned
            localOrchestrator.OnMetadataCleaning(args => cleaning++);
            localOrchestrator.OnMetadataCleaned(args =>
            {
                var cleanUpProductCategory = args.DatabaseMetadatasCleaned?.Tables.FirstOrDefault(dmc => dmc.TableName == "ProductCategory");
                var cleanUpProduct = args.DatabaseMetadatasCleaned?.Tables.FirstOrDefault(dmc => dmc.TableName == "Product");

                Assert.Equal(allRowsCount - 2, args.DatabaseMetadatasCleaned.RowsCleanedCount);
                Assert.Equal(allProductCategories.Count - 1, cleanUpProductCategory.RowsCleanedCount);
                Assert.Equal(allProducts.Count - 1, cleanUpProduct.RowsCleanedCount);
                cleaned++;
            });

            // Making a second empty sync.
            var s3 = await agent.SynchronizeAsync(scopeName);

            // Cleaning is called
            Assert.Equal(1, cleaning);
            Assert.Equal(1, cleaned);

            // Server side : Create a product category 
            await serverProvider.AddProductCategoryAsync();

            // Reset interceptors
            localOrchestrator.ClearInterceptors();

            cleaning = 0;
            cleaned = 0;

            // in this clean up, the product category row and product row are cleaned
            localOrchestrator.OnMetadataCleaning(args =>cleaning++);
            localOrchestrator.OnMetadataCleaned(args =>
            {
                cleaned++;
                var cleanUpProductCategory = args.DatabaseMetadatasCleaned?.Tables.FirstOrDefault(dmc => dmc.TableName == "ProductCategory");
                var cleanUpProduct = args.DatabaseMetadatasCleaned?.Tables.FirstOrDefault(dmc => dmc.TableName == "Product");

                Assert.Equal(2, args.DatabaseMetadatasCleaned.RowsCleanedCount);
                Assert.Equal(1, cleanUpProductCategory.RowsCleanedCount);
                Assert.Equal(1, cleanUpProduct.RowsCleanedCount);
            });
            var s4 = await agent.SynchronizeAsync(scopeName);

            Assert.Equal(1, cleaning);
            Assert.Equal(1, cleaned);

            // Server side : Create a product category and a product
            await serverProvider.AddProductCategoryAsync();
            await serverProvider.AddProductAsync();

            // Reset interceptors
            localOrchestrator.ClearInterceptors();
            cleaning = 0;
            cleaned = 0;

            // in this clean up the product category row is cleaned
            localOrchestrator.OnMetadataCleaning(args =>cleaning++);
            localOrchestrator.OnMetadataCleaned(args =>
            {
                cleaned++;
                var cleanUpProductCategory = args.DatabaseMetadatasCleaned?.Tables.FirstOrDefault(dmc => dmc.TableName == "ProductCategory");
                var cleanUpProduct = args.DatabaseMetadatasCleaned?.Tables.FirstOrDefault(dmc => dmc.TableName == "Product");

                Assert.Equal(1, args.DatabaseMetadatasCleaned.RowsCleanedCount);
                Assert.Equal(1, cleanUpProductCategory.RowsCleanedCount);
                Assert.Null(cleanUpProduct);
            });

            var s5 = await agent.SynchronizeAsync(scopeName);

            // cleaning is always called on N-1 rows, so nothing here should be called
            Assert.Equal(1, cleaning);
            Assert.Equal(1, cleaned);
        }
    }
}
