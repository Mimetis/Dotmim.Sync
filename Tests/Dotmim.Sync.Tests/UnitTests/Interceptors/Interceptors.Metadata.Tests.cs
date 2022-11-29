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

        private void TestReader(DbConnection connection, DbTransaction transaction, string add)
        {
            var sqlCommand = new SqlCommand("Select * from SalesLT.ProductCategory_tracking")
            {
                Connection = connection as SqlConnection,
                Transaction = transaction as SqlTransaction,
                CommandType = CommandType.Text
            };

            var syncTable = new SyncTable();

            using var reader = sqlCommand.ExecuteReader();

            syncTable.Load(reader);

            Console.WriteLine("ProductCategory tracking rows");
            Debug.WriteLine("ProductCategory tracking rows");

            foreach (var row in syncTable.Rows)
            {
                Debug.WriteLine($"{add}{row}");
                Console.WriteLine($"{add}{row}");
            }

            sqlCommand = new SqlCommand("Select * from SalesLT.Product_tracking")
            {
                Connection = connection as SqlConnection,
                Transaction = transaction as SqlTransaction,
                CommandType = CommandType.Text
            };

            syncTable = new SyncTable();

            reader.Close();

            using var reader2 = sqlCommand.ExecuteReader();

            syncTable.Load(reader2);

            Console.WriteLine("Product tracking rows");
            Debug.WriteLine("Product tracking rows");

            foreach (var row in syncTable.Rows)
            {
                Debug.WriteLine($"{add}{row}");
                Console.WriteLine($"{add}{row}");
            }
            reader2.Close();
        }

        [Fact]
        public async Task LocalOrchestrator_MetadataCleaning()
        {
            var dbNameSrv = HelperDatabase.GetRandomName("tcp_lo_srv_mc");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbNameSrv, true);

            var dbNameCli = HelperDatabase.GetRandomName("tcp_lo_cli_mc");
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
            var s = await agent.SynchronizeAsync(scopeName, this.Tables);

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

            var ts = await localOrchestrator.GetLocalTimestampAsync();
            Console.WriteLine($"Pass 1. TS:{ts}");
            Debug.WriteLine($"Pass 1. TS:{ts}");


            var cleaning = 0;
            var cleaned = 0;

            localOrchestrator.OnMetadataCleaning(args =>
            {
                cleaning++;
                Console.WriteLine($"Pass 1. {args.Message}");
                Debug.WriteLine($"Pass 1. {args.Message}");
                TestReader(args.Connection, args.Transaction, "OnMetadataCleaning Pass 1:");
            });

            localOrchestrator.OnMetadataCleaned(args =>
            {
                cleaned++;
                Assert.Equal(0, args.DatabaseMetadatasCleaned.RowsCleanedCount);
                Assert.Empty(args.DatabaseMetadatasCleaned.Tables);
                Console.WriteLine($"Pass 1. {args.Message}");
                Debug.WriteLine($"Pass 1. {args.Message}");
                TestReader(args.Connection, args.Transaction, "OnMetadataCleaned Pass 1:");

            });



            // Making a first sync, will call cleaning, but nothing is cleaned (still interceptors are called)
            var s2 = await agent.SynchronizeAsync(scopeName);

            Assert.Equal(1, cleaning);
            Assert.Equal(1, cleaned);

            // Reset interceptors
            localOrchestrator.ClearInterceptors();

            cleaning = 0;
            cleaned = 0;

            ts = await localOrchestrator.GetLocalTimestampAsync();
            Console.WriteLine($"Pass 2. TS:{ts}");
            Debug.WriteLine($"Pass 2. TS:{ts}");


            localOrchestrator.OnMetadataCleaning(args =>
            {
                cleaning++;
                Console.WriteLine($"Pass 2. {args.Message}");
                Debug.WriteLine($"Pass 2. {args.Message}");
                TestReader(args.Connection, args.Transaction, "OnMetadataCleaning Pass 2:");
            });

            localOrchestrator.OnMetadataCleaned(args =>
            {
                cleaned++;
                Console.WriteLine($"Pass 2. {args.Message}");
                Debug.WriteLine($"Pass 2. {args.Message}");
                TestReader(args.Connection, args.Transaction, "OnMetadataCleaned Pass 2:");
            });

            // Making a second empty sync.
            var s3 = await agent.SynchronizeAsync(scopeName);

            // If there is no changes on any tables, no metadata cleaning is called
            Assert.Equal(0, cleaning);
            Assert.Equal(0, cleaned);

            ts = await localOrchestrator.GetLocalTimestampAsync();
            Console.WriteLine($"Pass 3. TS:{ts}");
            Debug.WriteLine($"Pass 3. TS:{ts}");



            // Server side : Create a product category 
            productCategoryName = HelperDatabase.GetRandomName();
            productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

            using (var ctx = new AdventureWorksContext((dbNameSrv, ProviderType.Sql, serverProvider)))
            {
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);

                await ctx.SaveChangesAsync();
            }

            ts = await localOrchestrator.GetLocalTimestampAsync();
            Console.WriteLine($"Pass 3 bis. TS:{ts}");
            Debug.WriteLine($"Pass 3 bis. TS:{ts}");



            // Reset interceptors
            localOrchestrator.ClearInterceptors();

            cleaning = 0;
            cleaned = 0;


            localOrchestrator.OnMetadataCleaning(args =>
            {
                cleaning++;
                Console.WriteLine($"Pass 3. {args.Message}");
                Debug.WriteLine($"Pass 3. {args.Message}");

                TestReader(args.Connection, args.Transaction, "OnMetadataCleaning Pass 3:");


            });

            localOrchestrator.OnMetadataCleaned(args =>
            {
                cleaned++;
                Console.WriteLine($"Pass 3. {args.Message}");
                Debug.WriteLine($"Pass 3. {args.Message}");
                TestReader(args.Connection, args.Transaction, "OnMetadataCleaned Pass 3:");
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
            localOrchestrator.ClearInterceptors();
            cleaning = 0;
            cleaned = 0;


            localOrchestrator.OnMetadataCleaning(args =>
            {
                cleaning++;
                Console.WriteLine($"Pass 4. {args.Message}");
                Console.WriteLine($"Pass 4. OnMetadataCleaning cleaning count:{cleaning}");
            });

            localOrchestrator.OnMetadataCleaned(args =>
            {
                cleaned++;
                Assert.Equal(0, args.DatabaseMetadatasCleaned.RowsCleanedCount);
                Console.WriteLine($"Pass 4. {args.Message}");
                Console.WriteLine($"Pass 4. OnMetadataCleaning cleaned count:{cleaned}");
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
