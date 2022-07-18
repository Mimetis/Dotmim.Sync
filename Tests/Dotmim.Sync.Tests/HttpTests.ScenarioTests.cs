using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.SqlServer.Manager;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
#if NET5_0 || NET6_0 || NETCOREAPP3_1
using MySqlConnector;
#elif NETCOREAPP2_1
using MySql.Data.MySqlClient;
#endif
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests
{
    //[TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    public abstract partial class HttpTests : IClassFixture<HelperProvider>, IDisposable
    {


        [Fact]
        public virtual async Task Scenario_Adding_OneColumn_OneTable_With_TwoScopes()
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);


            var productCategoryTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.ProductCategory" : "ProductCategory";
            var productTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.Product" : "Product";

            // --------------------------
            // Step 1: Create a default scope and Sync clients
            // Note we are not including the [Attribute With Space] column
            var setup = new SyncSetup(new string[] { productCategoryTableName });
            setup.Tables[productCategoryTableName].Columns.AddRange(
                new string[] { "ProductCategoryId", "Name", "rowguid", "ModifiedDate" });

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName, setup);

            var serviceUri = this.Kestrell.Run();

            int productCategoryRowsCount = 0;
            using (var readCtx = new AdventureWorksContext(Server, this.UseFallbackSchema))
            {
                productCategoryRowsCount = readCtx.ProductCategory.AsNoTracking().Count();
            }

            // First sync to initialiaze client database, create table and fill product categories
            foreach (var client in this.Clients)
            {
                var webServerProxyOrchestrator = new WebRemoteOrchestrator(serviceUri);

                var agent = new SyncAgent(client.Provider, webServerProxyOrchestrator);

                var r = await agent.SynchronizeAsync();

                Assert.Equal(productCategoryRowsCount, r.TotalChangesDownloaded);
            }

            await this.Kestrell.StopAsync();


            // On server side, playing around with a direct RemoteOrchestrator
            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider);

            // Adding a new scope on the server with this new column and a new table
            // Creating a new scope called "V1" on server
            var setupV1 = new SyncSetup(new string[] { productCategoryTableName, productTableName });

            setupV1.Tables[productCategoryTableName].Columns.AddRange(
            new string[] { "ProductCategoryId", "Name", "rowguid", "ModifiedDate", "Attribute With Space" });

            var serverScope = await remoteOrchestrator.ProvisionAsync("v1", setupV1);

            // Create a server new ProductCategory with the new column value filled
            // and a Product related
            var productId = Guid.NewGuid();
            var productName = HelperDatabase.GetRandomName();
            var productNumber = productName.ToUpperInvariant().Substring(0, 10);
            var productCategoryName = HelperDatabase.GetRandomName();
            var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

            var newAttributeWithSpaceValue = HelperDatabase.GetRandomName();

            using (var ctx = new AdventureWorksContext(Server, this.UseFallbackSchema))
            {
                var pc = new ProductCategory
                {
                    ProductCategoryId = productCategoryId,
                    Name = productCategoryName,
                    AttributeWithSpace = newAttributeWithSpaceValue
                };
                ctx.ProductCategory.Add(pc);

                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber, ProductCategoryId = productCategoryId };
                ctx.Product.Add(product);

                await ctx.SaveChangesAsync();
            }

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName, setup);
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName, setupV1);

            serviceUri = this.Kestrell.Run();


            foreach (var client in this.Clients)
            {
                var commandText = client.ProviderType switch
                {
                    ProviderType.Sql => $@"ALTER TABLE {productCategoryTableName} ADD [Attribute With Space] nvarchar(250) NULL;",
                    ProviderType.Sqlite => @"ALTER TABLE ProductCategory ADD [Attribute With Space] text NULL;",
                    ProviderType.MySql => @"ALTER TABLE `ProductCategory` ADD `Attribute With Space` nvarchar(250) NULL;",
                    ProviderType.MariaDB => @"ALTER TABLE `ProductCategory` ADD `Attribute With Space` nvarchar(250) NULL;",
                    _ => throw new NotImplementedException()
                };

                var connection = client.Provider.CreateConnection();

                connection.Open();

                var command = connection.CreateCommand();
                command.CommandText = commandText;
                command.Connection = connection;
                await command.ExecuteNonQueryAsync();

                connection.Close();

                // Get scope from server (v1 because it contains the new table schema)
                var webServerProxyOrchestrator = new WebRemoteOrchestrator(serviceUri);
                serverScope = await webServerProxyOrchestrator.GetServerScopeInfoAsync("v1");

                // Creating a new table is quite easier since DMS can do it for us
                var localOrchestrator = new LocalOrchestrator(client.Provider);

                if (this.Server.ProviderType == ProviderType.Sql)
                    await localOrchestrator.CreateTableAsync(serverScope, "Product", "SalesLT");
                else
                    await localOrchestrator.CreateTableAsync(serverScope, "Product");

                // Once created we can provision the new scope, thanks to the serverScope instance we already have
                var clientScopeV1 = await localOrchestrator.ProvisionAsync(serverScope);

                // IF we launch synchronize on this new scope, it will get all the rows from the server
                // We are making a shadow copy of previous scope to get the last synchronization metadata
                var oldClientScopeInfo = await localOrchestrator.GetClientScopeInfoAsync();
                clientScopeV1.ShadowScope(oldClientScopeInfo);
                await localOrchestrator.SaveClientScopeInfoAsync(clientScopeV1);

                // We are ready to sync this new scope !
                var agent = new SyncAgent(client.Provider, Server.Provider);
                var r = await agent.SynchronizeAsync("v1");

                Assert.Equal(2, r.TotalChangesDownloaded);



            }





        }



        [Fact]
        public virtual async Task Scenario_Adding_OneColumn_OneTable_With_TwoScopes_OneClient_Still_OnOldScope_OneClient_OnNewScope()
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create 2 client databases
            // First one will update to new scope
            // Second one will stay on last scope
            // For this purpose, using two sqlite databases

            var client1DatabaseName = HelperDatabase.GetRandomName();
            var client2DatabaseName = HelperDatabase.GetRandomName();

            // Create the two databases
            await this.CreateDatabaseAsync(ProviderType.Sqlite, client1DatabaseName, true);
            await this.CreateDatabaseAsync(ProviderType.Sqlite, client2DatabaseName, true);

            var client1provider = new SqliteSyncProvider(HelperDatabase.GetSqliteFilePath(client1DatabaseName));
            var client2provider = new SqliteSyncProvider(HelperDatabase.GetSqliteFilePath(client2DatabaseName));

            // --------------------------
            // Step 1: Create a default scope and Sync clients
            // Note we are not including the [Attribute With Space] column
            var productCategoryTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.ProductCategory" : "ProductCategory";
            var productTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.Product" : "Product";

            var setup = new SyncSetup(new string[] { productCategoryTableName });
            setup.Tables[productCategoryTableName].Columns.AddRange(
                new string[] { "ProductCategoryId", "Name", "rowguid", "ModifiedDate" });

            // Counting product categories & products
            int productCategoryRowsCount = 0;
            int productsCount = 0;
            using (var readCtx = new AdventureWorksContext(Server, this.UseFallbackSchema))
            {
                productCategoryRowsCount = readCtx.ProductCategory.AsNoTracking().Count();
                productsCount = readCtx.Product.AsNoTracking().Count();
            }


            var agent1 = new SyncAgent(client1provider, Server.Provider);
            var r1 = await agent1.SynchronizeAsync(setup);
            Assert.Equal(productCategoryRowsCount, r1.TotalChangesDownloaded);

            var agent2 = new SyncAgent(client2provider, Server.Provider);
            var r2 = await agent2.SynchronizeAsync(setup);
            Assert.Equal(productCategoryRowsCount, r2.TotalChangesDownloaded);

            // From now, the client 1 will upgrade to new scope
            // the client 2 will remain on old scope

            // Adding a new scope
            var remoteOrchestrator = agent1.RemoteOrchestrator; // agent2.RemoteOrchestrator is the same, btw

            // Adding a new scope on the server with this new column and a new table
            // Creating a new scope called "V1" on server
            var setupV1 = new SyncSetup(new string[] { productCategoryTableName, productTableName });

            setupV1.Tables[productCategoryTableName].Columns.AddRange(
            new string[] { "ProductCategoryId", "Name", "rowguid", "ModifiedDate", "Attribute With Space" });

            var serverScope = await remoteOrchestrator.ProvisionAsync("v1", setupV1);


            // Create a server new ProductCategory with the new column value filled
            // and a Product related
            var productId = Guid.NewGuid();
            var productName = HelperDatabase.GetRandomName();
            var productNumber = productName.ToUpperInvariant().Substring(0, 10);
            var productCategoryName = HelperDatabase.GetRandomName();
            var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

            var newAttributeWithSpaceValue = HelperDatabase.GetRandomName();

            using (var ctx = new AdventureWorksContext(Server, this.UseFallbackSchema))
            {
                var pc = new ProductCategory
                {
                    ProductCategoryId = productCategoryId,
                    Name = productCategoryName,
                    AttributeWithSpace = newAttributeWithSpaceValue
                };
                ctx.ProductCategory.Add(pc);

                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber, ProductCategoryId = productCategoryId };
                ctx.Product.Add(product);

                await ctx.SaveChangesAsync();
            }

            // Add this new column on the client 1, with default value as null
            var connection = client1provider.CreateConnection();
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = @"ALTER TABLE ProductCategory ADD [Attribute With Space] text NULL;";
            command.Connection = connection;
            await command.ExecuteNonQueryAsync();
            connection.Close();

            // Creating a new table is quite easier since DMS can do it for us
            // Get scope from server (v1 because it contains the new table schema)
            // we already have it, but you cand call GetServerScopInfoAsync("v1") if needed
            // var serverScope = await remoteOrchestrator.GetServerScopeInfoAsync("v1");

            var localOrchestrator = new LocalOrchestrator(client1provider);
            if (this.Server.ProviderType == ProviderType.Sql)
                await localOrchestrator.CreateTableAsync(serverScope, "Product", "SalesLT");
            else
                await localOrchestrator.CreateTableAsync(serverScope, "Product");
            // Once created we can provision the new scope, thanks to the serverScope instance we already have
            var clientScopeV1 = await localOrchestrator.ProvisionAsync(serverScope);

            // IF we launch synchronize on this new scope, it will get all the rows from the server
            // We are making a shadow copy of previous scope to get the last synchronization metadata
            var oldClientScopeInfo = await localOrchestrator.GetClientScopeInfoAsync();
            clientScopeV1.ShadowScope(oldClientScopeInfo);
            await localOrchestrator.SaveClientScopeInfoAsync(clientScopeV1);

            // We are ready to sync this new scope !
            // we still can use the old agent, since it's already configured with correct providers
            // just be sure to set the correct scope
            r1 = await agent1.SynchronizeAsync("v1");
            Assert.Equal(2, r1.TotalChangesDownloaded);

            // make a sync on old scope for client 2
            r2 = await agent2.SynchronizeAsync();
            Assert.Equal(1, r2.TotalChangesDownloaded);

            // now check values on each client
            using (var ctx1 = new AdventureWorksContext((client1DatabaseName, ProviderType.Sqlite, client1provider), false))
            {
                var producCategory1 = ctx1.ProductCategory.First(pc => pc.ProductCategoryId == productCategoryId);
                Assert.Equal(newAttributeWithSpaceValue, producCategory1.AttributeWithSpace);
            }
            using (var ctx2 = new AdventureWorksContext((client2DatabaseName, ProviderType.Sqlite, client2provider), false))
            {
                var exc = Assert.ThrowsAny<Microsoft.Data.Sqlite.SqliteException>(() => ctx2.ProductCategory.First(pc => pc.ProductCategoryId == productCategoryId));
                Assert.Contains("no such column", exc.Message);
            }

            // Assuming we want to migrate the client 2 now
            var serverScope2 = await agent2.RemoteOrchestrator.GetServerScopeInfoAsync();

            // Create the new table locally
            if (this.Server.ProviderType == ProviderType.Sql)
                await localOrchestrator.CreateTableAsync(serverScope, "Product", "SalesLT");
            else
                await localOrchestrator.CreateTableAsync(serverScope, "Product");

            // Add this new column on the client 1, with default value as null
            connection = client2provider.CreateConnection();
            connection.Open();
            command = connection.CreateCommand();
            command.CommandText = @"ALTER TABLE ProductCategory ADD [Attribute With Space] text NULL;";
            command.Connection = connection;
            await command.ExecuteNonQueryAsync();
            connection.Close();

            // Don't bother to ShadowCopy metadata, since we are doing a reinit
            // Just Provision
            var clientScope2 = await agent2.LocalOrchestrator.ProvisionAsync(serverScope2);

            // Sync
            r2 = await agent2.SynchronizeAsync("v1", SyncType.Reinitialize);

            using (var readCtx = new AdventureWorksContext(Server, this.UseFallbackSchema))
            {
                productCategoryRowsCount = readCtx.ProductCategory.AsNoTracking().Count();
                productsCount = readCtx.Product.AsNoTracking().Count();
            }

            Assert.Equal((productCategoryRowsCount + productsCount), r2.TotalChangesDownloaded);

        }


    }
}
