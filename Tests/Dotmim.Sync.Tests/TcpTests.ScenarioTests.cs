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
#if NET5_0 || NET6_0 || NET7_0 || NETCOREAPP3_1
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
    public abstract partial class TcpTests : IClassFixture<HelperProvider>, IDisposable
    {

        [Fact]
        public virtual async Task Scenario_MultiFiltersAsync()
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            var productCategoryTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.ProductCategory" : "ProductCategory";
            var productTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.Product" : "Product";

            var setup = new SyncSetup(productCategoryTableName, productTableName, "Employee");

            setup.Tables[productCategoryTableName].Columns.AddRange("ProductCategoryID", "Name", "rowguid", "ModifiedDate");

            if (this.Server.ProviderType == ProviderType.Sql)
            {
                setup.Filters.Add("ProductCategory", "ProductCategoryID", "SalesLT");
                setup.Filters.Add("Product", "ProductCategoryID", "SalesLT");
            }
            else
            {
                setup.Filters.Add("ProductCategory", "ProductCategoryID");
                setup.Filters.Add("Product", "ProductCategoryID");
            }

            var pMount = new SyncParameters(("ProductCategoryID", "MOUNTB"));
            var pRoad = new SyncParameters(("ProductCategoryID", "ROADFR"));

            // First sync to initialiaze client database, create table and fill product categories
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider);
                var r1 = await agent.SynchronizeAsync("v1", setup, pMount);
                var r2 = await agent.SynchronizeAsync("v1", setup, pRoad);

                Assert.Equal(11, r1.TotalChangesDownloadedFromServer);
                Assert.Equal(6, r2.TotalChangesDownloadedFromServer);
            }
        }


        [Fact]
        public virtual async Task Scenario_Adding_OneColumn_OneTable_With_TwoScopes()
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // --------------------------
            // Step 1: Create a default scope and Sync clients
            // Note we are not including the [Attribute With Space] column

            var productCategoryTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.ProductCategory" : "ProductCategory";
            var productTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.Product" : "Product";

            var setup = new SyncSetup(new string[] { productCategoryTableName });
            setup.Tables[productCategoryTableName].Columns.AddRange(
                new string[] { "ProductCategoryId", "Name", "rowguid", "ModifiedDate" });

            int productCategoryRowsCount = 0;
            using (var readCtx = new AdventureWorksContext(Server, this.UseFallbackSchema))
            {
                productCategoryRowsCount = readCtx.ProductCategory.AsNoTracking().Count();
            }

            // First sync to initialiaze client database, create table and fill product categories
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider);
                var r = await agent.SynchronizeAsync("v1", setup);

                Assert.Equal(productCategoryRowsCount, r.TotalChangesDownloadedFromServer);
            }

            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider);

            // Adding a new scope on the server with this new column and a new table
            // Creating a new scope called "v2" on server
            var setupV2 = new SyncSetup(new string[] { productCategoryTableName, productTableName });

            setupV2.Tables[productCategoryTableName].Columns.AddRange(
            new string[] { "ProductCategoryId", "Name", "rowguid", "ModifiedDate", "Attribute With Space" });

            var serverScope = await remoteOrchestrator.ProvisionAsync("v2", setupV2);

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

            // Add this new column on the client, with default value as null

            foreach (var client in this.Clients)
            {
                var commandText = client.ProviderType switch
                {
                    ProviderType.Sql => $@"ALTER TABLE {productCategoryTableName} ADD [Attribute With Space] nvarchar(250) NULL;",
                    ProviderType.Sqlite => @"ALTER TABLE ProductCategory ADD [Attribute With Space] text NULL;",
                    ProviderType.MySql => @"ALTER TABLE `ProductCategory` ADD `Attribute With Space` nvarchar(250) NULL;",
                    ProviderType.MariaDB => @"ALTER TABLE `ProductCategory` ADD `Attribute With Space` nvarchar(250) NULL;",
                    ProviderType.Postgres => @"ALTER TABLE public.""ProductCategory"" ADD ""Attribute With Space"" character varying(250) NULL;",
                    _ => throw new NotImplementedException()
                };

                var connection = client.Provider.CreateConnection();

                connection.Open();

                var command = connection.CreateCommand();
                command.CommandText = commandText;
                command.Connection = connection;
                await command.ExecuteNonQueryAsync();

                connection.Close();

                // Creating a new table is quite easier since DMS can do it for us
                // Get scope from server (v1 because it contains the new table schema)
                // we already have it, but you cand call GetServerScopInfoAsync("v1") if needed
                // var serverScope = await remoteOrchestrator.GetScopeInfoAsync("v1");

                var localOrchestrator = new LocalOrchestrator(client.Provider);
                await localOrchestrator.CreateTableAsync(serverScope, "Product", "SalesLT");

                // Once created we can provision the new scope, thanks to the serverScope instance we already have
                var clientScopeV1 = await localOrchestrator.ProvisionAsync(serverScope);
                var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync("v2");

                // IF we launch synchronize on this new scope, it will get all the rows from the server
                // We are making a shadow copy of previous scope to get the last synchronization metadata
                var oldCScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync("v1");
                cScopeInfoClient.ShadowScope(oldCScopeInfoClient);
                await localOrchestrator.SaveScopeInfoClientAsync(cScopeInfoClient);

                // We are ready to sync this new scope !
                var agent = new SyncAgent(client.Provider, Server.Provider);
                var r = await agent.SynchronizeAsync("v2");

                Assert.Equal(2, r.TotalChangesDownloadedFromServer);
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

            var productCategoryTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.ProductCategory" : "ProductCategory";
            var productTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.Product" : "Product";
            // --------------------------
            // Step 1: Create a default scope and Sync clients
            // Note we are not including the [Attribute With Space] column
            var setup = new SyncSetup(new string[] { productCategoryTableName });
            setup.Tables[productCategoryTableName].Columns.AddRange(
                "ProductCategoryId", "Name", "ParentProductCategoryId", "rowguid", "ModifiedDate");

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
            Assert.Equal(productCategoryRowsCount, r1.TotalChangesDownloadedFromServer);

            var agent2 = new SyncAgent(client2provider, Server.Provider);
            var r2 = await agent2.SynchronizeAsync(setup);
            Assert.Equal(productCategoryRowsCount, r2.TotalChangesDownloadedFromServer);

            // From now, the client 1 will upgrade to new scope
            // the client 2 will remain on old scope

            // Adding a new scope
            var remoteOrchestrator = agent1.RemoteOrchestrator; // agent2.RemoteOrchestrator is the same, btw

            // Adding a new scope on the server with this new column and a new table
            // Creating a new scope called "V1" on server
            var setupV1 = new SyncSetup(new string[] { productCategoryTableName, productTableName });

            setupV1.Tables[productCategoryTableName].Columns.AddRange(
                "ProductCategoryId", "Name", "ParentProductCategoryId", "rowguid", "ModifiedDate", "Attribute With Space");

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
            // var serverScope = await remoteOrchestrator.GetScopeInfoAsync("v1");

            var localOrchestrator = new LocalOrchestrator(client1provider);

            if (this.Server.ProviderType == ProviderType.Sql)
                await localOrchestrator.CreateTableAsync(serverScope, "Product", "SalesLT");
            else
                await localOrchestrator.CreateTableAsync(serverScope, "Product");

            // Once created we can provision the new scope, thanks to the serverScope instance we already have
            var clientScopeV1 = await localOrchestrator.ProvisionAsync(serverScope);
            var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync("v1");

            // IF we launch synchronize on this new scope, it will get all the rows from the server
            // We are making a shadow copy of previous scope to get the last synchronization metadata
            var oldCScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync();

            cScopeInfoClient.ShadowScope(oldCScopeInfoClient);
            await localOrchestrator.SaveScopeInfoClientAsync(cScopeInfoClient);

            // We are ready to sync this new scope !
            // we still can use the old agent, since it's already configured with correct providers
            // just be sure to set the correct scope
            r1 = await agent1.SynchronizeAsync("v1");
            Assert.Equal(2, r1.TotalChangesDownloadedFromServer);

            // make a sync on old scope for client 2
            r2 = await agent2.SynchronizeAsync();
            Assert.Equal(1, r2.TotalChangesDownloadedFromServer);

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
            var serverScope2 = await agent2.RemoteOrchestrator.GetScopeInfoAsync();

            // Create the new table locally
            if (this.Server.ProviderType == ProviderType.Sql)
                await agent2.LocalOrchestrator.CreateTableAsync(serverScope2, "Product", "SalesLT");
            else
                await agent2.LocalOrchestrator.CreateTableAsync(serverScope2, "Product");

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

            Assert.Equal(productCategoryRowsCount + productsCount, r2.TotalChangesDownloadedFromServer);

        }


        [Fact]
        public virtual async Task Scenario_Adding_OneColumn_OneTable_On_SameScope()
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // --------------------------
            // Step 1: Create a default scope and Sync clients
            // Note we are not including the [Attribute With Space] column

            var productCategoryTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.ProductCategory" : "ProductCategory";
            var productTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.Product" : "Product";

            var setup = new SyncSetup(new string[] { productCategoryTableName });
            setup.Tables[productCategoryTableName].Columns.AddRange(
                new string[] { "ProductCategoryId", "Name", "rowguid", "ModifiedDate" });

            int productCategoryRowsCount = 0;
            using (var readCtx = new AdventureWorksContext(Server, this.UseFallbackSchema))
            {
                productCategoryRowsCount = readCtx.ProductCategory.AsNoTracking().Count();
            }

            // First sync to initialiaze client database, create table and fill product categories
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider);
                var r = await agent.SynchronizeAsync("v1", setup);

                Assert.Equal(productCategoryRowsCount, r.TotalChangesDownloadedFromServer);
            }

            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider);

            // Editing the current scope on the server with this new column and a new table
            setup.Tables.Add(productTableName);
            setup.Tables[productCategoryTableName].Columns.Clear();
            setup.Tables[productCategoryTableName].Columns.AddRange("ProductCategoryId", "Name", "rowguid", "ModifiedDate", "Attribute With Space");

            // overwrite the setup
            var serverScope = await remoteOrchestrator.ProvisionAsync("v1", setup, overwrite: true);

            if (Server.ProviderType == ProviderType.MySql || Server.ProviderType == ProviderType.MariaDB)
            {
                var connection = Server.Provider.CreateConnection();
                // tracking https://github.com/mysql-net/MySqlConnector/issues/924
                MySqlConnection.ClearPool(connection as MySqlConnection);
            }

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

            foreach (var client in this.Clients)
            {
                var commandText = client.ProviderType switch
                {
                    ProviderType.Sql => $@"ALTER TABLE {productCategoryTableName} ADD [Attribute With Space] nvarchar(250) NULL;",
                    ProviderType.Sqlite => @"ALTER TABLE ProductCategory ADD [Attribute With Space] text NULL;",
                    ProviderType.MySql => @"ALTER TABLE `ProductCategory` ADD `Attribute With Space` nvarchar(250) NULL;",
                    ProviderType.MariaDB => @"ALTER TABLE `ProductCategory` ADD `Attribute With Space` nvarchar(250) NULL;",
                    ProviderType.Postgres => @"ALTER TABLE public.""ProductCategory"" ADD ""Attribute With Space"" character varying(250) NULL;",
                    _ => throw new NotImplementedException()
                };

                var connection = client.Provider.CreateConnection();

                connection.Open();

                var command = connection.CreateCommand();
                command.CommandText = commandText;
                command.Connection = connection;
                await command.ExecuteNonQueryAsync();

                connection.Close();


                if (client.ProviderType == ProviderType.MySql || client.ProviderType == ProviderType.MariaDB)
                {
                    // tracking https://github.com/mysql-net/MySqlConnector/issues/924
                    MySqlConnection.ClearPool(connection as MySqlConnection);
                }

                // Creating a new table is quite easier since DMS can do it for us
                // Get scope from server (v1 because it contains the new table schema)
                // we already have it, but you cand call GetServerScopInfoAsync("v1") if needed
                // var serverScope = await remoteOrchestrator.GetScopeInfoAsync("v1");

                var localOrchestrator = new LocalOrchestrator(client.Provider);
                await localOrchestrator.CreateTableAsync(serverScope, "Product", "SalesLT");

                // Once created we can override the client scope, thanks to the serverScope instance we already have
                await localOrchestrator.ProvisionAsync(serverScope, overwrite: true);

                // We are ready to sync this new scope !
                var agent = new SyncAgent(client.Provider, Server.Provider);


                var r = await agent.SynchronizeAsync("v1");

                Assert.Equal(2, r.TotalChangesDownloadedFromServer);

            }

        }


        [Fact]
        public virtual async Task Scenario_Adding_OneColumn_OneTable_On_SameScope_Using_Interceptor()
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // --------------------------
            // Step 1: Create a default scope and Sync clients
            // Note we are not including the [Attribute With Space] column

            var productCategoryTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.ProductCategory" : "ProductCategory";
            var productTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.Product" : "Product";

            var setup = new SyncSetup(new string[] { productCategoryTableName });
            setup.Tables[productCategoryTableName].Columns.AddRange(
                new string[] { "ProductCategoryID", "Name", "rowguid", "ModifiedDate" });

            int productCategoryRowsCount = 0;
            using (var readCtx = new AdventureWorksContext(Server, this.UseFallbackSchema))
            {
                productCategoryRowsCount = readCtx.ProductCategory.AsNoTracking().Count();
            }

            // First sync to initialiaze client database, create table and fill product categories
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider);
                var r = await agent.SynchronizeAsync("v1", setup);

                Assert.Equal(productCategoryRowsCount, r.TotalChangesDownloadedFromServer);
            }

            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider);

            // Editing the current scope on the server with this new column and a new table
            setup.Tables.Add(productTableName);
            setup.Tables[productCategoryTableName].Columns.Clear();
            setup.Tables[productCategoryTableName].Columns.AddRange("ProductCategoryID", "Name", "rowguid", "ModifiedDate", "Attribute With Space");

            // overwrite the setup
            var serverScope = await remoteOrchestrator.ProvisionAsync("v1", setup, overwrite: true);

            if (Server.ProviderType == ProviderType.MySql || Server.ProviderType == ProviderType.MariaDB)
            {
                var connection = Server.Provider.CreateConnection();
                // tracking https://github.com/mysql-net/MySqlConnector/issues/924
                MySqlConnection.ClearPool(connection as MySqlConnection);
            }

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

            foreach (var client in this.Clients)
            {
                var commandText = client.ProviderType switch
                {
                    ProviderType.Sql => $@"ALTER TABLE {productCategoryTableName} ADD [Attribute With Space] nvarchar(250) NULL;",
                    ProviderType.Sqlite => @"ALTER TABLE ProductCategory ADD [Attribute With Space] text NULL;",
                    ProviderType.MySql => @"ALTER TABLE `ProductCategory` ADD `Attribute With Space` nvarchar(250) NULL;",
                    ProviderType.MariaDB => @"ALTER TABLE `ProductCategory` ADD `Attribute With Space` nvarchar(250) NULL;",
                    ProviderType.Postgres => @"ALTER TABLE public.""ProductCategory"" ADD ""Attribute With Space"" character varying(250) NULL;",
                    _ => throw new NotImplementedException()
                };

                var connection = client.Provider.CreateConnection();

                connection.Open();

                var command = connection.CreateCommand();
                command.CommandText = commandText;
                command.Connection = connection;
                await command.ExecuteNonQueryAsync();

                connection.Close();


                if (client.ProviderType == ProviderType.MySql || client.ProviderType == ProviderType.MariaDB)
                {
                    // tracking https://github.com/mysql-net/MySqlConnector/issues/924
                    MySqlConnection.ClearPool(connection as MySqlConnection);
                }

                // Creating a new table is quite easier since DMS can do it for us
                // Get scope from server (v1 because it contains the new table schema)
                // we already have it, but you cand call GetServerScopInfoAsync("v1") if needed
                // var serverScope = await remoteOrchestrator.GetScopeInfoAsync("v1");

                var localOrchestrator = new LocalOrchestrator(client.Provider);
                await localOrchestrator.CreateTableAsync(serverScope, "Product", "SalesLT");

                // We are ready to sync this new scope !
                var agent = new SyncAgent(client.Provider, Server.Provider);

                agent.LocalOrchestrator.OnConflictingSetup(async args =>
                {
                    if (args.ServerScopeInfo != null)
                    {
                        args.ClientScopeInfo = await localOrchestrator.ProvisionAsync(args.ServerScopeInfo, overwrite: true);
                        args.Action = ConflictingSetupAction.Continue;
                        return;
                    }
                    args.Action = ConflictingSetupAction.Abort;
                });

                var r = await agent.SynchronizeAsync("v1");

                Assert.Equal(2, r.TotalChangesDownloadedFromServer);

            }

        }

        [Fact]
        public virtual async Task Scenario_MultiTables_ThenAll_WithoutScopeFromServer()
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            var productCategoryTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.ProductCategory" : "ProductCategory";
            var productTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.Product" : "Product";
            var employeeTableName = "Employee";

            var setupEmployee = new SyncSetup(employeeTableName);
            var setupProductCategory = new SyncSetup(productCategoryTableName);
            var setupProduct = new SyncSetup(productTableName);
            var setupAll = new SyncSetup(productCategoryTableName, productTableName, employeeTableName);

            // Provision ALL scope on server
            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider);
            await remoteOrchestrator.ProvisionAsync("ALL", setupAll);

            // First sync to initialiaze client database, create table and fill product categories
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider);

                var rEmployees = await agent.SynchronizeAsync("employees", setupEmployee);
                var rProductCategories = await agent.SynchronizeAsync("productCategories", setupProductCategory);
                var rProducts = await agent.SynchronizeAsync("products", setupProduct);

                // Provision local without having to get scope from server

                Assert.Equal(3, rEmployees.TotalChangesDownloadedFromServer);
                Assert.Equal(11, rProductCategories.TotalChangesDownloadedFromServer);
                Assert.Equal(14, rProducts.TotalChangesDownloadedFromServer);
            }

            // ----------------------------------------------
            // SERVER SIDE: Add a product cat and product

            // Add a product and its product category
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                // product category and product items
                var productCategoryName = HelperDatabase.GetRandomName();
                var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                var productId = Guid.NewGuid();
                var productName = HelperDatabase.GetRandomName();
                var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.ProductCategory.Add(pc);
                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber, ProductCategory = pc };
                ctx.Product.Add(product);

                await ctx.SaveChangesAsync();
            }

            foreach (var client in this.Clients)
            {

                var agent = new SyncAgent(client.Provider, Server.Provider);
                // CLIENT SIDE: Create a local scope for all tables
                // --------------------------------------
                SyncSet syncSetAll = await agent.LocalOrchestrator.GetSchemaAsync(setupAll);

                if (this.Server.ProviderType == ProviderType.Sql)
                {
                    if (client.ProviderType != ProviderType.Sql)
                    {
                        syncSetAll.Tables["Product"].SchemaName = "SalesLT";
                        syncSetAll.Tables["ProductCategory"].SchemaName = "SalesLT";

                        foreach (var relation in syncSetAll.Relations)
                        {
                            foreach (var k in relation.Keys)
                                k.SchemaName = "SalesLT";

                            foreach (var k in relation.ParentKeys)
                                k.SchemaName = "SalesLT";
                        }
                    }
                }

                ScopeInfo cScopeInfo = new ScopeInfo
                {
                    Name = "ALL",
                    Schema = syncSetAll,
                    Setup = setupAll,
                    Version = SyncVersion.Current.ToString()
                };
                await agent.LocalOrchestrator.ProvisionAsync(cScopeInfo);

                // Get all scope info clients to get minimum Timestamp
                // --------------------------------------
                var cAllScopeInfoClients = await agent.LocalOrchestrator.GetAllScopeInfoClientsAsync();

                var minServerTimeStamp = cAllScopeInfoClients.Min(sic => sic.LastServerSyncTimestamp);
                var minClientTimeStamp = cAllScopeInfoClients.Min(sic => sic.LastSyncTimestamp);
                var minLastSync = cAllScopeInfoClients.Min(sic => sic.LastSync);

                // Get (and create) the scope info client for scope ALL
                // --------------------------------------
                var cScopeInfoClient = await agent.LocalOrchestrator.GetScopeInfoClientAsync("ALL");

                if (cScopeInfoClient.IsNewScope)
                {
                    cScopeInfoClient.IsNewScope = false;
                    cScopeInfoClient.LastSync = minLastSync;
                    cScopeInfoClient.LastSyncTimestamp = minClientTimeStamp;
                    cScopeInfoClient.LastServerSyncTimestamp = minServerTimeStamp;
                    await agent.LocalOrchestrator.SaveScopeInfoClientAsync(cScopeInfoClient);
                }

                var rAll = await agent.SynchronizeAsync("ALL");

                Assert.Equal(2, rAll.TotalChangesDownloadedFromServer);
            }
        }

        [Fact]
        public virtual async Task Scenario_MultiTables_ThenAll_WithScopeFromServer()
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            var productCategoryTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.ProductCategory" : "ProductCategory";
            var productTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.Product" : "Product";
            var employeeTableName = "Employee";

            var setupEmployee = new SyncSetup(employeeTableName);
            var setupProductCategory = new SyncSetup(productCategoryTableName);
            var setupProduct = new SyncSetup(productTableName);
            var setupAll = new SyncSetup(productCategoryTableName, productTableName, employeeTableName);

            // Provision ALL scope on server
            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider);
            var sScopeInfo = await remoteOrchestrator.ProvisionAsync("ALL", setupAll);

            // First sync to initialiaze client database, create table and fill product categories
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider);

                var rEmployees = await agent.SynchronizeAsync("employees", setupEmployee);
                var rProductCategories = await agent.SynchronizeAsync("productCategories", setupProductCategory);
                var rProducts = await agent.SynchronizeAsync("products", setupProduct);

                // Provision local without having to get scope from server

                Assert.Equal(3, rEmployees.TotalChangesDownloadedFromServer);
                Assert.Equal(11, rProductCategories.TotalChangesDownloadedFromServer);
                Assert.Equal(14, rProducts.TotalChangesDownloadedFromServer);
            }

            // ----------------------------------------------
            // SERVER SIDE: Add a product cat and product

            // Add a product and its product category
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                // product category and product items
                var productCategoryName = HelperDatabase.GetRandomName();
                var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                var productId = Guid.NewGuid();
                var productName = HelperDatabase.GetRandomName();
                var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.ProductCategory.Add(pc);
                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber, ProductCategory = pc };
                ctx.Product.Add(product);

                await ctx.SaveChangesAsync();
            }

            foreach (var client in this.Clients)
            {

                var agent = new SyncAgent(client.Provider, Server.Provider);
                // CLIENT SIDE: Create a local scope for all tables
                // --------------------------------------
                await agent.LocalOrchestrator.ProvisionAsync(sScopeInfo);

                // Get all scope info clients to get minimum Timestamp
                // --------------------------------------
                var cAllScopeInfoClients = await agent.LocalOrchestrator.GetAllScopeInfoClientsAsync();

                var minServerTimeStamp = cAllScopeInfoClients.Min(sic => sic.LastServerSyncTimestamp);
                var minClientTimeStamp = cAllScopeInfoClients.Min(sic => sic.LastSyncTimestamp);
                var minLastSync = cAllScopeInfoClients.Min(sic => sic.LastSync);

                // Get (and create) the scope info client for scope ALL
                // --------------------------------------
                var cScopeInfoClient = await agent.LocalOrchestrator.GetScopeInfoClientAsync("ALL");

                if (cScopeInfoClient.IsNewScope)
                {
                    cScopeInfoClient.IsNewScope = false;
                    cScopeInfoClient.LastSync = minLastSync;
                    cScopeInfoClient.LastSyncTimestamp = minClientTimeStamp;
                    cScopeInfoClient.LastServerSyncTimestamp = minServerTimeStamp;
                    await agent.LocalOrchestrator.SaveScopeInfoClientAsync(cScopeInfoClient);
                }

                var rAll = await agent.SynchronizeAsync("ALL");

                Assert.Equal(2, rAll.TotalChangesDownloadedFromServer);
            }

        }


        /// <summary>
        /// The idea here is to start with an existing client database, where we don't want to upload anything or download anything on first sync
        /// Manipulate the client scope as it should be marked as not new (and set the correct timestamps)
        /// Then trying to sync
        /// </summary>
        /// <returns></returns>
        [Fact]
        public virtual async Task Scenario_StartingWithAnExistingClientWithData()
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create all clients database with seeding.
            // we are "mimic" here the backup restore
            foreach (var client in this.Clients)
                await this.EnsureDatabaseSchemaAndSeedAsync(client, true, UseFallbackSchema);

            var productCategoryTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.ProductCategory" : "ProductCategory";
            var productTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.Product" : "Product";

            var setup = new SyncSetup(productCategoryTableName, productTableName, "Employee");
            var remoteOrchestrator = new RemoteOrchestrator(this.Server.Provider);

            // 2) Provision server database
            var serverScope = await remoteOrchestrator.ProvisionAsync(setup);

            // 3) Get the timestamp to use on the client
            var serverTimeStamp = await remoteOrchestrator.GetLocalTimestampAsync();


            // 4) Insert some rows in server
            // Create a new ProductCategory and a related Product
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

            // First sync to initialiaze client database, create table and fill product categories
            foreach (var client in this.Clients)
            {
                // get orchestrator
                var localOrchestrator = new LocalOrchestrator(client.Provider);
                // provision client side
                await localOrchestrator.ProvisionAsync(serverScope);

                // 6) Get the local timestamp
                var clientTimestamp = await localOrchestrator.GetLocalTimestampAsync();

                // 7) Get scopeinfoclient
                // ScopeInfoClient table contains all information fro the "next" sync to do (timestamp, parameters and so on ...)
                var scopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync();

                // As we have some existing lines, we say it's not a new sync
                scopeInfoClient.IsNewScope = false;

                // Affecting the correct timestamp, the local one and the server one
                scopeInfoClient.LastServerSyncTimestamp = serverTimeStamp;
                scopeInfoClient.LastSyncTimestamp = clientTimestamp;
                await localOrchestrator.SaveScopeInfoClientAsync(scopeInfoClient);

                var agent = new SyncAgent(client.Provider, Server.Provider);
                var r1 = await agent.SynchronizeAsync(setup);

                Assert.Equal(2, r1.TotalChangesDownloadedFromServer);
                Assert.Equal(2, r1.ChangesAppliedOnClient.TotalAppliedChanges);

            }
        }


        /// <summary>
        /// The idea here is to start from a client restore from server backup
        /// The server made a backup, then we are intiliazing DMS to track changes from that point
        /// Once the client has restored the database, we can setup the DMS things on client
        /// Manipulate the client scope as it should be marked as not new (and set the correct timestamps)
        /// Then trying to sync
        /// </summary>
        [Fact]
        public virtual async Task Scenario_StartingWithAClientBackup()
        {
            if (this.Server.ProviderType != ProviderType.Sql)
                return;

            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            var productCategoryTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.ProductCategory" : "ProductCategory";
            var productTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.Product" : "Product";

            var setup = new SyncSetup(productCategoryTableName, productTableName, "Employee");
            var remoteOrchestrator = new RemoteOrchestrator(this.Server.Provider);

            // 1) Make a backup
            HelperDatabase.BackupDatabase(Server.DatabaseName);

            // 2) Provision server database
            var serverScope = await remoteOrchestrator.ProvisionAsync(setup);

            // 3) Get the timestamp to use on the client
            var serverTimeStamp = await remoteOrchestrator.GetLocalTimestampAsync();

            // 4) Insert some rows in server
            // Create a new ProductCategory and a related Product
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

            // First sync to initialiaze client database, create table and fill product categories
            foreach (var client in this.Clients)
            {
                if (client.ProviderType != ProviderType.Sql)
                    continue;

                // 5) Restore a backup
                HelperDatabase.RestoreSqlDatabase(Server.DatabaseName, client.DatabaseName);

                // get orchestrator
                var localOrchestrator = new LocalOrchestrator(client.Provider);
                // provision client side
                await localOrchestrator.ProvisionAsync(serverScope);

                // 6) Get the local timestamp
                var clientTimestamp = await localOrchestrator.GetLocalTimestampAsync();

                // 7) Get scopeinfoclient
                // ScopeInfoClient table contains all information fro the "next" sync to do (timestamp, parameters and so on ...)
                var scopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync();

                // As we have some existing lines, we say it's not a new sync
                scopeInfoClient.IsNewScope = false;

                // Affecting the correct timestamp, the local one and the server one
                scopeInfoClient.LastServerSyncTimestamp = serverTimeStamp;
                scopeInfoClient.LastSyncTimestamp = clientTimestamp;
                await localOrchestrator.SaveScopeInfoClientAsync(scopeInfoClient);

                var agent = new SyncAgent(client.Provider, Server.Provider);
                var r1 = await agent.SynchronizeAsync(setup);

                Assert.Equal(2, r1.TotalChangesDownloadedFromServer);
                Assert.Equal(2, r1.ChangesAppliedOnClient.TotalAppliedChanges);

            }
        }


    }
}
