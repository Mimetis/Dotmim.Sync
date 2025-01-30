﻿using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
using Microsoft.EntityFrameworkCore;
#if NET6_0 || NET8_0
using MySqlConnector;
#elif NETCOREAPP3_1
using MySql.Data.MySqlClient;
#endif
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Tests.IntegrationTests
{
    public abstract partial class TcpTests
    {
        /// <summary>
        /// Ensures that SQL filters are not generated on the SQLite client side when
        /// DisableSqlFiltersGeneration is enabled.
        /// </summary>
        /// <remarks>
        /// This test sets up a synchronization between a SQLite client and a server,
        /// with SQL filters disabled on the client. It verifies that the command
        /// for selecting changes with filters does not include the filters.
        /// </remarks>
        /// <returns>A <see cref="Task"/> representing the asynchronous unit test.</returns>
        [Fact]
        public async Task EnsureFiltersNotGeneratedInSQLiteSideIfDisableSqlFiltersGenerationEnabled()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };
            var providers = this.GetClientProviders();
            providers.FirstOrDefault();
            SqliteSyncProvider clientProvider = (SqliteSyncProvider)providers.FirstOrDefault(p => p.GetShortProviderTypeName() == "SqliteSyncProvider");

            if (clientProvider == null)
                return;

            clientProvider.DisableSqlFiltersGeneration = true;

            var setup = this.GetFilteredSetup();

            var agent = new SyncAgent(clientProvider, this.serverProvider, options);
            var parameters = this.GetFilterParameters();
            var count = 0;

            agent.LocalOrchestrator.OnGetCommand(s =>
            {
                if (s.CommandType == Builders.DbCommandType.SelectChangesWithFilters && s.Table.TableName == "Customer")
                {
                    count++;
                    Assert.DoesNotContain("@CustomerID", s.Command.CommandText);
                }
            });
            var initializeSync = await agent.SynchronizeAsync(setup, parameters);

            var secondSync = await agent.SynchronizeAsync(setup, parameters);

            Assert.True(count > 0);
        }

        /// <summary>
        /// Ensures that SQL filters are generated on the SQLite client side when
        /// DisableSqlFiltersGeneration is disabled.
        /// </summary>
        /// <remarks>
        /// This test sets up a synchronization between a SQLite client and a server,
        /// with SQL filters enabled on the client. It verifies that the command
        /// for selecting changes with filters includes the filters.
        /// </remarks>
        /// <returns>A <see cref="Task"/> representing the asynchronous unit test.</returns>
        [Fact]
        public async Task EnsureFiltersGeneratedInSQLiteSideIfDisableSqlFiltersGenerationDisabled()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };
            var providers = this.GetClientProviders();
            SqliteSyncProvider clientProvider = (SqliteSyncProvider)providers.FirstOrDefault(p => p.GetShortProviderTypeName() == "SqliteSyncProvider");

            if (clientProvider == null)
            {
                return;
            }

            clientProvider.DisableSqlFiltersGeneration = false;

            var setup = this.GetFilteredSetup();
            var agent = new SyncAgent(clientProvider, this.serverProvider, options);
            var parameters = this.GetFilterParameters();
            var count = 0;
            agent.LocalOrchestrator.OnGetCommand(s =>
            {
                if (s.CommandType == Builders.DbCommandType.SelectChangesWithFilters && s.Table.TableName == "Customer")
                {
                    count++;
                    Assert.Contains("@custID", s.Command.CommandText);
                }
            });
            var initializeSync = await agent.SynchronizeAsync(setup, parameters);

            var secondSync = await agent.SynchronizeAsync(setup, parameters);

            Assert.True(count > 0);
        }

        /// <summary>
        /// Check if a multi parameters value sync can work.
        /// With only 1 Setup and multiple parameters values.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the asynchronous unit test.</returns>
        [Fact]
        public virtual async Task MultiFiltersOnSameScopeShouldWork()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // Get tables I need (with or without schema)
            var productCategorySetupTable = this.setup.Tables.First(t => t.TableName == "ProductCategory");
            var productSetupTable = this.setup.Tables.First(t => t.TableName == "Product");
            var employeeSetupTable = this.setup.Tables.First(t => t.TableName == "Employee");

            var setupMultiFilters = new SyncSetup(productCategorySetupTable.GetFullName(), productSetupTable.GetFullName(), employeeSetupTable.GetFullName());

            setupMultiFilters.Tables[productCategorySetupTable.GetFullName()].Columns.AddRange("ProductCategoryID", "Name", "rowguid", "ModifiedDate");

            setupMultiFilters.Filters.Add(productCategorySetupTable.TableName, "ProductCategoryID", string.IsNullOrEmpty(productCategorySetupTable.SchemaName) ? null : productCategorySetupTable.SchemaName);
            setupMultiFilters.Filters.Add(productSetupTable.TableName, "ProductCategoryID", string.IsNullOrEmpty(productSetupTable.SchemaName) ? null : productSetupTable.SchemaName);

            var pMount = new SyncParameters(("ProductCategoryID", "MOUNTB"));
            var pRoad = new SyncParameters(("ProductCategoryID", "ROADFR"));

            foreach (var clientProvider in this.clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, this.serverProvider, options);
                var r1 = await agent.SynchronizeAsync("v1", setupMultiFilters, pMount);
                var r2 = await agent.SynchronizeAsync("v1", setupMultiFilters, pRoad);

                Assert.Equal(11, r1.TotalChangesDownloadedFromServer);
                Assert.Equal(6, r2.TotalChangesDownloadedFromServer);
            }
        }

        [Fact]
        public virtual async Task AddingOneColumnInOneTableUsingTwoScopes()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // Deletes all tables in client
            foreach (var clientProvider in this.clientsProvider)
            {
                await clientProvider.DropAllTablesAsync(true);
            }

            // Get tables I need (with or without schema)
            var productCategoryTable = this.setup.Tables.First(t => t.TableName == "ProductCategory");
            var productTable = this.setup.Tables.First(t => t.TableName == "Product");

            // --------------------------
            // Step 1: Create a default scope and Sync clients
            // Note we are not including the [Attribute With Space] column
            var setupV1 = new SyncSetup(productCategoryTable.GetFullName());
            setupV1.Tables[productCategoryTable.GetFullName()].Columns.AddRange("ProductCategoryId", "Name", "rowguid", "ModifiedDate");

            int productCategoryRowsCount = 0;
            await using (var readCtx = new AdventureWorksContext(this.serverProvider))
            {
                productCategoryRowsCount = readCtx.ProductCategory.AsNoTracking().Count();
            }

            // First sync to initialiaze client database, create table and fill product categories
            foreach (var clientProvider in this.clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, this.serverProvider, options);
                var r = await agent.SynchronizeAsync("v1", setupV1);

                Assert.Equal(productCategoryRowsCount, r.TotalChangesDownloadedFromServer);
            }

            var remoteOrchestrator = new RemoteOrchestrator(this.serverProvider);

            // Adding a new scope on the server with this new column and a new table
            // Creating a new scope called "v2" on server
            var setupV2 = new SyncSetup(productCategoryTable.GetFullName(), productTable.GetFullName());

            setupV2.Tables[productCategoryTable.GetFullName()].Columns.AddRange("ProductCategoryId", "Name", "rowguid", "ModifiedDate", "Attribute With Space");

            var serverScope = await remoteOrchestrator.ProvisionAsync("v2", setupV2);

            // Add rows on server
            await this.serverProvider.AddProductCategoryAsync();
            await this.serverProvider.AddProductAsync();

            HelperDatabase.ClearAllPools();

            // Add this new column on the client, with default value as null
            foreach (var clientProvider in this.clientsProvider)
            {
                var (clientProviderType, _) = HelperDatabase.GetDatabaseType(clientProvider);

                // exception on postgres
                string schema = clientProviderType == ProviderType.Postgres && clientProvider.UseFallbackSchema() ? "SalesLT" : "public";

                var commandText = clientProviderType switch
                {
                    ProviderType.Sql => $"ALTER TABLE {productCategoryTable.GetFullName()} ADD [Attribute With Space] nvarchar(250) NULL;",
                    ProviderType.Sqlite => $"ALTER TABLE {productCategoryTable.TableName} ADD [Attribute With Space] text NULL;",
                    ProviderType.MySql => $"ALTER TABLE `{productCategoryTable.TableName}` ADD `Attribute With Space` nvarchar(250) NULL;",
                    ProviderType.MariaDB => $"ALTER TABLE `{productCategoryTable.TableName}` ADD `Attribute With Space` nvarchar(250) NULL;",
                    ProviderType.Postgres => $"ALTER TABLE \"{schema}\".\"{productCategoryTable.TableName}\" ADD \"Attribute With Space\" character varying(250) NULL;",
                    _ => throw new NotImplementedException(),
                };

                var connection = clientProvider.CreateConnection();

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
                var localOrchestrator = new LocalOrchestrator(clientProvider);
                await localOrchestrator.CreateTableAsync(serverScope, productTable.TableName, string.IsNullOrEmpty(productTable.SchemaName) ? null : productTable.SchemaName);

                // Once created we can provision the new scope, thanks to the serverScope instance we already have
                var clientScopeV1 = await localOrchestrator.ProvisionAsync(serverScope);
                var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync("v2");

                // IF we launch synchronize on this new scope, it will get all the rows from the server
                // We are making a shadow copy of previous scope to get the last synchronization metadata
                var oldCScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync("v1");
                cScopeInfoClient.ShadowScope(oldCScopeInfoClient);
                await localOrchestrator.SaveScopeInfoClientAsync(cScopeInfoClient);

                // We are ready to sync this new scope !
                var agent = new SyncAgent(clientProvider, this.serverProvider);
                var r = await agent.SynchronizeAsync("v2");

                Assert.Equal(2, r.TotalChangesDownloadedFromServer);
            }
        }

        [Fact]
        public virtual async Task MultiScopesMigrationWithChangesToUpload()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // Deletes all tables in client
            foreach (var clientProvider in this.clientsProvider)
            {
                await clientProvider.DropAllTablesAsync(true);
            }

            // Get tables I need (with or without schema)
            var productCategoryTable = this.setup.Tables.First(t => t.TableName == "ProductCategory");
            var productTable = this.setup.Tables.First(t => t.TableName == "Product");

            // --------------------------
            // Step 1: Create a default scope and Sync clients
            // Note we are not including the [Attribute With Space] column
            var setupV1 = new SyncSetup(productCategoryTable.GetFullName());
            setupV1.Tables[productCategoryTable.GetFullName()].Columns.AddRange("ProductCategoryId", "ParentProductCategoryId", "Name", "rowguid", "ModifiedDate");

            int productCategoryRowsCount = 0;
            await using (var readCtx = new AdventureWorksContext(this.serverProvider))
            {
                productCategoryRowsCount = readCtx.ProductCategory.AsNoTracking().Count();
            }

            // First sync to initialiaze client database, create table and fill product categories
            foreach (var clientProvider in this.clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, this.serverProvider, options);
                var r = await agent.SynchronizeAsync("v1", setupV1);

                Assert.Equal(productCategoryRowsCount, r.TotalChangesDownloadedFromServer);
            }

            var remoteOrchestrator = new RemoteOrchestrator(this.serverProvider);

            // Adding a new scope on the server with this new column and a new table
            // Creating a new scope called "v2" on server
            var setupV2 = new SyncSetup(productCategoryTable.GetFullName(), productTable.GetFullName());

            setupV2.Tables[productCategoryTable.GetFullName()].Columns.AddRange("ProductCategoryId", "ParentProductCategoryId", "Name", "rowguid", "ModifiedDate", "Attribute With Space");

            var serverScopeV2 = await remoteOrchestrator.ProvisionAsync("v2", setupV2);

            // Add rows on server
            await this.serverProvider.AddProductCategoryAsync();
            await this.serverProvider.AddProductAsync();

            HelperDatabase.ClearAllPools();

            // Add this new column on the client, with default value as null
            var d = 2;
            foreach (var clientProvider in this.clientsProvider)
            {
                var (clientProviderType, _) = HelperDatabase.GetDatabaseType(clientProvider);

                // exception on postgres
                string schema = clientProviderType == ProviderType.Postgres && clientProvider.UseFallbackSchema() ? "SalesLT" : "public";

                var commandText = clientProviderType switch
                {
                    ProviderType.Sql => $"ALTER TABLE {productCategoryTable.GetFullName()} ADD [Attribute With Space] nvarchar(250) NULL;",
                    ProviderType.Sqlite => $"ALTER TABLE {productCategoryTable.TableName} ADD [Attribute With Space] text NULL;",
                    ProviderType.MySql => $"ALTER TABLE `{productCategoryTable.TableName}` ADD `Attribute With Space` nvarchar(250) NULL;",
                    ProviderType.MariaDB => $"ALTER TABLE `{productCategoryTable.TableName}` ADD `Attribute With Space` nvarchar(250) NULL;",
                    ProviderType.Postgres => $"ALTER TABLE \"{schema}\".\"{productCategoryTable.TableName}\" ADD \"Attribute With Space\" character varying(250) NULL;",
                    _ => throw new NotImplementedException(),
                };

                var connection = clientProvider.CreateConnection();

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
                var localOrchestrator = new LocalOrchestrator(clientProvider);
                await localOrchestrator.CreateTableAsync(serverScopeV2, productTable.TableName, string.IsNullOrEmpty(productTable.SchemaName) ? null : productTable.SchemaName);

                HelperDatabase.ClearAllPools();

                // Once created we can provision the new scope, thanks to the serverScope instance we already have
                var clientScopeV1 = await localOrchestrator.ProvisionAsync(serverScopeV2);
                var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync("v2");

                // Add rows on Client before updating scopes
                await clientProvider.AddProductCategoryAsync();
                await clientProvider.AddProductAsync();

                // IF we launch synchronize on this new scope, it will get all the rows from the server
                // We are making a shadow copy of previous scope to get the last synchronization metadata
                var oldCScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync("v1");
                cScopeInfoClient.ShadowScope(oldCScopeInfoClient);
                await localOrchestrator.SaveScopeInfoClientAsync(cScopeInfoClient);

                // We are ready to sync this new scope !
                var agent = new SyncAgent(clientProvider, this.serverProvider);
                var r = await agent.SynchronizeAsync("v2");

                Assert.Equal(2, r.TotalChangesUploadedToServer);
                Assert.Equal(2, r.TotalChangesAppliedOnServer);

                Assert.Equal(d, r.TotalChangesDownloadedFromServer);
                Assert.Equal(d, r.TotalChangesAppliedOnClient);
                d += 2;
            }
        }

        [Fact]
        public virtual async Task AddingOneColumnInOneTableUsingTwoScopesButOneClientStillOnOldScopeAndOneClientOnNewScope()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // create 2 client databases
            // First one will update to new scope
            // Second one will stay on last scope
            // For this purpose, using two sqlite databases
            var client1DatabaseName = HelperDatabase.GetRandomName();
            var client2DatabaseName = HelperDatabase.GetRandomName();

            // Create the two databases
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sqlite, client1DatabaseName, true);
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sqlite, client2DatabaseName, true);

            var client1provider = new SqliteSyncProvider(HelperDatabase.GetSqliteFilePath(client1DatabaseName));
            var client2provider = new SqliteSyncProvider(HelperDatabase.GetSqliteFilePath(client2DatabaseName));

            var (serverProviderType, _) = HelperDatabase.GetDatabaseType(this.serverProvider);

            var productCategoryTableName = this.serverProvider.UseFallbackSchema() ? "SalesLT.ProductCategory" : "ProductCategory";
            var productTableName = this.serverProvider.UseFallbackSchema() ? "SalesLT.Product" : "Product";

            // --------------------------
            // Step 1: Create a default scope and Sync clients
            // Note we are not including the [Attribute With Space] column
            var setup = new SyncSetup(productCategoryTableName);
            setup.Tables[productCategoryTableName].Columns.AddRange(
                "ProductCategoryId", "Name", "ParentProductCategoryId", "rowguid", "ModifiedDate");

            // Counting product categories & products
            int productCategoryRowsCount = 0;
            int productsCount = 0;
            await using (var readCtx = new AdventureWorksContext(this.serverProvider))
            {
                productCategoryRowsCount = readCtx.ProductCategory.AsNoTracking().Count();
                productsCount = readCtx.Product.AsNoTracking().Count();
            }

            var agent1 = new SyncAgent(client1provider, this.serverProvider, options);
            var r1 = await agent1.SynchronizeAsync(setup);
            Assert.Equal(productCategoryRowsCount, r1.TotalChangesDownloadedFromServer);

            var agent2 = new SyncAgent(client2provider, this.serverProvider, options);
            var r2 = await agent2.SynchronizeAsync(setup);
            Assert.Equal(productCategoryRowsCount, r2.TotalChangesDownloadedFromServer);

            // From now, the client 1 will upgrade to new scope
            // the client 2 will remain on old scope

            // Adding a new scope
            var remoteOrchestrator = agent1.RemoteOrchestrator; // agent2.RemoteOrchestrator is the same, btw

            // Adding a new scope on the server with this new column and a new table
            // Creating a new scope called "V1" on server
            var setupV1 = new SyncSetup(productCategoryTableName, productTableName);

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

            await using (var ctx = new AdventureWorksContext(this.serverProvider))
            {
                var pc = new ProductCategory
                {
                    ProductCategoryId = productCategoryId,
                    Name = productCategoryName,
                    AttributeWithSpace = newAttributeWithSpaceValue,
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
            command.CommandText = $"ALTER TABLE ProductCategory ADD [Attribute With Space] text NULL;";
            command.Connection = connection;
            await command.ExecuteNonQueryAsync();
            connection.Close();

            // Creating a new table is quite easier since DMS can do it for us
            // Get scope from server (v1 because it contains the new table schema)
            // we already have it, but you cand call GetServerScopInfoAsync("v1") if needed
            // var serverScope = await remoteOrchestrator.GetScopeInfoAsync("v1");
            var localOrchestrator = new LocalOrchestrator(client1provider);

            if (serverProviderType == ProviderType.Sql || serverProviderType == ProviderType.Postgres)
            {
                await localOrchestrator.CreateTableAsync(serverScope, "Product", "SalesLT");
            }
            else
            {
                await localOrchestrator.CreateTableAsync(serverScope, "Product");
            }

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
            await using (var ctx1 = new AdventureWorksContext(client1provider, false))
            {
                var producCategory1 = ctx1.ProductCategory.First(pc => pc.ProductCategoryId == productCategoryId);
                Assert.Equal(newAttributeWithSpaceValue, producCategory1.AttributeWithSpace);
            }

            await using (var ctx2 = new AdventureWorksContext(client2provider, false))
            {
                var exc = Assert.ThrowsAny<Microsoft.Data.Sqlite.SqliteException>(() => ctx2.ProductCategory.First(pc => pc.ProductCategoryId == productCategoryId));
                Assert.Contains("no such column", exc.Message);
            }

            // Assuming we want to migrate the client 2 now
            var serverScope2 = await agent2.RemoteOrchestrator.GetScopeInfoAsync();

            // Create the new table locally
            if (serverProviderType == ProviderType.Sql || serverProviderType == ProviderType.Postgres)
            {
                await agent2.LocalOrchestrator.CreateTableAsync(serverScope2, "Product", "SalesLT");
            }
            else
            {
                await agent2.LocalOrchestrator.CreateTableAsync(serverScope2, "Product");
            }

            // Add this new column on the client 1, with default value as null
            connection = client2provider.CreateConnection();
            connection.Open();
            command = connection.CreateCommand();
            command.CommandText = $"ALTER TABLE ProductCategory ADD [Attribute With Space] text NULL;";
            command.Connection = connection;
            await command.ExecuteNonQueryAsync();
            connection.Close();

            // Don't bother to ShadowCopy metadata, since we are doing a reinit
            // Just Provision
            var clientScope2 = await agent2.LocalOrchestrator.ProvisionAsync(serverScope2);

            // Sync
            r2 = await agent2.SynchronizeAsync("v1", SyncType.Reinitialize);

            await using (var readCtx = new AdventureWorksContext(this.serverProvider))
            {
                productCategoryRowsCount = readCtx.ProductCategory.AsNoTracking().Count();
                productsCount = readCtx.Product.AsNoTracking().Count();
            }

            Assert.Equal(productCategoryRowsCount + productsCount, r2.TotalChangesDownloadedFromServer);
        }

        [Fact]
        public virtual async Task AddingOneColumnInOneTableAndOnSameScope()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // --------------------------
            // Step 1: Create a default scope and Sync clients
            // Note we are not including the [Attribute With Space] column

            // Deletes all tables in client
            foreach (var clientProvider in this.clientsProvider)
            {
                await clientProvider.DropAllTablesAsync(true);
            }

            // Get tables I need (with or without schema)
            var productCategoryTable = this.setup.Tables.First(t => t.TableName == "ProductCategory");
            var productTable = this.setup.Tables.First(t => t.TableName == "Product");

            var setupV1 = new SyncSetup(productCategoryTable.GetFullName());
            setupV1.Tables[productCategoryTable.GetFullName()].Columns.AddRange(
                "ProductCategoryId", "Name", "rowguid", "ModifiedDate");

            int productCategoryRowsCount = 0;
            await using (var readCtx = new AdventureWorksContext(this.serverProvider))
            {
                productCategoryRowsCount = readCtx.ProductCategory.AsNoTracking().Count();
            }

            // First sync to initialiaze client database, create table and fill product categories
            foreach (var clientProvider in this.clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, this.serverProvider, options);
                var r = await agent.SynchronizeAsync("v1", setupV1);

                Assert.Equal(productCategoryRowsCount, r.TotalChangesDownloadedFromServer);
            }

            var remoteOrchestrator = new RemoteOrchestrator(this.serverProvider);

            // Editing the current scope on the server with this new column and a new table
            setupV1.Tables.Add(productTable.GetFullName());
            setupV1.Tables[productCategoryTable.GetFullName()].Columns.Clear();
            setupV1.Tables[productCategoryTable.GetFullName()].Columns.AddRange(
                "ProductCategoryId", "Name", "rowguid", "ModifiedDate", "Attribute With Space");

            // overwrite the setup
            var serverScope = await remoteOrchestrator.ProvisionAsync("v1", setupV1, overwrite: true);

            await this.serverProvider.AddProductCategoryAsync();
            await this.serverProvider.AddProductAsync();

            HelperDatabase.ClearAllPools();

            foreach (var clientProvider in this.clientsProvider)
            {
                var (clientProviderType, _) = HelperDatabase.GetDatabaseType(clientProvider);

                // exception on postgres
                string schema = clientProviderType == ProviderType.Postgres && clientProvider.UseFallbackSchema() ? "SalesLT" : "public";

                var commandText = clientProviderType switch
                {
                    ProviderType.Sql => $"ALTER TABLE {productCategoryTable.GetFullName()} ADD [Attribute With Space] nvarchar(250) NULL;",
                    ProviderType.Sqlite => $"ALTER TABLE {productCategoryTable.TableName} ADD [Attribute With Space] text NULL;",
                    ProviderType.MySql => $"ALTER TABLE `{productCategoryTable.TableName}` ADD `Attribute With Space` nvarchar(250) NULL;",
                    ProviderType.MariaDB => $"ALTER TABLE `{productCategoryTable.TableName}` ADD `Attribute With Space` nvarchar(250) NULL;",
                    ProviderType.Postgres => $"ALTER TABLE \"{schema}\".\"{productCategoryTable.TableName}\" ADD \"Attribute With Space\" character varying(250) NULL;",

                    _ => throw new NotImplementedException(),
                };

                var connection = clientProvider.CreateConnection();

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
                var localOrchestrator = new LocalOrchestrator(clientProvider);
                await localOrchestrator.CreateTableAsync(serverScope, productTable.TableName, string.IsNullOrEmpty(productTable.SchemaName) ? null : productTable.SchemaName);

                // Once created we can override the client scope, thanks to the serverScope instance we already have
                await localOrchestrator.ProvisionAsync(serverScope, overwrite: true);

                // We are ready to sync this new scope !
                var agent = new SyncAgent(clientProvider, this.serverProvider, options);

                var r = await agent.SynchronizeAsync("v1");

                Assert.Equal(2, r.TotalChangesDownloadedFromServer);
            }
        }

        [Fact]
        public virtual async Task AddingOneColumnInOneTableAndOnSameScopeUsingInterceptor()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // --------------------------
            // Step 1: Create a default scope and Sync clients
            // Note we are not including the [Attribute With Space] column

            // Deletes all tables in client
            foreach (var clientProvider in this.clientsProvider)
            {
                await clientProvider.DropAllTablesAsync(true);
            }

            // Get tables I need (with or without schema)
            var productCategoryTable = this.setup.Tables.First(t => t.TableName == "ProductCategory");
            var productTable = this.setup.Tables.First(t => t.TableName == "Product");

            var setupV1 = new SyncSetup(productCategoryTable.GetFullName());
            setupV1.Tables[productCategoryTable.GetFullName()].Columns.AddRange(
                "ProductCategoryId", "Name", "rowguid", "ModifiedDate");

            int productCategoryRowsCount = 0;
            await using (var readCtx = new AdventureWorksContext(this.serverProvider))
            {
                productCategoryRowsCount = readCtx.ProductCategory.AsNoTracking().Count();
            }

            // First sync to initialiaze client database, create table and fill product categories
            foreach (var clientProvider in this.clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, this.serverProvider, options);
                var r = await agent.SynchronizeAsync("v1", setupV1);

                Assert.Equal(productCategoryRowsCount, r.TotalChangesDownloadedFromServer);
            }

            var remoteOrchestrator = new RemoteOrchestrator(this.serverProvider);

            // Editing the current scope on the server with this new column and a new table
            setupV1.Tables.Add(productTable.GetFullName());
            setupV1.Tables[productCategoryTable.GetFullName()].Columns.Clear();
            setupV1.Tables[productCategoryTable.GetFullName()].Columns.AddRange(
                "ProductCategoryId", "Name", "rowguid", "ModifiedDate", "Attribute With Space");

            // overwrite the setup
            var serverScope = await remoteOrchestrator.ProvisionAsync("v1", setupV1, overwrite: true);

            await this.serverProvider.AddProductCategoryAsync();
            await this.serverProvider.AddProductAsync();

            HelperDatabase.ClearAllPools();

            foreach (var clientProvider in this.clientsProvider)
            {
                var (clientProviderType, _) = HelperDatabase.GetDatabaseType(clientProvider);

                // exception on postgres
                string schema = clientProviderType == ProviderType.Postgres && clientProvider.UseFallbackSchema() ? "SalesLT" : "public";

                var commandText = clientProviderType switch
                {
                    ProviderType.Sql => $"ALTER TABLE {productCategoryTable.GetFullName()} ADD [Attribute With Space] nvarchar(250) NULL;",
                    ProviderType.Sqlite => $"ALTER TABLE {productCategoryTable.TableName} ADD [Attribute With Space] text NULL;",
                    ProviderType.MySql => $"ALTER TABLE `{productCategoryTable.TableName}` ADD `Attribute With Space` nvarchar(250) NULL;",
                    ProviderType.MariaDB => $"ALTER TABLE `{productCategoryTable.TableName}` ADD `Attribute With Space` nvarchar(250) NULL;",
                    ProviderType.Postgres => $"ALTER TABLE \"{schema}\".\"{productCategoryTable.TableName}\" ADD \"Attribute With Space\" character varying(250) NULL;",

                    _ => throw new NotImplementedException(),
                };

                var connection = clientProvider.CreateConnection();

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
                var localOrchestrator = new LocalOrchestrator(clientProvider);
                await localOrchestrator.CreateTableAsync(serverScope, productTable.TableName, string.IsNullOrEmpty(productTable.SchemaName) ? null : productTable.SchemaName);

                // We are ready to sync this new scope !
                var agent = new SyncAgent(clientProvider, this.serverProvider);

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
        public virtual async Task UsingOneScopePerTableThenOneScopeForAllTables()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // Get tables I need (with or without schema)
            var productCategoryTable = this.setup.Tables.First(t => t.TableName == "ProductCategory");
            var productTable = this.setup.Tables.First(t => t.TableName == "Product");
            var employeeTable = this.setup.Tables.First(t => t.TableName == "Employee");

            var setupEmployee = new SyncSetup(employeeTable.GetFullName());
            var setupProductCategory = new SyncSetup(productCategoryTable.GetFullName());
            var setupProduct = new SyncSetup(productTable.GetFullName());
            var setupAll = new SyncSetup(employeeTable.GetFullName(), productCategoryTable.GetFullName(), productTable.GetFullName());

            // Provision ALL scope on server
            var remoteOrchestrator = new RemoteOrchestrator(this.serverProvider);
            var allScopeInfoServer = await remoteOrchestrator.ProvisionAsync("ALL", setupAll);

            // First sync to initialiaze client database, create table and fill product categories
            foreach (var clientProvider in this.clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, this.serverProvider, options);

                var rEmployees = await agent.SynchronizeAsync("employees", setupEmployee);
                var rProductCategories = await agent.SynchronizeAsync("productCategories", setupProductCategory);
                var rProducts = await agent.SynchronizeAsync("products", setupProduct);

                Assert.Equal(3, rEmployees.TotalChangesDownloadedFromServer);
                Assert.Equal(11, rProductCategories.TotalChangesDownloadedFromServer);
                Assert.Equal(14, rProducts.TotalChangesDownloadedFromServer);
            }

            // ----------------------------------------------
            // SERVER SIDE: Add a product cat and product
            await this.serverProvider.AddProductAsync();
            await this.serverProvider.AddProductCategoryAsync();

            foreach (var clientProvider in this.clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, this.serverProvider, options);

                await agent.LocalOrchestrator.ProvisionAsync(allScopeInfoServer);

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
        /// Then trying to sync.
        /// </summary>

        [Fact]
        public virtual async Task StartingWithARestoredBackupOnClientWithData()
        {
            // create all clients database with seeding.
            // we are "mimic" here the backup restore
            var newClientsProviders = new List<CoreProvider>();

            foreach (var clientProvider in this.clientsProvider)
            {
                var (clientProviderType, _) = HelperDatabase.GetDatabaseType(clientProvider);

                var clientDatabaseName = HelperDatabase.GetRandomName("cli_start_restored");

                var newClientProvider = HelperDatabase.GetSyncProvider(clientProviderType, clientDatabaseName, clientProviderType == ProviderType.Sql || clientProviderType == ProviderType.Postgres);

                new AdventureWorksContext(newClientProvider).Database.EnsureCreated();
                if (clientProviderType == ProviderType.Sql)
                {
                    await HelperDatabase.ActivateChangeTracking(clientDatabaseName);
                }

                await newClientProvider.EnsureTablesAreCreatedAsync(true);

                newClientsProviders.Add(newClientProvider);
            }

            // Get tables I need (with or without schema)
            var productCategoryTable = this.setup.Tables.First(t => t.TableName == "ProductCategory");
            var productTable = this.setup.Tables.First(t => t.TableName == "Product");
            var employeeTable = this.setup.Tables.First(t => t.TableName == "Employee");

            var setupV1 = new SyncSetup(productCategoryTable.GetFullName(), productTable.GetFullName(), employeeTable.GetFullName());
            var remoteOrchestrator = new RemoteOrchestrator(this.serverProvider);

            // 2) Provision server database
            var serverScope = await remoteOrchestrator.ProvisionAsync(setupV1);

            // 3) Get the timestamp to use on the client
            var serverTimeStamp = await remoteOrchestrator.GetLocalTimestampAsync();

            // 4) Insert some rows in server
            await this.serverProvider.AddProductAsync();
            await this.serverProvider.AddProductCategoryAsync();

            // First sync to initialiaze client database, create table and fill product categories
            foreach (var clientProvider in newClientsProviders)
            {
                // get orchestrator
                var localOrchestrator = new LocalOrchestrator(clientProvider);

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

                var agent = new SyncAgent(clientProvider, this.serverProvider);
                var r1 = await agent.SynchronizeAsync(setupV1);

                Assert.Equal(2, r1.TotalChangesDownloadedFromServer);
                Assert.Equal(2, r1.ChangesAppliedOnClient.TotalAppliedChanges);
            }
        }

        ///// <summary>
        ///// The idea here is to start from a client restore from server backup
        ///// The server made a backup, then we are intiliazing DMS to track changes from that point
        ///// Once the client has restored the database, we can setup the DMS things on client
        ///// Manipulate the client scope as it should be marked as not new (and set the correct timestamps)
        ///// Then trying to sync
        ///// </summary>
        // [Fact]
        // public virtual async Task Scenario_StartingWithAClientBackup()
        // {

        // var setup = new SyncSetup(productCategoryTableName, productTableName, "Employee");
        //    var remoteOrchestrator = new RemoteOrchestrator(this.Server.Provider);

        // // 1) Make a backup
        //    HelperDatabase.BackupDatabase(Server.DatabaseName);

        // // 2) Provision server database
        //    var serverScope = await remoteOrchestrator.ProvisionAsync(setup);

        // // 3) Get the timestamp to use on the client
        //    var serverTimeStamp = await remoteOrchestrator.GetLocalTimestampAsync();

        // // 4) Insert some rows in server
        //    // Create a new ProductCategory and a related Product
        //    var productId = Guid.NewGuid();
        //    var productName = HelperDatabase.GetRandomName();
        //    var productNumber = productName.ToUpperInvariant().Substring(0, 10);
        //    var productCategoryName = HelperDatabase.GetRandomName();
        //    var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

        // var newAttributeWithSpaceValue = HelperDatabase.GetRandomName();

        // using (var ctx = new AdventureWorksContext(Server, this.UseFallbackSchema))
        //    {
        //        var pc = new ProductCategory
        //        {
        //            ProductCategoryId = productCategoryId,
        //            Name = productCategoryName,
        //            AttributeWithSpace = newAttributeWithSpaceValue
        //        };
        //        ctx.ProductCategory.Add(pc);

        // var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber, ProductCategoryId = productCategoryId };
        //        ctx.Product.Add(product);

        // await ctx.SaveChangesAsync();
        //    }

        // // First sync to initialiaze client database, create table and fill product categories
        //    foreach (var client in this.Clients)
        //    {
        //        if (client.ProviderType != ProviderType.Sql)
        //            continue;

        // // 5) Restore a backup
        //        HelperDatabase.RestoreSqlDatabase(Server.DatabaseName, client.DatabaseName);

        // // get orchestrator
        //        var localOrchestrator = new LocalOrchestrator(client.Provider);
        //        // provision client side
        //        await localOrchestrator.ProvisionAsync(serverScope);

        // // 6) Get the local timestamp
        //        var clientTimestamp = await localOrchestrator.GetLocalTimestampAsync();

        // // 7) Get scopeinfoclient
        //        // ScopeInfoClient table contains all information fro the "next" sync to do (timestamp, parameters and so on ...)
        //        var scopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync();

        // // As we have some existing lines, we say it's not a new sync
        //        scopeInfoClient.IsNewScope = false;

        // // Affecting the correct timestamp, the local one and the server one
        //        scopeInfoClient.LastServerSyncTimestamp = serverTimeStamp;
        //        scopeInfoClient.LastSyncTimestamp = clientTimestamp;
        //        await localOrchestrator.SaveScopeInfoClientAsync(scopeInfoClient);

        // var agent = new SyncAgent(client.Provider, Server.Provider);
        //        var r1 = await agent.SynchronizeAsync(setup);

        // Assert.Equal(2, r1.TotalChangesDownloadedFromServer);
        //        Assert.Equal(2, r1.ChangesAppliedOnClient.TotalAppliedChanges);

        // }
        // }
    }
}