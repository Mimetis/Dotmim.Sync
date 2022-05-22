using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
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
    public abstract partial class TcpTests : IClassFixture<HelperProvider>, IDisposable
    {


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
            var setup = new SyncSetup(new string[] { "SalesLT.ProductCategory" });
            setup.Tables["SalesLT.ProductCategory"].Columns.AddRange(
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
                var r = await agent.SynchronizeAsync(setup);

                Assert.Equal(productCategoryRowsCount, r.TotalChangesDownloaded);
            }

            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider);

            // Adding a new scope on the server with this new column and a new table
            // Creating a new scope called "V1" on server
            var setupV1 = new SyncSetup(new string[] { "SalesLT.ProductCategory", "SalesLT.Product" });

            setupV1.Tables["SalesLT.ProductCategory"].Columns.AddRange(
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

            // Add this new column on the client, with default value as null

            foreach (var client in this.Clients)
            {
                var commandText = client.ProviderType switch
                {
                    ProviderType.Sql => @"ALTER TABLE SalesLT.ProductCategory ADD [Attribute With Space] nvarchar(250) NULL;",
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

                // Creating a new table is quite easier since DMS can do it for us
                // Get scope from server (v1 because it contains the new table schema)
                // we already have it, but you cand call GetServerScopInfoAsync("v1") if needed
                // var serverScope = await remoteOrchestrator.GetServerScopeInfoAsync("v1");

                var localOrchestrator = new LocalOrchestrator(client.Provider);
                await localOrchestrator.CreateTableAsync(serverScope, "Product", "SalesLT");

                // Once created we can provision a new scope
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


    }
}
