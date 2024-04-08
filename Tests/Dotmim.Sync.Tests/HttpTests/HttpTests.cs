using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SampleConsole;
using Dotmim.Sync.Serialization;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Builder;
using System.Net.Http.Headers;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;
using System.IdentityModel.Tokens.Jwt;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Tests.Fixtures;
using System.Security.Cryptography;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestPlatform.CommunicationUtilities;

namespace Dotmim.Sync.Tests.IntegrationTests
{

    public abstract partial class HttpTests : DatabaseTest, IClassFixture<DatabaseServerFixture>, IDisposable
    {
        private CoreProvider serverProvider;
        private IEnumerable<CoreProvider> clientsProvider;
        private SyncSetup setup;
        private string serviceUri;

        protected HttpTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
            serverProvider = GetServerProvider();
            clientsProvider = GetClientProviders();
            setup = GetSetup();

            this.Kestrell.AddSyncServer(serverProvider, setup, new SyncOptions { DisableConstraintsOnApplyChanges = true });
            serviceUri = this.Kestrell.Run();
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public virtual async Task RowsCount(SyncOptions options)
        {
            // Get count of rows
            var rowsCount = serverProvider.GetDatabaseRowsCount();

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
            }
        }


        [Fact]
        public async Task CheckAdditionalPropertiesAreConstantAcrossHttpCallsUsingOnHttpSendingRequest()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Add one row on server
            await serverProvider.AddProductCategoryAsync();

            // Add one row in each client
            foreach (var clientProvider in clientsProvider)
                await clientProvider.AddProductCategoryAsync();

            // Stop Kestrell to reconfigure
            await this.Kestrell.StopAsync();

            // Add again the serverprovider
            this.Kestrell.AddSyncServer(serverProvider, setup, options);

            // override server handler to use OnHttpGettingRequest and OnHttpSendingResponse
            var serverHandler = new RequestDelegate(async context =>
            {
                var webServerAgent = context.RequestServices.GetService<WebServerAgent>();

                webServerAgent.OnHttpGettingRequest(args =>
                {
                    Assert.NotEmpty(args.Context.AdditionalProperties);
                    Assert.Single(args.Context.AdditionalProperties);
                });

                webServerAgent.OnHttpSendingResponse(args =>
                {
                    Assert.NotEmpty(args.Context.AdditionalProperties);
                    Assert.Single(args.Context.AdditionalProperties);
                });

                await webServerAgent.HandleRequestAsync(context);

            });

            var serviceUri = this.Kestrell.Run(serverHandler);

            var download = 1;
            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);
                var agent = new SyncAgent(clientProvider, webRemoteOrchestrator, options);

                webRemoteOrchestrator.OnHttpSendingRequest(args =>
                {
                    if (args.Context.AdditionalProperties == null)
                        args.Context.AdditionalProperties = new Dictionary<string, string> { { "A", "1" } };

                    Assert.NotEmpty(args.Context.AdditionalProperties);
                    Assert.Single(args.Context.AdditionalProperties);
                });

                webRemoteOrchestrator.OnHttpGettingResponse(args =>
                {
                    Assert.NotEmpty(args.Context.AdditionalProperties);
                    Assert.Single(args.Context.AdditionalProperties);
                });

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
        }

        [Fact]
        public async Task CheckAdditionalPropertiesAreConstantAcrossHttpCallsUsingOnSessionBegins()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Add one row on server
            await serverProvider.AddProductCategoryAsync();

            // Add one row in each client
            foreach (var clientProvider in clientsProvider)
                await clientProvider.AddProductCategoryAsync();

            // Stop Kestrell to reconfigure
            await this.Kestrell.StopAsync();

            // Add again the serverprovider
            this.Kestrell.AddSyncServer(serverProvider, setup, options);

            // override server handler to use OnHttpGettingRequest and OnHttpSendingResponse
            var serverHandler = new RequestDelegate(async context =>
            {
                var webServerAgent = context.RequestServices.GetService<WebServerAgent>();

                webServerAgent.OnHttpGettingRequest(args =>
                {
                    Assert.NotEmpty(args.Context.AdditionalProperties);
                    Assert.Single(args.Context.AdditionalProperties);
                });

                webServerAgent.OnHttpSendingResponse(args =>
                {
                    Assert.NotEmpty(args.Context.AdditionalProperties);
                    Assert.Single(args.Context.AdditionalProperties);
                });

                await webServerAgent.HandleRequestAsync(context);

            });

            var serviceUri = this.Kestrell.Run(serverHandler);

            var download = 1;
            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);
                var agent = new SyncAgent(clientProvider, webRemoteOrchestrator, options);

                agent.LocalOrchestrator.OnSessionBegin(args =>
                {
                    if (args.Context.AdditionalProperties == null)
                        args.Context.AdditionalProperties = new Dictionary<string, string> { { "A", "1" } };
                });

                webRemoteOrchestrator.OnHttpGettingResponse(args =>
                {
                    Assert.NotEmpty(args.Context.AdditionalProperties);
                    Assert.Single(args.Context.AdditionalProperties);
                });

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task MultiScopes(SyncOptions options)
        {
            // get the number of rows that have only primary keys (which do not accept any Update)
            int notUpdatedOnClientsCount;
            using (var serverDbCtx = new AdventureWorksContext(serverProvider))
            {
                var pricesListCategoriesCount = serverDbCtx.PricesListCategory.Count();
                var postTagsCount = serverDbCtx.PostTag.Count();
                notUpdatedOnClientsCount = pricesListCategoriesCount + postTagsCount;
            }

            // Get count of rows
            var rowsCount = this.serverProvider.GetDatabaseRowsCount();

            await this.Kestrell.StopAsync();

            this.Kestrell.AddSyncServer(serverProvider, setup,
                new SyncOptions { DisableConstraintsOnApplyChanges = true }, null, "v1", "db1");

            this.Kestrell.AddSyncServer(serverProvider, setup,
                new SyncOptions { DisableConstraintsOnApplyChanges = true }, null, "v2", "db1");

            this.Kestrell.AddSyncServer(serverProvider, setup,
                new SyncOptions { DisableConstraintsOnApplyChanges = true }, identifier: "db2");

            var serviceUri = this.Kestrell.Run();

            foreach (var clientProvider in clientsProvider)
            {
                var (clientProviderType, _) = HelperDatabase.GetDatabaseType(clientProvider);

                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri, identifier: "db1"), options);

                // On first sync, even tables with only primary keys are inserted
                var s = await agent.SynchronizeAsync("v1", setup);
                var clientRowsCount = clientProvider.GetDatabaseRowsCount();
                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(rowsCount, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(rowsCount, clientRowsCount);

                var s2 = await agent.SynchronizeAsync("v2", setup);

                clientRowsCount = clientProvider.GetDatabaseRowsCount();
                Assert.Equal(rowsCount, s2.TotalChangesDownloadedFromServer);

                // On second sync, tables with only primary keys are downloaded but not inserted or updated
                // except SQLite
                if (clientProviderType == ProviderType.Sqlite) // Sqlite make a REPLACE statement, so primary keys only tables will increment count
                    Assert.Equal(rowsCount, s2.TotalChangesAppliedOnClient);
                else
                    Assert.Equal(rowsCount - notUpdatedOnClientsCount, s2.TotalChangesAppliedOnClient);

                Assert.Equal(0, s2.TotalChangesUploadedToServer);
                Assert.Equal(rowsCount, clientRowsCount);
            }
        }

        [Fact]
        public async Task BadConnectionFromServerShouldRaiseError()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };
            var (serverProviderType, _) = HelperDatabase.GetDatabaseType(serverProvider);

            var badServerProvider = HelperDatabase.GetSyncProvider(serverProviderType, HelperDatabase.GetRandomName("tcp_srv_bad_"));
            badServerProvider.ConnectionString = $@"Server=unknown;Database=unknown;UID=sa;PWD=unknown";

            // Create a client provider, but it will not be used since server provider will raise an error before
            var clientProvider = clientsProvider.First();

            using var kestrell = new KestrellTestServer(this.UseFiddler);
            kestrell.AddSyncServer(badServerProvider, setup, options);
            var serviceUri = kestrell.Run();

            var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

            var se = await Assert.ThrowsAnyAsync<SyncException>(async () => await agent.SynchronizeAsync(setup));
        }

        [Fact]
        public async Task BadConnectionFromClientShouldRaiseError()
        {
            var badClientsProviders = new List<CoreProvider>();

            foreach (var clientProvider in clientsProvider)
            {
                var (clientProviderType, _) = HelperDatabase.GetDatabaseType(clientProvider);
                var badClientProvider = HelperDatabase.GetSyncProvider(clientProviderType, HelperDatabase.GetRandomName("tcp_bad_cli"));

                if (clientProviderType == ProviderType.Sqlite)
                    badClientProvider.ConnectionString = $@"Data Source=/dev/null/foo;";
                else
                    badClientProvider.ConnectionString = $@"Server=unknown;Database=unknown;UID=sa;PWD=unknown";

                badClientsProviders.Add(badClientProvider);
            }

            // Execute a sync on all clients and check results
            foreach (var badClientProvider in badClientsProviders)
            {
                var (t, d) = HelperDatabase.GetDatabaseType(badClientProvider);
                var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };
                var agent = new SyncAgent(badClientProvider, new WebRemoteOrchestrator(serviceUri), options);

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () => await agent.SynchronizeAsync(setup));
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task InsertOneRowInOneTableOnServerSide(SyncOptions options)
        {
            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            await serverProvider.AddProductCategoryAsync();

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task InsertTwoRowsInTwoTablesOnServerSide(SyncOptions options)
        {
            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            await serverProvider.AddProductCategoryAsync();
            await serverProvider.AddProductCategoryAsync();
            await serverProvider.AddProductAsync();
            await serverProvider.AddProductAsync();

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync();

                Assert.Equal(4, s.TotalChangesDownloadedFromServer);
                Assert.Equal(4, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task InsertOneRowThenUpdateThisRowOnServerSide(SyncOptions options)
        {
            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            var serverProductCategory = await serverProvider.AddProductCategoryAsync();

            var pcName = string.Concat(serverProductCategory.ProductCategoryId, "UPDATED");
            serverProductCategory.Name = pcName;

            await serverProvider.UpdateProductCategoryAsync(serverProductCategory);

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync();

                var clientProductCategory = await clientProvider.GetProductCategoryAsync(serverProductCategory.ProductCategoryId);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.Equal(pcName, clientProductCategory.Name);
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task InsertOneRowInOneTableOnClientSide(SyncOptions options)
        {
            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Add one row in each client
            foreach (var clientProvider in clientsProvider)
                await clientProvider.AddProductCategoryAsync();

            var download = 0;
            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task InsertTwoRowsInTwoTablesOnClientSide(SyncOptions options)
        {
            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Add one row in each client
            foreach (var clientProvider in clientsProvider)
            {
                await clientProvider.AddProductCategoryAsync();
                await clientProvider.AddProductCategoryAsync();
                await clientProvider.AddProductAsync();
                await clientProvider.AddProductAsync();
            }

            var download = 0;
            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync();

                Assert.Equal(download, s.TotalChangesDownloadedFromServer);
                Assert.Equal(4, s.TotalChangesUploadedToServer);
                Assert.Equal(4, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
                download += 4;
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task InsertTenThousandsRowsInOneTableOnClientSide(SyncOptions options)
        {
            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            var rowsCountToInsert = 10000;

            // Add one row in each client
            foreach (var clientProvider in clientsProvider)
                for (var i = 0; i < rowsCountToInsert; i++)
                    await clientProvider.AddProductCategoryAsync();

            var download = 0;
            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync();

                Assert.Equal(download, s.TotalChangesDownloadedFromServer);
                Assert.Equal(rowsCountToInsert, s.TotalChangesUploadedToServer);
                Assert.Equal(rowsCountToInsert, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
                download += rowsCountToInsert;
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task InsertOneRowAndDeleteOneRowInOneTableOnServerSide(SyncOptions options)
        {
            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            var firstProductCategory = await serverProvider.AddProductCategoryAsync();

            // sync this category on each client to be able to delete it after
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // add one row
            await serverProvider.AddProductCategoryAsync();
            // delete one row
            await serverProvider.DeleteProductCategoryAsync(firstProductCategory.ProductCategoryId);

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(2, s.TotalChangesAppliedOnClient);
                Assert.Equal(2, s.ChangesAppliedOnClient.TableChangesApplied.Count);
                Assert.Equal(1, s.ChangesAppliedOnClient.TableChangesApplied[0].Applied);
                Assert.Equal(1, s.ChangesAppliedOnClient.TableChangesApplied[1].Applied);

                var rowState = s.ChangesAppliedOnClient.TableChangesApplied[0].State;
                var otherRowState = rowState == SyncRowState.Modified ? SyncRowState.Deleted : SyncRowState.Modified;
                Assert.Equal(otherRowState, s.ChangesAppliedOnClient.TableChangesApplied[1].State);

                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task InsertOneRowWithByteArrayOnServerSide(SyncOptions options)
        {
            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            var thumbnail = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

            // add one row
            var product = await serverProvider.AddProductAsync(thumbNailPhoto: thumbnail);

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var clientProduct = await clientProvider.GetProductAsync(product.ProductId);

                Assert.Equal(product.ThumbNailPhoto, clientProduct.ThumbNailPhoto);

                for (var i = 0; i < product.ThumbNailPhoto.Length; i++)
                    Assert.Equal(product.ThumbNailPhoto[i], clientProduct.ThumbNailPhoto[i]);

                Assert.Equal(thumbnail.Length, clientProduct.ThumbNailPhoto.Length);
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task InsertOneRowInOneTableOnClientSideThenInsertAgainDuringGetChanges(SyncOptions options)
        {
            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Add one row in each client
            foreach (var clientProvider in clientsProvider)
            {
                await clientProvider.AddProductCategoryAsync();
                await clientProvider.AddProductAsync();
                await clientProvider.AddPriceListAsync();
            }

            // Sync all clients
            // First client  will upload 3 lines and will download nothing
            // Second client will upload 3 lines and will download 3 lines
            // thrid client  will upload 3 lines and will download 6 lines
            var download = 0;
            foreach (var clientProvider in clientsProvider)
            {
                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);

                // Sleep during a selecting changes on first sync
                async Task tableChangesSelected(TableChangesSelectedArgs changes)
                {
                    if (changes.TableChangesSelected.TableName != "PricesList")
                        return;
                    try
                    {
                        await clientProvider.AddPriceListAsync(transaction: changes.Transaction);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        throw;
                    }
                    return;
                };

                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                // Intercept TableChangesSelected
                agent.LocalOrchestrator.OnTableChangesSelected(tableChangesSelected);

                var s = await agent.SynchronizeAsync(setup);

                agent.LocalOrchestrator.ClearInterceptors();

                Assert.Equal(download, s.TotalChangesDownloadedFromServer);
                Assert.Equal(3, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
                download += 3;

            }

            // CLI1 (6 rows) : CLI1 will upload 1 row and download 3 rows from CLI2 and 3 rows from CLI3
            // CLI2 (4 rows) : CLI2 will upload 1 row and download 3 rows from CLI3 and 1 row from CLI1
            // CLI3 (2 rows) : CLI3 will upload 1 row and download 1 row from CLI1 and 1 row from CLI2
            download = 3 * (clientsProvider.Count() - 1);
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
                download -= 2;
            }


            // CLI1 (6) : CLI1 will download 1 row from CLI3 and 1 rows from CLI2
            // CLI2 (4) : CLI2 will download 1 row from CLI3
            // CLI3 (2) : CLI3 will download nothing
            download = clientsProvider.Count() - 1;
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download--, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // check rows count on server and on each client
            using var ctx = new AdventureWorksContext(serverProvider);

            var productRowCount = await ctx.Product.AsNoTracking().CountAsync();
            var productCategoryCount = await ctx.ProductCategory.AsNoTracking().CountAsync();
            var priceListCount = await ctx.PricesList.AsNoTracking().CountAsync();
            var rowsCount = serverProvider.GetDatabaseRowsCount();

            foreach (var clientProvider in clientsProvider)
            {
                Assert.Equal(rowsCount, clientProvider.GetDatabaseRowsCount());

                using var cliCtx = new AdventureWorksContext(clientProvider);
                var pCount = await cliCtx.Product.AsNoTracking().CountAsync();
                Assert.Equal(productRowCount, pCount);

                var pcCount = await cliCtx.ProductCategory.AsNoTracking().CountAsync();
                Assert.Equal(productCategoryCount, pcCount);

                var plCount = await cliCtx.PricesList.AsNoTracking().CountAsync();
                Assert.Equal(priceListCount, plCount);
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task UpdateOneRowInOneTableOnServerSide(SyncOptions options)
        {
            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            var productCategory = await serverProvider.AddProductCategoryAsync();

            // sync this category on each client to be able to update productCategory after
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            var updatedProductCategoryName = $"UPDATED_{productCategory.Name}";

            productCategory.Name = updatedProductCategoryName;
            await serverProvider.UpdateProductCategoryAsync(productCategory);

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Single(s.ChangesAppliedOnClient.TableChangesApplied);
                Assert.Equal(1, s.ChangesAppliedOnClient.TableChangesApplied[0].Applied);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var clientProductCategory = await clientProvider.GetProductCategoryAsync(productCategory.ProductCategoryId);
                Assert.Equal(updatedProductCategoryName, clientProductCategory.Name);
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task UpdateOneRowInOneTableOnClientSide(SyncOptions options)
        {
            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Update one address on each client
            // To avoid conflicts, each client will update differents lines
            // each address id is generated from the foreach index
            var addressId = 0;
            foreach (var clientProvider in clientsProvider)
            {
                using (var ctx = new AdventureWorksContext(clientProvider))
                {
                    var addresses = ctx.Address.OrderBy(a => a.AddressId).Where(a => !string.IsNullOrEmpty(a.AddressLine2)).Take(clientsProvider.ToList().Count).ToList();
                    var address = addresses[addressId];

                    // Update at least two properties
                    address.City = HelperDatabase.GetRandomName("City");
                    address.AddressLine1 = HelperDatabase.GetRandomName("Address");

                    await ctx.SaveChangesAsync();
                }
                addressId++;
            }

            // Sync
            var download = 0;
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Now sync again to be sure all clients have all lines
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options).SynchronizeAsync(setup);

            // get rows count
            var rowsCount = serverProvider.GetDatabaseRowsCount();

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(serverProvider))
            {
                // get all addresses
                var serverAddresses = await ctx.Address.AsNoTracking().ToListAsync();

                foreach (var clientProvider in clientsProvider)
                {
                    Assert.Equal(rowsCount, clientProvider.GetDatabaseRowsCount());

                    using var cliCtx = new AdventureWorksContext(clientProvider);
                    // get all addresses
                    var clientAddresses = await cliCtx.Address.AsNoTracking().ToListAsync();

                    // check row count
                    Assert.Equal(serverAddresses.Count, clientAddresses.Count);

                    foreach (var clientAddress in clientAddresses)
                    {
                        var serverAddress = serverAddresses.First(a => a.AddressId == clientAddress.AddressId);

                        // check column value
                        Assert.Equal(serverAddress.StateProvince, clientAddress.StateProvince);
                        Assert.Equal(serverAddress.AddressLine1, clientAddress.AddressLine1);
                        Assert.Equal(serverAddress.AddressLine2, clientAddress.AddressLine2);
                    }
                }
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task UpdateOneRowToNullInOneTableOnClientSide(SyncOptions options)
        {
            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Update one address on each client
            // To avoid conflicts, each client will update differents lines
            // each address id is generated from the foreach index
            var addressId = 0;
            foreach (var clientProvider in clientsProvider)
            {
                using (var ctx = new AdventureWorksContext(clientProvider))
                {
                    var addresses = ctx.Address.OrderBy(a => a.AddressId).Where(a => !string.IsNullOrEmpty(a.AddressLine2)).Take(clientsProvider.ToList().Count).ToList();
                    var address = addresses[addressId];

                    // Update a column to null value
                    address.AddressLine2 = null;

                    await ctx.SaveChangesAsync();
                }
                addressId++;
            }

            // Sync
            var download = 0;
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Now sync again to be sure all clients have all lines
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options).SynchronizeAsync(setup);

            // get rows count
            var rowsCount = serverProvider.GetDatabaseRowsCount();

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(serverProvider))
            {
                // get all addresses
                var serverAddresses = await ctx.Address.AsNoTracking().ToListAsync();

                foreach (var clientProvider in clientsProvider)
                {
                    Assert.Equal(rowsCount, clientProvider.GetDatabaseRowsCount());

                    using var cliCtx = new AdventureWorksContext(clientProvider);
                    // get all addresses
                    var clientAddresses = await cliCtx.Address.AsNoTracking().ToListAsync();

                    // check row count
                    Assert.Equal(serverAddresses.Count, clientAddresses.Count);

                    foreach (var clientAddress in clientAddresses)
                    {
                        var serverAddress = serverAddresses.First(a => a.AddressId == clientAddress.AddressId);

                        // check column value
                        Assert.Equal(serverAddress.AddressLine2, clientAddress.AddressLine2);
                    }
                }
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task UpdateOneRowToNullInOneTableOnServerSide(SyncOptions options)
        {
            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            Address address;
            // Update one address to null on server side
            using (var ctx = new AdventureWorksContext(serverProvider))
            {
                address = ctx.Address.OrderBy(a => a.AddressId).Where(a => !string.IsNullOrEmpty(a.AddressLine2)).First();
                address.AddressLine2 = null;
                await ctx.SaveChangesAsync();
            }

            // Sync
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                // Check value
                using var ctx = new AdventureWorksContext(clientProvider);
                var cliAddress = await ctx.Address.AsNoTracking().SingleAsync(a => a.AddressId == address.AddressId);
                Assert.Null(cliAddress.AddressLine2);
            }

            // Update one address previously null to not null on server side
            using (var ctx = new AdventureWorksContext(serverProvider))
            {
                address = await ctx.Address.SingleAsync(a => a.AddressId == address.AddressId);
                address.AddressLine2 = "NoT a null value !";
                await ctx.SaveChangesAsync();
            }

            // Sync
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                // Check value
                using var ctx = new AdventureWorksContext(clientProvider);
                var cliAddress = await ctx.Address.AsNoTracking().SingleAsync(a => a.AddressId == address.AddressId);
                Assert.Equal("NoT a null value !", cliAddress.AddressLine2);
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task DeleteOneRowInOneTableOnServerSide(SyncOptions options)
        {
            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            var firstProductCategory = await serverProvider.AddProductCategoryAsync();

            // sync this category on each client to be able to delete it after
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // delete one row
            await serverProvider.DeleteProductCategoryAsync(firstProductCategory.ProductCategoryId);

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Single(s.ChangesAppliedOnClient.TableChangesApplied);
                Assert.Equal(1, s.ChangesAppliedOnClient.TableChangesApplied[0].Applied);
                Assert.Equal(SyncRowState.Deleted, s.ChangesAppliedOnClient.TableChangesApplied[0].State);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task DeleteOneRowInOneTableOnClientSide(SyncOptions options)
        {
            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // To avoid conflicts, each client will add a product category
            // each address id is generated from the foreach index
            foreach (var clientProvider in clientsProvider)
                await clientProvider.AddProductCategoryAsync(name: $"CLI_{HelperDatabase.GetRandomName()}");

            // Execute two sync on all clients to be sure all clients have all lines
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Now delete rows on each client
            foreach (var clientsProvider in clientsProvider)
            {
                // Then delete all product category items
                using var ctx = new AdventureWorksContext(clientsProvider);
                foreach (var pc in ctx.ProductCategory.Where(pc => pc.Name.StartsWith("CLI_")))
                    ctx.ProductCategory.Remove(pc);
                await ctx.SaveChangesAsync();
            }

            var cpt = 0; // first client won't have any conflicts, but others will upload their deleted rows that are ALREADY deleted
            foreach (var clientProvider in clientsProvider)
            {
                var s = await new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options).SynchronizeAsync();

                // we are downloading deleted rows from server
                Assert.Equal(cpt, s.TotalChangesDownloadedFromServer);
                // but we should not have any rows applied locally
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                // anyway we are always uploading our deleted rows
                Assert.Equal(clientsProvider.ToList().Count, s.TotalChangesUploadedToServer);
                // w may have resolved conflicts locally
                Assert.Equal(cpt, s.TotalResolvedConflicts);

                cpt = clientsProvider.ToList().Count;
            }

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(serverProvider))
            {
                var serverPC = await ctx.ProductCategory.AsNoTracking().CountAsync();
                foreach (var clientProvider in clientsProvider)
                {
                    using var cliCtx = new AdventureWorksContext(clientProvider);
                    var clientPC = await cliCtx.ProductCategory.AsNoTracking().CountAsync();
                    Assert.Equal(serverPC, clientPC);
                }
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Reinitialize(SyncOptions options)
        {
            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Get count of rows
            var rowsCount = this.serverProvider.GetDatabaseRowsCount();

            // Reset stored proc needs it.
            options.DisableConstraintsOnApplyChanges = true;

            // Add one row in each client then Reinitialize
            foreach (var clientProvider in clientsProvider)
            {
                var productCategory = await clientProvider.AddProductCategoryAsync();

                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync(setup, SyncType.Reinitialize);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(rowsCount, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);

                // The row should not be present as it has been overwritten by Reinitiliaze
                var pc = await clientProvider.GetProductCategoryAsync(productCategory.ProductCategoryId);
                Assert.Null(pc);
            }

        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task ReinitializeWithUpload(SyncOptions options)
        {
            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Get count of rows
            var rowsCount = this.serverProvider.GetDatabaseRowsCount();

            // Reset stored proc needs it.
            options.DisableConstraintsOnApplyChanges = true;

            // Add one row in each client then ReinitializeWithUpload
            var download = 1;
            foreach (var clientProvider in clientsProvider)
            {
                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);
                var productCategory = await clientProvider.AddProductCategoryAsync();

                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync(setup, SyncType.ReinitializeWithUpload);

                Assert.Equal(rowsCount + download, s.TotalChangesDownloadedFromServer);
                Assert.Equal(rowsCount + download, s.TotalChangesAppliedOnClient);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnServer);

                // The row should be present 
                var pc = await clientProvider.GetProductCategoryAsync(productCategory.ProductCategoryId);
                Assert.NotNull(pc);
                download++;
            }

        }

        [Fact]
        public async Task UploadOnly()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            var setupV2 = GetSetup();

            foreach (var table in setupV2.Tables)
                table.SyncDirection = SyncDirection.UploadOnly;

            await this.Kestrell.StopAsync();

            this.Kestrell.AddSyncServer(serverProvider, setupV2, new SyncOptions { DisableConstraintsOnApplyChanges = true }, null, "uploadonly");


            var serviceUri = this.Kestrell.Run();

            // Should not download anything
            foreach (var clientProvider in clientsProvider)
            {
                var s = await new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options).SynchronizeAsync("uploadonly", setupV2);
                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
            }

            // Add one row in each client
            foreach (var clientProvider in clientsProvider)
                await clientProvider.AddProductCategoryAsync();

            // Add a pc on server
            await serverProvider.AddProductCategoryAsync();

            // Sync all clients
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync("uploadonly");

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
        }

        [Fact]
        public async Task DownloadOnly()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            var setupV2 = GetSetup();

            foreach (var table in setupV2.Tables)
                table.SyncDirection = SyncDirection.DownloadOnly;

            await this.Kestrell.StopAsync();

            this.Kestrell.AddSyncServer(serverProvider, setupV2,
                new SyncOptions { DisableConstraintsOnApplyChanges = true }, null, "downloadonly");

            var serviceUri = this.Kestrell.Run();

            // Get count of rows
            var rowsCount = this.serverProvider.GetDatabaseRowsCount();

            // Should not download anything
            foreach (var clientProvider in clientsProvider)
            {
                var s = await new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options).SynchronizeAsync("downloadonly", setupV2);
                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(rowsCount, s.TotalChangesAppliedOnClient);
            }

            // Add one row in each client
            foreach (var clientProvider in clientsProvider)
                await clientProvider.AddProductCategoryAsync();

            // Add a pc on server
            await serverProvider.AddProductCategoryAsync();

            // Sync all clients
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync("downloadonly");

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Snapshots(SyncOptions options)
        {
            // snapshot directory
            var snapshotDirctory = HelperDatabase.GetRandomName();
            var directory = Path.Combine(Environment.CurrentDirectory, snapshotDirctory);

            // Settings the options to enable snapshot
            options.SnapshotsDirectory = directory;
            options.BatchSize = 3000;
            // Disable constraints
            options.DisableConstraintsOnApplyChanges = true;

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            // Adding a row that I will delete after creating snapshot
            var productCategoryTodelete = await serverProvider.AddProductCategoryAsync();

            // Create a snapshot
            await remoteOrchestrator.CreateSnapshotAsync(setup);

            // Add rows after creating snapshot
            var pc1 = await serverProvider.AddProductCategoryAsync();
            var pc2 = await serverProvider.AddProductCategoryAsync();
            var p1 = await serverProvider.AddProductAsync();
            var p2 = await serverProvider.AddPriceListAsync();
            // Delete a row
            await serverProvider.DeleteProductCategoryAsync(productCategoryTodelete.ProductCategoryId);

            // Get count of rows
            var rowsCount = serverProvider.GetDatabaseRowsCount();

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var s = await agent.SynchronizeAsync(setup);

                // + 2 because
                // * 1 for the product category to delete, part of snapshot
                // * 1 for the product category to delete, actually deleted
                Assert.Equal(rowsCount + 2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(rowsCount + 2, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.Equal(rowsCount - 5 + 2, s.SnapshotChangesAppliedOnClient.TotalAppliedChanges);
                Assert.Equal(5, s.ChangesAppliedOnClient.TotalAppliedChanges);
                Assert.Equal(5, s.ServerChangesSelected.TotalChangesSelected);

                Assert.Equal(rowsCount, clientProvider.GetDatabaseRowsCount());

                // Check rows added or deleted
                var clipc = await clientProvider.GetProductCategoryAsync(productCategoryTodelete.ProductCategoryId);
                Assert.Null(clipc);
                var cliPC1 = await clientProvider.GetProductCategoryAsync(pc1.ProductCategoryId);
                Assert.NotNull(cliPC1);
                var cliPC2 = await clientProvider.GetProductCategoryAsync(pc2.ProductCategoryId);
                Assert.NotNull(cliPC2);
                var cliP1 = await clientProvider.GetProductAsync(p1.ProductId);
                Assert.NotNull(cliP1);
                var cliP2 = await clientProvider.GetPriceListAsync(p2.PriceListId);
                Assert.NotNull(cliP2);
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task SnapshotsThenReinitialize(SyncOptions options)
        {
            // snapshot directory
            var snapshotDirctory = HelperDatabase.GetRandomName();
            var directory = Path.Combine(Environment.CurrentDirectory, snapshotDirctory);

            // Settings the options to enable snapshot
            options.SnapshotsDirectory = directory;
            options.BatchSize = 3000;
            // Disable constraints
            options.DisableConstraintsOnApplyChanges = true;

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            // Adding a row that I will delete after creating snapshot
            var productCategoryTodelete = await serverProvider.AddProductCategoryAsync();

            // Create a snapshot
            await remoteOrchestrator.CreateSnapshotAsync(setup);

            // Add rows after creating snapshot
            var pc1 = await serverProvider.AddProductCategoryAsync();
            var pc2 = await serverProvider.AddProductCategoryAsync();
            var p1 = await serverProvider.AddProductAsync();
            var p2 = await serverProvider.AddPriceListAsync();
            // Delete a row
            await serverProvider.DeleteProductCategoryAsync(productCategoryTodelete.ProductCategoryId);

            // Execute a sync on all clients
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);
                await agent.SynchronizeAsync(setup);

                // Check rows added or deleted
                var clipc = await clientProvider.GetProductCategoryAsync(productCategoryTodelete.ProductCategoryId);
                Assert.Null(clipc);
                var cliPC1 = await clientProvider.GetProductCategoryAsync(pc1.ProductCategoryId);
                Assert.NotNull(cliPC1);
                var cliPC2 = await clientProvider.GetProductCategoryAsync(pc2.ProductCategoryId);
                Assert.NotNull(cliPC2);
                var cliP1 = await clientProvider.GetProductAsync(p1.ProductId);
                Assert.NotNull(cliP1);
                var cliP2 = await clientProvider.GetPriceListAsync(p2.PriceListId);
                Assert.NotNull(cliP2);
            }

            // Add one row in each client then ReinitializeWithUpload
            foreach (var clientProvider in clientsProvider)
            {
                var productCategory = await clientProvider.AddProductCategoryAsync();

                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync(setup, SyncType.ReinitializeWithUpload);

                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnServer);

                // Check rows added or deleted
                var pc = await clientProvider.GetProductCategoryAsync(productCategory.ProductCategoryId);
                Assert.NotNull(pc);
                var clipc = await clientProvider.GetProductCategoryAsync(productCategoryTodelete.ProductCategoryId);
                Assert.Null(clipc);
                var cliPC1 = await clientProvider.GetProductCategoryAsync(pc1.ProductCategoryId);
                Assert.NotNull(cliPC1);
                var cliPC2 = await clientProvider.GetProductCategoryAsync(pc2.ProductCategoryId);
                Assert.NotNull(cliPC2);
                var cliP1 = await clientProvider.GetProductAsync(p1.ProductId);
                Assert.NotNull(cliP1);
                var cliP2 = await clientProvider.GetPriceListAsync(p2.PriceListId);
                Assert.NotNull(cliP2);
            }

            // Execute a sync on all clients to be sure all clients have all rows
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options).SynchronizeAsync();

            // Get count of rows
            var rowsCount = serverProvider.GetDatabaseRowsCount();

            // Execute a sync on all clients to be sure all clients have all rows
            foreach (var clientProvider in clientsProvider)
                Assert.Equal(rowsCount, clientProvider.GetDatabaseRowsCount());
        }

        /// <summary>
        /// Insert one row on each client, should be sync on server and clients
        /// </summary>
        [Fact]
        public async Task BadConverterNotRegisteredOnServerShouldRaiseError()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                // Add a converter on the client.
                // But this converter is not register on the server side converters list.
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri, new DateConverter());
                webRemoteOrchestrator.SyncPolicy.RetryCount = 0;

                var agent = new SyncAgent(clientProvider, webRemoteOrchestrator, options);

                var exception = await Assert.ThrowsAsync<HttpSyncWebException>(async () =>
                {
                    var s = await agent.SynchronizeAsync();
                });

                Assert.Equal("HttpConverterNotConfiguredException", exception.TypeName);
            }
        }

        ///// <summary>
        ///// Check web interceptors are working correctly
        ///// </summary>
        ////[Theory]
        ////[ClassData(typeof(SyncOptionsData))]
        //public async Task Check_Interceptors_WebServerAgent(SyncOptions options)
        //{
        //    // create a server db and seed it
        //    await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

        //    // create empty client databases
        //    foreach (var client in this.Clients)
        //        await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

        //    // Get count of rows
        //    var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

        //    // configure server orchestrator
        //    this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
        //        new SyncSetup(Tables));

        //    // Create server web proxy
        //    var serverHandler = new RequestDelegate(async context =>
        //    {
        //        var webServerAgent = context.RequestServices.GetService(typeof(WebServerAgent)) as WebServerAgent;

        //        webServerAgent.OnHttpGettingRequest(r =>
        //        {
        //            Assert.NotNull(r.HttpContext);
        //            Assert.NotNull(r.Context);
        //        });

        //        webServerAgent.OnHttpSendingResponse(r =>
        //        {
        //            Assert.NotNull(r.HttpContext);
        //            Assert.NotNull(r.Context);
        //        });

        //        await webServerAgent.HandleRequestAsync(context);
        //    });

        //    var serviceUri = this.Kestrell.Run(serverHandler);

        //    // Execute a sync on all clients and check results
        //    foreach (var client in this.Clients)
        //    {
        //        var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);

        //        var s = await agent.SynchronizeAsync();

        //        Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
        //        Assert.Equal(0, s.TotalChangesUploadedToServer);
        //    }
        //}

        ///// <summary>
        ///// Check web interceptors are working correctly
        ///// </summary>
        ////[Theory]
        ////[ClassData(typeof(SyncOptionsData))]
        //public async Task Check_Interceptors_WebRemoteOrchestrator(SyncOptions options)
        //{
        //    // create a server schema without seeding
        //    await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

        //    // create empty client databases
        //    foreach (var client in this.Clients)
        //        await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

        //    // configure server orchestrator
        //    this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
        //        new SyncSetup(Tables));

        //    var serviceUri = this.Kestrell.Run();

        //    foreach (var client in Clients)
        //    {
        //        var wenClientOrchestrator = new WebRemoteOrchestrator(serviceUri);
        //        var agent = new SyncAgent(client.Provider, wenClientOrchestrator, options);

        //        // Interceptor on sending scopes
        //        wenClientOrchestrator.OnHttpGettingScopeResponse(sra =>
        //        {
        //            // check we a scope name
        //            Assert.NotNull(sra.Context);
        //        });

        //        var s = await agent.SynchronizeAsync();

        //        Assert.Equal(0, s.TotalChangesDownloadedFromServer);
        //        Assert.Equal(0, s.TotalChangesUploadedToServer);
        //        Assert.Equal(0, s.TotalResolvedConflicts);

        //        wenClientOrchestrator.ClearInterceptors();
        //    }

        //    // Insert one line on each client
        //    foreach (var client in Clients)
        //    {
        //        var name = HelperDatabase.GetRandomName();
        //        var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

        //        var product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };

        //        using var serverDbCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
        //        serverDbCtx.Product.Add(product);
        //        await serverDbCtx.SaveChangesAsync();
        //    }

        //    // Sync all clients
        //    // First client  will upload one line and will download nothing
        //    // Second client will upload one line and will download one line
        //    // thrid client  will upload one line and will download two lines
        //    int download = 0;
        //    foreach (var client in Clients)
        //    {
        //        var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);
        //        var agent = new SyncAgent(client.Provider, webRemoteOrchestrator, options);

        //        // Just before sending changes, get changes sent
        //        webRemoteOrchestrator.OnHttpSendingChangesRequest(sra =>
        //        {
        //            // check we have rows
        //            Assert.True(sra.Request.Changes.HasRows);
        //        });


        //        var s = await agent.SynchronizeAsync();

        //        Assert.Equal(download++, s.TotalChangesDownloadedFromServer);
        //        Assert.Equal(1, s.TotalChangesUploadedToServer);
        //        Assert.Equal(0, s.TotalResolvedConflicts);

        //        webRemoteOrchestrator.ClearInterceptors();
        //    }

        //}

        [Fact]
        public async Task ConverterRegisteredShouldConvertDateTime()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            var serverProductCategoryModifiedDate = new DateTime(2022, 1, 12, 16, 46, 21, DateTimeKind.Utc);
            var serverProductCategoryModifiedDateTicks = serverProductCategoryModifiedDate.Ticks;
            Debug.WriteLine($"serverProductCategoryModifiedDate:{serverProductCategoryModifiedDate}");
            Debug.WriteLine($"serverProductCategoryModifiedDateTicks:{serverProductCategoryModifiedDateTicks}");

            var clientProductCategoryModifiedDate = new DateTime(2020, 1, 12, 16, 46, 21, DateTimeKind.Utc);
            var clientProductCategoryModifiedDateTicks = clientProductCategoryModifiedDate.Ticks;
            Debug.WriteLine($"clientProductCategoryModifiedDate:{clientProductCategoryModifiedDate}");
            Debug.WriteLine($"clientProductCategoryModifiedDateTicks:{clientProductCategoryModifiedDateTicks}");

            // Add one row in each client
            foreach (var clientProvider in clientsProvider)
                await clientProvider.AddProductCategoryAsync(modifiedDate: clientProductCategoryModifiedDate, attributeWithSpace: "CLI");

            // Add one row on server
            await serverProvider.AddProductCategoryAsync(modifiedDate: serverProductCategoryModifiedDate, attributeWithSpace: "SRV");

            // Add a date converter
            var webServerOptions = new WebServerOptions();
            webServerOptions.Converters.Add(new DateConverter());

            // Stop Kestrell to reconfigure
            await this.Kestrell.StopAsync();

            // Add again the serverprovider
            this.Kestrell.AddSyncServer(serverProvider, setup, options, webServerOptions);

            // Create server web proxy
            var serverHandler = new RequestDelegate(async context =>
            {
                var webServerAgent = context.RequestServices.GetService(typeof(WebServerAgent)) as WebServerAgent;

                // When getting a row from client
                webServerAgent.OnHttpGettingChanges(cca =>
                {
                    if (cca.Request.Changes == null || cca.Request.Changes.Tables == null)
                        return;

                    var table = cca.Request.Changes.Tables[0];
                    Assert.NotEmpty(table.Rows);

                    foreach (var row in table.Rows)
                    {
                        Assert.IsType<long>(row[5]);
                        var ticks = (long)row[5];

                        if (row[6].ToString() == "SRV")
                            Assert.Equal(serverProductCategoryModifiedDateTicks, ticks);
                        else
                            Assert.Equal(clientProductCategoryModifiedDateTicks, ticks);
                    }
                });

                // When sending a row from server to client
                webServerAgent.OnHttpSendingChanges(sra =>
                {
                    if (sra.Response.Changes == null || sra.Response.Changes.Tables == null)
                        return;

                    var table = sra.Response.Changes.Tables[0];
                    Assert.NotEmpty(table.Rows);

                    foreach (var row in table.Rows)
                    {
                        Assert.IsType<long>(row[5]);
                        var ticks = (long)row[5];

                        if (row[6].ToString() == "SRV")
                            Assert.Equal(serverProductCategoryModifiedDateTicks, ticks);
                        else
                            Assert.Equal(clientProductCategoryModifiedDateTicks, ticks);

                    }
                });


                await webServerAgent.HandleRequestAsync(context);
            });

            var serviceUri = this.Kestrell.Run(serverHandler);

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri, new DateConverter());

                var agent = new SyncAgent(clientProvider, webRemoteOrchestrator, options);

                var s = await agent.SynchronizeAsync();
            }
        }

        [Fact]
        public async Task IsOutdatedShouldWorkIfCorrectAction()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);

                // Call a server delete metadata to update the last valid timestamp value in scope_info_server table
                var remoteOrchestrator = new RemoteOrchestrator(serverProvider);
                var dmc = await remoteOrchestrator.DeleteMetadatasAsync();

                // Client side : Create a product category and a product
                await clientProvider.AddProductAsync();
                await clientProvider.AddProductCategoryAsync();

                // Generate an outdated situation
                await HelperDatabase.ExecuteScriptAsync(clientProviderType, clientDatabaseName,
                                    $"Update scope_info_client set scope_last_server_sync_timestamp=-1");


                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                var se = await Assert.ThrowsAsync<SyncException>(async () =>
                {
                    var tmpR = await agent.SynchronizeAsync();
                });

                Assert.Equal("OutOfDateException", se.TypeName);

                // Intercept outdated event, and make a reinitialize with upload action
                agent.LocalOrchestrator.OnOutdated(oa =>
                {
                    oa.Action = OutdatedAction.ReinitializeWithUpload;
                });

                var r = await agent.SynchronizeAsync();
                var rowsCount = serverProvider.GetDatabaseRowsCount();
                var clientRowsCount = clientProvider.GetDatabaseRowsCount();

                Assert.Equal(rowsCount, r.TotalChangesDownloadedFromServer);
                Assert.Equal(2, r.TotalChangesUploadedToServer);

                Assert.Equal(rowsCount, clientRowsCount);


            }
        }

        [Fact]
        public virtual async Task HandlingDifferentIdentifiers()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            var rowsCount = serverProvider.GetDatabaseRowsCount();

            await this.Kestrell.StopAsync();
            this.Kestrell.AddSyncServer(serverProvider, setup, options, identifier: "c1");
            this.Kestrell.AddSyncServer(serverProvider, new SyncSetup("Customer"), options, scopeName: "customScope1", identifier: "c2");
            var serviceUri = this.Kestrell.Run();

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri, identifier: "c2"), options);

                var s = await agent.SynchronizeAsync("customScope1");

                Assert.Equal(4, s.TotalChangesDownloadedFromServer);
                Assert.Equal(4, s.TotalChangesAppliedOnClient);

                agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri, identifier: "c1"), options);

                s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
            }
        }

        [Fact]
        public async Task GetChangesBeforeServerIsInitialized()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // Get count of rows
            var rowsCount = serverProvider.GetDatabaseRowsCount();

            foreach (var clientProvider in clientsProvider)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);
                var agent = new SyncAgent(clientProvider, webRemoteOrchestrator, options);

                // Ensure scope is created locally
                var cScopeInfoClient = await agent.LocalOrchestrator.GetScopeInfoClientAsync();

                // get changes from server, without any changes sent from client side
                var serverSyncChanges = await webRemoteOrchestrator.GetChangesAsync(cScopeInfoClient);

                Assert.Equal(rowsCount, serverSyncChanges.ServerChangesSelected.TotalChangesSelected);
            }
        }

        [Fact]
        public async Task GetChangesAfterServerIsInitialized()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // Get count of rows
            var rowsCount = serverProvider.GetDatabaseRowsCount();

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }


            // ----------------------------------
            // Add rows on server AFTER first sync (so everything should be initialized)
            // ----------------------------------
            await serverProvider.AddProductAsync();
            await serverProvider.AddProductCategoryAsync();

            // ----------------------------------
            // Get changes
            // ----------------------------------
            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);
                var agent = new SyncAgent(clientProvider, webRemoteOrchestrator, options);

                // Ensure scope is created locally
                var cScopeInfoClient = await agent.LocalOrchestrator.GetScopeInfoClientAsync();

                // get changes from server, without any changes sent from client side
                var serverSyncChanges = await webRemoteOrchestrator.GetChangesAsync(cScopeInfoClient);

                Assert.Equal(2, serverSyncChanges.ServerChangesSelected.TotalChangesSelected);
            }
        }

        [Fact]
        public async Task GetEstimatedChangesCountBeforeServerIsInitialized()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // Get count of rows
            var rowsCount = serverProvider.GetDatabaseRowsCount();

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);
                var agent = new SyncAgent(clientProvider, webRemoteOrchestrator, options);

                // Ensure scope is created locally
                var cScopeInfoClient = await agent.LocalOrchestrator.GetScopeInfoClientAsync();

                // get changes from server, without any changes sent from client side
                var changes = await webRemoteOrchestrator.GetEstimatedChangesCountAsync(cScopeInfoClient);

                Assert.Equal(rowsCount, changes.ServerChangesSelected.TotalChangesSelected);
            }
        }

        [Fact]
        public async Task GetEstimatedChangesAfterServerIsInitialized()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // Get count of rows
            var rowsCount = serverProvider.GetDatabaseRowsCount();

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
            // ----------------------------------
            // Add rows on server AFTER first sync (so everything should be initialized)
            // ----------------------------------
            await serverProvider.AddProductAsync();
            await serverProvider.AddProductCategoryAsync();

            // ----------------------------------
            // Get estimated changes
            // ----------------------------------
            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);
                var agent = new SyncAgent(clientProvider, webRemoteOrchestrator, options);

                // Ensure scope is created locally
                var cScopeInfoClient = await agent.LocalOrchestrator.GetScopeInfoClientAsync();

                // get changes from server, without any changes sent from client side
                var serverSyncChanges = await webRemoteOrchestrator.GetEstimatedChangesCountAsync(cScopeInfoClient);

                Assert.Equal(2, serverSyncChanges.ServerChangesSelected.TotalChangesSelected);
            }
        }

        [Fact]
        public async Task SessionIsLostDuringApplyChangesButChangesAreNotLost()
        {
            // Arrange
            var options = new SyncOptions { BatchSize = 100, DisableConstraintsOnApplyChanges = true };

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            var rowsToSend = 3000;
            foreach (var clientProvider in clientsProvider)
            {
                var (clientDatabaseType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);

                var products = Enumerable.Range(1, rowsToSend).Select(i =>
                    new Product
                    {
                        ProductId = Guid.NewGuid(),
                        Name = Guid.NewGuid().ToString("N"),
                        ProductNumber = $"ZZ-{i}{clientDatabaseType}"
                    });

                using var clientDbCtx = new AdventureWorksContext(clientProvider);
                clientDbCtx.Product.AddRange(products);
                await clientDbCtx.SaveChangesAsync();
            }

            // stop current kestrell
            await this.Kestrell.StopAsync();

            // for each client, fake that the sync session is interrupted
            var clientCount = 0;
            foreach (var clientProvider in clientsProvider)
            {
                using var kestrell = new KestrellTestServer(this.UseFiddler);

                // Configure server orchestrator
                kestrell.AddSyncServer(serverProvider, setup, options);

                var batchIndex = 0;

                // Create server web proxy
                var serverHandler = new RequestDelegate(async context =>
                {
                    var webServerAgent = context.RequestServices.GetService(typeof(WebServerAgent)) as WebServerAgent;

                    // Tracing http request received by server side
                    webServerAgent.OnHttpGettingRequest(args =>
                    {
                        var cScopeInfoClientId = args.HttpContext.GetClientScopeId();
                        var cScopeInfoClientSessionId = args.HttpContext.GetClientSessionId();
                        var cStep = args.HttpContext.GetCurrentStep();

                        Debug.WriteLine($"RECEIVE Session Id:{cScopeInfoClientSessionId} ClientId:{cScopeInfoClientId} Step:{(HttpStep)Convert.ToInt32(cStep)}");
                    });

                    // Tracing http response sent by server side, and drop session to generate a session lost
                    webServerAgent.OnHttpSendingResponse(async args =>
                    {
                        // SendChangesInProgress is occuring when server is receiving data from client
                        // We are droping session on the second batch
                        if (args.HttpStep == HttpStep.SendChangesInProgress && batchIndex == 1)
                        {
                            Debug.WriteLine($"DROPING Session Id {args.HttpContext.Session.Id} on batch {batchIndex}.");
                            args.HttpContext.Session.Clear();
                            await args.HttpContext.Session.CommitAsync();
                        }
                        if (args.HttpStep == HttpStep.SendChangesInProgress)
                            batchIndex++;

                    });

                    await webServerAgent.HandleRequestAsync(context);
                });

                var serviceUri = kestrell.Run(serverHandler);

                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);

                webRemoteOrchestrator.OnHttpPolicyRetrying(args => Debug.WriteLine(args.Message));
                webRemoteOrchestrator.OnHttpSendingRequest(args =>
                {
                    var cScopeInfoClientId = "";
                    var cScopeInfoClientSessionId = "";
                    var cStep = "";
                    if (args.Request.Headers.TryGetValues("dotmim-sync-scope-id", out var scopeIds))
                        cScopeInfoClientId = scopeIds.ToList()[0];

                    if (args.Request.Headers.TryGetValues("dotmim-sync-session-id", out var sessionIds))
                        cScopeInfoClientSessionId = sessionIds.ToList()[0];

                    if (args.Request.Headers.TryGetValues("dotmim-sync-step", out var steps))
                        cStep = steps.ToList()[0];

                    Debug.WriteLine($"SEND    Session Id:{cScopeInfoClientSessionId} ClientId:{cScopeInfoClientId} Step:{(HttpStep)Convert.ToInt32(cStep)}");
                });

                var agent = new SyncAgent(clientProvider, webRemoteOrchestrator, options);

                var ex = await Assert.ThrowsAsync<HttpSyncWebException>(() => agent.SynchronizeAsync());

                // Assert
                Assert.NotNull(ex); //"exception required!"
                Assert.Equal("HttpSessionLostException", ex.TypeName);

                await kestrell.StopAsync();

            }

            foreach (var clientProvider in clientsProvider)
            {
                using var kestrell = new KestrellTestServer(this.UseFiddler);

                // Configure server orchestrator
                kestrell.AddSyncServer(serverProvider, setup, options);

                var serviceUri = kestrell.Run();

                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);

                // Act 2: Ensure client can recover
                var agent2 = new SyncAgent(clientProvider, webRemoteOrchestrator, options);

                var s2 = await agent2.SynchronizeAsync();

                Assert.Equal(rowsToSend, s2.TotalChangesUploadedToServer);
                Assert.Equal(rowsToSend * clientCount, s2.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s2.TotalResolvedConflicts);

                clientCount++;

                using var serverDbCtx = new AdventureWorksContext(serverProvider);
                var serverCount = serverDbCtx.Product.Count(p => p.ProductNumber.StartsWith($"ZZ-"));
                Assert.Equal(rowsToSend * clientCount, serverCount);

                await kestrell.StopAsync();
            }

        }


        [Fact]
        public async Task SessionIsLostDuringGetChangesButChangesAreNotLost()
        {
            // Arrange
            var options = new SyncOptions { BatchSize = 100, DisableConstraintsOnApplyChanges = true };

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            var rowsToReceive = 5000;

            var (serverProviderType, _) = HelperDatabase.GetDatabaseType(serverProvider);

            var products = Enumerable.Range(1, rowsToReceive).Select(i =>
                new Product
                {
                    ProductId = Guid.NewGuid(),
                    Name = Guid.NewGuid().ToString("N"),
                    ProductNumber = $"ZZ-{i}{serverProviderType}"
                });

            using var ctx = new AdventureWorksContext(serverProvider);
            ctx.Product.AddRange(products);
            await ctx.SaveChangesAsync();

            // stop current kestrell
            await this.Kestrell.StopAsync();

            // for each client, fake that the sync session is interrupted
            foreach (var clientProvider in clientsProvider)
            {
                using var kestrell = new KestrellTestServer(this.UseFiddler);

                // Configure server orchestrator
                kestrell.AddSyncServer(serverProvider, setup, options);

                var serviceUri = kestrell.Run();

                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);

                // To simulate a lost session, just send another session id
                webRemoteOrchestrator.OnHttpSendingRequest(args =>
                {
                    args.Request.Headers.Remove("dotmim-sync-session-id");
                    args.Request.Headers.Add("dotmim-sync-session-id", Guid.NewGuid().ToString());
                });

                var agent = new SyncAgent(clientProvider, webRemoteOrchestrator, options);
                var ex = await Assert.ThrowsAsync<HttpSyncWebException>(() => agent.SynchronizeAsync());
                Assert.Equal("HttpSessionLostException", ex.TypeName);

                await kestrell.StopAsync();
            }

            foreach (var clientProvider in clientsProvider)
            {
                using var kestrell = new KestrellTestServer(this.UseFiddler);

                // Configure server orchestrator
                kestrell.AddSyncServer(serverProvider, setup, options);

                var serviceUri = kestrell.Run();

                // restreint parallelism degrees to be sure the batch index is not downloaded at the end
                // (This will not raise the error if the batchindex 1 is downloaded as the last part)
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri, maxDownladingDegreeOfParallelism: 1);

                // Act 2: Ensure client can recover
                var agent2 = new SyncAgent(clientProvider, webRemoteOrchestrator, options);

                var s2 = await agent2.SynchronizeAsync();

                Assert.Equal(0, s2.TotalChangesUploadedToServer);
                Assert.Equal(rowsToReceive, s2.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s2.TotalResolvedConflicts);

                using var clientDbCtx = new AdventureWorksContext(clientProvider);
                var clientCount = clientDbCtx.Product.Count(p => p.ProductNumber.StartsWith($"ZZ-"));
                Assert.Equal(rowsToReceive, clientCount);

                await kestrell.StopAsync();
            }

        }




        ///// <summary>
        ///// Insert one row on server, should be correctly sync on all clients
        ///// </summary>
        //[Theory]
        //[ClassData(typeof(SyncOptionsData))]
        //public async Task Parallel_Sync_For_TwentyClients(SyncOptions options)
        //{
        //    // create a server database
        //    await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

        //    // Get count of rows
        //    var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

        //    var setup = new SyncSetup(Tables);
        //    // Provision server, to be sure no clients will try to do something that could break server
        //    var remoteOrchestrator = new RemoteOrchestrator(this.Server.Provider, options);

        //    // Ensure schema is ready on server side. Will create everything we need (triggers, tracking, stored proc, scopes)
        //    var sScopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        //    await remoteOrchestrator.ProvisionAsync(sScopeInfo);

        //    // configure server orchestrator
        //    this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
        //        new SyncSetup(Tables));

        //    var serviceUri = this.Kestrell.Run();

        //    var providers = this.Clients.Select(c => c.ProviderType).Distinct();
        //    var createdDatabases = new List<(ProviderType ProviderType, string DatabaseName)>();

        //    var clientProviders = new List<CoreProvider>();
        //    foreach (var provider in providers)
        //    {
        //        for (int i = 0; i < 10; i++)
        //        {
        //            // Create the provider
        //            var dbCliName = HelperDatabase.GetRandomName("http_cli_");
        //            var localProvider = this.CreateProvider(provider, dbCliName);

        //            clientProviders.Add(localProvider);

        //            // Create the database
        //            await this.CreateDatabaseAsync(provider, dbCliName, true);
        //            createdDatabases.Add((provider, dbCliName));
        //        }
        //    }

        //    var allTasks = new List<Task<SyncResult>>();

        //    // Execute a sync on all clients and add the task to a list of tasks
        //    foreach (var clientProvider in clientProviders)
        //    {
        //        var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);
        //        allTasks.Add(agent.SynchronizeAsync());
        //    }

        //    // Await all tasks
        //    await Task.WhenAll(allTasks);

        //    foreach (var s in allTasks)
        //    {
        //        Assert.Equal(rowsCount, s.Result.TotalChangesDownloadedFromServer);
        //        Assert.Equal(0, s.Result.TotalChangesUploadedToServer);
        //        Assert.Equal(0, s.Result.TotalResolvedConflicts);
        //    }


        //    // Create a new product on server 
        //    var name = HelperDatabase.GetRandomName();
        //    var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

        //    var product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };

        //    using (var serverDbCtx = new AdventureWorksContext(this.Server))
        //    {
        //        serverDbCtx.Product.Add(product);
        //        await serverDbCtx.SaveChangesAsync();
        //    }

        //    allTasks = new List<Task<SyncResult>>();

        //    // Execute a sync on all clients to get the new server row
        //    foreach (var clientProvider in clientProviders)
        //    {
        //        var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);
        //        allTasks.Add(agent.SynchronizeAsync());
        //    }

        //    // Await all tasks
        //    await Task.WhenAll(allTasks);

        //    foreach (var s in allTasks)
        //    {
        //        Assert.Equal(1, s.Result.TotalChangesDownloadedFromServer);
        //        Assert.Equal(0, s.Result.TotalChangesUploadedToServer);
        //        Assert.Equal(0, s.Result.TotalResolvedConflicts);
        //    }

        //    foreach (var db in createdDatabases)
        //    {
        //        try
        //        {
        //            HelperDatabase.DropDatabase(db.ProviderType, db.DatabaseName);
        //        }
        //        catch (Exception) { }
        //    }

        //}



        /// <summary>
        /// Insert one row on server, should be correctly sync on all clients
        /// </summary>
        [Fact]
        public async Task IntermitentConnectionUsingSyncPolicyRetryOnHttpGettingRequest()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            var interrupted = new Dictionary<HttpStep, bool>();
            var rowsCount = serverProvider.GetDatabaseRowsCount();

            await this.Kestrell.StopAsync();
            this.Kestrell.AddSyncServer(serverProvider, setup, options);


            // Create server web proxy
            var serverHandler = new RequestDelegate(async context =>
            {
                var webServerAgent = context.RequestServices.GetService(typeof(WebServerAgent)) as WebServerAgent;

                // When Server Orchestrator send back the response, we will make an interruption
                webServerAgent.OnHttpGettingRequest(args =>
                {
                    interrupted.TryAdd(args.HttpStep, false);

                    // interrupt each step to see if it's working
                    if (!interrupted[args.HttpStep])
                    {
                        interrupted[args.HttpStep] = true;
                        throw new TimeoutException($"Timeout exception raised on step {args.HttpStep}");
                    }

                });

                await webServerAgent.HandleRequestAsync(context);
            });

            var serviceUri = this.Kestrell.Run(serverHandler);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);

                var policyRetries = 0;
                webRemoteOrchestrator.OnHttpPolicyRetrying(args => policyRetries++);

                var agent = new SyncAgent(clientProvider, webRemoteOrchestrator, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.InRange(policyRetries, 5, 7);
                interrupted.Clear();
            }

            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                await agent.SynchronizeAsync(setup);
            }

            var finalRowsCount = serverProvider.GetDatabaseRowsCount();

            foreach (var clientProvider in clientsProvider)
                Assert.Equal(rowsCount, clientProvider.GetDatabaseRowsCount());

        }

        /// <summary>
        /// On Intermittent connection, should work even if server has done its part
        /// </summary>
        [Fact]
        public async Task IntermitentConnectionUsingSyncPolicyOnRetryOnHttpSendingResponse()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            var interrupted = new Dictionary<HttpStep, bool>();
            var rowsCount = serverProvider.GetDatabaseRowsCount();

            await this.Kestrell.StopAsync();
            this.Kestrell.AddSyncServer(serverProvider, setup, options);

            // Create server web proxy
            var serverHandler = new RequestDelegate(async context =>
            {
                var webServerAgent = context.RequestServices.GetService(typeof(WebServerAgent)) as WebServerAgent;

                // When Server Orchestrator send back the response, we will make an interruption
                webServerAgent.OnHttpSendingResponse(args =>
                {
                    interrupted.TryAdd(args.HttpStep, false);

                    // interrupt each step to see if it's working
                    if (!interrupted[args.HttpStep])
                    {
                        interrupted[args.HttpStep] = true;
                        throw new TimeoutException($"Timeout exception raised on step {args.HttpStep}");
                    }

                });

                await webServerAgent.HandleRequestAsync(context);
            });

            var serviceUri = this.Kestrell.Run(serverHandler);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);

                var policyRetries = 0;
                webRemoteOrchestrator.OnHttpPolicyRetrying(args =>
                {
                    policyRetries++;
                });

                var agent = new SyncAgent(clientProvider, webRemoteOrchestrator, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.InRange(policyRetries, 5, 7);
                interrupted.Clear();
            }
        }


        /// <summary>
        /// On Intermittent connection, should work even if server has already applied a batch  and then timeout for some reason 
        /// Client will resend the batch again, but that's ok, since we are merging
        /// </summary>
        [Fact]
        public async Task IntermitentConnectionUsingSyncPolicyInsertClientRowShouldWork()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            var rowsCount = serverProvider.GetDatabaseRowsCount();

            await this.Kestrell.StopAsync();
            this.Kestrell.AddSyncServer(serverProvider, setup, options);

            var interruptedBatch = false;

            // Create server web proxy
            var serverHandler = new RequestDelegate(async context =>
            {
                var webServerAgent = context.RequestServices.GetService(typeof(WebServerAgent)) as WebServerAgent;

                // When Server Orchestrator send back the response, we will make an interruption
                webServerAgent.OnHttpSendingResponse(args =>
                {
                    // Throw error when sending changes to server
                    if (args.HttpStep == HttpStep.SendChangesInProgress && !interruptedBatch)
                    {
                        interruptedBatch = true;
                        throw new TimeoutException($"Timeout exception raised on step {args.HttpStep}");
                    }

                });

                await webServerAgent.HandleRequestAsync(context);
            });

            var serviceUri = this.Kestrell.Run(serverHandler);


            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);

                var agent = new SyncAgent(clientProvider, webRemoteOrchestrator, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }


            // Insert one line on each client
            foreach (var clientProvider in clientsProvider)
                for (var i = 0; i < 1000; i++)
                    await clientProvider.AddProductCategoryAsync();


            // Sync all clients
            // First client  will upload one line and will download nothing
            // Second client will upload one line and will download one line
            // thrid client  will upload one line and will download two lines
            var download = 0;
            foreach (var clientProvider in clientsProvider)
            {
                interruptedBatch = false;

                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);
                var s = await agent.SynchronizeAsync();

                Assert.Equal(download, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1000, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                // We have one batch that has been sent 2 times; it will be merged correctly on server
                Assert.InRange(s.ChangesAppliedOnServer.TotalAppliedChanges, 1000, 2000);
                Assert.Equal(1000, s.ClientChangesSelected.TotalChangesSelected);

                // Get count of rows
                var finalRowsCount = serverProvider.GetDatabaseRowsCount();
                var clientRowsCount = clientProvider.GetDatabaseRowsCount();
                Assert.Equal(finalRowsCount, clientRowsCount);

                download += 1000;
            }
        }

        [Fact]
        public virtual async Task BlobShouldBeConsistentAndSizeShouldBeMaintained()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };
            var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);

            // Execute a sync on all clients to initialize schemas
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, webRemoteOrchestrator, options).SynchronizeAsync();

            // Create a new product on server with a big thumbnail photo
            var thumbnail = new byte[20000];

            // add one row
            await serverProvider.AddProductAsync(name: $"BLOB_SERVER", thumbNailPhoto: thumbnail);

            // Create a new product on client with a big thumbnail photo
            foreach (var clientProvider in clientsProvider)
            {
                var clientName = HelperDatabase.GetRandomName();
                var clientProductNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);
                await clientProvider.AddProductAsync(name: $"BLOB_{clientName}", productNumber: clientProductNumber, thumbNailPhoto: new byte[20000]);
            }

            // Two sync to be sure all clients have all rows from all
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, webRemoteOrchestrator, options).SynchronizeAsync();

            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, webRemoteOrchestrator, options).SynchronizeAsync();

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(serverProvider))
            {
                var products = await ctx.Product.AsNoTracking().Where(p => p.Name.StartsWith("BLOB_")).ToListAsync();
                foreach (var p in products)
                    Assert.Equal(20000, p.ThumbNailPhoto.Length);
            }

            foreach (var clientProvider in clientsProvider)
            {
                using var cliCtx = new AdventureWorksContext(clientProvider);

                var products = await cliCtx.Product.AsNoTracking().Where(p => p.Name.StartsWith("BLOB_")).ToListAsync();
                foreach (var p in products)
                    Assert.Equal(20000, p.ThumbNailPhoto.Length);
            }
        }

        /// <summary>
        /// Insert one row on each client, should be sync on server and clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task UsingExistingClientDatabaseAndUpdateUntrackedRowsAsync(SyncOptions options)
        {
            var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);
            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
            {
                // Get count of rows
                var rowsCount = serverProvider.GetDatabaseRowsCount();

                await clientProvider.AddProductAsync();

                var agent = new SyncAgent(clientProvider, webRemoteOrchestrator, options);

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                // mark local row as tracked
                await agent.LocalOrchestrator.UpdateUntrackedRowsAsync();
                s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                Assert.Equal(serverProvider.GetDatabaseRowsCount(), clientProvider.GetDatabaseRowsCount());

            }
        }

        [Fact]
        /// <summary>
        /// Asynchronously verifies that the action registered with the WebRemoteOrchestrator's OnHttpResponseFailure method is invoked
        /// when a failed HTTP response occurs due to authorization error (401).
        /// </summary>
        /// <returns>A Task representing the asynchronous unit test method.</returns>
        /// <remarks>
        /// This unit test ensures that the WebRemoteOrchestrator correctly invokes the registered action when encountering
        /// a failed HTTP response with a status code indicating an authorization error (401). It sets up the necessary
        /// environment by enabling authorization in Kestrell, initializing a WebRemoteOrchestrator with the specified service URI,
        /// and registering an action that asserts the status code of the failed response. Then, it iterates over a collection
        /// of client providers, creates SyncAgent instances for each, and attempts synchronization. The test verifies that
        /// the action is invoked as expected when a HTTPSyncWebException is caught, indicating a failed synchronization due to
        /// an authorization error.
        /// </remarks>
        public virtual async Task OnHttpResponseFailure_RegisterAction_ActionIsInvokedOnHttpResponseFailureAsync()
        {
            // Arrange
            this.Kestrell.IsAuthorisationEnabled = true;
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };
            var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);

            int count = 0;

            // Register action to assert the status code of the failed response
            webRemoteOrchestrator.OnHttpResponseFailure(s =>
            {
                Assert.NotNull(s);
                Assert.Equal(401, s.StatusCode);
                count++;
            });

            // Act
            // Execute synchronization on all clients
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, webRemoteOrchestrator, options);

                try
                {
                    var s = await agent.SynchronizeAsync();
                }
                catch (HttpSyncWebException)
                {
                    // Exception caught indicates failed synchronization due to authorization error
                }
            }
            Assert.True(count > 0);
            // Clean up
            this.Kestrell.IsAuthorisationEnabled = false;
        }
    }
}
