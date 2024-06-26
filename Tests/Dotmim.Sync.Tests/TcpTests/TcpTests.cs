﻿using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.SqlServer.Manager;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Fixtures;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using Xunit;
using Xunit.Abstractions;
using static System.Net.Mime.MediaTypeNames;

namespace Dotmim.Sync.Tests.IntegrationTests
{


    public abstract partial class TcpTests : DatabaseTest, IClassFixture<DatabaseServerFixture>, IDisposable
    {
        private CoreProvider serverProvider;
        private IEnumerable<CoreProvider> clientsProvider;
        private SyncSetup setup;

        public TcpTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
            serverProvider = GetServerProvider();
            clientsProvider = GetClientProviders();
            setup = GetSetup();
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task RowsCount(SyncOptions options)
        {
            // Get count of rows
            var rowsCount = serverProvider.GetDatabaseRowsCount();

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

                var s = await agent.SynchronizeAsync(setup);
                var clientRowsCount = clientProvider.GetDatabaseRowsCount();

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(rowsCount, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(rowsCount, clientRowsCount);
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task RowsCountWithExistingSchema(SyncOptions options)
        {
            // Get count of rows
            var rowsCount = serverProvider.GetDatabaseRowsCount();

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

                var s = await agent.SynchronizeAsync(setup);
                var clientRowsCount = clientProvider.GetDatabaseRowsCount();

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(rowsCount, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(rowsCount, clientRowsCount);

                using var ctxServer = new AdventureWorksContext(serverProvider);
                using var ctxClient = new AdventureWorksContext(clientProvider);

                var serverSaleHeaders = ctxServer.SalesOrderHeader.AsNoTracking().ToList();
                var clientSaleHeaders = ctxClient.SalesOrderHeader.AsNoTracking().ToList();

                foreach (var clientSaleHeader in clientSaleHeaders)
                {
                    var serverSaleHeader = serverSaleHeaders.First(h => h.SalesOrderId == clientSaleHeader.SalesOrderId);

                    // decimal
                    Assert.Equal(serverSaleHeader.SubTotal, clientSaleHeader.SubTotal);
                    Assert.Equal(serverSaleHeader.Freight, clientSaleHeader.Freight);
                    Assert.Equal(serverSaleHeader.TaxAmt, clientSaleHeader.TaxAmt);
                    // string
                    Assert.Equal(serverSaleHeader.Comment, clientSaleHeader.Comment);
                    Assert.Equal(serverSaleHeader.AccountNumber, clientSaleHeader.AccountNumber);
                    Assert.Equal(serverSaleHeader.CreditCardApprovalCode, clientSaleHeader.CreditCardApprovalCode);
                    Assert.Equal(serverSaleHeader.PurchaseOrderNumber, clientSaleHeader.PurchaseOrderNumber);
                    Assert.Equal(serverSaleHeader.SalesOrderNumber, clientSaleHeader.SalesOrderNumber);
                    // int
                    Assert.Equal(serverSaleHeader.BillToAddressId, clientSaleHeader.BillToAddressId);
                    Assert.Equal(serverSaleHeader.SalesOrderId, clientSaleHeader.SalesOrderId);
                    Assert.Equal(serverSaleHeader.ShipToAddressId, clientSaleHeader.ShipToAddressId);
                    // guid
                    Assert.Equal(serverSaleHeader.CustomerId, clientSaleHeader.CustomerId);
                    Assert.Equal(serverSaleHeader.Rowguid, clientSaleHeader.Rowguid);
                    // bool
                    Assert.Equal(serverSaleHeader.OnlineOrderFlag, clientSaleHeader.OnlineOrderFlag);
                    // short
                    Assert.Equal(serverSaleHeader.RevisionNumber, clientSaleHeader.RevisionNumber);

                    // Check DateTime DateTimeOffset
                    Assert.Equal(serverSaleHeader.ShipDate, clientSaleHeader.ShipDate);

                    // If Postgres Server

                    Assert.Equal(serverSaleHeader.OrderDate.Value.ToUniversalTime(), clientSaleHeader.OrderDate.Value.ToUniversalTime());
                    Assert.Equal(serverSaleHeader.DueDate.Value.ToUniversalTime(), clientSaleHeader.DueDate.Value.ToUniversalTime());
                    Assert.Equal(serverSaleHeader.ModifiedDate.Value.ToUniversalTime(), clientSaleHeader.ModifiedDate.Value.ToUniversalTime());
                }
            }
        }

        [Fact]
        public async Task Schema()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // As we don't want to poluate the already created clients tables, we will create some new ones
            var newClientsProvider = new List<CoreProvider>();
            foreach (var clientProvider in clientsProvider)
            {
                var (dbtype, _) = HelperDatabase.GetDatabaseType(clientProvider);
                var dbName = HelperDatabase.GetRandomName("tcp_cli");

                await HelperDatabase.CreateDatabaseAsync(dbtype, dbName, true);
                newClientsProvider.Add(HelperDatabase.GetSyncProvider(dbtype, dbName, clientProvider.UseFallbackSchema()));
            }

            // Get count of rows
            var rowsCount = serverProvider.GetDatabaseRowsCount();
            var (serverProviderType, serverDatabaseName) = HelperDatabase.GetDatabaseType(serverProvider);

            // Execute a sync on all clients and check results
            foreach (var clientProvider in newClientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

                var s = await agent.SynchronizeAsync(setup);
                var clientRowsCount = clientProvider.GetDatabaseRowsCount();

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(rowsCount, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(rowsCount, clientRowsCount);
            }

            foreach (var clientProvider in newClientsProvider)
            {
                // Check we have the correct columns replicated
                using var clientConnection = clientProvider.CreateConnection();
                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);

                await clientConnection.OpenAsync();

                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

                // force to get schema from database by calling the GetSchemaAsync (that will not read the ScopInfo record, but will make a full read of the database schema)
                // The schema get here is not serialized / deserialiazed, like the remote schema (loaded from database)
                var clientSchema = await agent.LocalOrchestrator.GetSchemaAsync(setup);

                var serverScope = await agent.RemoteOrchestrator.GetScopeInfoAsync();
                var serverSchema = serverScope.Schema;

                foreach (var setupTable in setup.Tables)
                {
                    SyncTable clientTable;
                    SyncTable serverTable;

                    if (clientProviderType == ProviderType.Sqlite || clientProviderType == ProviderType.MySql)
                        clientTable = clientSchema.Tables[setupTable.TableName];
                    else
                        clientTable = clientSchema.Tables[setupTable.TableName, setupTable.SchemaName];


                    if (serverProviderType == ProviderType.Sqlite || serverProviderType == ProviderType.MySql)
                        serverTable = serverSchema.Tables[setupTable.TableName];
                    else
                        serverTable = serverSchema.Tables[setupTable.TableName, setupTable.SchemaName];

                    Assert.Equal(clientTable.Columns.Count, serverTable.Columns.Count);

                    foreach (var serverColumn in serverTable.Columns)
                    {
                        var clientColumn = clientTable.Columns.FirstOrDefault(c => c.ColumnName == serverColumn.ColumnName);

                        Assert.NotNull(clientColumn);

                        if (serverProviderType == clientProviderType && serverProviderType == ProviderType.Sql)
                        {
                            Assert.Equal(serverColumn.DataType, clientColumn.DataType);
                            Assert.Equal(serverColumn.IsUnicode, clientColumn.IsUnicode);
                            Assert.Equal(serverColumn.IsUnsigned, clientColumn.IsUnsigned);

                            var maxPrecision = Math.Min(SqlDbMetadata.PRECISION_MAX, serverColumn.Precision);
                            var maxScale = Math.Min(SqlDbMetadata.SCALE_MAX, serverColumn.Scale);

                            // dont assert max length since numeric reset this value
                            //Assert.Equal(serverColumn.MaxLength, clientColumn.MaxLength);

                            Assert.Equal(maxPrecision, clientColumn.Precision);
                            Assert.Equal(serverColumn.PrecisionIsSpecified, clientColumn.PrecisionIsSpecified);
                            Assert.Equal(maxScale, clientColumn.Scale);
                            Assert.Equal(serverColumn.ScaleIsSpecified, clientColumn.ScaleIsSpecified);

                            Assert.Equal(serverColumn.DefaultValue, clientColumn.DefaultValue);
                            Assert.Equal(serverColumn.ExtraProperty1, clientColumn.ExtraProperty1);
                            Assert.Equal(serverColumn.OriginalDbType, clientColumn.OriginalDbType);

                            // We don't replicate unique indexes
                            //Assert.Equal(serverColumn.IsUnique, clientColumn.IsUnique);

                            Assert.Equal(serverColumn.AutoIncrementSeed, clientColumn.AutoIncrementSeed);
                            Assert.Equal(serverColumn.AutoIncrementStep, clientColumn.AutoIncrementStep);
                            Assert.Equal(serverColumn.IsAutoIncrement, clientColumn.IsAutoIncrement);

                            //Assert.Equal(serverColumn.OriginalTypeName, clientColumn.OriginalTypeName);

                            // IsCompute is not replicated, because we are not able to replicate formulat
                            // Instead, we are allowing null for the column
                            //Assert.Equal(serverColumn.IsCompute, clientColumn.IsCompute);

                            // Readonly is not replicated, because we are not able to replicate formulat
                            // Instead, we are allowing null for the column
                            //Assert.Equal(serverColumn.IsReadOnly, clientColumn.IsReadOnly);

                            // Decimal is conflicting with Numeric
                            //Assert.Equal(serverColumn.DbType, clientColumn.DbType);

                            Assert.Equal(serverColumn.Ordinal, clientColumn.Ordinal);
                            Assert.Equal(serverColumn.AllowDBNull, clientColumn.AllowDBNull);
                        }

                        Assert.Equal(serverColumn.ColumnName, clientColumn.ColumnName);

                    }

                }
                clientConnection.Close();

            }

            //// Delete all clients databases
            //foreach (var clientProvider in newClientsProvider)
            //{
            //    var (dt, n) = HelperDatabase.GetDatabaseType(clientProvider);
            //    HelperDatabase.DropDatabase(dt, n);
            //}

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
            var rowsCount = serverProvider.GetDatabaseRowsCount();

            foreach (var clientProvider in clientsProvider)
            {
                var (clientProviderType, _) = HelperDatabase.GetDatabaseType(clientProvider);

                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

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
            var (serverProviderType, _) = HelperDatabase.GetDatabaseType(serverProvider);

            var badServerProvider = HelperDatabase.GetSyncProvider(serverProviderType, HelperDatabase.GetRandomName("tcp_srv_bad_"));
            badServerProvider.ConnectionString = $@"Server=unknown;Database=unknown;UID=sa;PWD=unknown";

            // Create a client provider, but it will not be used since server provider will raise an error before
            var clientProvider = clientsProvider.First();

            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };
            var agent = new SyncAgent(clientProvider, badServerProvider, options);
            agent.LogErrors();

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
                var agent = new SyncAgent(badClientProvider, serverProvider, options);
                agent.LogErrors();

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () => await agent.SynchronizeAsync(setup));
            }
        }

        [Fact]
        public async Task BadTableWithoutPrimaryKeysShouldRaiseError()
        {
            // Create the table on the server
            await serverProvider.ExecuteSqlRawAsync("create table tabletest (testid int, testname varchar(50))");

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync("tabletest", new string[] { "tabletest" });
                });

                Assert.Equal("MissingPrimaryKeyException", se.TypeName);
            }

            // Create the table on the server
            await serverProvider.ExecuteSqlRawAsync("drop table tabletest");
        }

        [Fact]
        public async Task BadColumnSetupDoesNotExistInSchemaShouldRaiseError()
        {
            // Creating a fake setup
            var badSetup = new SyncSetup("Employee");

            // Add a malformatted column name
            badSetup.Tables["Employee"].Columns.AddRange(new string[] { "EmployeeID", "FirstName", "LastNam" });

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync("noColumn", badSetup);
                });

                Assert.Equal("MissingColumnException", se.TypeName);
            }
        }

        [Fact]
        public async Task BadTableSetupDoesNotExistInSchemaShouldRaiseError()
        {
            var badSetup = new SyncSetup("WeirdTable");

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync("WeirdTable", badSetup);
                });

                Assert.Equal("MissingTableException", se.TypeName);
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
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

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
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

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
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

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

            int download = 0;
            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

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

            int download = 0;
            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

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
                for (int i = 0; i < rowsCountToInsert; i++)
                    await clientProvider.AddProductCategoryAsync();

            int download = 0;
            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

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
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

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
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

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
            int download = 0;
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

                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

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
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

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
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download--, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // check rows count on server and on each client
            using var ctx = new AdventureWorksContext(serverProvider, serverProvider.UseFallbackSchema());

            var productRowCount = await ctx.Product.AsNoTracking().CountAsync();
            var productCategoryCount = await ctx.ProductCategory.AsNoTracking().CountAsync();
            var priceListCount = await ctx.PricesList.AsNoTracking().CountAsync();

            var rowsCount = serverProvider.GetDatabaseRowsCount();

            foreach (var clientProvider in clientsProvider)
            {
                Assert.Equal(rowsCount, clientProvider.GetDatabaseRowsCount());

                using var cliCtx = new AdventureWorksContext(clientProvider, clientProvider.UseFallbackSchema());
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
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

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
            int addressId = 0;
            foreach (var clientProvider in clientsProvider)
            {
                using (var ctx = new AdventureWorksContext(clientProvider, clientProvider.UseFallbackSchema()))
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
            int download = 0;
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Now sync again to be sure all clients have all lines
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // get rows count
            var rowsCount = serverProvider.GetDatabaseRowsCount();

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(serverProvider, serverProvider.UseFallbackSchema()))
            {
                // get all addresses
                var serverAddresses = await ctx.Address.AsNoTracking().ToListAsync();

                foreach (var clientProvider in clientsProvider)
                {
                    Assert.Equal(rowsCount, clientProvider.GetDatabaseRowsCount());

                    using var cliCtx = new AdventureWorksContext(clientProvider, clientProvider.UseFallbackSchema());
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
            int addressId = 0;
            foreach (var clientProvider in clientsProvider)
            {
                using (var ctx = new AdventureWorksContext(clientProvider, clientProvider.UseFallbackSchema()))
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
            int download = 0;
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Now sync again to be sure all clients have all lines
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // get rows count
            var rowsCount = serverProvider.GetDatabaseRowsCount();

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(serverProvider, serverProvider.UseFallbackSchema()))
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
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

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
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

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
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

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
                var s = await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync();

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
        public async Task DeleteOneRowInOneTableOnClientSideThatIsNotTrackedYetOne(SyncOptions options)
        {
            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
            {
                // generate a random server name
                var sqlServerRandomDatabaseName = HelperDatabase.GetRandomName("server_");

                // create a server provider
                var provider = HelperDatabase.GetSyncProvider(ServerProviderType, sqlServerRandomDatabaseName,
                    ServerProviderType == ProviderType.Sql || ServerProviderType == ProviderType.Postgres);

                // Create database and schema on server side (+ seeding)
                new AdventureWorksContext(provider, true).Database.EnsureCreated();

                if (ServerProviderType == ProviderType.Sql)
                    await HelperDatabase.ActivateChangeTracking(sqlServerRandomDatabaseName);

                // before 1st sync, adding a server product categgory
                var pc = await serverProvider.AddProductCategoryAsync(name: $"SRV_{HelperDatabase.GetRandomName()}");

                // Sync to initialize client
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

                // now on client delete this product category that is not tracked on server as it's a default row
                await clientProvider.DeleteProductCategoryAsync(pc.ProductCategoryId);

                // sync and check
                var s = await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync();

                // we are uploading deleted rows from client
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                // nothing downloaded
                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                // but we should not have any rows applied locally
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                // anyway we are always uploading our deleted rows and should be applied on server
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                // w may have resolved conflicts locally
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
        }

        [Fact]
        public async Task ProvisionAndDeprovision()
        {
            foreach (var clientProvider in clientsProvider)
            {
                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);
                var localOrchestrator = new LocalOrchestrator(clientProvider);

                var provision = SyncProvision.ScopeInfo | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

                // just check interceptor
                var onTableCreatedCount = 0;
                localOrchestrator.OnTableCreated(args => onTableCreatedCount++);

                var remoteOrchestrator = new RemoteOrchestrator(serverProvider);
                var schema = await remoteOrchestrator.GetSchemaAsync(setup);

                // Read client scope
                var clientScope = await localOrchestrator.GetScopeInfoAsync();

                var serverScope = new ScopeInfo
                {
                    Name = clientScope.Name,
                    Schema = schema,
                    Setup = setup,
                    Version = clientScope.Version
                };

                // Provision the database with all tracking tables, stored procedures, triggers and scope
                clientScope = await localOrchestrator.ProvisionAsync(serverScope, provision);

                //--------------------------
                // ASSERTION
                //--------------------------

                // check if scope table is correctly created
                var scopeInfoTableExists = await localOrchestrator.ExistScopeInfoTableAsync();
                Assert.True(scopeInfoTableExists);

                // get the db manager
                foreach (var setupTable in setup.Tables)
                {
                    Assert.True(await localOrchestrator.ExistTrackingTableAsync(clientScope, setupTable.TableName, setupTable.SchemaName));

                    Assert.True(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Delete));
                    Assert.True(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Insert));
                    Assert.True(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Update));

                    if (clientProviderType == ProviderType.Sql)
                    {
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkTableType));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkDeleteRows));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkUpdateRows));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChanges));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChanges));
                    }
                    if (clientProviderType == ProviderType.Sql || clientProviderType == ProviderType.MySql || clientProviderType == ProviderType.MariaDB)
                    {
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.Reset));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectRow));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.UpdateRow));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteRow));

                        // No filters here
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChangesWithFilters));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChangesWithFilters));
                    }

                }

                //localOrchestrator.OnTableProvisioned(null);

                //// Deprovision the database with all tracking tables, stored procedures, triggers and scope

                await localOrchestrator.DeprovisionAsync(provision);

                // check if scope table is correctly created
                scopeInfoTableExists = await localOrchestrator.ExistScopeInfoTableAsync();
                Assert.False(scopeInfoTableExists);

                // get the db manager
                foreach (var setupTable in setup.Tables)
                {
                    Assert.False(await localOrchestrator.ExistTrackingTableAsync(clientScope, setupTable.TableName, setupTable.SchemaName));

                    Assert.False(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Delete));
                    Assert.False(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Insert));
                    Assert.False(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Update));

                    if (clientProviderType == ProviderType.Sql)
                    {
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkDeleteRows));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkTableType));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkUpdateRows));
                    }
                    if (clientProviderType == ProviderType.Sql || clientProviderType == ProviderType.MySql || clientProviderType == ProviderType.MariaDB)
                    {
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteRow));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.Reset));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChanges));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChanges));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectRow));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.UpdateRow));

                        // No filters here
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChangesWithFilters));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChangesWithFilters));
                    }
                }
            }
        }

        [Fact]
        public async Task CheckCompositeKeys()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                var localOrchestrator = new LocalOrchestrator(clientProvider);
                var clientScope = await localOrchestrator.GetScopeInfoAsync();
                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);

                // Open connection as we are using internal methods requiring a connection argument
                using var connection = clientProvider.CreateConnection();
                await connection.OpenAsync();
                using var transaction = connection.BeginTransaction();

                var tablePricesListCategory = localOrchestrator.GetTableBuilder(clientScope.Schema.Tables["PricesListCategory"], clientScope);
                Assert.NotNull(tablePricesListCategory);

                var relations = (await tablePricesListCategory.GetRelationsAsync(connection, transaction)).ToList();
                Assert.Single(relations);

                if (clientProviderType != ProviderType.Sqlite)
                    Assert.StartsWith("FK_PricesListCategory_PricesList_PriceListId", relations[0].ForeignKey);

                Assert.Single(relations[0].Columns);

                var tablePricesListDetail = localOrchestrator.GetTableBuilder(clientScope.Schema.Tables["PricesListDetail"], clientScope);

                Assert.NotNull(tablePricesListDetail);

                var relations2 = (await tablePricesListDetail.GetRelationsAsync(connection, transaction)).ToArray();
                Assert.Single(relations2);

                if (clientProviderType != ProviderType.Sqlite)
                    Assert.StartsWith("FK_PricesListDetail_PricesListCategory_PriceListId", relations2[0].ForeignKey);

                Assert.Equal(2, relations2[0].Columns.Count);

                var tableEmployeeAddress = localOrchestrator.GetTableBuilder(clientScope.Schema.Tables["EmployeeAddress"], clientScope);
                Assert.NotNull(tableEmployeeAddress);

                var relations3 = (await tableEmployeeAddress.GetRelationsAsync(connection, transaction)).ToArray();
                Assert.Equal(2, relations3.Count());

                if (clientProviderType != ProviderType.Sqlite)
                {
                    Assert.StartsWith("FK_EmployeeAddress_Address_AddressID", relations3[0].ForeignKey);
                    Assert.StartsWith("FK_EmployeeAddress_Employee_EmployeeID", relations3[1].ForeignKey);

                }
                Assert.Single(relations3[0].Columns);
                Assert.Single(relations3[1].Columns);

                transaction.Commit();
                connection.Close();

            }

        }

        [Fact]
        public async Task ForceFailingConstraintsButWorksWithDisableConstraintsOnApplyChanges()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            var productCategory = await serverProvider.AddProductCategoryAsync();
            var product = await serverProvider.AddProductAsync(productCategoryId: productCategory.ProductCategoryId);

            // forcing a fk exception
            var foreignKeysFailureAction = new Action<RowsChangesApplyingArgs>((args) =>
            {
                if (args.SchemaTable.TableName != "Product")
                    return;

                if (args.SyncRows == null || args.SyncRows.Count <= 0)
                    return;
                var row = args.SyncRows[0];

                if (row["ProductCategoryId"] != null && row["ProductCategoryId"].ToString() == productCategory.ProductCategoryId)
                    row["ProductCategoryId"] = "BBBBB"; // <- Generate a foreign key error

            });

            options.DisableConstraintsOnApplyChanges = false;
            // Sync all clients to get these 2 new rows
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

                // Generate the foreignkey error
                agent.LocalOrchestrator.OnRowsChangesApplying(foreignKeysFailureAction);

                var ex = await Assert.ThrowsAsync<SyncException>(async () =>
                {
                    var res = await agent.SynchronizeAsync();
                });

                Assert.IsType<SyncException>(ex);
            }
            options.DisableConstraintsOnApplyChanges = true;

            // Should work now
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

                // Generate the foreignkey error
                agent.LocalOrchestrator.OnRowsChangesApplying(foreignKeysFailureAction);

                var res = await agent.SynchronizeAsync();
                Assert.Equal(2, res.TotalChangesDownloadedFromServer);
                Assert.Equal(2, res.TotalChangesAppliedOnClient);
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
            var rowsCount = serverProvider.GetDatabaseRowsCount();

            // Reset stored proc needs it.
            options.DisableConstraintsOnApplyChanges = true;

            // Add one row in each client then Reinitialize
            foreach (var clientProvider in clientsProvider)
            {
                var productCategory = await clientProvider.AddProductCategoryAsync();

                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

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
            var rowsCount = serverProvider.GetDatabaseRowsCount();

            // Reset stored proc needs it.
            options.DisableConstraintsOnApplyChanges = true;

            // Add one row in each client then ReinitializeWithUpload
            int download = 1;
            foreach (var clientProvider in clientsProvider)
            {
                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);
                var productCategory = await clientProvider.AddProductCategoryAsync();

                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

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

            // Should not download anything
            foreach (var clientProvider in clientsProvider)
            {
                var s = await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync("uploadonly", setupV2);
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
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

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

            // Get count of rows
            var rowsCount = serverProvider.GetDatabaseRowsCount();

            // Should not download anything
            foreach (var clientProvider in clientsProvider)
            {
                var s = await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync("downloadonly", setupV2);
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
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

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
                agent.LogErrors();

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
                var agent = new SyncAgent(clientProvider, serverProvider, options);
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

                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

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
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync();

            // Get count of rows
            var rowsCount = serverProvider.GetDatabaseRowsCount();

            // Execute a sync on all clients to be sure all clients have all rows
            foreach (var clientProvider in clientsProvider)
                Assert.Equal(rowsCount, clientProvider.GetDatabaseRowsCount());
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task SerializeAndDeserialize(SyncOptions options)
        {
            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

#pragma warning disable SYSLIB0022 // Type or member is obsolete
            var myRijndael = new RijndaelManaged();
#pragma warning restore SYSLIB0022 // Type or member is obsolete
            myRijndael.GenerateKey();
            myRijndael.GenerateIV();

            var writringRowsTables = new ConcurrentDictionary<string, int>();
            var readingRowsTables = new ConcurrentDictionary<string, int>();

            var serializingRowsAction = new Func<SerializingRowArgs, Task>((args) =>
            {
                // Assertion
                writringRowsTables.AddOrUpdate(args.SchemaTable.GetFullName(), 1, (key, oldValue) => oldValue + 1);

                var strSet = JsonConvert.SerializeObject(args.RowArray);
                using var encryptor = myRijndael.CreateEncryptor(myRijndael.Key, myRijndael.IV);
                using var msEncrypt = new MemoryStream();
                using var csEncrypt = new CryptoStream(msEncrypt, encryptor, CryptoStreamMode.Write);
                using (var swEncrypt = new StreamWriter(csEncrypt))
                    swEncrypt.Write(strSet);

                args.Result = Convert.ToBase64String(msEncrypt.ToArray());

                return Task.CompletedTask;
            });

            var deserializingRowsAction = new Func<DeserializingRowArgs, Task>((args) =>
            {
                // Assertion
                readingRowsTables.AddOrUpdate(args.SchemaTable.GetFullName(), 1, (key, oldValue) => oldValue + 1);

                string value;
                var byteArray = Convert.FromBase64String(args.RowString);
                using var decryptor = myRijndael.CreateDecryptor(myRijndael.Key, myRijndael.IV);
                using var msDecrypt = new MemoryStream(byteArray);
                using var csDecrypt = new CryptoStream(msDecrypt, decryptor, CryptoStreamMode.Read);
                using (var swDecrypt = new StreamReader(csDecrypt))
                    value = swDecrypt.ReadToEnd();

                var array = JsonConvert.DeserializeObject<object[]>(value);

                args.Result = array;
                return Task.CompletedTask;

            });

            var rowsCount = serverProvider.GetDatabaseRowsCount();

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                writringRowsTables.Clear();
                readingRowsTables.Clear();

                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;


                localOrchestrator.OnSerializingSyncRow(serializingRowsAction);
                remoteOrchestrator.OnSerializingSyncRow(serializingRowsAction);

                localOrchestrator.OnDeserializingSyncRow(deserializingRowsAction);
                remoteOrchestrator.OnDeserializingSyncRow(deserializingRowsAction);

                var result = await agent.SynchronizeAsync(setup);

                foreach (var table in result.ChangesAppliedOnClient.TableChangesApplied)
                {
                    var fullName = string.IsNullOrEmpty(table.SchemaName) ? table.TableName : $"{table.SchemaName}.{table.TableName}";
                    writringRowsTables.TryGetValue(fullName, out int writedRows);
                    Assert.Equal(table.Applied, writedRows);
                }

                foreach (var table in result.ServerChangesSelected.TableChangesSelected)
                {
                    var fullName = string.IsNullOrEmpty(table.SchemaName) ? table.TableName : $"{table.SchemaName}.{table.TableName}";
                    readingRowsTables.TryGetValue(fullName, out int readRows);
                    Assert.Equal(table.TotalChanges, readRows);
                }

                var clientRowsCount = clientProvider.GetDatabaseRowsCount();

                Assert.Equal(clientRowsCount, rowsCount);
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
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);

                // Call a server delete metadata to update the last valid timestamp value in scope_info_server table
                var dmc = await agent.RemoteOrchestrator.DeleteMetadatasAsync();

                // Client side : Create a product category and a product
                await clientProvider.AddProductAsync();
                await clientProvider.AddProductCategoryAsync();

                // Generate an outdated situation
                await HelperDatabase.ExecuteScriptAsync(clientProviderType, clientDatabaseName,
                                    $"Update scope_info_client set scope_last_server_sync_timestamp=-1");

                // Making a first sync, will initialize everything we need
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

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task ChangeBidirectionalToUploadOnlyShouldWork(SyncOptions options)
        {
            // Execute a sync on all clients
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Create a new setup
            var setupUploadOnly = GetSetup();

            // Remove all tables that are not the 4 tables we want to work with
            var productCategorySetupTable = setupUploadOnly.Tables.First(t => t.TableName == "ProductCategory");
            var productSetupTable = setupUploadOnly.Tables.First(t => t.TableName == "Product");
            var customerSetupTable = setupUploadOnly.Tables.First(t => t.TableName == "Customer");
            var priceListSetupTable = setupUploadOnly.Tables.First(t => t.TableName == "PricesList");

            foreach (var table in setupUploadOnly.Tables.ToArray())
            {
                if (table.TableName != "ProductCategory" && table.TableName != "Product" && table.TableName != "Customer" && table.TableName != "PricesList")
                    setupUploadOnly.Tables.Remove(table);
            }

            productCategorySetupTable.SyncDirection = SyncDirection.UploadOnly;
            productSetupTable.SyncDirection = SyncDirection.UploadOnly;
            customerSetupTable.SyncDirection = SyncDirection.UploadOnly;
            priceListSetupTable.SyncDirection = SyncDirection.Bidirectional;

            // Prepare remote scope
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);
            var serverScope = await remoteOrchestrator.ProvisionAsync("BidirToUp", setupUploadOnly);

            // Insert lines on each client
            foreach (var clientProvider in clientsProvider)
            {
                await clientProvider.AddProductAsync();
                await clientProvider.AddProductCategoryAsync();
                await clientProvider.AddPriceListAsync();
                await clientProvider.AddCustomerAsync();
            }

            // Insert lines or server
            await serverProvider.AddProductAsync();
            await serverProvider.AddProductCategoryAsync();
            await serverProvider.AddPriceListAsync();
            await serverProvider.AddCustomerAsync();

            // Prepare local scope
            foreach (var clientProvider in clientsProvider)
            {
                var localOrchestrator = new LocalOrchestrator(clientProvider, options);

                // Once created we can provision the new scope, thanks to the serverScope instance we already have
                var clientScopeV1 = await localOrchestrator.ProvisionAsync(serverScope);
                var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync("BidirToUp");

                // IF we launch synchronize on this new scope, it will get all the rows from the server
                // We are making a shadow copy of previous scope to get the last synchronization metadata
                var oldCScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync();
                cScopeInfoClient.ShadowScope(oldCScopeInfoClient);
                await localOrchestrator.SaveScopeInfoClientAsync(cScopeInfoClient);
            }

            var download = 1;
            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

                var s = await agent.SynchronizeAsync("BidirToUp", setupUploadOnly);

                Assert.Equal(download, s.TotalChangesDownloadedFromServer); // Only PriceList rows
                Assert.Equal(download, s.TotalChangesAppliedOnClient); // Only PriceList rows
                Assert.Equal(4, s.TotalChangesUploadedToServer); // Only ProductCategory, Product and Customer
                Assert.Equal(4, s.TotalChangesAppliedOnServer); // Only ProductCategory, Product and Customer
                Assert.Equal(0, s.TotalResolvedConflicts);
                download += 1;
            }

        }

        /// <summary>
        /// Insert one row on server, should be correctly sync on all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task ParallelSyncForTwentyClients(SyncOptions options)
        {
            // Provision server, to be sure no clients will try to do something that could break server
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            // Ensure schema is ready on server side. Will create everything we need (triggers, tracking, stored proc, scopes)
            var serverScope = await remoteOrchestrator.GetScopeInfoAsync(setup);
            await remoteOrchestrator.ProvisionAsync(serverScope);

            // Get clients providers
            var providersTypes = clientsProvider.Select(c => HelperDatabase.GetDatabaseType(c).ProviderType).Distinct();

            // all clients providers
            var clientProviders = new List<CoreProvider>();

            var createdDatabases = new List<(ProviderType ProviderType, string DatabaseName)>();

            foreach (var providerType in providersTypes)
            {
                for (int i = 0; i < 10; i++)
                {
                    // Create the provider
                    var dbCliName = HelperDatabase.GetRandomName("tcp_cli_");
                    var localProvider = HelperDatabase.GetSyncProvider(providerType, dbCliName);

                    clientProviders.Add(localProvider);

                    // Create the database
                    await HelperDatabase.CreateDatabaseAsync(providerType, dbCliName, true);

                    createdDatabases.Add((providerType, dbCliName));
                }
            }

            var allTasks = new List<Task<SyncResult>>();

            // Execute a sync on all clients and add the task to a list of tasks
            foreach (var clientProvider in clientProviders)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

                allTasks.Add(agent.SynchronizeAsync());
            }

            // Await all tasks
            await Task.WhenAll(allTasks);

            var rowsCount = serverProvider.GetDatabaseRowsCount();

            foreach (var s in allTasks)
            {
                var result = await s;
                Assert.Equal(rowsCount, result.TotalChangesDownloadedFromServer);
                Assert.Equal(0, result.TotalChangesUploadedToServer);
                Assert.Equal(0, result.TotalResolvedConflicts);
            }

            // Create a new product on server 
            await serverProvider.AddProductAsync();
            await serverProvider.AddProductCategoryAsync();

            allTasks = new List<Task<SyncResult>>();

            // Execute a sync on all clients to get the new server row
            foreach (var clientProvider in clientProviders)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                agent.LogErrors();

                allTasks.Add(agent.SynchronizeAsync());
            }

            // Await all tasks
            await Task.WhenAll(allTasks);

            foreach (var s in allTasks)
            {
                var result = await s;
                Assert.Equal(2, result.TotalChangesDownloadedFromServer);
                Assert.Equal(2, result.TotalChangesAppliedOnClient);
                Assert.Equal(0, result.TotalChangesUploadedToServer);
                Assert.Equal(0, result.TotalResolvedConflicts);
            }

            //foreach (var db in createdDatabases)
            //{
            //    try
            //    {
            //        HelperDatabase.DropDatabase(db.ProviderType, db.DatabaseName);
            //    }
            //    catch (Exception) { }
            //}
        }


        [Fact]
        public async Task DeleteMetadatas()
        {
            var (serverProviderType, serverDatabaseName) = HelperDatabase.GetDatabaseType(serverProvider);

            if (serverProviderType == ProviderType.Sql && serverProvider.GetProviderTypeName().Contains("ChangeTracking"))
                return;

            var dict = new Dictionary<string, long?>();
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                await agent.SynchronizeAsync(setup);
                var scope = await agent.LocalOrchestrator.GetScopeInfoAsync();
                Assert.Null(scope.LastCleanupTimestamp);
            }

            // Add 2 products on server that will be synced on client
            await serverProvider.AddProductCategoryAsync();
            await serverProvider.AddProductCategoryAsync();

            // get rows count
            var allRowsCount = serverProvider.GetDatabaseRowsCount();
            var allProductCategories = await serverProvider.GetProductCategoriesAsync();

            // as cleanup happens on any client after 3 syncs, making 2 syncs before checking cleanup
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                await agent.SynchronizeAsync();

                var scope = await agent.LocalOrchestrator.GetScopeInfoAsync();

                Assert.Null(scope.LastCleanupTimestamp);

                var scopeClient = await agent.LocalOrchestrator.GetScopeInfoClientAsync();

                // this value will be the one used to clean up
                dict.Add(clientProvider.GetProviderTypeName(), scopeClient.LastSyncTimestamp);
            }


            // on this sync, the 2 added product categories will not be cleaned up.
            // only the initials rows will be cleaned up
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                bool hasReachedOnMetadataCleaning = false;
                bool hasReachedOnMetadataCleaned = false;

                var expectedTimestamp = dict[string.Concat(clientProvider.GetProviderTypeName())];
                agent.LocalOrchestrator.OnMetadataCleaning(args =>
                {
                    hasReachedOnMetadataCleaning = true;
                    Assert.Equal(expectedTimestamp, args.TimeStampStart);
                });

                agent.LocalOrchestrator.OnMetadataCleaned((args) =>
                {
                    hasReachedOnMetadataCleaned = true;
                    var cleanUpProductCategory = args.DatabaseMetadatasCleaned?.Tables.FirstOrDefault(dmc => dmc.TableName == "ProductCategory");

                    // the two product categories added will be cleaned up on next sync
                    Assert.Equal(allProductCategories.Count - 2, cleanUpProductCategory.RowsCleanedCount);
                    Assert.Equal(allRowsCount - 2, args.DatabaseMetadatasCleaned.RowsCleanedCount);
                });

                await agent.SynchronizeAsync(setup);

                var scope = await agent.LocalOrchestrator.GetScopeInfoAsync();
                Assert.Equal(expectedTimestamp, scope.LastCleanupTimestamp);
                Assert.True(hasReachedOnMetadataCleaned);
                Assert.True(hasReachedOnMetadataCleaning);

                var scopeClient = await agent.LocalOrchestrator.GetScopeInfoClientAsync();
                // this value will be the one used to clean up
                dict[clientProvider.GetProviderTypeName()] = scopeClient.LastSyncTimestamp;

            }

            // 2nd sync, the 2 added product categories will be cleaned up
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                bool hasReachedOnMetadataCleaning = false;
                bool hasReachedOnMetadataCleaned = false;

                var expectedTimestamp = dict[clientProvider.GetProviderTypeName()];
                agent.LocalOrchestrator.OnMetadataCleaning(args =>
                {
                    hasReachedOnMetadataCleaning = true;
                    Assert.Equal(expectedTimestamp, args.TimeStampStart);
                });

                agent.LocalOrchestrator.OnMetadataCleaned((args) =>
                {
                    hasReachedOnMetadataCleaned = true;
                    var cleanUpProductCategory = args.DatabaseMetadatasCleaned?.Tables.FirstOrDefault(dmc => dmc.TableName == "ProductCategory");

                    // the two product categories added will be cleaned up on next sync
                    Assert.Equal(2, cleanUpProductCategory.RowsCleanedCount);
                    Assert.Equal(2, args.DatabaseMetadatasCleaned.RowsCleanedCount);
                });

                await agent.SynchronizeAsync(setup);
                var scope = await agent.LocalOrchestrator.GetScopeInfoAsync();
                Assert.Equal(expectedTimestamp, scope.LastCleanupTimestamp);
                Assert.True(hasReachedOnMetadataCleaned);
                Assert.True(hasReachedOnMetadataCleaning);
            }

            // delete metadata on server
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider);
            var deletedMetadatas = await remoteOrchestrator.DeleteMetadatasAsync();

            Assert.Equal(2, deletedMetadatas.RowsCleanedCount);
        }

    }
}
