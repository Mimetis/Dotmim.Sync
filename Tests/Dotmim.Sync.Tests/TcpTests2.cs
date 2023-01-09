using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer.Manager;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Fixtures;
using Dotmim.Sync.Tests.Models;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using static System.Net.Mime.MediaTypeNames;

namespace Dotmim.Sync.Tests
{

    public class SqlServerInterceptorTests : TcpTests2<SqlServerFixtureType>
    {
        public SqlServerInterceptorTests(ITestOutputHelper output, DatabaseServerFixture<SqlServerFixtureType> fixture) : base(output, fixture)
        {
        }
    }

    public abstract class TcpTests2<T> : BaseTest<T>, IDisposable where T : RelationalFixture
    {
        private CoreProvider serverProvider;
        private IEnumerable<CoreProvider> clientsProvider;
        private SyncSetup setup;
        private int rowsCount;

        public TcpTests2(ITestOutputHelper output, DatabaseServerFixture<T> fixture) : base(output, fixture)
        {
            serverProvider = Fixture.GetServerProvider();
            clientsProvider = Fixture.GetClientProviders();
            setup = Fixture.GetSyncSetup();
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task RowsCount(SyncOptions options)
        {
            // Drop everything in client database
            foreach (var clientProvider in clientsProvider)
                await Fixture.DropAllTablesAsync(clientProvider, true);

            // Get count of rows
            var rowsCount = this.Fixture.GetDatabaseRowsCount(serverProvider);

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var s = await agent.SynchronizeAsync(setup);
                var clientRowsCount = Fixture.GetDatabaseRowsCount(clientProvider);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(rowsCount, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(rowsCount, clientRowsCount);

                using var ctxServer = new AdventureWorksContext(serverProvider, Fixture.UseFallbackSchema);
                using var ctxClient = new AdventureWorksContext(clientProvider, Fixture.UseFallbackSchema);

                var serverSaleHeaders = ctxServer.SalesOrderHeader.AsNoTracking().ToList();
                var clientSaleHeaders = ctxClient.SalesOrderHeader.AsNoTracking().ToList();

                foreach (var clientSaleHeader in clientSaleHeaders)
                {
                    var serverSaleHeader = serverSaleHeaders.First(h => h.SalesOrderId == clientSaleHeader.SalesOrderId);

                    // decimal
                    Assert.Equal(clientSaleHeader.SubTotal, serverSaleHeader.SubTotal);
                    Assert.Equal(clientSaleHeader.Freight, serverSaleHeader.Freight);
                    Assert.Equal(clientSaleHeader.TaxAmt, serverSaleHeader.TaxAmt);
                    // string
                    Assert.Equal(clientSaleHeader.Comment, serverSaleHeader.Comment);
                    Assert.Equal(clientSaleHeader.AccountNumber, serverSaleHeader.AccountNumber);
                    Assert.Equal(clientSaleHeader.CreditCardApprovalCode, serverSaleHeader.CreditCardApprovalCode);
                    Assert.Equal(clientSaleHeader.PurchaseOrderNumber, serverSaleHeader.PurchaseOrderNumber);
                    Assert.Equal(clientSaleHeader.SalesOrderNumber, serverSaleHeader.SalesOrderNumber);
                    // int
                    Assert.Equal(clientSaleHeader.BillToAddressId, serverSaleHeader.BillToAddressId);
                    Assert.Equal(clientSaleHeader.SalesOrderId, serverSaleHeader.SalesOrderId);
                    Assert.Equal(clientSaleHeader.ShipToAddressId, serverSaleHeader.ShipToAddressId);
                    // guid
                    Assert.Equal(clientSaleHeader.CustomerId, serverSaleHeader.CustomerId);
                    Assert.Equal(clientSaleHeader.Rowguid, serverSaleHeader.Rowguid);
                    // bool
                    Assert.Equal(clientSaleHeader.OnlineOrderFlag, serverSaleHeader.OnlineOrderFlag);
                    // short
                    Assert.Equal(clientSaleHeader.RevisionNumber, serverSaleHeader.RevisionNumber);

                    // Check DateTime DateTimeOffset
                    Assert.Equal(clientSaleHeader.ShipDate, serverSaleHeader.ShipDate);
                    Assert.Equal(clientSaleHeader.OrderDate, serverSaleHeader.OrderDate);
                    Assert.Equal(clientSaleHeader.DueDate, serverSaleHeader.DueDate);
                    Assert.Equal(clientSaleHeader.ModifiedDate, serverSaleHeader.ModifiedDate);
                }
            }
        }


        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task RowsCountWithExistingSchema(SyncOptions options)
        {
            // Drop everything in client database
            foreach (var clientProvider in clientsProvider)
                await Fixture.EmptyAllTablesAsync(clientProvider);

            // Get count of rows
            var rowsCount = this.Fixture.GetDatabaseRowsCount(serverProvider);

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var s = await agent.SynchronizeAsync(setup);
                var clientRowsCount = Fixture.GetDatabaseRowsCount(clientProvider);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(rowsCount, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(rowsCount, clientRowsCount);

                using var ctxServer = new AdventureWorksContext(serverProvider, Fixture.UseFallbackSchema);
                using var ctxClient = new AdventureWorksContext(clientProvider, Fixture.UseFallbackSchema);

                var serverSaleHeaders = ctxServer.SalesOrderHeader.AsNoTracking().ToList();
                var clientSaleHeaders = ctxClient.SalesOrderHeader.AsNoTracking().ToList();

                foreach (var clientSaleHeader in clientSaleHeaders)
                {
                    var serverSaleHeader = serverSaleHeaders.First(h => h.SalesOrderId == clientSaleHeader.SalesOrderId);

                    // decimal
                    Assert.Equal(clientSaleHeader.SubTotal, serverSaleHeader.SubTotal);
                    Assert.Equal(clientSaleHeader.Freight, serverSaleHeader.Freight);
                    Assert.Equal(clientSaleHeader.TaxAmt, serverSaleHeader.TaxAmt);
                    // string
                    Assert.Equal(clientSaleHeader.Comment, serverSaleHeader.Comment);
                    Assert.Equal(clientSaleHeader.AccountNumber, serverSaleHeader.AccountNumber);
                    Assert.Equal(clientSaleHeader.CreditCardApprovalCode, serverSaleHeader.CreditCardApprovalCode);
                    Assert.Equal(clientSaleHeader.PurchaseOrderNumber, serverSaleHeader.PurchaseOrderNumber);
                    Assert.Equal(clientSaleHeader.SalesOrderNumber, serverSaleHeader.SalesOrderNumber);
                    // int
                    Assert.Equal(clientSaleHeader.BillToAddressId, serverSaleHeader.BillToAddressId);
                    Assert.Equal(clientSaleHeader.SalesOrderId, serverSaleHeader.SalesOrderId);
                    Assert.Equal(clientSaleHeader.ShipToAddressId, serverSaleHeader.ShipToAddressId);
                    // guid
                    Assert.Equal(clientSaleHeader.CustomerId, serverSaleHeader.CustomerId);
                    Assert.Equal(clientSaleHeader.Rowguid, serverSaleHeader.Rowguid);
                    // bool
                    Assert.Equal(clientSaleHeader.OnlineOrderFlag, serverSaleHeader.OnlineOrderFlag);
                    // short
                    Assert.Equal(clientSaleHeader.RevisionNumber, serverSaleHeader.RevisionNumber);

                    // Check DateTime DateTimeOffset
                    //Assert.Equal(clientSaleHeader.ShipDate, serverSaleHeader.ShipDate);
                    Assert.Equal(clientSaleHeader.OrderDate, serverSaleHeader.OrderDate);
                    Assert.Equal(clientSaleHeader.DueDate, serverSaleHeader.DueDate);
                    Assert.Equal(clientSaleHeader.ModifiedDate, serverSaleHeader.ModifiedDate);
                }
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Schema(SyncOptions options)
        {
            // Drop everything in client database
            foreach (var clientProvider in clientsProvider)
                await Fixture.DropAllTablesAsync(clientProvider, true);

            // Get count of rows
            var rowsCount = this.Fixture.GetDatabaseRowsCount(serverProvider);

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var s = await agent.SynchronizeAsync(setup);
                var clientRowsCount = Fixture.GetDatabaseRowsCount(clientProvider);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(rowsCount, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(rowsCount, clientRowsCount);
            }


            foreach (var clientProvider in Fixture.GetClientProviders())
            {
                // Check we have the correct columns replicated
                using var clientConnection = clientProvider.CreateConnection();
                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);

                await clientConnection.OpenAsync();

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                // force to get schema from database by calling the GetSchemaAsync (that will not read the ScopInfo record, but will make a full read of the database schema)
                // The schema get here is not serialized / deserialiazed, like the remote schema (loaded from database)
                var clientSchema = await agent.LocalOrchestrator.GetSchemaAsync(setup);

                var serverScope = await agent.RemoteOrchestrator.GetScopeInfoAsync();
                var serverSchema = serverScope.Schema;

                foreach (var setupTable in setup.Tables)
                {
                    var clientTable = clientProviderType == ProviderType.Sql ? clientSchema.Tables[setupTable.TableName, setupTable.SchemaName] : clientSchema.Tables[setupTable.TableName];
                    var serverTable = serverSchema.Tables[setupTable.TableName, setupTable.SchemaName];

                    Assert.Equal(clientTable.Columns.Count, serverTable.Columns.Count);

                    foreach (var serverColumn in serverTable.Columns)
                    {
                        var clientColumn = clientTable.Columns.FirstOrDefault(c => c.ColumnName == serverColumn.ColumnName);

                        Assert.NotNull(clientColumn);

                        if (Fixture.ServerProviderType == clientProviderType && Fixture.ServerProviderType == ProviderType.Sql)
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

        }


        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task MultiScopes(SyncOptions options)
        {
            // Drop everything in client database
            foreach (var clientProvider in clientsProvider)
                await Fixture.DropAllTablesAsync(clientProvider, true);

            // get the number of rows that have only primary keys (which do not accept any Update)
            int notUpdatedOnClientsCount;
            using (var serverDbCtx = new AdventureWorksContext(serverProvider, Fixture.UseFallbackSchema))
            {
                var pricesListCategoriesCount = serverDbCtx.PricesListCategory.Count();
                var postTagsCount = serverDbCtx.PostTag.Count();
                notUpdatedOnClientsCount = pricesListCategoriesCount + postTagsCount;
            }

            // Get count of rows
            var rowsCount = this.Fixture.GetDatabaseRowsCount(serverProvider);

            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                // On first sync, even tables with only primary keys are inserted
                var s = await agent.SynchronizeAsync("v1", setup);
                var clientRowsCount = Fixture.GetDatabaseRowsCount(clientProvider);
                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(rowsCount, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(rowsCount, clientRowsCount);

                var s2 = await agent.SynchronizeAsync("v2", setup);

                // On second sync, tables with only primary keys are downloaded but not inserted or updated
                clientRowsCount = Fixture.GetDatabaseRowsCount(clientProvider);
                Assert.Equal(rowsCount, s2.TotalChangesDownloadedFromServer);
                Assert.Equal(rowsCount - notUpdatedOnClientsCount, s2.TotalChangesAppliedOnClient);
                Assert.Equal(0, s2.TotalChangesUploadedToServer);
                Assert.Equal(rowsCount, clientRowsCount);
            }
        }

        [Fact]
        public async Task Bad_ConnectionFromServer_ShouldRaiseError()
        {
            // change the remote orchestrator connection string
            serverProvider.ConnectionString = $@"Server=unknown;Database=unknown;UID=sa;PWD=unknown";

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {

                var agent = new SyncAgent(clientProvider, serverProvider);

                var onReconnect = new Action<ReConnectArgs>(args =>
                    Console.WriteLine($"[Retry Connection] Can't connect to database {args.Connection?.Database}. " +
                    $"Retry N°{args.Retry}. " +
                    $"Waiting {args.WaitingTimeSpan.Milliseconds}. Exception:{args.HandledException.Message}."));

                agent.LocalOrchestrator.OnReConnect(onReconnect);
                agent.RemoteOrchestrator.OnReConnect(onReconnect);

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync(setup);
                });
            }
        }

        /// <summary>
        /// Check a bad connection should raise correct error
        /// </summary>
        [Fact]
        public async Task Bad_ConnectionFromClient_ShouldRaiseError()
        {
            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                // change the local orchestrator connection string
                // Set a connection string that will faile everywhere (event Sqlite)
                clientProvider.ConnectionString = $@"Data Source=*;";

                var agent = new SyncAgent(clientProvider, serverProvider);

                var onReconnect = new Action<ReConnectArgs>(args =>
                    Console.WriteLine($"[Retry Connection] Can't connect to database {args.Connection?.Database}. Retry N°{args.Retry}. Waiting {args.WaitingTimeSpan.Milliseconds}. Exception:{args.HandledException.Message}."));

                agent.LocalOrchestrator.OnReConnect(onReconnect);
                agent.RemoteOrchestrator.OnReConnect(onReconnect);

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync(setup);
                });
            }
        }


        [Fact]
        public async Task Bad_TableWithoutPrimaryKeys_ShouldRaiseError()
        {
            // Create the table on the server
            await HelperDatabase.ExecuteScriptAsync(Fixture.ServerProviderType, Fixture.ServerDatabaseName,
                "create table tabletest (testid int, testname varchar(50))");

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider);

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync(new string[] { "tabletest" });
                });

                Assert.Equal("MissingPrimaryKeyException", se.TypeName);
            }

            // Create the table on the server
            await HelperDatabase.ExecuteScriptAsync(Fixture.ServerProviderType, Fixture.ServerDatabaseName,
                "drop table tabletest");
        }

        [Fact]
        public async Task Bad_ColumnSetup_DoesNotExistInSchema_ShouldRaiseError()
        {
            // Add a malformatted column name
            setup.Tables["Employee"].Columns.AddRange(new string[] { "EmployeeID", "FirstName", "LastNam" });

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider);

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync("noColumn", setup);
                });

                Assert.Equal("MissingColumnException", se.TypeName);
            }
        }

        [Fact]
        public async Task Bad_TableSetup_DoesNotExistInSchema_ShouldRaiseError()
        {
            setup.Tables.Add("WeirdTable");

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider);

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync("WeirdTable", setup);
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

            await Fixture.AddProductCategoryAsync(serverProvider);

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

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

            await Fixture.AddProductCategoryAsync(serverProvider);
            await Fixture.AddProductCategoryAsync(serverProvider);
            await Fixture.AddProductAsync(serverProvider);
            await Fixture.AddProductAsync(serverProvider);

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

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

            var serverProductCategory = await Fixture.AddProductCategoryAsync(serverProvider);

            var pcName = string.Concat(serverProductCategory.ProductCategoryId, "UPDATED");
            serverProductCategory.Name = pcName;

            await Fixture.UpdateProductCategoryAsync(serverProvider, serverProductCategory);

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync();

                var clientProductCategory = await Fixture.GetProductCategoryAsync(clientProvider, serverProductCategory.ProductCategoryId);

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
                await Fixture.AddProductCategoryAsync(clientProvider);

            int download = 0;
            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

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
                await Fixture.AddProductCategoryAsync(clientProvider);
                await Fixture.AddProductCategoryAsync(clientProvider);
                await Fixture.AddProductAsync(clientProvider);
                await Fixture.AddProductAsync(clientProvider);
            }

            int download = 0;
            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

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
                    await Fixture.AddProductCategoryAsync(clientProvider);

            int download = 0;
            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

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

            var firstProductCategory = await Fixture.AddProductCategoryAsync(serverProvider);

            // sync this category on each client to be able to delete it after
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // add one row
            await Fixture.AddProductCategoryAsync(serverProvider);
            // delete one row
            await Fixture.DeleteProductCategoryAsync(serverProvider, firstProductCategory.ProductCategoryId);

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

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
            var product = await Fixture.AddProductAsync(serverProvider, thumbNailPhoto: thumbnail);

            // Execute a sync on all clients and check results
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                // don' need to specify scope name (default will be used) nor setup, since it already exists
                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var clientProduct = await Fixture.GetProductAsync(clientProvider, product.ProductId);

                Assert.Equal(product.ThumbNailPhoto, clientProduct.ThumbNailPhoto);
                
                for (var i = 0; i < product.ThumbNailPhoto.Length; i++)
                    Assert.Equal(product.ThumbNailPhoto[i], clientProduct.ThumbNailPhoto[i]);

                Assert.Equal(thumbnail.Length, clientProduct.ThumbNailPhoto.Length);
            }
        }

    }
}
