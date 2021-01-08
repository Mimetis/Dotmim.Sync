using Dotmim.Sync.MySql;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Web.Server;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Web.Client;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using MySql.Data.MySqlClient;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests.IntegrationTests
{
    public class SqlServerToSqliteTransactionTests : HttpTestsBase
    {
        private Stopwatch stopwatch;

        public SqlServerToSqliteTransactionTests(HelperProvider fixture, ITestOutputHelper output) : base(fixture, output)
        {
        }

        public override string[] Tables => new string[]
        {
            "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product", "Employee", "Customer", "Address", "CustomerAddress", "EmployeeAddress",
            "SalesLT.SalesOrderHeader", "SalesLT.SalesOrderDetail", "dbo.Sql", "Posts", "Tags", "PostTag",
            "PricesList", "PricesListCategory", "PricesListDetail"
        };

        public override List<ProviderType> ClientsType => new List<ProviderType>
            { ProviderType.Sqlite};

        public override ProviderType ServerType => ProviderType.Sql;
        
        public override CoreProvider CreateProvider(ProviderType providerType, string dbName)
        {
            var cs = HelperDatabase.GetConnectionString(providerType, dbName);
            switch (providerType)
            {
                case ProviderType.MySql:
                    return new MySqlSyncProvider(cs);
                case ProviderType.Sqlite:
                    return new SqliteSyncProvider(cs);
                case ProviderType.Sql:
                default:
                    return new SqlSyncProvider(cs);
            }
        }

        public override bool UseFiddler => false;

        public override async Task EnsureDatabaseSchemaAndSeedAsync((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) t, bool useSeeding = false, bool useFallbackSchema = false)
        {
            AdventureWorksContext ctx = null;
            try
            {
                ctx = new AdventureWorksContext(t, useFallbackSchema, useSeeding);
                await ctx.Database.EnsureCreatedAsync();

            }
            catch (Exception)
            {
            }
            finally
            {
                if (ctx != null)
                    ctx.Dispose();
            }
        }

        public override Task CreateDatabaseAsync(ProviderType providerType, string dbName, bool recreateDb = true)
        {
            return HelperDatabase.CreateDatabaseAsync(providerType, dbName, recreateDb);
        }

        /// <summary>
        /// Get the server database rows count
        /// </summary>
        /// <returns></returns>
        public override int GetServerDatabaseRowsCount((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) t)
        {
            int totalCountRows = 0;

            using (var serverDbCtx = new AdventureWorksContext(t))
            {
                totalCountRows += serverDbCtx.Address.Count();
                totalCountRows += serverDbCtx.Customer.Count();
                totalCountRows += serverDbCtx.CustomerAddress.Count();
                totalCountRows += serverDbCtx.Employee.Count();
                totalCountRows += serverDbCtx.EmployeeAddress.Count();
                totalCountRows += serverDbCtx.Log.Count();
                totalCountRows += serverDbCtx.Posts.Count();
                totalCountRows += serverDbCtx.PostTag.Count();
                totalCountRows += serverDbCtx.PricesList.Count();
                totalCountRows += serverDbCtx.PricesListCategory.Count();
                totalCountRows += serverDbCtx.PricesListDetail.Count();
                totalCountRows += serverDbCtx.Product.Count();
                totalCountRows += serverDbCtx.ProductCategory.Count();
                totalCountRows += serverDbCtx.ProductModel.Count();
                totalCountRows += serverDbCtx.SalesOrderDetail.Count();
                totalCountRows += serverDbCtx.SalesOrderHeader.Count();
                totalCountRows += serverDbCtx.Sql.Count();
                totalCountRows += serverDbCtx.Tags.Count();
            }

            return totalCountRows;
        }

        /// <summary>
        /// Note: This test basically ensures, that no other transaction can write to the database while the SqliteSyncProvider
        /// retrieves the local changes. Otherwise, the local timestamp by those writes could be lower than the next sync timestamp.
        /// Therefore, those rows would be ignored during the current sync and in **all** future syncs!
        /// </summary>
        /// <returns></returns>
        [Fact, TestPriority(1)]
        public async Task EnsureLocalSqliteProvier_DoesNotUseDeferredTransactions_WhenSelectingChanges()
        {
            // Arrange
            var options = new SyncOptions { BatchSize = 100 };
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);
            var client = Clients.Single();
            await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri), options);
            var s = await agent.SynchronizeAsync();

            Assert.Equal(rowsCount, s.TotalChangesDownloaded);
            Assert.Equal(0, s.TotalChangesUploaded);
            Assert.Equal(0, s.TotalResolvedConflicts);

            var ev0 = new SemaphoreSlim(0, 1);
            var ev1 = new SemaphoreSlim(0, 1);
            var ev2 = new SemaphoreSlim(0, 1);
            var ev3 = new SemaphoreSlim(0, 1);
            var cts = new CancellationTokenSource(1000);

            // create brand new client and setup locks
            agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri), options);
            agent.LocalOrchestrator.OnTableChangesSelecting((e) =>
            {
                ev1.Release();
                ev2.Wait(cts.Token);
            });
            agent.LocalOrchestrator.OnTableChangesSelected(e =>
            {
                ev3.Release();
            });

            SyncResult t0Session = null;
            var t0 = Task.Run(async () =>
            {
                await ev0.WaitAsync(cts.Token);
                t0Session = await agent.SynchronizeAsync();
            });

            var t1 = Task.Run(async () =>
            {
                using (var c = new SqliteConnection(client.Provider.ConnectionString))
                {
                    ev0.Release();
                    await ev1.WaitAsync(cts.Token);

                    // Note: Without a timeout specified in cts, this next operation MUST deadlock
                    // Otherwise, this means the SqliteSyncProvider uses a deferred transaction (since Microsoft.Data.Sqlite 5.0!)
                    // see here: https://docs.microsoft.com/en-us/dotnet/standard/data/sqlite/transactions
                    using (var tx = c.BeginTransaction())
                    {
                        var productId = Guid.NewGuid();
                        InsertProduct(c, "locally added prod", productId, tx);

                        var ts = ReadProductTimestamp(c, productId, tx);

                        ev2.Release();
                        await ev3.WaitAsync(cts.Token);

                        tx.Commit();
                    }
                }
            });
            OperationCanceledException exception = null;

            try
            {
                // Act
                await Task.WhenAll(t0, t1);
            }
            catch (SyncException x)
            {
                exception = x.InnerException as OperationCanceledException;
                
            }

            // Assert
            Assert.NotNull(exception); // the operations MUSt be cancelled - otherwise the transaction will overlap - possibly because of BeginTransaction(deferred=true)!!
        }

        private static void InsertProduct(SqliteConnection c, string title, Guid productId, SqliteTransaction tx)
        {
            var uc = c.CreateCommand();
            uc.Transaction = tx;
            uc.CommandText = "insert into product (name, productid, productnumber, modifieddate) " +
                             "values (@title, @id, @number, @creationtime)";
            uc.Parameters.AddWithValue("title", title);
            uc.Parameters.AddWithValue("id", productId);
            uc.Parameters.AddWithValue("productnumber", productId.ToString("N"));
            uc.Parameters.AddWithValue("creationtime", DateTime.UtcNow);
            uc.ExecuteNonQuery();
        }
        private static async Task<(Guid id, long timestamp)> ReadProductTimestamp(SqliteConnection c, Guid productId, SqliteTransaction tx)
        {
            var rc = c.CreateCommand();
            rc.Transaction = tx;
            rc.CommandText = "select id, timestamp from product_tracking where productid = @id";
            rc.Parameters.AddWithValue("id", productId);
            var rcr = await rc.ExecuteReaderAsync();
            if (rcr.Read())
            {
                var id = rcr.GetGuid(0);
                var timestamp = rcr.GetInt64(1);
                return (id, timestamp);
            }

            return (productId, 0);
        }
    }
}
