
using Dotmim.Sync.MariaDB;
using Dotmim.Sync.MySql;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests.IntegrationTests
{
    public class SqlServerChangeTrackingTcpTests : TcpTests
    {
        public override List<ProviderType> ClientsType => new List<ProviderType>
           {  ProviderType.Sql, ProviderType.Sqlite, ProviderType.MySql,  ProviderType.MariaDB};

        public SqlServerChangeTrackingTcpTests(HelperProvider fixture, ITestOutputHelper output) : base(fixture, output)
        {
        }

        public override string[] Tables => new string[]
        {
            "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product", "Employee", "Customer", "Address", "CustomerAddress", "EmployeeAddress",
            "SalesLT.SalesOrderHeader", "SalesLT.SalesOrderDetail", "Posts", "Tags", "PostTag",
            "PricesList", "PricesListCategory", "PricesListDetail", "Log"
        };


        public override ProviderType ServerType =>
            ProviderType.Sql;

        public override CoreProvider CreateProvider(ProviderType providerType, string dbName)
        {
            var cs = HelperDatabase.GetConnectionString(providerType, dbName);
            switch (providerType)
            {
                case ProviderType.MySql:
                    return new MySqlSyncProvider(cs);
                case ProviderType.MariaDB:
                    return new MariaDBSyncProvider(cs);
                case ProviderType.Sqlite:
                    return new SqliteSyncProvider(cs);
                case ProviderType.Sql:
                default:
                    return new SqlSyncChangeTrackingProvider(cs);
            }
        }

  
        public override async Task EnsureDatabaseSchemaAndSeedAsync((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) t
            , bool useSeeding = false, bool useFallbackSchema = false)
        {
            AdventureWorksContext ctx = null;
            try
            {
                ctx = new AdventureWorksContext(t, useFallbackSchema, useSeeding);
                await ctx.Database.EnsureCreatedAsync();

                if (t.ProviderType == ProviderType.Sql)
                    await this.ActivateChangeTracking(t.DatabaseName);
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

        public override async Task CreateDatabaseAsync(ProviderType providerType, string dbName, bool recreateDb = true)
        {
            await HelperDatabase.CreateDatabaseAsync(providerType, dbName, recreateDb);

            if (providerType == ProviderType.Sql)
                await this.ActivateChangeTracking(dbName);
        }


        private async Task ActivateChangeTracking(string dbName)
        {

            var c = new SqlConnection(Setup.GetSqlDatabaseConnectionString(dbName));

            // Check if we are using change tracking and it's enabled on the source
            var isChangeTrackingEnabled = await SqlManagementUtils.IsChangeTrackingEnabledAsync(c, null).ConfigureAwait(false);

            if (isChangeTrackingEnabled)
                return;

            using var masterConnection = new SqlConnection(Setup.GetSqlDatabaseConnectionString("master"));

            var script = $"ALTER DATABASE {dbName} SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)";


            masterConnection.Open();

            using (var cmdCT = new SqlCommand(script, masterConnection))
                await cmdCT.ExecuteNonQueryAsync();

            masterConnection.Close();
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
                //totalCountRows += serverDbCtx.Sql.Count();
                totalCountRows += serverDbCtx.Tags.Count();
            }

            return totalCountRows;
        }


        /// <summary>
        /// Testing an insert / update on a table where a column is not part of the sync setup, and should stay alive after a sync
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
#pragma warning disable xUnit1026 // Theory methods should use all of their parameters
        public override Task OneColumn_NotInSetup_AfterCleanMetadata_IsTracked_ButNotUpdated(SyncOptions options)
#pragma warning restore xUnit1026 // Theory methods should use all of their parameters
            => Task.CompletedTask;

     
    }
}
