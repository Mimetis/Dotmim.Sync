
using Dotmim.Sync.MySql;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Web.Server;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Dotmim.Sync.MariaDB;

namespace Dotmim.Sync.Tests.IntegrationTests
{
    public class SqlServerChangeTrackingTcpTests : TcpTests
    {
        public SqlServerChangeTrackingTcpTests(HelperProvider fixture, ITestOutputHelper output) : base(fixture, output)
        {
        }

        public override string[] Tables => new string[]
        {
            "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product", "Employee", "Customer", "Address", "CustomerAddress", "EmployeeAddress",
            "SalesLT.SalesOrderHeader", "SalesLT.SalesOrderDetail", "Posts", "Tags", "PostTag",
            "PricesList", "PricesListCategory", "PricesListDetail", "Log"
        };

        public override List<ProviderType> ClientsType => new List<ProviderType>
            {  ProviderType.Sql};

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

        //public T CreateOrchestrator<T>(ProviderType providerType, string dbName, bool useChangeTracking = false) where T : IOrchestrator
        //{
        //    // Get connection string
        //    var cs = HelperDatabase.GetConnectionString(providerType, dbName);

        //    CoreProvider provider = null;
        //    IOrchestrator orchestrator = null;

        //    switch (providerType)
        //    {
        //        case ProviderType.Sql:
        //            provider = useChangeTracking ? new SqlSyncChangeTrackingProvider(cs) : new SqlSyncProvider(cs);
        //            break;
        //        case ProviderType.MySql:
        //            provider = new MySqlSyncProvider(cs);
        //            break;
        //        case ProviderType.Sqlite:
        //            provider = new SqliteSyncProvider(cs);
        //            break;
        //    }

        //    if (typeof(T) == typeof(RemoteOrchestrator))
        //        orchestrator = new RemoteOrchestrator(provider);
        //    else if (typeof(T) == typeof(LocalOrchestrator))
        //        orchestrator = new LocalOrchestrator(provider);
        //    else if (typeof(T) == typeof(WebServerOrchestrator))
        //        orchestrator = new WebServerOrchestrator(provider);

        //    if (orchestrator == null)
        //        throw new Exception("Orchestrator does not exists");

        //    return (T)orchestrator;
        //}


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
        /// Since we do not have control on the change tracking mechanism, any row updated will be marked as updated
        /// Even if the value is the same or if the column is not part of sync setup
        /// </summary>
        public override Task OneColumn_NotInSetup_AfterCleanMetadata_ShouldNotBe_Tracked_AND_ShouldNotBe_UploadedToServer(SyncOptions options)
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Since we do not have control on the change tracking mechanism, any row updated will be marked as updated
        /// Even if the value is the same or if the column is not part of sync setup
        /// </summary>
        public override Task OneColumn_NotInSetup_ShouldNotBe_UploadToServer(SyncOptions options) {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Since we do not have control on the change tracking mechanism, any row updated will be marked as updated
        /// Even if the value is the same or if the column is not part of sync setup
        /// </summary>
        public override Task OneColumn_NotInSetup_IfServerSendsChanges_UpdatesLocalRow_AndDoesNotClear_OneColumn(SyncOptions options)
        {
            return Task.CompletedTask;
        }

    }
}
