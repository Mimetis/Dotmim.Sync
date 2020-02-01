using Dotmim.Sync.MySql;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests
{
    public class MySqlTcpTests : TcpTests
    {
        public MySqlTcpTests(HelperProvider fixture, ITestOutputHelper output) : base(fixture, output)
        {
        }

        public override string[] Tables => new string[]
        {
            "ProductCategory", "ProductModel", "Product", "Employee", "Customer", "Address", "CustomerAddress", "EmployeeAddress",
            "SalesOrderHeader", "SalesOrderDetail", "Sql", "Posts", "Tags", "PostTag",
            "PricesList", "PricesListCategory", "PricesListDetail"
        };

        public override List<ProviderType> ClientsType => new List<ProviderType>
            { ProviderType.MySql, ProviderType.Sql, ProviderType.Sqlite};

        public override ProviderType ServerType =>
            ProviderType.MySql;


        public override RemoteOrchestrator CreateRemoteOrchestrator(ProviderType providerType, string dbName)
        {
            var cs = HelperDatabase.GetConnectionString(ProviderType.MySql, dbName);
            var orchestrator = new RemoteOrchestrator(new MySqlSyncProvider(cs));

            return orchestrator;
        }

        public override LocalOrchestrator CreateLocalOrchestrator(ProviderType providerType, string dbName)
        {
            var cs = HelperDatabase.GetConnectionString(providerType, dbName);
            var orchestrator = new LocalOrchestrator();

            switch (providerType)
            {
                case ProviderType.Sql:
                    orchestrator.Provider = new SqlSyncProvider(cs);
                    break;
                case ProviderType.MySql:
                    orchestrator.Provider = new MySqlSyncProvider(cs);
                    break;
                case ProviderType.Sqlite:
                    orchestrator.Provider = new SqliteSyncProvider(cs);
                    break;
            }

            return orchestrator;
        }


        /// <summary>
        /// Get the server database rows count
        /// </summary>
        /// <returns></returns>
        public override int GetServerDatabaseRowsCount((string DatabaseName, ProviderType ProviderType, IOrchestrator Orchestrator) t)
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

        public override async Task EnsureDatabaseSchemaAndSeedAsync((string DatabaseName, ProviderType ProviderType, IOrchestrator Orchestrator) t, bool useSeeding = false, bool useFallbackSchema = false)
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

    }
}
