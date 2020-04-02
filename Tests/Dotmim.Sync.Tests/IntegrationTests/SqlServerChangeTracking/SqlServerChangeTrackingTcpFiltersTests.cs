
using Dotmim.Sync.MySql;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Web.Server;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests.IntegrationTests
{
    public class SqlServerChangeTrackingTcpFiltersTests : TcpFilterTests
    {
        public SqlServerChangeTrackingTcpFiltersTests(HelperProvider fixture, ITestOutputHelper output) : base(fixture, output)
        {
        }

        public override SyncSetup FilterSetup
        {
            get
            {
                var setup = new SyncSetup(new string[] { "SalesLT.Product", "Customer",
                            "Address", "CustomerAddress",
                            "SalesLT.SalesOrderHeader",
                            "SalesLT.SalesOrderDetail" });

                // Filter columns
                setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName" });
                setup.Tables["Address"].Columns.AddRange(new string[] { "AddressID", "AddressLine1", "City", "PostalCode" });


                // Filters clause

                // 1) EASY Way:
                setup.Filters.Add("CustomerAddress", "CustomerID");
                setup.Filters.Add("SalesOrderHeader", "CustomerID", "SalesLT");


                // 2) Same, but decomposed in 3 Steps

                var customerFilter = new SetupFilter("Customer");
                customerFilter.AddParameter("CustomerId", "Customer", true);
                customerFilter.AddWhere("CustomerId", "Customer", "CustomerId");
                setup.Filters.Add(customerFilter);

                // 3) Create your own filter

                // Create a filter on table Address
                var addressFilter = new SetupFilter("Address");
                addressFilter.AddParameter("CustomerID", "Customer");
                addressFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "AddressId", "Address", "AddressId");
                addressFilter.AddWhere("CustomerId", "CustomerAddress", "CustomerId");
                setup.Filters.Add(addressFilter);
                // ----------------------------------------------------

                // Create a filter on table SalesLT.SalesOrderDetail
                var salesOrderDetailFilter = new SetupFilter("SalesOrderDetail", "SalesLT");
                salesOrderDetailFilter.AddParameter("CustomerID", "Customer");
                salesOrderDetailFilter.AddJoin(Join.Left, "SalesLT.SalesOrderHeader").On("SalesLT.SalesOrderHeader", "SalesOrderId", "SalesLT.SalesOrderDetail", "SalesOrderId");
                salesOrderDetailFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesLT.SalesOrderHeader", "CustomerId");
                salesOrderDetailFilter.AddWhere("CustomerId", "CustomerAddress", "CustomerId");
                setup.Filters.Add(salesOrderDetailFilter);
                // ----------------------------------------------------

                // 4) Custom Wheres on Product.
                var productFilter = new SetupFilter("Product", "SalesLT");
                productFilter.AddCustomerWhere("ProductCategoryID IS NOT NULL");
                setup.Filters.Add(productFilter);


                return setup;
            }
        }

        public override SyncParameters FilterParameters => new SyncParameters
        {
                new SyncParameter("CustomerID", AdventureWorksContext.CustomerIdForFilter),
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
                case ProviderType.Sqlite:
                    return new SqliteSyncProvider(cs);
                case ProviderType.Sql:
                default:
                    return new SqlSyncChangeTrackingProvider(cs);
            }
        }

        public override async Task EnsureDatabaseSchemaAndSeedAsync((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) t, bool useSeeding = false, bool useFallbackSchema = false)
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

            var script = $"ALTER DATABASE {dbName} SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)";
            using (var masterConnection = new SqlConnection(Setup.GetSqlDatabaseConnectionString("master")))
            {
                masterConnection.Open();

                using (var cmdCT = new SqlCommand(script, masterConnection))
                    await cmdCT.ExecuteNonQueryAsync();

                masterConnection.Close();
            }
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
                totalCountRows += serverDbCtx.Address.Where(a => a.CustomerAddress.Any(ca => ca.CustomerId == AdventureWorksContext.CustomerIdForFilter)).Count();
                totalCountRows += serverDbCtx.Customer.Where(c => c.CustomerId == AdventureWorksContext.CustomerIdForFilter).Count();
                totalCountRows += serverDbCtx.CustomerAddress.Where(c => c.CustomerId == AdventureWorksContext.CustomerIdForFilter).Count();
                totalCountRows += serverDbCtx.SalesOrderDetail.Where(sod => sod.SalesOrder.CustomerId == AdventureWorksContext.CustomerIdForFilter).Count();
                totalCountRows += serverDbCtx.SalesOrderHeader.Where(c => c.CustomerId == AdventureWorksContext.CustomerIdForFilter).Count();
                totalCountRows += serverDbCtx.Product.Where(p => !String.IsNullOrEmpty(p.ProductCategoryId)).Count();
            }

            return totalCountRows;
        }


    }
}
