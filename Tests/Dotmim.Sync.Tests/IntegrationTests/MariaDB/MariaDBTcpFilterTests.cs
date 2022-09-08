
using Dotmim.Sync.MariaDB;
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

namespace Dotmim.Sync.Tests.IntegrationTests
{
    public class MariaDBTcpFilterTests : TcpFilterTests
    {
        public override List<ProviderType> ClientsType => new List<ProviderType>
            { ProviderType.MariaDB,  ProviderType.MySql, ProviderType.Sql, ProviderType.Sqlite};

        public MariaDBTcpFilterTests(HelperProvider fixture, ITestOutputHelper output) : base(fixture, output)
        {
        }

        public override SyncSetup FilterSetup
        {
            get
            {
                var setup = new SyncSetup(new string[] { "Product", "Customer", "Address", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" });

                // Filter columns
                setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName" });
                setup.Tables["Address"].Columns.AddRange(new string[] { "AddressID", "AddressLine1", "City", "PostalCode" });

                // Filters clause

                // 1) EASY Way:
                setup.Filters.Add("CustomerAddress", "CustomerID");
                setup.Filters.Add("SalesOrderHeader", "CustomerID");

                // 2) Same, but decomposed in 3 Steps
                var customerFilter = new SetupFilter("Customer");
                customerFilter.AddParameter("CustomerID", "Customer", true);
                customerFilter.AddWhere("CustomerID", "Customer", "CustomerID");
                setup.Filters.Add(customerFilter);

                // 3) Create your own filter
                // Create a filter on table Address
                var addressFilter = new SetupFilter("Address");
                addressFilter.AddParameter("CustomerID", "Customer");
                addressFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "AddressId", "Address", "AddressId");
                addressFilter.AddWhere("CustomerID", "CustomerAddress", "CustomerID");
                setup.Filters.Add(addressFilter);
                // ----------------------------------------------------

                // Create a filter on table SalesOrderDetail
                var salesOrderDetailFilter = new SetupFilter("SalesOrderDetail");
                salesOrderDetailFilter.AddParameter("CustomerID", "Customer");
                salesOrderDetailFilter.AddJoin(Join.Left, "SalesOrderHeader").On("SalesOrderHeader", "SalesOrderId", "SalesOrderDetail", "SalesOrderId");
                salesOrderDetailFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerID", "SalesOrderHeader", "CustomerID");
                salesOrderDetailFilter.AddWhere("CustomerID", "CustomerAddress", "CustomerID");
                setup.Filters.Add(salesOrderDetailFilter);
                // ----------------------------------------------------

                // 4) Custom Wheres on Product.
                var productFilter = new SetupFilter("Product");
                productFilter.AddCustomWhere("ProductCategoryID IS NOT NULL");
                setup.Filters.Add(productFilter);

                return setup;
            }
        }

        public override SyncParameters FilterParameters => new SyncParameters(("CustomerID", AdventureWorksContext.CustomerId1ForFilter));

        public override ProviderType ServerType => ProviderType.MariaDB;

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
                    return new SqlSyncProvider(cs);
            }
        }



        /// <summary>
        /// Get the server database rows count
        /// </summary>
        /// <returns></returns>
        public override int GetServerDatabaseRowsCount((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) t, Guid? customerId = null)
        {
            int totalCountRows = 0;

            if (!customerId.HasValue)
                customerId = AdventureWorksContext.CustomerId1ForFilter;


            using (var serverDbCtx = new AdventureWorksContext(t, false))
            {
                totalCountRows += serverDbCtx.Address.Where(a => a.CustomerAddress.Any(ca => ca.CustomerId == customerId)).Count();
                totalCountRows += serverDbCtx.Customer.Where(c => c.CustomerId == customerId).Count();
                totalCountRows += serverDbCtx.CustomerAddress.Where(c => c.CustomerId == customerId).Count();
                totalCountRows += serverDbCtx.SalesOrderDetail.Where(sod => sod.SalesOrder.CustomerId == customerId).Count();
                totalCountRows += serverDbCtx.SalesOrderHeader.Where(c => c.CustomerId == customerId).Count();
                totalCountRows += serverDbCtx.Product.Where(p => !String.IsNullOrEmpty(p.ProductCategoryId)).Count();
            }

            return totalCountRows;
        }

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

    }
}
