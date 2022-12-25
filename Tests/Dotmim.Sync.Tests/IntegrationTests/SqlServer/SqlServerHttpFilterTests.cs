using Dotmim.Sync.MariaDB;
using Dotmim.Sync.MySql;
using Dotmim.Sync.PostgreSql;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Web.Server;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests.IntegrationTests
{
    public class SqlServerHttpFilterTests : HttpFilterTests
    {

        public override List<ProviderType> ClientsType => new List<ProviderType>
            {  ProviderType.Sql, ProviderType.Sqlite};

        public SqlServerHttpFilterTests(HelperProvider fixture, ITestOutputHelper output) : base(fixture, output)
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

                // Create a filter on table SalesLT.SalesOrderDetail
                var salesOrderDetailFilter = new SetupFilter("SalesOrderDetail", "SalesLT");
                salesOrderDetailFilter.AddParameter("CustomerID", "Customer");
                salesOrderDetailFilter.AddJoin(Join.Left, "SalesLT.SalesOrderHeader").On("SalesLT.SalesOrderHeader", "SalesOrderId", "SalesLT.SalesOrderDetail", "SalesOrderId");
                salesOrderDetailFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerID", "SalesLT.SalesOrderHeader", "CustomerID");
                salesOrderDetailFilter.AddWhere("CustomerID", "CustomerAddress", "CustomerID");
                setup.Filters.Add(salesOrderDetailFilter);
                // ----------------------------------------------------

                // 4) Custom Wheres on Product.
                var productFilter = new SetupFilter("Product", "SalesLT");
                productFilter.AddCustomWhere("ProductCategoryID IS NOT NULL OR side.sync_row_is_tombstone = 1");
                setup.Filters.Add(productFilter);


                return setup;
            }
        }

        public override SyncParameters FilterParameters => new SyncParameters
        {
                new SyncParameter("CustomerID", AdventureWorksContext.CustomerId1ForFilter),
        };


        public override ProviderType ServerType => ProviderType.Sql;


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
        public override int GetServerDatabaseRowsCount((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) t, Guid? customerId = null)
        {
            int totalCountRows = 0;

            if (!customerId.HasValue)
                customerId = AdventureWorksContext.CustomerId1ForFilter;

            using (var serverDbCtx = new AdventureWorksContext(t))
            {

                var addressesCount = serverDbCtx.Address.Where(a => a.CustomerAddress.Any(ca => ca.CustomerId == customerId)).Count();
                var customersCount = serverDbCtx.Customer.Where(c => c.CustomerId == customerId).Count();
                var customerAddressesCount = serverDbCtx.CustomerAddress.Where(c => c.CustomerId == customerId).Count();
                var salesOrdersDetailsCount = serverDbCtx.SalesOrderDetail.Where(sod => sod.SalesOrder.CustomerId == customerId).Count();
                var salesOrdersHeadersCount = serverDbCtx.SalesOrderHeader.Where(c => c.CustomerId == customerId).Count();
                var productsCount = serverDbCtx.Product.Where(p => !String.IsNullOrEmpty(p.ProductCategoryId)).Count();

                totalCountRows = addressesCount + customersCount + customerAddressesCount + salesOrdersDetailsCount + salesOrdersHeadersCount + productsCount;
            }

            return totalCountRows;
        }


    }
}
