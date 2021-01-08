using Dotmim.Sync.MariaDB;
using Dotmim.Sync.MySql;
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
    public class MySqlHttpFilterTests : HttpFilterTests
    {
        public MySqlHttpFilterTests(HelperProvider fixture, ITestOutputHelper output) : base(fixture, output)
        {
        }

        public override SyncSetup FilterSetup
        {
            get
            {
                var setup = new SyncSetup(new string[] { "Product", "Customer",
                            "Address", "CustomerAddress",
                            "SalesOrderHeader",
                            "SalesOrderDetail" });

                // Filter columns
                setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName" });
                setup.Tables["Address"].Columns.AddRange(new string[] { "AddressID", "AddressLine1", "City", "PostalCode" });


                // Filters clause

                // 1) EASY Way:
                setup.Filters.Add("CustomerAddress", "CustomerID");
                setup.Filters.Add("SalesOrderHeader", "CustomerID");


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

                // Create a filter on table SalesOrderDetail
                var salesOrderDetailFilter = new SetupFilter("SalesOrderDetail");
                salesOrderDetailFilter.AddParameter("CustomerID", "Customer");
                salesOrderDetailFilter.AddJoin(Join.Left, "SalesOrderHeader").On("SalesOrderHeader", "SalesOrderId", "SalesOrderDetail", "SalesOrderId");
                salesOrderDetailFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
                salesOrderDetailFilter.AddWhere("CustomerId", "CustomerAddress", "CustomerId");
                setup.Filters.Add(salesOrderDetailFilter);
                // ----------------------------------------------------

                // 4) Custom Wheres on Product.
                var productFilter = new SetupFilter("Product");
                productFilter.AddCustomWhere("ProductCategoryID IS NOT NULL");
                setup.Filters.Add(productFilter);


                return setup;
            }
        }

        public override SyncParameters FilterParameters => new SyncParameters
        {
                new SyncParameter("CustomerID", AdventureWorksContext.CustomerIdForFilter),
        };


        public override List<ProviderType> ClientsType => new List<ProviderType>
            { ProviderType.MySql, ProviderType.Sql, ProviderType.Sqlite};

        public override ProviderType ServerType => ProviderType.MySql;


        public override bool UseFiddler => false;

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

                var addressesCount =  serverDbCtx.Address.Where(a => a.CustomerAddress.Any(ca => ca.CustomerId == AdventureWorksContext.CustomerIdForFilter)).Count();
                var customersCount = serverDbCtx.Customer.Where(c => c.CustomerId == AdventureWorksContext.CustomerIdForFilter).Count();
                var customerAddressesCount = serverDbCtx.CustomerAddress.Where(c => c.CustomerId == AdventureWorksContext.CustomerIdForFilter).Count();
                var salesOrdersDetailsCount = serverDbCtx.SalesOrderDetail.Where(sod => sod.SalesOrder.CustomerId == AdventureWorksContext.CustomerIdForFilter).Count();
                var salesOrdersHeadersCount = serverDbCtx.SalesOrderHeader.Where(c => c.CustomerId == AdventureWorksContext.CustomerIdForFilter).Count();
                var productsCount = serverDbCtx.Product.Where(p => !String.IsNullOrEmpty(p.ProductCategoryId)).Count();

                totalCountRows = addressesCount + customersCount + customerAddressesCount + salesOrdersDetailsCount + salesOrdersHeadersCount + productsCount;
            }

            return totalCountRows;
        }


    }
}
