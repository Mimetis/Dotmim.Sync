using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

namespace Dotmim.Sync.Tests.Fixtures
{
    public class DatabaseFilterServerFixture<T> : DatabaseServerFixture<T>, IDisposable where T : RelationalFixture
    {
        public override List<ProviderType> ClientsType => new List<ProviderType> { ProviderType.Sqlite };

        public DatabaseFilterServerFixture() : base() { }
        public virtual SyncParameters GetFilterParameters() => new SyncParameters(("CustomerID", AdventureWorksContext.CustomerId1ForFilter));

        public override SyncSetup GetSyncSetup()
        {
            var setup = base.GetSyncSetup();

            // Filter columns
            setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName" });
            setup.Tables["Address"].Columns.AddRange(new string[] { "AddressID", "AddressLine1", "City", "PostalCode" });

            // Filters clause

            // 1) EASY Way:
            setup.Filters.Add("CustomerAddress", "CustomerID");
            setup.Filters.Add("SalesOrderHeader", "CustomerID", UseFallbackSchema ? "SalesLT" : null);


            // 2) Same, but decomposed in 3 Steps

            var customerFilter = new SetupFilter("Customer");
            customerFilter.AddParameter("CustomerID", "Customer", true);
            customerFilter.AddWhere("CustomerID", "Customer", "CustomerID");
            setup.Filters.Add(customerFilter);

            // 3) Create your own filter

            // Create a filter on table Address
            var addressFilter = new SetupFilter("Address");
            addressFilter.AddParameter("CustomerID", "Customer");
            addressFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "AddressID", "Address", "AddressID");
            addressFilter.AddWhere("CustomerID", "CustomerAddress", "CustomerID");
            setup.Filters.Add(addressFilter);
            // ----------------------------------------------------

            // Create a filter on table SalesLT.SalesOrderDetail
            var salesOrderDetailFilter = new SetupFilter("SalesOrderDetail", UseFallbackSchema ? "SalesLT" : null);
            salesOrderDetailFilter.AddParameter("CustomerID", "Customer");
            salesOrderDetailFilter.AddJoin(Join.Left, $"{salesSchema}SalesOrderHeader").On($"{salesSchema}.SalesOrderHeader", "SalesOrderID", $"{salesSchema}.SalesOrderDetail", "SalesOrderID");
            salesOrderDetailFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerID", $"{salesSchema}.SalesOrderHeader", "CustomerID");
            salesOrderDetailFilter.AddWhere("CustomerID", "CustomerAddress", "CustomerID");
            setup.Filters.Add(salesOrderDetailFilter);
            // ----------------------------------------------------

            // 4) Custom Wheres on Product.
            var productFilter = new SetupFilter("Product", UseFallbackSchema ? "SalesLT" : null);
            var escapeChar =  "\"";

            productFilter.AddCustomWhere($"{escapeChar}ProductCategoryID{escapeChar} IS NOT NULL");
            setup.Filters.Add(productFilter);

            return setup;
        }

        /// <summary>
        /// Get the server database rows count
        /// </summary>
        /// <returns></returns>
        public override int GetDatabaseRowsCount(CoreProvider coreProvider) => GetDatabaseRowsCount(coreProvider, default);

        /// <summary>
        /// Get the server database rows count
        /// </summary>
        /// <returns></returns>
        public int GetDatabaseRowsCount(CoreProvider coreProvider, Guid? customerId)
        {
            int totalCountRows = 0;

            if (!customerId.HasValue)
                customerId = AdventureWorksContext.CustomerId1ForFilter;

            using var ctx = new AdventureWorksContext(coreProvider, UseFallbackSchema, false);

            totalCountRows += ctx.Address.Where(a => a.CustomerAddress.Any(ca => ca.CustomerId == customerId)).Count();
            totalCountRows += ctx.Customer.Where(c => c.CustomerId == customerId).Count();
            totalCountRows += ctx.CustomerAddress.Where(c => c.CustomerId == customerId).Count();
            totalCountRows += ctx.SalesOrderDetail.Where(sod => sod.SalesOrder.CustomerId == customerId).Count();
            totalCountRows += ctx.SalesOrderHeader.Where(c => c.CustomerId == customerId).Count();
            totalCountRows += ctx.Product.Where(p => !String.IsNullOrEmpty(p.ProductCategoryId)).Count();

            totalCountRows += ctx.Employee.Count();
            totalCountRows += ctx.EmployeeAddress.Count();
            totalCountRows += ctx.Log.Count();
            totalCountRows += ctx.Posts.Count();
            totalCountRows += ctx.PostTag.Count();
            totalCountRows += ctx.PricesList.Count();
            totalCountRows += ctx.PricesListCategory.Count();
            totalCountRows += ctx.PricesListDetail.Count();
            totalCountRows += ctx.ProductCategory.Count();
            totalCountRows += ctx.ProductModel.Count();
            totalCountRows += ctx.Tags.Count();

            return totalCountRows;
        }


    }
}
