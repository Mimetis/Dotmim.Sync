using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.Tests.V2
{
    public class SqlServerHttpTests : HttpTests
    {
        public SqlServerHttpTests(HelperProvider fixture) : base(fixture)
        {
        }

        public override string[] Tables => new string[]
        {
            "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product", "Employee", "Customer", "Address", "CustomerAddress", "EmployeeAddress",
            "SalesLT.SalesOrderHeader", "SalesLT.SalesOrderDetail", "dbo.Sql", "Posts", "Tags", "PostTag",
            "PricesList", "PricesListCategory", "PricesListDetail"
        };

        public override ProviderType ClientsType =>
            ProviderType.Sql | ProviderType.Sqlite | ProviderType.MySql;

        public override ProviderType ServerType =>
            ProviderType.Sql;

        /// <summary>
        /// Get the server database rows count
        /// </summary>
        /// <returns></returns>
        public override int GetServerDatabaseRowsCount(ProviderType providerType, CoreProvider provider)
        {
            int totalCountRows = 0;

            using (var serverDbCtx = new AdventureWorksContext(providerType, provider.ConnectionString, true, true))
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
        /// Get the server database rows count when filtered
        /// </summary>
        public int GetServerFilteredDatabaseRowsCount(ProviderType providerType, CoreProvider provider)
        {
            int totalCountRows = 0;

            var filter = AdventureWorksContext.CustomerIdForFilter;

            using (var serverDbCtx = new AdventureWorksContext(providerType, provider.ConnectionString, true, true))
            {
                totalCountRows += serverDbCtx.Address.Count();
                totalCountRows += serverDbCtx.Customer.Where(c => c.CustomerId == filter).Count();
                totalCountRows += serverDbCtx.CustomerAddress.Where(c => c.CustomerId == filter).Count();
                totalCountRows += serverDbCtx.SalesOrderDetail.Count();
                totalCountRows += serverDbCtx.SalesOrderHeader.Where(c => c.CustomerId == filter).Count();
            }

            return totalCountRows;
        }
    }
}
