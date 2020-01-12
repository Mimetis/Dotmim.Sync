using Dotmim.Sync.Filter;
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
    public class SqlServerTcpFilterTests : TcpFilterTests
    {
        public SqlServerTcpFilterTests(HelperProvider fixture, ITestOutputHelper output) : base(fixture, output)
        {
        }

        public override SyncSetup FilterSetup
        {
            get
            {
                var setup = new SyncSetup(new string[] { "Customer", "Address", "CustomerAddress", "SalesLT.SalesOrderHeader", "SalesLT.SalesOrderDetail" });

                // Filter columns
                setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName" });
                setup.Tables["Address"].Columns.AddRange(new string[] { "AddressID", "AddressLine1", "City", "PostalCode" });

                // Filter rows
                setup.Filters.Add("Customer", "CustomerID");
                setup.Filters.Add("CustomerAddress", "CustomerID");
                setup.Filters.Add("SalesOrderHeader", "CustomerID", "SalesLT");

                return setup;
            }
        }

        public override List<SyncParameter> FilterParameters => new List<SyncParameter>
        {
                new SyncParameter("Customer", "CustomerID", null, AdventureWorksContext.CustomerIdForFilter),
                new SyncParameter("CustomerAddress", "CustomerID", null, AdventureWorksContext.CustomerIdForFilter),
                new SyncParameter("SalesOrderHeader", "CustomerID", "SalesLT", AdventureWorksContext.CustomerIdForFilter),
        };


        public override List<ProviderType> ClientsType => new List<ProviderType>
            { ProviderType.Sql, ProviderType.Sqlite};

        public override ProviderType ServerType =>
            ProviderType.Sql;



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
                totalCountRows += serverDbCtx.Customer.Where(c => c.CustomerId == AdventureWorksContext.CustomerIdForFilter).Count();
                totalCountRows += serverDbCtx.CustomerAddress.Where(c => c.CustomerId == AdventureWorksContext.CustomerIdForFilter).Count();
                totalCountRows += serverDbCtx.SalesOrderDetail.Count();
                totalCountRows += serverDbCtx.SalesOrderHeader.Where(c => c.CustomerId == AdventureWorksContext.CustomerIdForFilter).Count();
            }

            return totalCountRows;
        }


    }
}
