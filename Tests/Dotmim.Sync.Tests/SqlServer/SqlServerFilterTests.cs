using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
using Xunit;

namespace Dotmim.Sync.Tests.SqlServer
{
    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    [Collection("SqlServerFilter")]
    public class SqlServerFilterTests : BasicTestsBase, IClassFixture<SqlServerFixture>
    {

        static SqlServerFilterTests()
        {
            Configure = providerFixture =>
            {
                // Set tables to be used for your provider
                var sqlTables = new string[]
                {
                    "Customer", "Address", "CustomerAddress",
                    "SalesLT.SalesOrderHeader", "SalesLT.SalesOrderDetail"
                };

                // 1) Add database name
                providerFixture.AddDatabaseName("SqlAdventureWorksFilter");
                // 2) Add tables
                providerFixture.AddTables(sqlTables, 28);
                // 3) Options
                 providerFixture.DeleteAllDatabasesOnDispose = false;

                // add a filter
                providerFixture.Filters.Add(new SyncFilter("Customer", "CustomerID"));
                providerFixture.FilterParameters.Add(new Filter.SyncParameter("Customer", "CustomerID", null, AdventureWorksContext.CustomerIdForFilter));
                providerFixture.Filters.Add(new SyncFilter("CustomerAddress", "CustomerID"));
                providerFixture.FilterParameters.Add(new Filter.SyncParameter("CustomerAddress", "CustomerID", null, AdventureWorksContext.CustomerIdForFilter));

                if (!Setup.IsOnAzureDev)
                {
                    providerFixture.AddRun(NetworkType.Tcp, ProviderType.Sql );
                }
                else
                {
                    providerFixture.AddRun(NetworkType.Tcp, ProviderType.MySql | ProviderType.Sql | ProviderType.Sqlite);
                    providerFixture.AddRun(NetworkType.Http, ProviderType.MySql | ProviderType.Sql | ProviderType.Sqlite);
                }
            };

        }
        public SqlServerFilterTests(SqlServerFixture fixture) : base(fixture)
        {
        }

        [Fact, TestPriority(1)]
        public override Task Initialize()
        {
            return base.Initialize();
        }
    }
}
