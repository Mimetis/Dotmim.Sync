using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using Xunit;

namespace Dotmim.Sync.Tests.SqlServer
{
    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    [Collection("SqlServer")]
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
                providerFixture.AddTables(sqlTables, 109);
                // 3) Options
                // providerFixture.DeleteAllDatabasesOnDispose = false;

                // add a filter
                providerFixture.Filters.Add(new Filter.FilterClause("Customer", "Title"));
                providerFixture.FilterParameters.Add(new Filter.SyncParameter("Customer", "Title", "Mr."));

                if (!Setup.IsOnAzureDev)
                {
                    providerFixture.AddRun(NetworkType.Tcp, ProviderType.Sql | ProviderType.Sqlite);
                }
                else
                {
                    providerFixture.AddRun(NetworkType.Tcp, ProviderType.MySql | ProviderType.Sql | ProviderType.Sqlite);
                    providerFixture.AddRun(NetworkType.Http, ProviderType.MySql | ProviderType.Sql | ProviderType.Sqlite);
                }
            };

        }
        public SqlServerFilterTests(ProviderFixture fixture) : base(fixture)
        {
        }

        [Fact, TestPriority(2)]
        public override Task Initialize()
        {
            return base.Initialize();
        }
    }
}
