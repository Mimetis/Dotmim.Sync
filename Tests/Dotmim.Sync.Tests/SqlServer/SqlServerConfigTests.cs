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
    [Collection("SqlServerConfig")]
    public class SqlServerConfigTests : BasicTestsBase, IClassFixture<SqlServerFixture>
    {

        static SqlServerConfigTests()
        {
            Configure = providerFixture =>
            {
                // Set tables to be used for your provider
                var sqlTables = new string[]
                {
                    "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product", "Employee", "Customer", "Address", "CustomerAddress", "EmployeeAddress",
                    "SalesLT.SalesOrderHeader", "SalesLT.SalesOrderDetail", "dbo.Sql", "Posts", "Tags", "PostTag",
                    "PricesList", "PriceListCategory", "PriceListDetail"
                };

                // 1) Add database name
                providerFixture.AddDatabaseName("SqlAdventureWorksConf");
                // 2) Add tables
                providerFixture.AddTables(sqlTables, 109);

                if (!Setup.IsOnAzureDev)
                {
                    providerFixture.AddRun(NetworkType.Tcp,ProviderType.Sql | ProviderType.Sqlite);
                    providerFixture.AddRun(NetworkType.Http, ProviderType.Sql | ProviderType.Sqlite);
                }
                else
                {
                    providerFixture.AddRun(NetworkType.Tcp, ProviderType.MySql | ProviderType.Sql | ProviderType.Sqlite);
                    providerFixture.AddRun(NetworkType.Http, ProviderType.MySql | ProviderType.Sql | ProviderType.Sqlite);
                }
            };

        }
        public SqlServerConfigTests(SqlServerFixture fixture) : base(fixture)
        {
        }

        [Fact, TestPriority(1)]
        public override async Task Initialize()
        {
            try
            {

                var conf = new SyncSchema
                {
                    StoredProceduresPrefix = "s",
                    StoredProceduresSuffix = "",
                    TrackingTablesPrefix = "t",
                    TrackingTablesSuffix = ""
                };

                var results = await this.testRunner.RunTestsAsync(conf);

                foreach (var trr in results)
                {
                    Assert.Equal(this.fixture.RowsCount, trr.Results.TotalChangesDownloaded);
                    Assert.Equal(0, trr.Results.TotalChangesUploaded);
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                throw;
            }
        }
    }
}
