using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Test.Misc;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Tests.SqlServer
{
    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    [Collection("SqlServer")]
    public class SqlServerReInitTests : BasicTestsBase, IClassFixture<SqlServerFixture>
    {
        static SqlServerReInitTests()
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
                providerFixture.AddDatabaseName("SqlAdventureWorks");
                // 2) Add tables
                providerFixture.AddTables(sqlTables, 2109);

                if (!Setup.IsOnAzureDev)
                {
                    providerFixture.AddRun(NetworkType.Tcp, ProviderType.Sqlite | ProviderType.Sql);
                    providerFixture.DeleteAllDatabasesOnDispose = false;
                }
                else
                {
                    providerFixture.AddRun(NetworkType.Tcp, ProviderType.Sql | ProviderType.MySql | ProviderType.Sqlite);
                    providerFixture.AddRun(NetworkType.Http, ProviderType.MySql | ProviderType.Sqlite | ProviderType.Sql);
                    providerFixture.DeleteAllDatabasesOnDispose = true;
                }

            };

        }

        public SqlServerReInitTests(SqlServerFixture fixture) : base(fixture)
        {

        }

        //[Fact, TestPriority(1)]
        //public Task InitializeDatabase()
        //{
        //    foreach (var options in TestConfigurations.GetOptions())
        //    {
        //        foreach (var run in this.fixture.ClientRuns)
        //        {
        //            var agent = new SyncAgent(run.ClientProvider, this.fixture.ServerProvider, this.fixture.Tables, options);

        //            var s = agent.SynchronizeAsync(SyncType.Reinitialize);
        //        }
        //    }
        //}


    }
}
