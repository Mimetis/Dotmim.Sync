//using System;
//using System.Collections.Generic;
//using System.Text;
//using System.Threading.Tasks;
//using Dotmim.Sync.Test.Misc;
//using Dotmim.Sync.Tests.Core;
//using Dotmim.Sync.Tests.Misc;
//using Dotmim.Sync.Tests.Models;
//using Xunit;

//namespace Dotmim.Sync.Tests.SqlServer
//{
//    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
//    [Collection("SqlServerFilter")]
//    public class SqlServerFilterTests : BasicTestsBase, IClassFixture<SqlServerFixture>
//    {

//        static SqlServerFilterTests()
//        {
//            Configure = providerFixture =>
//            {
//                // Set tables to be used for your provider
//                var sqlTables = new string[]
//                {
//                    "Customer", "Address", "CustomerAddress",
//                    "SalesLT.SalesOrderHeader", "SalesLT.SalesOrderDetail"
//                };

//                // 1) Add database name
//                providerFixture.AddDatabaseName("SqlAdventureWorksFilter");
//                // 2) Add tables
//                providerFixture.AddTables(sqlTables);
//                // 3) Options
//                 providerFixture.DeleteAllDatabasesOnDispose = true;

//                // add a filter
//                providerFixture.Filters.Add(new SetupFilter("Customer", "CustomerID"));
//                providerFixture.FilterParameters.Add(new Filter.SyncParameter("Customer", "CustomerID", null, AdventureWorksContext.CustomerIdForFilter));
//                providerFixture.Filters.Add(new SetupFilter("CustomerAddress", "CustomerID"));
//                providerFixture.FilterParameters.Add(new Filter.SyncParameter("CustomerAddress", "CustomerID", null, AdventureWorksContext.CustomerIdForFilter));

//                if (!Setup.IsOnAzureDev)
//                {
//                    providerFixture.AddRun(NetworkType.Tcp, ProviderType.Sql);
//                }
//                else
//                {
//                    providerFixture.AddRun(NetworkType.Tcp, ProviderType.Sql | ProviderType.MySql | ProviderType.Sqlite);
//                    providerFixture.AddRun(NetworkType.Http, ProviderType.MySql | ProviderType.Sqlite | ProviderType.Sql);
//                }
//            };

//        }
//        public SqlServerFilterTests(SqlServerFixture fixture) : base(fixture)
//        {
//        }

//        [Fact, TestPriority(1)]
//        public override async Task Initialize()
//        {
//            try
//            {
//                var option = TestConfigurations.GetOptions()[0];

//                // get rows count filtered;
//                var rowsCount = GetServerFilteredDatabaseRowsCount();

//                // reset
//                var results = await this.testRunner.RunTestsAsync(option);

//                foreach (var trr in results)
//                {
//                    Assert.Equal(rowsCount, trr.Results.TotalChangesDownloaded);
//                    Assert.Equal(0, trr.Results.TotalChangesUploaded);
//                }
//            }
//            catch (Exception ex)
//            {
//                Console.WriteLine(ex);
//                throw;
//            }
//        }
//    }
//}
