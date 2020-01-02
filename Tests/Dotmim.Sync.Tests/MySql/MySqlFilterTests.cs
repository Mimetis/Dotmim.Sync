using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Dotmim.Sync.Test.Misc;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
using Xunit;

namespace Dotmim.Sync.Tests.MySql
{
    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    [Collection("MySqlServerFilter")]
    public class MySqlFilterTests : BasicTestsBase, IClassFixture<MySqlFixture>
    {

        static MySqlFilterTests()
        {
            Configure = providerFixture =>
            {
                // Set tables to be used for your provider
                var tables = new string[]
                {
                    "Customer", "Address", "CustomerAddress",
                    "SalesOrderHeader", "SalesOrderDetail"
                };

                // 1) Add database name
                providerFixture.AddDatabaseName("MySqlAdventureWorksFilter");
                // 2) Add tables
                providerFixture.AddTables(tables);
                // 3) Options
                // providerFixture.DeleteAllDatabasesOnDispose = false;

                // add a filter
                providerFixture.Filters.Add(new SyncFilter("Customer", "CustomerID"));
                providerFixture.FilterParameters.Add(new Filter.SyncParameter("Customer", "CustomerID", null, AdventureWorksContext.CustomerIdForFilter));
                providerFixture.Filters.Add(new SyncFilter("CustomerAddress", "CustomerID"));
                providerFixture.FilterParameters.Add(new Filter.SyncParameter("CustomerAddress", "CustomerID", null, AdventureWorksContext.CustomerIdForFilter));

                if (!Setup.IsOnAzureDev)
                {
                    providerFixture.AddRun(NetworkType.Tcp, ProviderType.MySql | ProviderType.Sql | ProviderType.Sqlite);
                    providerFixture.AddRun(NetworkType.Http, ProviderType.MySql | ProviderType.Sql | ProviderType.Sqlite);
                }
                else
                {
                    providerFixture.AddRun(NetworkType.Tcp, ProviderType.MySql | ProviderType.Sql | ProviderType.Sqlite);
                    providerFixture.AddRun(NetworkType.Http, ProviderType.MySql | ProviderType.Sql | ProviderType.Sqlite);
                }
            };

        }
        public MySqlFilterTests(MySqlFixture fixture) : base(fixture)
        {
        }

        [Fact, TestPriority(1)]
        public override async Task Initialize()
        {
            try
            {
                var option = TestConfigurations.GetOptions()[0];

                // reset
                var results = await this.testRunner.RunTestsAsync(option);

                foreach (var trr in results)
                {
                    Assert.Equal(28, trr.Results.TotalChangesDownloaded);
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
