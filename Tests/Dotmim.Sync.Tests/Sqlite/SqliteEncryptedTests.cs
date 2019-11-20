using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync;
using System;
using System.Threading.Tasks;
using Xunit;
using Dotmim.Sync.Tests.SqlServer;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.Tests.Models;

namespace Dotmim.Sync.Tests.Sqlite
{
    /// <summary>
    /// this is the class which implements concret fixture with SqlSyncProviderFixture 
    /// and will call all the base tests
    /// </summary>
    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    [Collection("Sqlite")]
    public class SqliteEncryptedTests
    {
        private string[] sqlTables;
        private SqlSyncProvider serverProvider;
        private SqliteSyncProvider clientProvider;
        private string serverCString = HelperDB.GetConnectionString(ProviderType.Sql, "AdvWorksForEncrypted");
        private string clientCString = HelperDB.GetConnectionString(ProviderType.Sqlite, "EncryptedAdventureWorks");

        public SqliteEncryptedTests()
        {
            this.sqlTables = new string[]
             {
                "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product", "Employee", "Customer", "Address", "CustomerAddress", "EmployeeAddress",
                "SalesLT.SalesOrderHeader", "SalesLT.SalesOrderDetail", "dbo.Sql", "Posts", "Tags", "PostTag",
                "PricesList", "PriceListCategory", "PriceListDetail"
             };


            this.serverProvider = new SqlSyncProvider(serverCString);
            this.clientProvider = new SqliteSyncProvider(clientCString);
        }

        [Fact, TestPriority(1)]
        public async Task Initialize()
        {
            try
            {

                using (var ctx = new AdventureWorksContext())
                {
                    ctx.ProviderType = ProviderType.Sql;
                    ctx.ConnectionString = this.serverCString;
                    ctx.useSeeding = true;
                    ctx.useSchema = true;

                    ctx.Database.EnsureDeleted();
                    ctx.Database.EnsureCreated();
                }

                var agent = new SyncAgent(this.clientProvider, this.serverProvider, this.sqlTables);

                agent.LocalOrchestrator.OnConnectionOpen(args =>
                {
                    var keyCommand = args.Connection.CreateCommand();
                    keyCommand.CommandText = "PRAGMA key = 'password';";
                    keyCommand.ExecuteNonQuery();

                });

                var s = await agent.SynchronizeAsync();

                Assert.Equal(109, s.TotalChangesDownloaded);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                throw;
            }
        }

    }
}
