//using Dotmim.Sync.Enumerations;
//using Dotmim.Sync.SqlServer;
//using Dotmim.Sync.Tests.Misc;
//using Microsoft.Data.Sqlite;
//using System;
//using System.IO;
//using System.Threading.Tasks;
//using Xunit;

//namespace Dotmim.Sync.Tests.Old.SqlServer
//{
//    public class SyncDirectionFixture : IDisposable
//    {
        
//        private string serverDbName = "AdventureWorksLT2012";
//        private string client1DbName = "AdventureWorksLT2012ClientDirection";
//        public String ClientSqliteConnectionString { get; set; }
//        public string ClientSqliteFilePath => Path.Combine(Directory.GetCurrentDirectory(), "AdventureWorksLT2012Sqlite.db");

//        public String ServerConnectionString => HelperDB.GetSqlDatabaseConnectionString(serverDbName);
//        public String Client1ConnectionString => HelperDB.GetSqlDatabaseConnectionString(client1DbName);

//        public string[] Tables => new string[]
//            {   "SalesLT.ProductCategory",
//                "SalesLT.ProductModel",
//                "SalesLT.ProductDescription",
//                "SalesLT.Product",
//                "SalesLT.ProductModelProductDescription",
//                "SalesLT.Customer",
//                "SalesLT.Address",
//                "SalesLT.CustomerAddress",
//                "SalesLT.SalesOrderHeader",
//                "SalesLT.SalesOrderDetail",
//                "ErrorLog"
//            };

//        public SyncDirectionFixture()
//        {
//            GC.Collect();
//            GC.WaitForPendingFinalizers();

//            var builder = new SqliteConnectionStringBuilder { DataSource = ClientSqliteFilePath };
//            this.ClientSqliteConnectionString = builder.ConnectionString;

//            if (File.Exists(ClientSqliteFilePath))
//                File.Delete(ClientSqliteFilePath);

//            var backup = Path.Combine(Directory.GetCurrentDirectory(), "Backup", "AdventureWorksLT2012.bak");
//            // create databases
//            HelperDB.RestoreDatabase(serverDbName, backup);
//            HelperDB.CreateDatabase(client1DbName);
//        }
//        public void Dispose()
//        {
//            HelperDB.DropSqlDatabase(serverDbName);
//            HelperDB.DropSqlDatabase(client1DbName);

//            GC.Collect();
//            GC.WaitForPendingFinalizers();

//            if (File.Exists(ClientSqliteFilePath))
//                File.Delete(ClientSqliteFilePath);
//        }
//    }

//    [Collection("ADVLT2012")]
//    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
//    public class SyncDirectionTests : IClassFixture<SyncDirectionFixture>
//    {
//        SqlSyncProvider serverProvider;
//        SqlSyncProvider clientProvider;
//        SyncDirectionFixture fixture;
//        SyncAgent agent;

//        public SyncDirectionTests(SyncDirectionFixture fixture)
//        {
//            this.fixture = fixture;

//        }

//        [Fact, TestPriority(1)]
//        public async Task Initialize()
//        {
//            serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
//            clientProvider = new SqlSyncProvider(fixture.Client1ConnectionString);

//            agent = new SyncAgent(clientProvider, serverProvider, fixture.Tables);
//            foreach (var dmTable in agent.Configuration.Schema.Tables)
//                dmTable.SyncDirection = SyncDirection.UploadOnly;

//            var session = await agent.SynchronizeAsync();

//            Assert.Equal(0, session.TotalChangesDownloaded);
//            Assert.Equal(0, session.TotalChangesUploaded);
//        }

        

//    }
//}
