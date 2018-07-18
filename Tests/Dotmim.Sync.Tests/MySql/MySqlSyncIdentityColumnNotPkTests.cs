using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Test.SqlUtils;
using System;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Xunit;
using Dotmim.Sync.MySql;

namespace Dotmim.Sync.Test
{
    public class MySqlSyncIdentityColumnNotPkFixture : IDisposable
    {
        private string createTableScript =
        $@"if (not exists (select * from sys.tables where name = 'Simple'))
            CREATE TABLE [dbo].[Simple](
	            [PrimaryId] [int] NOT NULL,
	            [Id] [int] IDENTITY NOT NULL,
	            [Title] [nvarchar](max) NULL,
                    CONSTRAINT [PK_Simple] PRIMARY KEY CLUSTERED ([PrimaryId] ASC)
            ) ON [PRIMARY]
        ";

        private string datas =
        $@"
            INSERT [dbo].[Simple] ([PrimaryId], [Title]) VALUES (1001, N'Best Boutiques on the Eastside')
            INSERT [dbo].[Simple] ([PrimaryId], [Title]) VALUES (1002, N'Avoiding over-priced Hipster joints')
            INSERT [dbo].[Simple] ([PrimaryId], [Title]) VALUES (1003, N'Where to buy Mars Bars')
            INSERT [dbo].[Simple] ([PrimaryId], [Title]) VALUES (1004, N'Luxury Wine')
        ";

        private HelperDB helperDb = new HelperDB();
        private string serverDbName = "TestMySqlServerIdentityField";
        private string client1DbName = "testmysqlclientidentityfield";

        public string[] Tables => new string[] { "Simple" };

        public String ServerConnectionString => HelperDB.GetDatabaseConnectionString(serverDbName);
        public String Client1ConnectionString => HelperDB.GetMySqlDatabaseConnectionString(client1DbName);

        public MySqlSyncIdentityColumnNotPkFixture()
        {
            // create databases
            helperDb.CreateDatabase(serverDbName);
            helperDb.CreateMySqlDatabase(client1DbName);

            // create table
            helperDb.ExecuteScript(serverDbName, createTableScript);

            // insert table
            helperDb.ExecuteScript(serverDbName, datas);
        }
        public void Dispose()
        {
            helperDb.DeleteDatabase(serverDbName);
            helperDb.DropMySqlDatabase(client1DbName);
        }

    }


    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    public class MySqlSyncIdentityColumnNotPkTests : IClassFixture<MySqlSyncIdentityColumnNotPkFixture>
    {
        SqlSyncProvider serverProvider;
        MySqlSyncProvider clientProvider;
        MySqlSyncIdentityColumnNotPkFixture fixture;
        SyncAgent agent;

        public MySqlSyncIdentityColumnNotPkTests(MySqlSyncIdentityColumnNotPkFixture fixture)
        {
            this.fixture = fixture;

            serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
            clientProvider = new MySqlSyncProvider(fixture.Client1ConnectionString);

            agent = new SyncAgent(clientProvider, serverProvider, fixture.Tables);
        }

        [Fact, TestPriority(0)]
        public async Task Initialize()
        {
            var session = await agent.SynchronizeAsync();

            Assert.Equal(4, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }

      
    }
}
