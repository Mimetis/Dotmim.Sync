using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Test.SqlUtils;
using System;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Xunit;
using System.IO;
using Microsoft.Data.Sqlite;
using Dotmim.Sync.Sqlite;

namespace Dotmim.Sync.Test
{
    public class SqliteSyncIdentityColumnNotPkFixture : IDisposable
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
        private string serverDbName = "TestServerSqliteIdentityField";
        private string client1DbName = "TestClientSqliteIdentityField.db";

        public string[] Tables => new string[] { "Simple" };

        public String ServerConnectionString => HelperDB.GetDatabaseConnectionString(serverDbName);
        public String ClientSqliteConnectionString { get; set; }
        public string ClientSqliteFilePath =>
            Path.Combine(Directory.GetCurrentDirectory(), client1DbName);

        public SqliteSyncIdentityColumnNotPkFixture()
        {

            // create databases
            var builder = new SqliteConnectionStringBuilder
            {
                DataSource = ClientSqliteFilePath
            };
            this.ClientSqliteConnectionString = builder.ConnectionString;

            if (File.Exists(ClientSqliteFilePath))
                File.Delete(ClientSqliteFilePath);

            helperDb.CreateDatabase(serverDbName);

            // create table
            helperDb.ExecuteScript(serverDbName, createTableScript);

            // insert table
            helperDb.ExecuteScript(serverDbName, datas);
        }
        public void Dispose()
        {
            helperDb.DeleteDatabase(serverDbName);
            GC.Collect();
            GC.WaitForPendingFinalizers();

            if (File.Exists(ClientSqliteFilePath))
                File.Delete(ClientSqliteFilePath);
        }

    }


    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    public class SqliteSyncIdentityColumnNotPkTests : IClassFixture<SqliteSyncIdentityColumnNotPkFixture>
    {
        SqlSyncProvider serverProvider;
        SqliteSyncProvider clientProvider;
        SqliteSyncIdentityColumnNotPkFixture fixture;
        SyncAgent agent;

        public SqliteSyncIdentityColumnNotPkTests(SqliteSyncIdentityColumnNotPkFixture fixture)
        {
            this.fixture = fixture;

            serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
            clientProvider = new SqliteSyncProvider(fixture.ClientSqliteFilePath);

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
