using Dotmim.Sync.SQLite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Test.Misc;
using Dotmim.Sync.Test.SqlUtils;
using System;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Test
{
    public class SQLiteTwoTablesFixture : IDisposable
    {
        public string serverDbName => "Test_SQLite_TwoTables_Server";
        public string client1DbName => "TestSqliteTwoTablesClient.db";
        public string[] Tables => new string[] { "Customers", "ServiceTickets" };

        private string createTableScript =
        $@"
        if (not exists (select * from sys.tables where name = 'ServiceTickets'))
        begin
            CREATE TABLE [ServiceTickets](
	        [ServiceTicketID] [uniqueidentifier] NOT NULL,
	        [Title] [nvarchar](max) NOT NULL,
	        [Description] [nvarchar](max) NULL,
	        [StatusValue] [int] NOT NULL,
	        [EscalationLevel] [int] NOT NULL,
	        [Opened] [datetime] NULL,
	        [Closed] [datetime] NULL,
	        [CustomerID] [int] NULL,
            CONSTRAINT [PK_ServiceTickets] PRIMARY KEY CLUSTERED ( [ServiceTicketID] ASC ));
        end;
        if (not exists (select * from sys.tables where name = 'Customers'))
        begin
            CREATE TABLE [Customers](
	        [CustomerID] [int] NOT NULL,
	        [FirstName] [nvarchar](max) NOT NULL,
	        [LastName] [nvarchar](max) NULL
            CONSTRAINT [PK_Customers] PRIMARY KEY CLUSTERED ( [CustomerID] ASC ));
        end;
        if (not exists (select * from sys.foreign_keys where name = 'FK_ServiceTickets_Customers'))
        begin
            ALTER TABLE ServiceTickets ADD CONSTRAINT FK_ServiceTickets_Customers 
            FOREIGN KEY ( CustomerID ) 
            REFERENCES Customers ( CustomerID ) 
        end
        ";

        private string datas =
        $@"
            INSERT [Customers] ([CustomerID], [FirstName], [LastName]) VALUES (1, N'John', N'Doe');
            INSERT [Customers] ([CustomerID], [FirstName], [LastName]) VALUES (10, N'Jane', N'Robinson');

            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
        ";

        private HelperDB helperDb = new HelperDB();

        public String ServerConnectionString => HelperDB.GetDatabaseConnectionString(serverDbName);
        public string ClientSQLiteFilePath => Path.Combine(Directory.GetCurrentDirectory(), client1DbName);

        public SQLiteTwoTablesFixture()
        {
            //var builder = new SQLiteConnectionStringBuilder { DataSource = ClientSQLiteFilePath };
            GC.Collect();
            GC.WaitForPendingFinalizers();

            if (File.Exists(ClientSQLiteFilePath))
                File.Delete(ClientSQLiteFilePath);

            // create databases
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

            if (File.Exists(ClientSQLiteFilePath))
                File.Delete(ClientSQLiteFilePath);
        }

    }

    [TestCaseOrderer("Dotmim.Sync.Test.Misc.PriorityOrderer", "Dotmim.Sync.Core.Test")]
    public class SQLiteTwoTablesTests : IClassFixture<SQLiteTwoTablesFixture>
    {
        SQLiteTwoTablesFixture fixture;
        SqlSyncProvider serverProvider;
        SQLiteSyncProvider clientProvider;
        SyncAgent agent;

        public SQLiteTwoTablesTests(SQLiteTwoTablesFixture fixture)
        {
            this.fixture = fixture;

            serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
            clientProvider = new SQLiteSyncProvider(fixture.ClientSQLiteFilePath);
            var simpleConfiguration = new SyncConfiguration(fixture.Tables);

            agent = new SyncAgent(clientProvider, serverProvider, simpleConfiguration);
        }

        [Fact, TestPriority(1)]
        public async Task Initialize()
        {
            var session = await agent.SynchronizeAsync();

            Assert.Equal(7, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);

            // check relation has been created on client :
            //int foreignKeysCount=0;
            //using (var sqlConnection = new SQLiteConnection(fixture.ClientSQLiteConnectionString))
            //{
            //    var script = $@"PRAGMA foreign_key_list('ServiceTickets')";

            //    using (var sqlCmd = new SQLiteCommand(script, sqlConnection))
            //    {
            //        sqlConnection.Open();

            //        var reader = sqlCmd.ExecuteReader();
            //        while (reader.Read())
            //            foreignKeysCount++;

            //        sqlConnection.Close();
            //    }
            //}
            //Assert.Equal(1, foreignKeysCount);
        }

        [Fact, TestPriority(2)]
        public async Task SyncNoRows()
        {
            var session = await agent.SynchronizeAsync();

            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }


        [Fact, TestPriority(3)]
        public async Task CascadeDeleteServer()
        {
            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                var script = $@"Delete from ServiceTickets; Delete from Customers";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            var session = await agent.SynchronizeAsync();

            Assert.Equal(7, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }
    }
}
