//using Dotmim.Sync.Enumerations;
//using Dotmim.Sync.SqlServer;
//using Dotmim.Sync.Tests.Misc;
//using System;
//using System.Data.SqlClient;
//using System.IO;
//using System.Threading.Tasks;
//using Xunit;

//namespace Dotmim.Sync.Tests.Old.SqlServer
//{
//    public class SyncProvAndDeprovTestsFixture : IDisposable
//    {
//        public string serverDbName => "Test_ProvDeprov_Server";
//        public string client1DbName => "Test_ProvDeprov_Client";
//        public string[] Tables => new string[] { "Customers", "ServiceTickets" };

//        private string createTableScript =
//        $@"
//        if (not exists (select * from sys.tables where name = 'ServiceTickets'))
//        begin
//            CREATE TABLE [ServiceTickets](
//	        [ServiceTicketID] [uniqueidentifier] NOT NULL,
//	        [Title] [nvarchar](max) NOT NULL,
//	        [Description] [nvarchar](max) NULL,
//	        [StatusValue] [int] NOT NULL,
//	        [EscalationLevel] [int] NOT NULL,
//	        [Opened] [datetime] NULL,
//	        [Closed] [datetime] NULL,
//	        [CustomerID] [int] NULL,
//            CONSTRAINT [PK_ServiceTickets] PRIMARY KEY CLUSTERED ( [ServiceTicketID] ASC ));
//        end;
//        if (not exists (select * from sys.tables where name = 'Customers'))
//        begin
//            CREATE TABLE [Customers](
//	        [CustomerID] [int] NOT NULL,
//	        [FirstName] [nvarchar](max) NOT NULL,
//	        [LastName] [nvarchar](max) NULL
//            CONSTRAINT [PK_Customers] PRIMARY KEY CLUSTERED ( [CustomerID] ASC ));
//        end;
//        if (not exists (select * from sys.foreign_keys where name = 'FK_ServiceTickets_Customers'))
//        begin
//            ALTER TABLE ServiceTickets ADD CONSTRAINT FK_ServiceTickets_Customers 
//            FOREIGN KEY ( CustomerID ) 
//            REFERENCES Customers ( CustomerID ) 
//        end
//        ";

//        private string datas =
//        $@"
//            INSERT [Customers] ([CustomerID], [FirstName], [LastName]) VALUES (1, N'John', N'Doe');
//            INSERT [Customers] ([CustomerID], [FirstName], [LastName]) VALUES (10, N'Jane', N'Robinson');

//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
//        ";

        

//        public String ServerConnectionString => HelperDB.GetSqlDatabaseConnectionString(serverDbName);
//        public String Client1ConnectionString => HelperDB.GetSqlDatabaseConnectionString(client1DbName);

//        public SyncProvAndDeprovTestsFixture()
//        {
//            // create databases
//            HelperDB.CreateDatabase(serverDbName);
//            HelperDB.CreateDatabase(client1DbName);

//            // create table
//            HelperDB.ExecuteSqlScript(serverDbName, createTableScript);

//            // insert table
//            HelperDB.ExecuteSqlScript(serverDbName, datas);
//        }
//        public void Dispose()
//        {
//            HelperDB.DropSqlDatabase(serverDbName);
//            HelperDB.DropSqlDatabase(client1DbName);

//            var filepathSqlite = Path.Combine(Directory.GetCurrentDirectory(), "Test_TwoTables_Client.sdf");
//            if (File.Exists(filepathSqlite))
//                File.Delete(filepathSqlite);
//        }

//    }

//    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
//    public class SyncProvAndDeprovTests : IClassFixture<SyncProvAndDeprovTestsFixture>
//    {
//        SyncProvAndDeprovTestsFixture fixture;
//        SqlSyncProvider serverProvider;
//        SqlSyncProvider clientProvider;
//        SyncAgent agent;
//        public SyncProvAndDeprovTests(SyncProvAndDeprovTestsFixture fixture)
//        {
//            this.fixture = fixture;

//            serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
//            clientProvider = new SqlSyncProvider(fixture.Client1ConnectionString);
//        }


//        [Fact, TestPriority(1)]
//        public async Task DeprovisionAll()
//        {
//            agent = new SyncAgent(clientProvider, serverProvider, fixture.Tables);

//            var session = await agent.SynchronizeAsync();

//            Assert.Equal(7, session.TotalChangesDownloaded);
//            Assert.Equal(0, session.TotalChangesUploaded);

//            await clientProvider.DeprovisionAsync(agent.Configuration, SyncProvision.All | SyncProvision.Table);

//            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
//            {
//                sqlConnection.Open();

//                string commandText = "Select count(*) from sys.tables";
//                using (var cmd = new SqlCommand(commandText, sqlConnection))
//                {
//                    int nb = (int)cmd.ExecuteScalar();
//                    Assert.Equal(0, nb); // "On purpose, the flag SyncProvision.All does not include the SyncProvision.Table, too dangerous..."
//                }
//                commandText = "Select count(*) from sys.procedures";
//                using (var cmd = new SqlCommand(commandText, sqlConnection))
//                {
//                    int nb = (int)cmd.ExecuteScalar();
//                    Assert.Equal(0, nb);
//                }
//                commandText = "Select count(*) from sys.triggers";
//                using (var cmd = new SqlCommand(commandText, sqlConnection))
//                {
//                    int nb = (int)cmd.ExecuteScalar();
//                    Assert.Equal(0, nb);
//                }

//                sqlConnection.Close();

//            }

//        }

//        [Fact, TestPriority(2)]
//        public async Task DeprovisionStoredProcedures()
//        {
//            agent = new SyncAgent(clientProvider, serverProvider, fixture.Tables);

//            // Overwrite configuration to apply the schem on the database, even if we have already made a sync before
//            agent.DatabaseApplying += (s, e) => e.OverwriteSchema = true;

//            var session = await agent.SynchronizeAsync();

//            await clientProvider.DeprovisionAsync(agent.Configuration, SyncProvision.StoredProcedures);

//            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
//            {
//                sqlConnection.Open();
//                string commandText = "Select count(*) from sys.tables";
//                using (var cmd = new SqlCommand(commandText, sqlConnection))
//                {
//                    int nb = (int)cmd.ExecuteScalar();
//                    Assert.Equal(5, nb);
//                }
//                commandText = "Select count(*) from sys.procedures";
//                using (var cmd = new SqlCommand(commandText, sqlConnection))
//                {
//                    int nb = (int)cmd.ExecuteScalar();
//                    Assert.Equal(0, nb);
//                }
//                sqlConnection.Close();

//            }

//        }


//        [Fact, TestPriority(3)]
//        public async Task DeprovisionTrackingTable()
//        {
//            agent = new SyncAgent(clientProvider, serverProvider, fixture.Tables);

//            // Overwrite configuration to apply the schem on the database, even if we have already made a sync before
//            agent.DatabaseApplying += (s, e) => e.OverwriteSchema = true;

//            var session = await agent.SynchronizeAsync();

//            await clientProvider.DeprovisionAsync(agent.Configuration, SyncProvision.TrackingTable);

//            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
//            {
//                sqlConnection.Open();
//                string commandText = "Select count(*) from sys.tables";
//                using (var cmd = new SqlCommand(commandText, sqlConnection))
//                {
//                    int nb = (int)cmd.ExecuteScalar();
//                    Assert.Equal(3, nb);
//                }
//                sqlConnection.Close();

//            }

//        }

//        [Fact, TestPriority(4)]
//        public async Task DeprovisionTable()
//        {
//            agent = new SyncAgent(clientProvider, serverProvider, fixture.Tables);

//            agent.DatabaseApplying += (s, e) => e.OverwriteSchema = true;

//            var session = await agent.SynchronizeAsync();

//            await clientProvider.DeprovisionAsync(agent.Configuration, SyncProvision.Table);

//            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
//            {
//                sqlConnection.Open();
//                string commandText = "Select count(*) from sys.tables";
//                using (var cmd = new SqlCommand(commandText, sqlConnection))
//                {
//                    int nb = (int)cmd.ExecuteScalar();
//                    Assert.Equal(3, nb);
//                }
//                sqlConnection.Close();

//            }

//        }

//        [Fact, TestPriority(5)]
//        public async Task DeprovisionScope()
//        {
//            agent = new SyncAgent(clientProvider, serverProvider, fixture.Tables);

//            agent.DatabaseApplying += (s, e) => e.OverwriteSchema = true;

//            var session = await agent.SynchronizeAsync();

//            await clientProvider.DeprovisionAsync(agent.Configuration, SyncProvision.Scope);

//            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
//            {
//                sqlConnection.Open();
//                string commandText = "Select count(*) from sys.tables where name = 'scope_info'";
//                using (var cmd = new SqlCommand(commandText, sqlConnection))
//                {
//                    int nb = (int)cmd.ExecuteScalar();
//                    Assert.Equal(0, nb);
//                }
//                sqlConnection.Close();

//            }

//        }

//        [Fact, TestPriority(6)]
//        public async Task DeprovisionAllExceptTables()
//        {
//            agent = new SyncAgent(clientProvider, serverProvider, fixture.Tables);
//            agent.DatabaseApplying += (s, e) => e.OverwriteSchema = true;
//            var session = await agent.SynchronizeAsync();

//            await clientProvider.DeprovisionAsync(agent.Configuration, 
//                    SyncProvision.Scope | SyncProvision.StoredProcedures | SyncProvision.TrackingTable | SyncProvision.Triggers );

//            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
//            {
//                sqlConnection.Open();
//                string commandText = "Select count(*) from sys.tables";
//                using (var cmd = new SqlCommand(commandText, sqlConnection))
//                {
//                    int nb = (int)cmd.ExecuteScalar();
//                    Assert.Equal(2, nb);
//                }
//                commandText = "Select count(*) from sys.procedures";
//                using (var cmd = new SqlCommand(commandText, sqlConnection))
//                {
//                    int nb = (int)cmd.ExecuteScalar();
//                    Assert.Equal(0, nb);
//                }
//                commandText = "Select count(*) from sys.triggers";
//                using (var cmd = new SqlCommand(commandText, sqlConnection))
//                {
//                    int nb = (int)cmd.ExecuteScalar();
//                    Assert.Equal(0, nb);
//                }
//                sqlConnection.Close();

//            }

//        }
//    }
//}
