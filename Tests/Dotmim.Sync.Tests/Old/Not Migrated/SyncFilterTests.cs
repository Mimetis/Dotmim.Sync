//using Dotmim.Sync.Filter;
//using Dotmim.Sync.SqlServer;
//using Dotmim.Sync.Tests.Misc;
//using System;
//using System.Data.SqlClient;
//using System.Threading.Tasks;
//using Xunit;

//namespace Dotmim.Sync.Tests.Old.SqlServer
//{

//    public class SyncFilterFixture : IDisposable
//    {
//        private string createTableScript =
//        $@"if (not exists (select * from sys.tables where name = 'ServiceTickets'))
//            begin
//                CREATE TABLE [ServiceTickets](
//	            [ServiceTicketID] [uniqueidentifier] NOT NULL,
//	            [Title] [nvarchar](max) NOT NULL,
//	            [Description] [nvarchar](max) NULL,
//	            [StatusValue] [int] NOT NULL,
//	            [EscalationLevel] [int] NOT NULL,
//	            [Opened] [datetime] NULL,
//	            [Closed] [datetime] NULL,
//	            [CustomerID] [int] NULL,
//                CONSTRAINT [PK_ServiceTickets] PRIMARY KEY CLUSTERED ( [ServiceTicketID] ASC ));
//            end";

//        private string datas =
//        $@"
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
//            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
//          ";

        
//        private string serverDbName = "Test_Filter_ServerDB";
//        private string client1DbName = "Test_Filter_Client1";
//        private string client2DbName = "Test_Filter_Client2";
//        private string client3DbName = "Test_Filter_Client3";

//        public String ServerConnectionString => HelperDB.GetSqlDatabaseConnectionString(serverDbName);
//        public String Client1ConnectionString => HelperDB.GetSqlDatabaseConnectionString(client1DbName);
//        public String Client2ConnectionString => HelperDB.GetSqlDatabaseConnectionString(client2DbName);
//        public String Client3ConnectionString => HelperDB.GetSqlDatabaseConnectionString(client3DbName);

//        public SyncFilterFixture()
//        {
//            // create databases
//            HelperDB.CreateDatabase(serverDbName);
//            HelperDB.CreateDatabase(client1DbName);
//            HelperDB.CreateDatabase(client2DbName);
//            HelperDB.CreateDatabase(client3DbName);

//            // create table
//            HelperDB.ExecuteSqlScript(serverDbName, createTableScript);

//            // insert table
//            HelperDB.ExecuteSqlScript(serverDbName, datas);

//        }

//        public void Dispose()
//        {
//            HelperDB.DropSqlDatabase(serverDbName);
//            HelperDB.DropSqlDatabase(client1DbName);
//            HelperDB.DropSqlDatabase(client2DbName);
//            HelperDB.DropSqlDatabase(client3DbName);
//        }
//    }

//    [Collection("Sync")]
//    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
//    public class SyncFilterTests : IClassFixture<SyncFilterFixture>
//    {
//        SyncFilterFixture fixture;

//        public SyncFilterTests(SyncFilterFixture fixture)
//        {
//            this.fixture = fixture;
//        }

//        [Fact, TestPriority(1)]
//        public async Task Initialize()
//        {
//            SqlSyncProvider serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
//            SqlSyncProvider clientProvider = new SqlSyncProvider(fixture.Client1ConnectionString);

//            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new[] { "ServiceTickets" });
//            agent.Configuration.Filters.Add(new FilterClause("ServiceTickets", "CustomerID"));
//            agent.Parameters.Add("ServiceTickets", "CustomerID", 1);

//            var session = await agent.SynchronizeAsync();

//            // Only 4 lines should be downloaded
//            Assert.Equal(40, session.TotalChangesDownloaded);
//            Assert.Equal(0, session.TotalChangesUploaded);

//        }

//        [Fact, TestPriority(2)]
//        public async Task AddFilterTableAndNormalTable()
//        {
//            var serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
//            var cli2Provider = new SqlSyncProvider(fixture.Client2ConnectionString);
//            var cli3Provider = new SqlSyncProvider(fixture.Client3ConnectionString);

 
//            // first agent with parameter for filter
//            SyncAgent agent2 = new SyncAgent(cli2Provider, serverProvider, new[] { "ServiceTickets" });
//            agent2.Configuration.Filters.Add(new FilterClause("ServiceTickets", "CustomerID"));
//            agent2.Parameters.Add("ServiceTickets", "CustomerID", 1);

//            // second agent with no parameter
//            SyncAgent agent3 = new SyncAgent(cli3Provider, serverProvider, new[] { "ServiceTickets" });
//            agent3.Configuration.Filters.Add(new FilterClause("ServiceTickets", "CustomerID"));

//            var session2 = await agent2.SynchronizeAsync();
//            var session3 = await agent3.SynchronizeAsync();

//            // Only 4 lines should be downloaded
//            Assert.Equal(40, session2.TotalChangesDownloaded);
//            Assert.Equal(50, session3.TotalChangesDownloaded);

//            session2 = await agent2.SynchronizeAsync();
//            session3 = await agent3.SynchronizeAsync();

//            Assert.Equal(0, session2.TotalChangesDownloaded);
//            Assert.Equal(0, session3.TotalChangesDownloaded);

//            // Update one server row outside filter
//            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
//            {
//                var textUpdateOneRow = "Update ServiceTickets set Opened=getdate() where CustomerID=10";

//                using (var sqlCmd = new SqlCommand(textUpdateOneRow, sqlConnection))
//                {
//                    sqlConnection.Open();
//                    sqlCmd.ExecuteNonQuery();
//                    sqlConnection.Close();
//                }
//            }

//            session2 = await agent2.SynchronizeAsync();
//            session3 = await agent3.SynchronizeAsync();

//            Assert.Equal(0, session2.TotalChangesDownloaded);
//            Assert.Equal(10, session3.TotalChangesDownloaded);


//        }
//        [Fact, TestPriority(3)]
//        public async Task UpdateNoFilter()
//        {
//            // Update one server row outside filter
//            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
//            {
//                var textUpdateOneRow = "Update ServiceTickets set Opened=getdate() where CustomerID=10";

//                using (var sqlCmd = new SqlCommand(textUpdateOneRow, sqlConnection))
//                {
//                    sqlConnection.Open();
//                    sqlCmd.ExecuteNonQuery();
//                    sqlConnection.Close();
//                }
//            }

//            var serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
//            var cli1Provider = new SqlSyncProvider(fixture.Client2ConnectionString);
//            var cli2Provider = new SqlSyncProvider(fixture.Client3ConnectionString);

//            SyncConfiguration configuration = new SyncConfiguration(new[] { "ServiceTickets" });


//            // first agent with parameter for filter
//            SyncAgent agent1 = new SyncAgent(cli1Provider, serverProvider, new[] { "ServiceTickets" });
//            agent1.Configuration.Filters.Add(new FilterClause("ServiceTickets", "CustomerID"));
//            agent1.Parameters.Add("ServiceTickets", "CustomerID", 1);

//            // second agent with no parameter
//            SyncAgent agent2 = new SyncAgent(cli2Provider, serverProvider, new[] { "ServiceTickets" });
//            agent2.Configuration.Filters.Add(new FilterClause("ServiceTickets", "CustomerID"));


//            var session1 = await agent1.SynchronizeAsync();
//            var session2 = await agent2.SynchronizeAsync();

//            Assert.Equal(0, session1.TotalChangesDownloaded);
//            Assert.Equal(10, session2.TotalChangesDownloaded);


//        }
//        [Fact, TestPriority(4)]
//        public async Task UpdateFilter()
//        {
//            // Update one server row outside filter
//            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
//            {
//                var textUpdateOneRow = "Update ServiceTickets set Opened=getdate() where CustomerID=1";

//                using (var sqlCmd = new SqlCommand(textUpdateOneRow, sqlConnection))
//                {
//                    sqlConnection.Open();
//                    sqlCmd.ExecuteNonQuery();
//                    sqlConnection.Close();
//                }
//            }

//            var serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
//            var cli1Provider = new SqlSyncProvider(fixture.Client2ConnectionString);
//            var cli2Provider = new SqlSyncProvider(fixture.Client3ConnectionString);




//            // first agent with parameter for filter
//            SyncAgent agent1 = new SyncAgent(cli1Provider, serverProvider, new[] { "ServiceTickets" });
//            agent1.Configuration.Filters.Add(new FilterClause("ServiceTickets", "CustomerID"));
//            agent1.Parameters.Add("ServiceTickets", "CustomerID", 1);

//            // second agent with no parameter
//            SyncAgent agent2 = new SyncAgent(cli2Provider, serverProvider, new[] { "ServiceTickets" });
//            agent2.Configuration.Filters.Add(new FilterClause("ServiceTickets", "CustomerID"));


//            var session1 = await agent1.SynchronizeAsync();
//            var session2 = await agent2.SynchronizeAsync();

//            Assert.Equal(40, session1.TotalChangesDownloaded);
//            Assert.Equal(40, session2.TotalChangesDownloaded);


//        }
//        [Fact, TestPriority(5)]
//        public async Task DeleteNoFilter()
//        {
//            // Update one server row outside filter
//            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
//            {
//                var textUpdateOneRow = "Delete ServiceTickets Where CustomerID=10";

//                using (var sqlCmd = new SqlCommand(textUpdateOneRow, sqlConnection))
//                {
//                    sqlConnection.Open();
//                    sqlCmd.ExecuteNonQuery();
//                    sqlConnection.Close();
//                }
//            }

//            var serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
//            var cli1Provider = new SqlSyncProvider(fixture.Client2ConnectionString);
//            var cli2Provider = new SqlSyncProvider(fixture.Client3ConnectionString);

//            SyncConfiguration configuration = new SyncConfiguration(new[] { "ServiceTickets" });


//            // first agent with parameter for filter
//            SyncAgent agent1 = new SyncAgent(cli1Provider, serverProvider, new[] { "ServiceTickets" });
//            // Add a filter
//            agent1.Configuration.Filters.Add(new FilterClause("ServiceTickets", "CustomerID"));
//            agent1.Parameters.Add("ServiceTickets", "CustomerID", 1);

//            // second agent with no parameter
//            SyncAgent agent2 = new SyncAgent(cli2Provider, serverProvider, new[] { "ServiceTickets" });
//            agent2.Configuration.Filters.Add(new FilterClause("ServiceTickets", "CustomerID"));


//            var session1 = await agent1.SynchronizeAsync();
//            var session2 = await agent2.SynchronizeAsync();

//            Assert.Equal(0, session1.TotalChangesDownloaded);
//            Assert.Equal(10, session2.TotalChangesDownloaded);


//        }

//    }
//}
