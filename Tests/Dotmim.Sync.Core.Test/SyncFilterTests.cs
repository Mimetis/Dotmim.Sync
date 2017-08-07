using Dotmim.Sync.Core.Test.Misc;
using Dotmim.Sync.Core.Test.SqlUtils;
using Dotmim.Sync.SqlServer;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Core.Test
{

    public class SyncFilterFixture : IDisposable
    {
        private string createTableScript =
        $@"if (not exists (select * from sys.tables where name = 'ServiceTickets'))
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
            end";

        private string datas =
        $@"
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
          ";

        private HelperDB helperDb = new HelperDB();
        private string serverDbName = "Test_Filter_ServerDB";
        private string client1DbName = "Test_Filter_Client1";
        private string client2DbName = "Test_Filter_Client2";
        private string client3DbName = "Test_Filter_Client3";

        public String ServerConnectionString => HelperDB.GetDatabaseConnectionString(serverDbName);
        public String Client1ConnectionString => HelperDB.GetDatabaseConnectionString(client1DbName);
        public String Client2ConnectionString => HelperDB.GetDatabaseConnectionString(client2DbName);
        public String Client3ConnectionString => HelperDB.GetDatabaseConnectionString(client3DbName);

        public SyncFilterFixture()
        {
            // create databases
            helperDb.CreateDatabase(serverDbName);
            helperDb.CreateDatabase(client1DbName);
            helperDb.CreateDatabase(client2DbName);
            helperDb.CreateDatabase(client3DbName);

            // create table
            helperDb.ExecuteScript(serverDbName, createTableScript);

            // insert table
            helperDb.ExecuteScript(serverDbName, datas);

        }

        public void Dispose()
        {
            helperDb.DeleteDatabase(serverDbName);
            helperDb.DeleteDatabase(client1DbName);
            helperDb.DeleteDatabase(client2DbName);
            helperDb.DeleteDatabase(client3DbName);
        }
    }

    [Collection("Sync")]
    [TestCaseOrderer("Dotmim.Sync.Core.Test.Misc.PriorityOrderer", "Dotmim.Sync.Core.Test")]
    public class SyncFilterTests : IClassFixture<SyncFilterFixture>
    {
        SyncFilterFixture fixture;

        public SyncFilterTests(SyncFilterFixture fixture)
        {
            this.fixture = fixture;
        }

        [Fact, TestPriority(1)]
        public async Task InitializeOnTableFiltered()
        {
            SqlSyncProvider serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
            SqlSyncProvider clientProvider = new SqlSyncProvider(fixture.Client1ConnectionString);

            ServiceConfiguration configuration = new ServiceConfiguration(new[] { "ServiceTickets" });

            // Add a filter
            configuration.Filters.Add("ServiceTickets", "CustomerID");

            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);

            // Add the parameter
            agent.Parameters.Add("ServiceTickets", "CustomerID", 1);

            var session = await agent.SynchronizeAsync();

            // Only 4 lines should be downloaded
            Assert.Equal(4, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);

        }

        [Fact, TestPriority(2)]
        public async Task InitializeOneTableFilteredOneTableNotFiltered()
        {
            var serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
            var cli1Provider = new SqlSyncProvider(fixture.Client2ConnectionString);
            var cli2Provider = new SqlSyncProvider(fixture.Client3ConnectionString);

            ServiceConfiguration configuration = new ServiceConfiguration(new[] { "ServiceTickets" });

            // Add a filter
            configuration.Filters.Add("ServiceTickets", "CustomerID");

            // first agent with parameter for filter
            SyncAgent agent1 = new SyncAgent(cli1Provider, serverProvider, configuration);
            agent1.Parameters.Add("ServiceTickets", "CustomerID", 1);

            // second agent with no parameter
            SyncAgent agent2 = new SyncAgent(cli2Provider, serverProvider, configuration);

            var session1 = await agent1.SynchronizeAsync();
            var session2 = await agent2.SynchronizeAsync();

            // Only 4 lines should be downloaded
            Assert.Equal(4, session1.TotalChangesDownloaded);
            Assert.Equal(5, session2.TotalChangesDownloaded);

            session1 = await agent1.SynchronizeAsync();
            session2 = await agent2.SynchronizeAsync();

            Assert.Equal(0, session1.TotalChangesDownloaded);
            Assert.Equal(0, session2.TotalChangesDownloaded);

            // Update one server row outside filter
            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                var textUpdateOneRow = "Update ServiceTickets set Opened=getdate() where CustomerID=10";

                using (var sqlCmd = new SqlCommand(textUpdateOneRow, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            session1 = await agent1.SynchronizeAsync();
            session2 = await agent2.SynchronizeAsync();

            Assert.Equal(0, session1.TotalChangesDownloaded);
            Assert.Equal(1, session2.TotalChangesDownloaded);


        }
        [Fact, TestPriority(3)]
        public async Task UpdateOneRowOutOfFilter()
        {
            // Update one server row outside filter
            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                var textUpdateOneRow = "Update ServiceTickets set Opened=getdate() where CustomerID=10";

                using (var sqlCmd = new SqlCommand(textUpdateOneRow, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            var serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
            var cli1Provider = new SqlSyncProvider(fixture.Client2ConnectionString);
            var cli2Provider = new SqlSyncProvider(fixture.Client3ConnectionString);

            ServiceConfiguration configuration = new ServiceConfiguration(new[] { "ServiceTickets" });

            // Add a filter
            configuration.Filters.Add("ServiceTickets", "CustomerID");

            // first agent with parameter for filter
            SyncAgent agent1 = new SyncAgent(cli1Provider, serverProvider, configuration);
            agent1.Parameters.Add("ServiceTickets", "CustomerID", 1);

            // second agent with no parameter
            SyncAgent agent2 = new SyncAgent(cli2Provider, serverProvider, configuration);


            var session1 = await agent1.SynchronizeAsync();
            var session2 = await agent2.SynchronizeAsync();

            Assert.Equal(0, session1.TotalChangesDownloaded);
            Assert.Equal(1, session2.TotalChangesDownloaded);


        }
        [Fact, TestPriority(4)]
        public async Task UpdateOneRowInFilter()
        {
            // Update one server row outside filter
            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                var textUpdateOneRow = "Update ServiceTickets set Opened=getdate() where CustomerID=1";

                using (var sqlCmd = new SqlCommand(textUpdateOneRow, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            var serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
            var cli1Provider = new SqlSyncProvider(fixture.Client2ConnectionString);
            var cli2Provider = new SqlSyncProvider(fixture.Client3ConnectionString);

            ServiceConfiguration configuration = new ServiceConfiguration(new[] { "ServiceTickets" });

            // Add a filter
            configuration.Filters.Add("ServiceTickets", "CustomerID");

            // first agent with parameter for filter
            SyncAgent agent1 = new SyncAgent(cli1Provider, serverProvider, configuration);
            agent1.Parameters.Add("ServiceTickets", "CustomerID", 1);

            // second agent with no parameter
            SyncAgent agent2 = new SyncAgent(cli2Provider, serverProvider, configuration);


            var session1 = await agent1.SynchronizeAsync();
            var session2 = await agent2.SynchronizeAsync();

            Assert.Equal(4, session1.TotalChangesDownloaded);
            Assert.Equal(4, session2.TotalChangesDownloaded);


        }
        [Fact, TestPriority(5)]
        public async Task DeleteOneRowOutofFilter()
        {
            // Update one server row outside filter
            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                var textUpdateOneRow = "Delete ServiceTickets Where CustomerID=10";

                using (var sqlCmd = new SqlCommand(textUpdateOneRow, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            var serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
            var cli1Provider = new SqlSyncProvider(fixture.Client2ConnectionString);
            var cli2Provider = new SqlSyncProvider(fixture.Client3ConnectionString);

            ServiceConfiguration configuration = new ServiceConfiguration(new[] { "ServiceTickets" });

            // Add a filter
            configuration.Filters.Add("ServiceTickets", "CustomerID");

            // first agent with parameter for filter
            SyncAgent agent1 = new SyncAgent(cli1Provider, serverProvider, configuration);
            agent1.Parameters.Add("ServiceTickets", "CustomerID", 1);

            // second agent with no parameter
            SyncAgent agent2 = new SyncAgent(cli2Provider, serverProvider, configuration);


            var session1 = await agent1.SynchronizeAsync();
            var session2 = await agent2.SynchronizeAsync();

            Assert.Equal(0, session1.TotalChangesDownloaded);
            Assert.Equal(1, session2.TotalChangesDownloaded);


        }
        [Fact, TestPriority(6)]
        public async Task DeleteOneRowInFilter()
        {
            // Update one server row outside filter
            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                var textUpdateOneRow = "Delete ServiceTickets Where CustomerID=1";

                using (var sqlCmd = new SqlCommand(textUpdateOneRow, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            var serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
            var cli1Provider = new SqlSyncProvider(fixture.Client2ConnectionString);
            var cli2Provider = new SqlSyncProvider(fixture.Client3ConnectionString);

            ServiceConfiguration configuration = new ServiceConfiguration(new[] { "ServiceTickets" });

            // Add a filter
            configuration.Filters.Add("ServiceTickets", "CustomerID");

            // first agent with parameter for filter
            SyncAgent agent1 = new SyncAgent(cli1Provider, serverProvider, configuration);
            agent1.Parameters.Add("ServiceTickets", "CustomerID", 1);

            // second agent with no parameter
            SyncAgent agent2 = new SyncAgent(cli2Provider, serverProvider, configuration);


            var session1 = await agent1.SynchronizeAsync();
            var session2 = await agent2.SynchronizeAsync();

            Assert.Equal(4, session1.TotalChangesDownloaded);
            Assert.Equal(4, session2.TotalChangesDownloaded);


        }
    }
}
