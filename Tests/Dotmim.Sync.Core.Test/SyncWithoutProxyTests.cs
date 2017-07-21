using Dotmim.Sync.Core.Test.Misc;
using Dotmim.Sync.Core.Test.SqlUtils;
using Dotmim.Sync.SqlServer;
using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Core.Test
{
    [TestCaseOrderer("Dotmim.Sync.Core.Test.Misc.PriorityOrderer", "Dotmim.Sync.Core.Test")]
    public class SyncWithoutProxyTests : IClassFixture<CreateServerAndClientDatabase>
    {
        CreateServerAndClientDatabase fixture;

        public SyncWithoutProxyTests(CreateServerAndClientDatabase fixture)
        {
            this.fixture = fixture;
        }

        [Fact, TestPriority(1)]
        public async Task Sync_Create_Tables_And_Sync_One_Table()
        {
            SqlSyncProvider serverProvider = new SqlSyncProvider(fixture.ServerDatabaseString);
            SqlSyncProvider clientProvider = new SqlSyncProvider(fixture.ClientDatabaseString);

            ServiceConfiguration configuration = new ServiceConfiguration(new string[] { "ServiceTickets" });

            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);

            var session = await agent.SynchronizeAsync();

            Assert.Equal(10, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);

        }

        [Fact, TestPriority(2)]
        public async Task Sync_No_Rows()
        {

            SqlSyncProvider serverProvider = new SqlSyncProvider(fixture.ServerDatabaseString);
            SqlSyncProvider clientProvider = new SqlSyncProvider(fixture.ClientDatabaseString);

            ServiceConfiguration configuration = new ServiceConfiguration(new string[] { "ServiceTickets" });

            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);

            var session = await agent.SynchronizeAsync();

            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }

        [Fact, TestPriority(2)]
        public async Task Sync_One_Row_From_Server()
        {

            using (var sqlConnection = new SqlConnection(fixture.ServerDatabaseString))
            {
                var script = @"INSERT [ServiceTickets] 
                            ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                            VALUES 
                            (N'6124B1E8-6617-4B20-B73A-921618F79FA6', N'Insert line into server', N'Description server', 1, 0, getdate(), NULL, 1)";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }

            }


            SqlSyncProvider serverProvider = new SqlSyncProvider(fixture.ServerDatabaseString);
            SqlSyncProvider clientProvider = new SqlSyncProvider(fixture.ClientDatabaseString);

            ServiceConfiguration configuration = new ServiceConfiguration(new string[] { "ServiceTickets" });

            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);

            var session = await agent.SynchronizeAsync();

            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }

        [Fact, TestPriority(3)]
        public async Task Sync_One_Row_From_Client()
        {

            using (var sqlConnection = new SqlConnection(fixture.ClientDatabaseString))
            {
                var script = @"INSERT [ServiceTickets] 
                            ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                            VALUES 
                            (N'CF3096E5-9057-4B29-8DBB-5D9D996643BD', N'Insert line into client', N'Description client', 1, 0, getdate(), NULL, 1)";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }

            }

            SqlSyncProvider serverProvider = new SqlSyncProvider(fixture.ServerDatabaseString);
            SqlSyncProvider clientProvider = new SqlSyncProvider(fixture.ClientDatabaseString);

            ServiceConfiguration configuration = new ServiceConfiguration(new string[] { "ServiceTickets" });

            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);

            var session = await agent.SynchronizeAsync();

            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
        }



        [Fact, TestPriority(2)]
        public async Task Sync_Bad_Server_Connection()
        {
            var ex = await Assert.ThrowsAsync(typeof(Exception), async () =>
            {

                SqlSyncProvider serverProvider = new SqlSyncProvider(@"Data Source=(localdb)\MSSQLLocalDB; Initial Catalog=WrongDB; Integrated Security=true;");
                SqlSyncProvider clientProvider = new SqlSyncProvider(fixture.ClientDatabaseString);

                ServiceConfiguration configuration = new ServiceConfiguration(new string[] { "ServiceTickets" });

                SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);

                var session = await agent.SynchronizeAsync();

            });
        }


    }
}
