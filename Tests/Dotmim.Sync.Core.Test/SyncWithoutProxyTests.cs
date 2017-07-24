using Dotmim.Sync.Core.Test.Misc;
using Dotmim.Sync.Core.Test.SqlUtils;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
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
        public async Task Enable_Sync()
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
        public async Task No_Rows()
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
        public async Task One_Row_From_Server()
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
        public async Task One_Row_From_Client()
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

        [Fact, TestPriority(3)]
        public async Task Conflict_Insert_Insert_ServerWins()
        {

            Guid id = Guid.NewGuid();

            using (var sqlConnection = new SqlConnection(fixture.ClientDatabaseString))
            {
                var script = $@"INSERT [ServiceTickets] 
                            ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                            VALUES 
                            (N'{id.ToString()}', N'Conflict Line Client', N'Description client', 1, 0, getdate(), NULL, 1)";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            using (var sqlConnection = new SqlConnection(fixture.ServerDatabaseString))
            {
                var script = $@"INSERT [ServiceTickets] 
                            ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                            VALUES 
                            (N'{id.ToString()}', N'Conflict Line Server', N'Description client', 1, 0, getdate(), NULL, 1)";

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

            // action on conflict : Server wins
            agent.ApplyChangedFailed += (s, args) =>
            {
                args.Action = Enumerations.ApplyAction.Continue;
            };

            SyncContext session = null;

            // launch sync and check if event is raised
            await Assert.RaisesAsync<ApplyChangeFailedEventArgs>(
                h => agent.ApplyChangedFailed += h,
                h => agent.ApplyChangedFailed -= h, async () =>
                {
                    session = await agent.SynchronizeAsync();
                });

            // check statistics
            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
            Assert.Equal(1, session.TotalSyncConflicts);

            string expectedRes = string.Empty;
            using (var sqlConnection = new SqlConnection(fixture.ClientDatabaseString))
            {
                var script = $@"Select Title from [ServiceTickets] Where ServiceTicketID='{id.ToString()}'";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    expectedRes = sqlCmd.ExecuteScalar() as string;
                    sqlConnection.Close();
                }
            }

            // check good title on client
            Assert.Equal("Conflict Line Server", expectedRes);
        }

        [Fact, TestPriority(3)]
        public async Task Conflict_Update_Update_ServerWins()
        {

            using (var sqlConnection = new SqlConnection(fixture.ClientDatabaseString))
            {
                var script = $@"Update [ServiceTickets] 
                                Set Title = 'Updated from Client'
                                Where ServiceTicketId = '2CA388CF-8D9A-4B7A-9BD1-B0BC6617A766'";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            using (var sqlConnection = new SqlConnection(fixture.ServerDatabaseString))
            {
                var script = $@"Update [ServiceTickets] 
                                Set Title = 'Updated from Server'
                                Where ServiceTicketId = '2CA388CF-8D9A-4B7A-9BD1-B0BC6617A766'";

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

            SyncContext session = null;

            // launch sync and check if event is raised
            await Assert.RaisesAsync<ApplyChangeFailedEventArgs>(
                h => agent.ApplyChangedFailed += h,
                h => agent.ApplyChangedFailed -= h, async () =>
                {
                    session = await agent.SynchronizeAsync();
                });

            // check statistics
            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
            Assert.Equal(1, session.TotalSyncConflicts);

            string expectedRes = string.Empty;
            using (var sqlConnection = new SqlConnection(fixture.ClientDatabaseString))
            {
                var script = $@"Select Title from [ServiceTickets] Where ServiceTicketID='2CA388CF-8D9A-4B7A-9BD1-B0BC6617A766'";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    expectedRes = sqlCmd.ExecuteScalar() as string;
                    sqlConnection.Close();
                }
            }

            // check good title on client
            Assert.Equal("Updated from Server", expectedRes);
        }

        [Fact, TestPriority(3)]
        public async Task Conflict_Update_Update_ClientWins()
        {

            using (var sqlConnection = new SqlConnection(fixture.ClientDatabaseString))
            {
                var script = $@"Update [ServiceTickets] 
                                Set Title = 'Updated from Client'
                                Where ServiceTicketId = '2CA388CF-8D9A-4B7A-9BD1-B0BC6617A766'";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            using (var sqlConnection = new SqlConnection(fixture.ServerDatabaseString))
            {
                var script = $@"Update [ServiceTickets] 
                                Set Title = 'Updated from Server'
                                Where ServiceTicketId = '2CA388CF-8D9A-4B7A-9BD1-B0BC6617A766'";

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
            configuration.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);

            SyncContext session = null;

            // launch sync and check if event is raised
            await Assert.RaisesAsync<ApplyChangeFailedEventArgs>(
                h => agent.ApplyChangedFailed += h,
                h => agent.ApplyChangedFailed -= h, async () =>
                {
                    session = await agent.SynchronizeAsync();
                });

            // check statistics
            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
            Assert.Equal(1, session.TotalSyncConflicts);

            string expectedRes = string.Empty;
            using (var sqlConnection = new SqlConnection(fixture.ClientDatabaseString))
            {
                var script = $@"Select Title from [ServiceTickets] Where ServiceTicketID='2CA388CF-8D9A-4B7A-9BD1-B0BC6617A766'";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    expectedRes = sqlCmd.ExecuteScalar() as string;
                    sqlConnection.Close();
                }
            }

            // check good title on client
            Assert.Equal("Updated from Client", expectedRes);

            using (var sqlConnection = new SqlConnection(fixture.ServerDatabaseString))
            {
                var script = $@"Select Title from [ServiceTickets] Where ServiceTicketID='2CA388CF-8D9A-4B7A-9BD1-B0BC6617A766'";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    expectedRes = sqlCmd.ExecuteScalar() as string;
                    sqlConnection.Close();
                }
            }

            // check good title on client
            Assert.Equal("Updated from Client", expectedRes);

        }

        [Fact, TestPriority(3)]
        public async Task Conflict_Insert_Insert_HandleApplyChangedFailed_ClientWins()
        {
            Guid id = Guid.NewGuid();

            using (var sqlConnection = new SqlConnection(fixture.ClientDatabaseString))
            {
                var script = $@"INSERT [ServiceTickets] 
                            ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                            VALUES 
                            (N'{id.ToString()}', N'Conflict Line Client', N'Description client', 1, 0, getdate(), NULL, 1)";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            using (var sqlConnection = new SqlConnection(fixture.ServerDatabaseString))
            {
                var script = $@"INSERT [ServiceTickets] 
                            ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                            VALUES 
                            (N'{id.ToString()}', N'Conflict Line Server', N'Description client', 1, 0, getdate(), NULL, 1)";

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

            // action on conflict : Client wins
            agent.ApplyChangedFailed += (s, args) =>
            {
                args.Action = Enumerations.ApplyAction.RetryWithForceWrite;
            };

            SyncContext session = null;

            // launch sync and check if event is raised
            await Assert.RaisesAsync<ApplyChangeFailedEventArgs>(
                h => agent.ApplyChangedFailed += h,
                h => agent.ApplyChangedFailed -= h, async () =>
                {
                    session = await agent.SynchronizeAsync();
                });

            // check statistics
            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
            Assert.Equal(1, session.TotalSyncConflicts);

            string expectedRes = string.Empty;
            using (var sqlConnection = new SqlConnection(fixture.ServerDatabaseString))
            {
                var script = $@"Select Title from [ServiceTickets] Where ServiceTicketID='{id.ToString()}'";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    expectedRes = sqlCmd.ExecuteScalar() as string;
                    sqlConnection.Close();
                }
            }

            // check good title on client
            Assert.Equal("Conflict Line Client", expectedRes);
        }

        [Fact, TestPriority(3)]
        public async Task Conflict_Insert_Insert_OverrideConfiguration_ClientWins()
        {

            Guid id = Guid.NewGuid();

            using (var sqlConnection = new SqlConnection(fixture.ClientDatabaseString))
            {
                var script = $@"INSERT [ServiceTickets] 
                            ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                            VALUES 
                            (N'{id.ToString()}', N'Conflict Line Client', N'Description client', 1, 0, getdate(), NULL, 1)";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            using (var sqlConnection = new SqlConnection(fixture.ServerDatabaseString))
            {
                var script = $@"INSERT [ServiceTickets] 
                            ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                            VALUES 
                            (N'{id.ToString()}', N'Conflict Line Server', N'Description client', 1, 0, getdate(), NULL, 1)";

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
            configuration.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);

            // action on conflict : Client wins
            agent.ApplyChangedFailed += (s, args) =>
            {
                args.Action = Enumerations.ApplyAction.RetryWithForceWrite;
            };

            SyncContext session = null;

            // launch sync and check if event is raised
            await Assert.RaisesAsync<ApplyChangeFailedEventArgs>(
                h => agent.ApplyChangedFailed += h,
                h => agent.ApplyChangedFailed -= h, async () =>
                {
                    session = await agent.SynchronizeAsync();
                });

            // check statistics
            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
            Assert.Equal(1, session.TotalSyncConflicts);

            string expectedRes = string.Empty;
            using (var sqlConnection = new SqlConnection(fixture.ServerDatabaseString))
            {
                var script = $@"Select Title from [ServiceTickets] Where ServiceTicketID='{id.ToString()}'";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    expectedRes = sqlCmd.ExecuteScalar() as string;
                    sqlConnection.Close();
                }
            }

            // check good title on client
            Assert.Equal("Conflict Line Client", expectedRes);
        }

        [Fact, TestPriority(2)]
        public async Task Bad_Server_Connection()
        {
            SqlSyncProvider serverProvider = new SqlSyncProvider(@"Data Source=(localdb)\MSSQLLocalDB; Initial Catalog=WrongDB; Integrated Security=true;");
            SqlSyncProvider clientProvider = new SqlSyncProvider(fixture.ClientDatabaseString);

            ServiceConfiguration configuration = new ServiceConfiguration(new string[] { "ServiceTickets" });

            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);

            var session = await agent.SynchronizeAsync();

            Assert.NotNull(session.Error);
        }


    }
}
