using Dotmim.Sync.Core.Enumerations;
using Dotmim.Sync.Core.Test.Misc;
using Dotmim.Sync.Core.Test.SqlUtils;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Core.Test
{
    [Collection("Sync")]
    public class SyncConflictsTests : IClassFixture<CreateServerAndClientDatabase>
    {
        CreateServerAndClientDatabase fixture;

        // making tests only on the simple database
        PairDatabases simpleDb;
        public SyncConflictsTests(CreateServerAndClientDatabase fixture)
        {
            this.fixture = fixture;

            simpleDb = fixture.PairDatabases.First(pd => pd.Key == "SimpleSync");

            fixture.GenerateDatabasesAndTables(simpleDb, false, false, false);
        }

        private async Task<SyncContext> LaunchSync(EventHandler<ApplyChangeFailedEventArgs> eventHandler = null)
        {
            SqlSyncProvider serverProvider = new SqlSyncProvider(simpleDb.ServerConnectionString);
            SqlSyncProvider clientProvider = new SqlSyncProvider(simpleDb.ClientConnectionString);

            ServiceConfiguration configuration = new ServiceConfiguration(simpleDb.Tables.ToArray());

            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);

            // action on conflict
            if (eventHandler != null)
            {
                agent.ApplyChangedFailed += (s, args) =>
                {
                    eventHandler(s, args);
                };
            }

            SyncContext session = null;

            // launch sync and check if event is raised
            if (eventHandler != null)
            {
                await Assert.RaisesAsync<ApplyChangeFailedEventArgs>(
                    h => agent.ApplyChangedFailed += h,
                    h => agent.ApplyChangedFailed -= h, async () =>
                    {
                        session = await agent.SynchronizeAsync();
                    });
            }
            else
            {
                session = await agent.SynchronizeAsync();
            }


            return session;
        }


        [Fact]
        public async Task Conflict_Insert_Insert_ServerWins()
        {
            Guid id = Guid.NewGuid();

            using (var sqlConnection = new SqlConnection(simpleDb.ClientConnectionString))
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

            using (var sqlConnection = new SqlConnection(simpleDb.ServerConnectionString))
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

            // default action : Server wins
            var session = await LaunchSync();

            // check statistics
            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
            Assert.Equal(1, session.TotalSyncConflicts);

            string expectedRes = string.Empty;
            using (var sqlConnection = new SqlConnection(simpleDb.ClientConnectionString))
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

        [Fact]
        public async Task Conflict_Update_Update_ServerWins()
        {
            var id = Guid.NewGuid().ToString();

            using (var sqlConnection = new SqlConnection(simpleDb.ClientConnectionString))
            {
                var script = $@"INSERT [ServiceTickets] 
                            ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                            VALUES 
                            (N'{id}', N'Line Client', N'Description client', 1, 0, getdate(), NULL, 1)";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            var s = await LaunchSync();

            //just check, even if it's not the real test :)
            // check statistics
            Assert.Equal(0, s.TotalChangesDownloaded);
            Assert.Equal(1, s.TotalChangesUploaded);
            Assert.Equal(0, s.TotalSyncConflicts);


            using (var sqlConnection = new SqlConnection(simpleDb.ClientConnectionString))
            {
                var script = $@"Update [ServiceTickets] 
                                Set Title = 'Updated from Client'
                                Where ServiceTicketId = '{id}'";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            using (var sqlConnection = new SqlConnection(simpleDb.ServerConnectionString))
            {
                var script = $@"Update [ServiceTickets] 
                                Set Title = 'Updated from Server'
                                Where ServiceTicketId = '{id}'";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            var session = await LaunchSync();

            // check statistics
            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
            Assert.Equal(1, session.TotalSyncConflicts);

            string expectedRes = string.Empty;
            using (var sqlConnection = new SqlConnection(simpleDb.ClientConnectionString))
            {
                var script = $@"Select Title from [ServiceTickets] Where ServiceTicketID='{id}'";

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



        [Fact]
        public async Task Conflict_Update_Update_ClientWins()
        {
            var id = Guid.NewGuid().ToString();

            using (var sqlConnection = new SqlConnection(simpleDb.ClientConnectionString))
            {
                var script = $@"INSERT [ServiceTickets] 
                            ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                            VALUES 
                            (N'{id}', N'Line Client', N'Description client', 1, 0, getdate(), NULL, 1)";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            var s = await LaunchSync();

            //just check, even if it's not the real test :)
            // check statistics
            Assert.Equal(0, s.TotalChangesDownloaded);
            Assert.Equal(1, s.TotalChangesUploaded);
            Assert.Equal(0, s.TotalSyncConflicts);


            using (var sqlConnection = new SqlConnection(simpleDb.ClientConnectionString))
            {
                var script = $@"Update [ServiceTickets] 
                                Set Title = 'Updated from Client'
                                Where ServiceTicketId = '{id}'";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            using (var sqlConnection = new SqlConnection(simpleDb.ServerConnectionString))
            {
                var script = $@"Update [ServiceTickets] 
                                Set Title = 'Updated from Server'
                                Where ServiceTicketId = '{id}'";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            var session = await LaunchSync((ss, args) => args.Action = ApplyAction.RetryWithForceWrite);

            // check statistics
            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
            Assert.Equal(1, session.TotalSyncConflicts);

            string expectedRes = string.Empty;
            using (var sqlConnection = new SqlConnection(simpleDb.ServerConnectionString))
            {
                var script = $@"Select Title from [ServiceTickets] Where ServiceTicketID='{id}'";

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


        [Fact]
        public async Task Conflict_Insert_Insert_OverrideConfiguration_ClientWins()
        {

            Guid id = Guid.NewGuid();

            using (var sqlConnection = new SqlConnection(simpleDb.ClientConnectionString))
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

            using (var sqlConnection = new SqlConnection(simpleDb.ServerConnectionString))
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

            SqlSyncProvider serverProvider = new SqlSyncProvider(simpleDb.ServerConnectionString);
            SqlSyncProvider clientProvider = new SqlSyncProvider(simpleDb.ClientConnectionString);

            ServiceConfiguration configuration = new ServiceConfiguration(simpleDb.Tables.ToArray());
            configuration.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);


            SyncContext session = null;

            session = await agent.SynchronizeAsync();

            // check statistics
            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
            Assert.Equal(1, session.TotalSyncConflicts);

            string expectedRes = string.Empty;
            using (var sqlConnection = new SqlConnection(simpleDb.ServerConnectionString))
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
    }
}
