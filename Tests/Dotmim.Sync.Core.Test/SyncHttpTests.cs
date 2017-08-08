using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Proxy;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Test.Misc;
using Dotmim.Sync.Test.SqlUtils;
using Microsoft.AspNetCore.Http;
using System;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Test
{

    [Collection("SyncSimple")]
    [TestCaseOrderer("Dotmim.Sync.Test.Misc.PriorityOrderer", "Dotmim.Sync.Core.Test")]
    public class SyncHttpTests : IClassFixture<SyncSimpleFixture>
    {
        SqlSyncProvider serverProvider;
        SqlSyncProvider clientProvider;
        WebProxyServerProvider proxyServerProvider;
        WebProxyClientProvider proxyClientProvider;
        SyncConfiguration configuration;

        SyncSimpleFixture fixture;
        SyncAgent agent;

        public SyncHttpTests(SyncSimpleFixture fixture)
        {
            this.fixture = fixture;

            serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
            proxyServerProvider = new WebProxyServerProvider(serverProvider);

            clientProvider = new SqlSyncProvider(fixture.Client1ConnectionString);
            proxyClientProvider = new WebProxyClientProvider();

            configuration = new SyncConfiguration(this.fixture.Tables);

            agent = new SyncAgent(clientProvider, proxyClientProvider);

        }

        [Fact, TestPriority(1)]
        public async Task Initialize()
        {
            using (var server = new KestrellTestServer())
            {
                var serverHandler = new RequestDelegate(async context =>
                {
                    serverProvider.SetConfiguration(configuration);
                    proxyServerProvider.SerializationFormat = SerializationFormat.Json;

                    await proxyServerProvider.HandleRequestAsync(context);
                });
                var clientHandler = new ResponseDelegate(async (serviceUri) =>
                {
                    proxyClientProvider.ServiceUri = new Uri(serviceUri);
                    proxyClientProvider.SerializationFormat = SerializationFormat.Json;

                    var session = await agent.SynchronizeAsync();

                    Assert.Equal(50, session.TotalChangesDownloaded);
                    Assert.Equal(0, session.TotalChangesUploaded);
                });
                await server.Run(serverHandler, clientHandler);
            }
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(2)]
        public async Task SyncNoRows(SyncConfiguration conf)
        {
            using (var server = new KestrellTestServer())
            {
                var serverHandler = new RequestDelegate(async context =>
                {
                    conf.Tables = fixture.Tables;
                    serverProvider.SetConfiguration(conf);
                    proxyServerProvider.SerializationFormat = conf.SerializationFormat;

                    await proxyServerProvider.HandleRequestAsync(context);
                });
                var clientHandler = new ResponseDelegate(async (serviceUri) =>
                {
                    proxyClientProvider.ServiceUri = new Uri(serviceUri);
                    proxyClientProvider.SerializationFormat = conf.SerializationFormat;

                    var session = await agent.SynchronizeAsync();

                    Assert.Equal(0, session.TotalChangesDownloaded);
                    Assert.Equal(0, session.TotalChangesUploaded);
                });
                await server.Run(serverHandler, clientHandler);
            }

        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(3)]
        public async Task InsertFromServer(SyncConfiguration conf)
        {
            var insertRowScript =
            $@"INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                VALUES (newid(), N'Insert One Row', N'Description Insert One Row', 1, 0, getdate(), NULL, 1)";

            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                using (var sqlCmd = new SqlCommand(insertRowScript, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            using (var server = new KestrellTestServer())
            {
                var serverHandler = new RequestDelegate(async context =>
                {
                    conf.Tables = fixture.Tables;
                    serverProvider.SetConfiguration(conf);
                    proxyServerProvider.SerializationFormat = conf.SerializationFormat;

                    await proxyServerProvider.HandleRequestAsync(context);
                });
                var clientHandler = new ResponseDelegate(async (serviceUri) =>
                {
                    proxyClientProvider.ServiceUri = new Uri(serviceUri);
                    proxyClientProvider.SerializationFormat = conf.SerializationFormat;

                    var session = await agent.SynchronizeAsync();

                    Assert.Equal(1, session.TotalChangesDownloaded);
                    Assert.Equal(0, session.TotalChangesUploaded);
                });
                await server.Run(serverHandler, clientHandler);
            }
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(4)]
        public async Task InsertFromClient(SyncConfiguration conf)
        {
            var insertRowScript =
            $@"INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                VALUES (newid(), N'Insert One Row', N'Description Insert One Row', 1, 0, getdate(), NULL, 1)";

            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
            {
                using (var sqlCmd = new SqlCommand(insertRowScript, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            using (var server = new KestrellTestServer())
            {
                var serverHandler = new RequestDelegate(async context =>
                {
                    conf.Tables = fixture.Tables;
                    serverProvider.SetConfiguration(conf);
                    proxyServerProvider.SerializationFormat = conf.SerializationFormat;

                    await proxyServerProvider.HandleRequestAsync(context);
                });
                var clientHandler = new ResponseDelegate(async (serviceUri) =>
                {
                    proxyClientProvider.ServiceUri = new Uri(serviceUri);
                    proxyClientProvider.SerializationFormat = conf.SerializationFormat;

                    var session = await agent.SynchronizeAsync();

                    Assert.Equal(0, session.TotalChangesDownloaded);
                    Assert.Equal(1, session.TotalChangesUploaded);
                });
                await server.Run(serverHandler, clientHandler);
            }
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(5)]
        public async Task UpdateFromClient(SyncConfiguration conf)
        {
            var updateRowScript =
            $@" Declare @id uniqueidentifier;
                Select top 1 @id = ServiceTicketID from ServiceTickets;
                Update [ServiceTickets] Set [Title] = 'Updated !' Where ServiceTicketId = @id";

            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
            {
                using (var sqlCmd = new SqlCommand(updateRowScript, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            using (var server = new KestrellTestServer())
            {
                var serverHandler = new RequestDelegate(async context =>
                {
                    conf.Tables = fixture.Tables;
                    serverProvider.SetConfiguration(conf);
                    proxyServerProvider.SerializationFormat = conf.SerializationFormat;

                    await proxyServerProvider.HandleRequestAsync(context);
                });
                var clientHandler = new ResponseDelegate(async (serviceUri) =>
                {
                    proxyClientProvider.ServiceUri = new Uri(serviceUri);
                    proxyClientProvider.SerializationFormat = conf.SerializationFormat;

                    var session = await agent.SynchronizeAsync();

                    Assert.Equal(0, session.TotalChangesDownloaded);
                    Assert.Equal(1, session.TotalChangesUploaded);
                });
                await server.Run(serverHandler, clientHandler);
            }
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(6)]
        public async Task UpdateFromServer(SyncConfiguration conf)
        {
            var updateRowScript =
            $@" Declare @id uniqueidentifier;
                Select top 1 @id = ServiceTicketID from ServiceTickets;
                Update [ServiceTickets] Set [Title] = 'Updated from server' Where ServiceTicketId = @id";

            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                using (var sqlCmd = new SqlCommand(updateRowScript, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            using (var server = new KestrellTestServer())
            {
                var serverHandler = new RequestDelegate(async context =>
                {
                    conf.Tables = fixture.Tables;
                    serverProvider.SetConfiguration(conf);
                    proxyServerProvider.SerializationFormat = conf.SerializationFormat;

                    await proxyServerProvider.HandleRequestAsync(context);
                });
                var clientHandler = new ResponseDelegate(async (serviceUri) =>
                {
                    proxyClientProvider.ServiceUri = new Uri(serviceUri);
                    proxyClientProvider.SerializationFormat = conf.SerializationFormat;

                    var session = await agent.SynchronizeAsync();

                    Assert.Equal(1, session.TotalChangesDownloaded);
                    Assert.Equal(0, session.TotalChangesUploaded);
                });
                await server.Run(serverHandler, clientHandler);
            }
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(7)]
        public async Task DeleteFromServer(SyncConfiguration conf)
        {
            var updateRowScript =
            $@" Declare @id uniqueidentifier;
                Select top 1 @id = ServiceTicketID from ServiceTickets;
                Delete From [ServiceTickets] Where ServiceTicketId = @id";

            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                using (var sqlCmd = new SqlCommand(updateRowScript, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            using (var server = new KestrellTestServer())
            {
                var serverHandler = new RequestDelegate(async context =>
                {
                    conf.Tables = fixture.Tables;
                    serverProvider.SetConfiguration(conf);
                    proxyServerProvider.SerializationFormat = conf.SerializationFormat;

                    await proxyServerProvider.HandleRequestAsync(context);
                });
                var clientHandler = new ResponseDelegate(async (serviceUri) =>
                {
                    proxyClientProvider.ServiceUri = new Uri(serviceUri);
                    proxyClientProvider.SerializationFormat = conf.SerializationFormat;

                    var session = await agent.SynchronizeAsync();

                    Assert.Equal(1, session.TotalChangesDownloaded);
                    Assert.Equal(0, session.TotalChangesUploaded);
                });
                await server.Run(serverHandler, clientHandler);
            }
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(8)]
        public async Task DeleteFromClient(SyncConfiguration conf)
        {
            int count;
            var selectcount = $@"Select count(*) From [ServiceTickets]";
            var updateRowScript = $@"Delete From [ServiceTickets]";

            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCmd = new SqlCommand(selectcount, sqlConnection))
                    count = (int)sqlCmd.ExecuteScalar();
                using (var sqlCmd = new SqlCommand(updateRowScript, sqlConnection))
                    sqlCmd.ExecuteNonQuery();
                sqlConnection.Close();
            }

            using (var server = new KestrellTestServer())
            {
                var serverHandler = new RequestDelegate(async context =>
                {
                    conf.Tables = fixture.Tables;
                    serverProvider.SetConfiguration(conf);
                    proxyServerProvider.SerializationFormat = conf.SerializationFormat;

                    await proxyServerProvider.HandleRequestAsync(context);
                });
                var clientHandler = new ResponseDelegate(async (serviceUri) =>
                {
                    proxyClientProvider.ServiceUri = new Uri(serviceUri);
                    proxyClientProvider.SerializationFormat = conf.SerializationFormat;

                    var session = await agent.SynchronizeAsync();

                    Assert.Equal(0, session.TotalChangesDownloaded);
                    Assert.Equal(count, session.TotalChangesUploaded);
                });
                await server.Run(serverHandler, clientHandler);
            }

            // check all rows deleted on server side
            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCmd = new SqlCommand(selectcount, sqlConnection))
                    count = (int)sqlCmd.ExecuteScalar();
            }
            Assert.Equal(0, count);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(9)]
        public async Task ConflictInsertInsertServerWins(SyncConfiguration conf)
        {
            Guid id = Guid.NewGuid();

            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
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

            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
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

            using (var server = new KestrellTestServer())
            {
                var serverHandler = new RequestDelegate(async context =>
                {
                    conf.Tables = fixture.Tables;
                    serverProvider.SetConfiguration(conf);
                    proxyServerProvider.SerializationFormat = conf.SerializationFormat;

                    await proxyServerProvider.HandleRequestAsync(context);
                });
                var clientHandler = new ResponseDelegate(async (serviceUri) =>
                {
                    proxyClientProvider.ServiceUri = new Uri(serviceUri);
                    proxyClientProvider.SerializationFormat = conf.SerializationFormat;

                    var session = await agent.SynchronizeAsync();

                    // check statistics
                    Assert.Equal(1, session.TotalChangesDownloaded);
                    Assert.Equal(1, session.TotalChangesUploaded);
                    Assert.Equal(1, session.TotalSyncConflicts);
                });
                await server.Run(serverHandler, clientHandler);
            }
            string expectedRes = string.Empty;
            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
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

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(10)]
        public async Task ConflictUpdateUpdateServerWins(SyncConfiguration conf)
        {
            var id = Guid.NewGuid().ToString();

            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
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

            using (var server = new KestrellTestServer())
            {
                var serverHandler = new RequestDelegate(async context =>
                {
                    conf.Tables = fixture.Tables;
                    serverProvider.SetConfiguration(conf);
                    proxyServerProvider.SerializationFormat = conf.SerializationFormat;

                    await proxyServerProvider.HandleRequestAsync(context);
                });
                var clientHandler = new ResponseDelegate(async (serviceUri) =>
                {
                    proxyClientProvider.ServiceUri = new Uri(serviceUri);
                    proxyClientProvider.SerializationFormat = conf.SerializationFormat;

                    var session = await agent.SynchronizeAsync();

                    // check statistics
                    Assert.Equal(0, session.TotalChangesDownloaded);
                    Assert.Equal(1, session.TotalChangesUploaded);
                    Assert.Equal(0, session.TotalSyncConflicts);
                });
                await server.Run(serverHandler, clientHandler);
            }


            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
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

            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
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

            using (var server = new KestrellTestServer())
            {
                var serverHandler = new RequestDelegate(async context =>
                {
                    conf.Tables = fixture.Tables;
                    serverProvider.SetConfiguration(conf);
                    proxyServerProvider.SerializationFormat = conf.SerializationFormat;

                    await proxyServerProvider.HandleRequestAsync(context);
                });
                var clientHandler = new ResponseDelegate(async (serviceUri) =>
                {
                    proxyClientProvider.ServiceUri = new Uri(serviceUri);
                    proxyClientProvider.SerializationFormat = conf.SerializationFormat;

                    var session = await agent.SynchronizeAsync();

                    // check statistics
                    Assert.Equal(1, session.TotalChangesDownloaded);
                    Assert.Equal(1, session.TotalChangesUploaded);
                    Assert.Equal(1, session.TotalSyncConflicts);
                });
                await server.Run(serverHandler, clientHandler);
            }

            string expectedRes = string.Empty;
            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
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

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(11)]
        public async Task ConflictUpdateUpdateClientWins(SyncConfiguration conf)
        {
            var id = Guid.NewGuid().ToString();

            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
            {
                var script = $@"INSERT [ServiceTickets] 
                            ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                            VALUES 
                            (N'{id}', N'Line for conflict', N'Description client', 1, 0, getdate(), NULL, 1)";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            using (var server = new KestrellTestServer())
            {
                var serverHandler = new RequestDelegate(async context =>
                {
                    conf.Tables = fixture.Tables;
                    serverProvider.SetConfiguration(conf);
                    proxyServerProvider.SerializationFormat = conf.SerializationFormat;

                    await proxyServerProvider.HandleRequestAsync(context);
                });
                var clientHandler = new ResponseDelegate(async (serviceUri) =>
                {
                    proxyClientProvider.ServiceUri = new Uri(serviceUri);
                    proxyClientProvider.SerializationFormat = conf.SerializationFormat;

                    var session = await agent.SynchronizeAsync();

                    // check statistics
                    Assert.Equal(0, session.TotalChangesDownloaded);
                    Assert.Equal(1, session.TotalChangesUploaded);
                    Assert.Equal(0, session.TotalSyncConflicts);
                });
                await server.Run(serverHandler, clientHandler);
            }


            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
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

            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
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


            using (var server = new KestrellTestServer())
            {
                var serverHandler = new RequestDelegate(async context =>
                {
                    conf.Tables = fixture.Tables;
                    serverProvider.SetConfiguration(conf);
                    proxyServerProvider.SerializationFormat = conf.SerializationFormat;

                    // Since we move to server side, it's server to handle errors
                    serverProvider.ApplyChangedFailed += (s, args) =>
                    {
                        args.Action = ApplyAction.RetryWithForceWrite;
                    };


                    await proxyServerProvider.HandleRequestAsync(context);
                });
                var clientHandler = new ResponseDelegate(async (serviceUri) =>
                {
                    proxyClientProvider.ServiceUri = new Uri(serviceUri);
                    proxyClientProvider.SerializationFormat = conf.SerializationFormat;

                    SyncContext session = null;
                    await Assert.RaisesAsync<ApplyChangeFailedEventArgs>(
                        h => serverProvider.ApplyChangedFailed += h,
                        h => serverProvider.ApplyChangedFailed -= h, async () =>
                        {
                            session = await agent.SynchronizeAsync();
                        });

                    // check statistics
                    Assert.Equal(0, session.TotalChangesDownloaded);
                    Assert.Equal(1, session.TotalChangesUploaded);
                    Assert.Equal(1, session.TotalSyncConflicts);
                });
                await server.Run(serverHandler, clientHandler);
            }

            string expectedRes = string.Empty;
            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
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

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(12)]
        public async Task ConflictInsertInsertConfigurationClientWins(SyncConfiguration conf)
        {

            Guid id = Guid.NewGuid();

            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
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

            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
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

            using (var server = new KestrellTestServer())
            {
                var serverHandler = new RequestDelegate(async context =>
                {
                    conf.Tables = fixture.Tables;
                    conf.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                    serverProvider.SetConfiguration(conf);
                    proxyServerProvider.SerializationFormat = conf.SerializationFormat;

                    await proxyServerProvider.HandleRequestAsync(context);
                });
                var clientHandler = new ResponseDelegate(async (serviceUri) =>
                {
                    proxyClientProvider.ServiceUri = new Uri(serviceUri);
                    proxyClientProvider.SerializationFormat = conf.SerializationFormat;

                    var session = await agent.SynchronizeAsync();

                    // check statistics
                    Assert.Equal(0, session.TotalChangesDownloaded);
                    Assert.Equal(1, session.TotalChangesUploaded);
                    Assert.Equal(1, session.TotalSyncConflicts);
                });
                await server.Run(serverHandler, clientHandler);
            }


            string expectedRes = string.Empty;
            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
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
