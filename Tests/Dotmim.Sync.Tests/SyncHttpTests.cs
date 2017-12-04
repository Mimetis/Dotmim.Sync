using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Web;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Test.SqlUtils;
using Microsoft.AspNetCore.Http;
using System;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Test
{

    public class SyncSimpleHttpFixture : IDisposable
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
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
          ";

        private HelperDB helperDb = new HelperDB();
        private string serverDbName = "Test_SimpleHttp_Server";
        private string client1DbName = "Test_SimpleHttp_Client";
        private string client2DbName = "Test_SimpleHttp_Client2";

        public string[] Tables => new string[] { "ServiceTickets" };

        public String ServerConnectionString => HelperDB.GetDatabaseConnectionString(serverDbName);
        public String Client1ConnectionString => HelperDB.GetDatabaseConnectionString(client1DbName);
        public String Client2ConnectionString => HelperDB.GetDatabaseConnectionString(client2DbName);

        public SyncSimpleHttpFixture()
        {
            // create databases
            helperDb.CreateDatabase(serverDbName);
            helperDb.CreateDatabase(client1DbName);
            helperDb.CreateDatabase(client2DbName);

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
        }

    }


    [Collection("Http")]
    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    public class SyncHttpTests : IClassFixture<SyncSimpleHttpFixture>
    {
        SqlSyncProvider serverProvider;
        SqlSyncProvider client1Provider;
        SqlSyncProvider client2Provider;
        WebProxyServerProvider proxyServerProvider;
        WebProxyClientProvider proxyClientProvider;
        SyncConfiguration configuration;

        SyncSimpleHttpFixture fixture;
        SyncAgent agent;

        public SyncHttpTests(SyncSimpleHttpFixture fixture)
        {
            this.fixture = fixture;

            serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
            proxyServerProvider = new WebProxyServerProvider(serverProvider);

            client1Provider = new SqlSyncProvider(fixture.Client1ConnectionString);
            client2Provider = new SqlSyncProvider(fixture.Client2ConnectionString);
            proxyClientProvider = new WebProxyClientProvider();

            configuration = new SyncConfiguration(this.fixture.Tables);

            agent = new SyncAgent(client1Provider, proxyClientProvider);

        }

        [Fact, TestPriority(1)]
        public async Task Initialize()
        {
            var serverHandler = new RequestDelegate(async context =>
            {
                //configuration.AddTable(fixture.Tables);
                serverProvider.SetConfiguration(configuration);
                proxyServerProvider.SerializationFormat = SerializationFormat.Json;

                await proxyServerProvider.HandleRequestAsync(context);
            });
            using (var server = new KestrellTestServer())
            {
                var clientHandler = new ResponseDelegate(async (serviceUri) =>
                {
                    proxyClientProvider.ServiceUri = new Uri(serviceUri);
                    proxyClientProvider.SerializationFormat = SerializationFormat.Json;

                    var syncAgent = new SyncAgent(client1Provider, proxyClientProvider);
                    var session = await syncAgent.SynchronizeAsync();

                    Assert.Equal(50, session.TotalChangesDownloaded);
                    Assert.Equal(0, session.TotalChangesUploaded);
                });
                await server.Run(serverHandler, clientHandler);
            }
            using (var server = new KestrellTestServer())
            {
                var clientHandler = new ResponseDelegate(async (serviceUri) =>
                {
                    proxyClientProvider.ServiceUri = new Uri(serviceUri);
                    proxyClientProvider.SerializationFormat = SerializationFormat.Json;

                    var syncAgent = new SyncAgent(client2Provider, proxyClientProvider);
                    var session = await syncAgent.SynchronizeAsync();

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
                    conf.Add(fixture.Tables);
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
                    conf.Add(fixture.Tables);
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
                    conf.Add(fixture.Tables);
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
            string title = $"Update from client at {DateTime.Now.Ticks.ToString()}";

            var updateRowScript =
            $@" Declare @id uniqueidentifier;
                Select top 1 @id = ServiceTicketID from ServiceTickets;
                Update [ServiceTickets] Set [Title] = '{title}' Where ServiceTicketId = @id";

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
                    conf.Add(fixture.Tables);
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
            string title = $"Update from server at {DateTime.Now.Ticks.ToString()}";

            var updateRowScript =
            $@" Declare @id uniqueidentifier;
                Select top 1 @id = ServiceTicketID from ServiceTickets;
                Update [ServiceTickets] Set [Title] = '{title}' Where ServiceTicketId = @id";

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
                    conf.Add(fixture.Tables);
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
                    conf.Add(fixture.Tables);
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
                    conf.Add(fixture.Tables);
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
                    conf.Add(fixture.Tables);
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
                    conf.Add(fixture.Tables);
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
                    conf.Add(fixture.Tables);
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
                    conf.Add(fixture.Tables);
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
                    conf.Add(fixture.Tables);
                    serverProvider.SetConfiguration(conf);
                    proxyServerProvider.SerializationFormat = conf.SerializationFormat;

                    // Since we move to server side, it's server to handle errors
                    serverProvider.ApplyChangedFailed += (s, args) =>
                    {
                        args.Action = ConflictAction.ClientWins;
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
                    conf.Add(fixture.Tables);
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

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(13)]
        public async Task ConflictUpdateUpdateDoubleBase(SyncConfiguration conf)
        {
            var serverHandler = new RequestDelegate(async context =>
            {
                conf.Add(fixture.Tables);
                serverProvider.SetConfiguration(conf);
                proxyServerProvider.SerializationFormat = conf.SerializationFormat;

                await proxyServerProvider.HandleRequestAsync(context);
            });

            // Just be sure the bases are synced correctly
            using (var server = new KestrellTestServer())
            {
                var client1Handler = new ResponseDelegate(async (serviceUri) =>
                {
                    proxyClientProvider.ServiceUri = new Uri(serviceUri);
                    proxyClientProvider.SerializationFormat = conf.SerializationFormat;

                    var agentSync = new SyncAgent(client1Provider, proxyClientProvider);

                    var session = await agentSync.SynchronizeAsync();
                });
                await server.Run(serverHandler, client1Handler);
            }

            using (var server = new KestrellTestServer())
            {
                var client2Handler = new ResponseDelegate(async (serviceUri) =>
                {
                    proxyClientProvider.ServiceUri = new Uri(serviceUri);
                    proxyClientProvider.SerializationFormat = conf.SerializationFormat;

                    var agentSync = new SyncAgent(client2Provider, proxyClientProvider);

                    var session = await agentSync.SynchronizeAsync();
                });
                await server.Run(serverHandler, client2Handler);
            }


            var id = Guid.NewGuid().ToString();

            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                var script = $@"INSERT [ServiceTickets] 
                            ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                            VALUES 
                            (N'{id}', N'Line Client', N'Description Server', 1, 0, getdate(), NULL, 1)";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            using (var server = new KestrellTestServer())
            {
                var client1Handler = new ResponseDelegate(async (serviceUri) =>
                {
                    proxyClientProvider.ServiceUri = new Uri(serviceUri);
                    proxyClientProvider.SerializationFormat = conf.SerializationFormat;

                    var agentSync = new SyncAgent(client1Provider, proxyClientProvider);

                    var session = await agentSync.SynchronizeAsync();

                    // check statistics
                    Assert.Equal(1, session.TotalChangesDownloaded);
                    Assert.Equal(0, session.TotalChangesUploaded);
                    Assert.Equal(0, session.TotalSyncConflicts);
                });
                await server.Run(serverHandler, client1Handler);
            }

            using (var server = new KestrellTestServer())
            {
                var client2Handler = new ResponseDelegate(async (serviceUri) =>
                {
                    proxyClientProvider.ServiceUri = new Uri(serviceUri);
                    proxyClientProvider.SerializationFormat = conf.SerializationFormat;

                    var agentSync = new SyncAgent(client2Provider, proxyClientProvider);

                    var session = await agentSync.SynchronizeAsync();

                    // check statistics
                    Assert.Equal(1, session.TotalChangesDownloaded);
                    Assert.Equal(0, session.TotalChangesUploaded);
                    Assert.Equal(0, session.TotalSyncConflicts);
                });
                await server.Run(serverHandler, client2Handler);
            }

            // Generating double conflicts
            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
            {
                var script = $@"Update [ServiceTickets] 
                                Set Title = 'Updated from Client 1'
                                Where ServiceTicketId = '{id}'";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            using (var sqlConnection = new SqlConnection(fixture.Client2ConnectionString))
            {
                var script = $@"Update [ServiceTickets] 
                                Set Title = 'Updated from Client 2'
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
                var client1Handler = new ResponseDelegate(async (serviceUri) =>
                {
                    proxyClientProvider.ServiceUri = new Uri(serviceUri);
                    proxyClientProvider.SerializationFormat = conf.SerializationFormat;

                    var agentSync = new SyncAgent(client1Provider, proxyClientProvider);

                    var session = await agentSync.SynchronizeAsync();

                    // check statistics
                    Assert.Equal(0, session.TotalChangesDownloaded);
                    Assert.Equal(1, session.TotalChangesUploaded);
                    Assert.Equal(0, session.TotalSyncConflicts);
                });
                await server.Run(serverHandler, client1Handler);
            }
            using (var server = new KestrellTestServer())
            {
                var client2Handler = new ResponseDelegate(async (serviceUri) =>
                {
                    proxyClientProvider.ServiceUri = new Uri(serviceUri);
                    proxyClientProvider.SerializationFormat = conf.SerializationFormat;

                    var agentSync = new SyncAgent(client2Provider, proxyClientProvider);

                    var session = await agentSync.SynchronizeAsync();

                    // check statistics
                    Assert.Equal(1, session.TotalChangesDownloaded);
                    Assert.Equal(1, session.TotalChangesUploaded);
                    Assert.Equal(1, session.TotalSyncConflicts);
                });
                await server.Run(serverHandler, client2Handler);
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
            Assert.Equal("Updated from Client 1", expectedRes);

            expectedRes = string.Empty;
            using (var sqlConnection = new SqlConnection(fixture.Client2ConnectionString))
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
            Assert.Equal("Updated from Client 1", expectedRes);
        }
    }
}
