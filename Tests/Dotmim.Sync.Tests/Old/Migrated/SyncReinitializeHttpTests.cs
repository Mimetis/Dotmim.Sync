//using Dotmim.Sync.Enumerations;
//using Dotmim.Sync.SqlServer;
//using Dotmim.Sync.Tests.Core;
//using Dotmim.Sync.Tests.Misc;
//using Dotmim.Sync.Web.Client;
//using Dotmim.Sync.Web.Server;
//using Microsoft.AspNetCore.Http;
//using System;
//using System.Data.SqlClient;
//using System.IO;
//using System.Threading.Tasks;
//using Xunit;

//namespace Dotmim.Sync.Tests.Old.SqlServer
//{
//    public class SyncReinitializeTestsHttpFixture : IDisposable
//    {
//        public string serverDbName => "Test_ReinitHttp_Server";
//        public string client1DbName => "Test_ReinitHttp_Client";
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

//        public SyncReinitializeTestsHttpFixture()
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
//    public class SyncReinitializeTestsHttp : IClassFixture<SyncReinitializeTestsHttpFixture>
//    {
//        SyncReinitializeTestsHttpFixture fixture;
//        SqlSyncProvider serverProvider;
//        SqlSyncProvider clientProvider;
//        WebProxyServerProvider proxyServerProvider;
//        WebProxyClientProvider proxyClientProvider;
//        SyncConfiguration configuration;

//        SyncAgent agent;

//        public SyncReinitializeTestsHttp(SyncReinitializeTestsHttpFixture fixture)
//        {
//            this.fixture = fixture;

//            serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
//            proxyServerProvider = new WebProxyServerProvider(serverProvider);

//            clientProvider = new SqlSyncProvider(fixture.Client1ConnectionString);
//            proxyClientProvider = new WebProxyClientProvider();

//            configuration = new SyncConfiguration(fixture.Tables);

//            agent = new SyncAgent(clientProvider, proxyClientProvider);
//        }

//        [Fact, TestPriority(1)]
//        public async Task Initialize()
//        {
//            var serverHandler = new RequestDelegate(async context =>
//            {
//                //configuration.AddTable(fixture.Tables);
//                proxyServerProvider.Configuration = configuration;
                

//                await proxyServerProvider.HandleRequestAsync(context);
//            });

//            using (var server = new KestrellTestServer())
//            {
//                var clientHandler = new ResponseDelegate(async (serviceUri) =>
//                {
//                    proxyClientProvider.ServiceUri = new Uri(serviceUri);

//                    var syncAgent = new SyncAgent(clientProvider, proxyClientProvider);
//                    var session = await syncAgent.SynchronizeAsync();

//                    Assert.Equal(7, session.TotalChangesDownloaded);
//                    Assert.Equal(0, session.TotalChangesUploaded);
//                });
//                await server.Run(serverHandler, clientHandler);
//            }

//            // check relation has been created on client :
//            int foreignKeysCount;
//            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
//            {
//                var script = $@"Select count(*) from sys.foreign_keys where name = 'FK_ServiceTickets_Customers'";

//                using (var sqlCmd = new SqlCommand(script, sqlConnection))
//                {
//                    sqlConnection.Open();
//                    foreignKeysCount = (int)sqlCmd.ExecuteScalar();
//                    sqlConnection.Close();
//                }
//            }
//            Assert.Equal(1, foreignKeysCount);
//        }

//        [Fact, TestPriority(2)]
//        public async Task SyncReinitialize()
//        {
//            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
//            {
//                var script = $@"UPDATE Customers SET LastName='DoeClient' WHERE CustomerID=1;";

//                using (var sqlCmd = new SqlCommand(script, sqlConnection))
//                {
//                    sqlConnection.Open();
//                    sqlCmd.ExecuteNonQuery();
//                    sqlConnection.Close();
//                }
//            }

//            var serverHandler = new RequestDelegate(async context =>
//            {
//                //configuration.AddTable(fixture.Tables);
//                proxyServerProvider.Configuration = configuration;
                

//                await proxyServerProvider.HandleRequestAsync(context);
//            });

//            using (var server = new KestrellTestServer())
//            {
//                var clientHandler = new ResponseDelegate(async (serviceUri) =>
//                {
//                    proxyClientProvider.ServiceUri = new Uri(serviceUri);

//                    var syncAgent = new SyncAgent(clientProvider, proxyClientProvider);
//                    var session = await syncAgent.SynchronizeAsync(SyncType.Reinitialize);

//                    Assert.Equal(7, session.TotalChangesDownloaded);
//                    Assert.Equal(0, session.TotalChangesUploaded);
//                });
//                await server.Run(serverHandler, clientHandler);
//            }


//            string lastName = null;
//            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
//            {
//                var script = $@"SELECT LastName FROM Customers WHERE CustomerID=1;";

//                using (var sqlCmd = new SqlCommand(script, sqlConnection))
//                {
//                    sqlConnection.Open();
//                    lastName = (string)sqlCmd.ExecuteScalar();
//                    sqlConnection.Close();
//                }
//            }

//            Assert.Equal("Doe", lastName);


//        }


//        [Fact, TestPriority(3)]
//        public async Task SyncReinitializeWithUpload()
//        {
//            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
//            {
//                var script = $@"UPDATE Customers SET LastName='DoeClient' WHERE CustomerID=1;";

//                using (var sqlCmd = new SqlCommand(script, sqlConnection))
//                {
//                    sqlConnection.Open();
//                    sqlCmd.ExecuteNonQuery();
//                    sqlConnection.Close();
//                }
//            }
//            var serverHandler = new RequestDelegate(async context =>
//            {
//                //configuration.AddTable(fixture.Tables);
//                proxyServerProvider.Configuration = configuration;
                

//                await proxyServerProvider.HandleRequestAsync(context);
//            });

//            using (var server = new KestrellTestServer())
//            {
//                var clientHandler = new ResponseDelegate(async (serviceUri) =>
//                {
//                    proxyClientProvider.ServiceUri = new Uri(serviceUri);

//                    var syncAgent = new SyncAgent(clientProvider, proxyClientProvider);
//                    var session = await syncAgent.SynchronizeAsync(SyncType.ReinitializeWithUpload);

//                    Assert.Equal(7, session.TotalChangesDownloaded);
//                    Assert.Equal(1, session.TotalChangesUploaded);
//                });
//                await server.Run(serverHandler, clientHandler);
//            }

//            string lastName = null;
//            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
//            {
//                var script = $@"SELECT LastName FROM Customers WHERE CustomerID=1;";

//                using (var sqlCmd = new SqlCommand(script, sqlConnection))
//                {
//                    sqlConnection.Open();
//                    lastName = (string)sqlCmd.ExecuteScalar();
//                    sqlConnection.Close();
//                }
//            }

//            Assert.Equal("DoeClient", lastName);


//        }
//    }
//}
