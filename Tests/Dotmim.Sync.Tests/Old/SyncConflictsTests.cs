//using Dotmim.Sync.Enumerations;
//using Dotmim.Sync.SqlServer;
//using Dotmim.Sync.Tests.Misc;
//using System;
//using System.Data.SqlClient;
//using System.Threading.Tasks;
//using Xunit;

//namespace Dotmim.Sync.Tests.Old.SqlServer
//{
//    public class SyncConflictFixture : IDisposable
//    {
//        private string createTableScript =
//        $@"if (not exists (select * from sys.tables where name = 'ServiceTickets'))
//            begin
//                CREATE TABLE [ServiceTickets](
//	            [ServiceTicketID] [uniqueidentifier] NOT NULL,
//	            [UniqueID] [int] NOT NULL,
//	            [Title] [nvarchar](max) NOT NULL,
//	            [Description] [nvarchar](max) NULL,
//                CONSTRAINT [PK_ServiceTickets] PRIMARY KEY CLUSTERED ([ServiceTicketID] ASC ),
//                CONSTRAINT [IX_ServiceTickets] UNIQUE NONCLUSTERED ([UniqueID] ASC ));
//            end";

//        private string datas =
//        $@"
//            INSERT [ServiceTickets] ([ServiceTicketID], [UniqueID], [Title], [Description]) VALUES (newid(), 1, N'Titre 1', N'Description 1')
//            INSERT [ServiceTickets] ([ServiceTicketID], [UniqueID], [Title], [Description]) VALUES (newid(), 2, N'Titre 2', N'Description 2')
//          ";

        
//        private string serverDbName = "Test_Conflict_Server";
//        private string client1DbName = "Test_Conflict_Client";
//        //private string client1SqliteName = "Test_Conflict_Client";

//        public string[] Tables => new string[] { "ServiceTickets" };

//        public String ServerConnectionString => HelperDB.GetSqlDatabaseConnectionString(serverDbName);
//        public String Client1ConnectionString => HelperDB.GetSqlDatabaseConnectionString(client1DbName);

//        public SyncConflictFixture()
//        {
//            // create databases
//            HelperDB.CreateDatabase(serverDbName);
//            HelperDB.CreateDatabase(client1DbName);

//            //var builder = new SqliteConnectionStringBuilder
//            //{
//            //    DataSource = ClientSqliteFilePath
//            //};
//            //this.ClientSqliteConnectionString = builder.ConnectionString;

//            //if (File.Exists(ClientSqliteFilePath))
//            //    File.Delete(ClientSqliteFilePath);

//            // create table on server
//            HelperDB.ExecuteSqlScript(serverDbName, createTableScript);
//            // create table on client
//            HelperDB.ExecuteSqlScript(client1DbName, createTableScript);

//            // insert table
//            HelperDB.ExecuteSqlScript(serverDbName, datas);
//        }
//        public void Dispose()
//        {
//            HelperDB.DropSqlDatabase(serverDbName);
//            HelperDB.DropSqlDatabase(client1DbName);

//            //GC.Collect();
//            //GC.WaitForPendingFinalizers();

//            //if (File.Exists(ClientSqliteFilePath))
//            //    File.Delete(ClientSqliteFilePath);

//        }

//    }


//    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]

//    public class SyncConflictsTests : IClassFixture<SyncConflictFixture>
//    {
//        SqlSyncProvider serverProvider;
//        SqlSyncProvider clientProvider;
//        //SqliteSyncProvider sqliteSyncProvider;
//        SyncConflictFixture fixture;
//        SyncAgent agent;

//        public SyncConflictsTests(SyncConflictFixture fixture)
//        {
//            this.fixture = fixture;

//            serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
//            clientProvider = new SqlSyncProvider(fixture.Client1ConnectionString);
//            //sqliteSyncProvider = new SqliteSyncProvider(fixture.ClientSqliteFilePath);

//            agent = new SyncAgent(clientProvider, serverProvider, fixture.Tables);

//        }

//        [Fact, TestPriority(0)]
//        public async Task Initialize()
//        {
//            var session = await agent.SynchronizeAsync();

//            Assert.Equal(2, session.TotalChangesDownloaded);
//            Assert.Equal(0, session.TotalChangesUploaded);
//        }


//        [Fact, TestPriority(1)]
//        public async Task ConflictOnUniqueConstraint()
//        {
//            SyncContext session = null;

//            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
//            {
//                var script = $@"INSERT INTO [ServiceTickets] 
//                            ([ServiceTicketID], [UniqueID], [Title], [Description]) 
//                            VALUES 
//                            (newid(), 99, N'Title client 99', N'Description client 99')";

//                using (var sqlCmd = new SqlCommand(script, sqlConnection))
//                {
//                    sqlConnection.Open();
//                    sqlCmd.ExecuteNonQuery();
//                    sqlConnection.Close();
//                }
//            }

//            //using (var sqliteConnection = new SqliteConnection(fixture.ClientSqliteConnectionString))
//            //{
//            //    var newId = Guid.NewGuid();
//            //    var script = $@"INSERT INTO [ServiceTickets] 
//            //                ([ServiceTicketID], [UniqueID], [Title], [Description]) 
//            //                VALUES 
//            //                (@newId, 99, 'Title client 99', 'Description client 99')";

//            //    using (var sqlCmd = new SqliteCommand(script, sqliteConnection))
//            //    {
//            //        SqliteParameter p = new SqliteParameter("@newId", SqliteType.Blob);
//            //        p.Value = newId;
//            //        sqlCmd.Parameters.Add(p);
//            //        sqliteConnection.Open();
//            //        sqlCmd.ExecuteNonQuery();
//            //        sqliteConnection.Close();
//            //    }
//            //}

//            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
//            {
//                var script = $@"INSERT INTO [ServiceTickets] 
//                            ([ServiceTicketID], [UniqueID], [Title], [Description]) 
//                            VALUES 
//                            (newid(), 99, N'Title server 99', N'Description server 99')";

//                using (var sqlCmd = new SqlCommand(script, sqlConnection))
//                {
//                    sqlConnection.Open();
//                    sqlCmd.ExecuteNonQuery();
//                    sqlConnection.Close();
//                }
//            }

//            // Since the line will be in error
//            // The only way to make it working as expected
//            // is to disabled the bulk mode
//            agent.Configuration.UseBulkOperations = false;

//            agent.ApplyChangedFailed += (s, args) =>
//            {
//                args.Action = ConflictAction.MergeRow;
//                args.FinalRow["UniqueID"] = 101;

//            };

//            await Assert.RaisesAsync<ApplyChangeFailedEventArgs>(
//                h => agent.ApplyChangedFailed += h,
//                h => agent.ApplyChangedFailed -= h, async () =>
//                {
//                    session = await agent.SynchronizeAsync();
//                });

//            // check statistics
//            Assert.Equal(2, session.TotalChangesDownloaded);
//            Assert.Equal(1, session.TotalChangesUploaded);
//            Assert.Equal(0, session.TotalSyncConflicts);


//        }
//    }
//}
