//using Dotmim.Sync.Sqlite;
//using Dotmim.Sync.SqlServer;
//using Dotmim.Sync.Tests.Misc;
//using Microsoft.Data.Sqlite;
//using System;
//using System.Data;
//using System.Data.Common;
//using System.Data.SqlClient;
//using System.Diagnostics;
//using System.IO;
//using System.Threading.Tasks;
//using Xunit;

//namespace Dotmim.Sync.Tests.Old.SqlServer
//{


//    public class SyncWithClientInitialDatasTestsFixture : IDisposable
//    {
//        private string createTableScriptServer =
//        $@"if (not exists (select * from sys.tables where name = 'ServiceTickets'))
//            begin
//                CREATE TABLE [ServiceTickets](
//	            [ServiceTicketID] [uniqueidentifier] NOT NULL,
//	            [Title] [nvarchar](max) NOT NULL,
//	            [StatusValue] [int] NOT NULL,
//	            [Opened] [datetime] NULL,
//                CONSTRAINT [PK_ServiceTickets] PRIMARY KEY CLUSTERED ( [ServiceTicketID] ASC ));
//            end";

//        private string createTableScriptClient =
//        $@"CREATE TABLE IF NOT EXISTS [ServiceTickets](
//	            [ServiceTicketID] blob NOT NULL,
//	            [Title] text NOT NULL,
//	            [StatusValue] integer NOT NULL,
//	            [Opened] datetime NULL,
//                PRIMARY KEY ( [ServiceTicketID] ASC ))";

        
//        private string serverDbName = "Test_InitData_Server";

//        public string[] Tables => new string[] { "ServiceTickets" };

//        public String ServerConnectionString => HelperDB.GetSqlDatabaseConnectionString(serverDbName);
//        public SyncAgent Agent { get; set; }

//        public String ClientSqliteConnectionString { get; set; }
//        public string ClientSqliteFilePath => 
//            Path.Combine(Directory.GetCurrentDirectory(), "sqliteInitDataTmpDb.db");

//        public SyncWithClientInitialDatasTestsFixture()
//        {
//            var builder = new SqliteConnectionStringBuilder
//            {
//                DataSource = ClientSqliteFilePath
//            };
//            this.ClientSqliteConnectionString = builder.ConnectionString;

//            if (File.Exists(ClientSqliteFilePath))
//                File.Delete(ClientSqliteFilePath);

//            // create databases
//            HelperDB.CreateDatabase(serverDbName);

//            // create table
//            HelperDB.ExecuteSqlScript(serverDbName, createTableScriptServer);
//            HelperDB.ExecuteSqliteScript(this.ClientSqliteConnectionString, createTableScriptClient);

//            // insert table
//            CreateTableAndDatas(new SqlConnection(HelperDB.GetSqlDatabaseConnectionString(serverDbName)));
//            CreateTableAndDatas(new SqliteConnection(this.ClientSqliteConnectionString));

//        }

//        public void CreateTableAndDatas(DbConnection connection)
//        {

//            var command = connection.CreateCommand();
//            command.CommandText = $@"INSERT INTO [ServiceTickets] ([ServiceTicketID], [Title], [StatusValue], [Opened]) 
//                                     VALUES (@ServiceTicketID, @Title, @StatusValue, @Opened)";

//            DbParameter parameter = null;
//            parameter = command.CreateParameter();
//            parameter.DbType = DbType.Guid;
//            parameter.ParameterName = "@ServiceTicketID";
//            parameter.Value = Guid.NewGuid();
//            command.Parameters.Add(parameter);

//            parameter = command.CreateParameter();
//            parameter.DbType = DbType.String;
//            parameter.ParameterName = "@Title";
//            parameter.Value = $"Title - {Guid.NewGuid().ToString()}";
//            command.Parameters.Add(parameter);

//            parameter = command.CreateParameter();
//            parameter.DbType = DbType.Int32;
//            parameter.ParameterName = "@StatusValue";
//            parameter.Value = new Random().Next(0, 10);
//            command.Parameters.Add(parameter);

//            parameter = command.CreateParameter();
//            parameter.DbType = DbType.DateTime;
//            parameter.ParameterName = "@Opened";
//            parameter.Value = DateTime.Now;
//            command.Parameters.Add(parameter);

//            try
//            {
//                connection.Open();
//                command.ExecuteNonQuery();
//                connection.Close();
//            }
//            catch (Exception ex)
//            {
//                Debug.WriteLine(ex.Message);
//                throw;
//            }
//            finally
//            {
//                if (connection.State != ConnectionState.Closed)
//                    connection.Close();
//            }

//        }

//        public void Dispose()
//        {
//            HelperDB.DropSqlDatabase(serverDbName);

//            GC.Collect();
//            GC.WaitForPendingFinalizers();

//            if (File.Exists(ClientSqliteFilePath))
//                File.Delete(ClientSqliteFilePath);
//        }

//    }


//    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
//    public class SyncWithClientInitialDatasTests : IClassFixture<SyncWithClientInitialDatasTestsFixture>
//    {
//        SqlSyncProvider serverProvider;
//        SqliteSyncProvider clientProvider;
//        SyncWithClientInitialDatasTestsFixture fixture;
//        SyncAgent agent;

//        public SyncWithClientInitialDatasTests(SyncWithClientInitialDatasTestsFixture fixture)
//        {
//            this.fixture = fixture;

//            serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
//            clientProvider = new SqliteSyncProvider(fixture.ClientSqliteFilePath);
//            var simpleConfiguration = new SyncConfiguration(fixture.Tables);

//            agent = new SyncAgent(clientProvider, serverProvider, fixture.Tables);
//        }

//        [Fact, TestPriority(0)]
//        public async Task Initialize()
//        {
//            serverProvider.SyncProgress += (s, e) => Debug.WriteLine($"[Server]:{e.Message} {e.PropertiesMessage}");
//            clientProvider.SyncProgress += (s, e) => Debug.WriteLine($"[Client]:{e.Message} {e.PropertiesMessage}");

//            var session = await agent.SynchronizeAsync();

//            Assert.Equal(1, session.TotalChangesDownloaded);
//            Assert.Equal(1, session.TotalChangesUploaded);
//        }

//        private void ServerProvider_SyncProgress(object sender, ProgressEventArgs e)
//        {
            
//        }
//    }
       
//}
