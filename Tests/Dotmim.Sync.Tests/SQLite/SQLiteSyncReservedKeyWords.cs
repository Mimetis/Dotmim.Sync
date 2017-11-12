using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Test.SqlUtils;
using System;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Xunit;
using System.IO;
using System.Data.SQLite;
using Dotmim.Sync.SQLite;

namespace Dotmim.Sync.Tests
{

    public class SQLiteSyncReservedKeyWordsFixture : IDisposable
    {

        private string createTableScript =
        $@"if (not exists (select * from sys.tables where name = 'Sql'))
            begin
                CREATE TABLE [Sql](
	            [SqlId]     [uniqueidentifier] NOT NULL,
	            [File]      [nvarchar](max) NOT NULL,
	            [Read]      [nvarchar](max) NULL,
	            [From]      [nvarchar](max) NULL,
	            [To]        [nvarchar](max) NULL,
	            [Select]    [nvarchar](max) NULL,
	            [Array]     [nvarchar](max) NULL,
                [String]    [nvarchar](max) NULL,
                CONSTRAINT [PK_Sql] PRIMARY KEY CLUSTERED ( [SqlId] ASC ));
            end";

        private string datas =
        $@"
            INSERT [Sql] ([SqlId], [File], [Read], [From], [To], 
                    [Select], [Array], [String]) 
            VALUES (newid(), N'File Info', N'Read on!', 'spertus@microsoft.com', 'an.to@free.fr', 
                    'Select', 'Array', 'String')";

        private HelperDB helperDb = new HelperDB();
        private string serverDbName = "Test_SQLITE_ReservedKeyWord_Server";

        public string[] Tables => new string[] { "Sql" };

        public String ServerConnectionString => HelperDB.GetDatabaseConnectionString(serverDbName);
        public String ClientSQLiteConnectionString { get; set; }
        public string ClientSQLiteFilePath => Path.Combine(Directory.GetCurrentDirectory(), "sqliteReservedKeyWords.db");

        public SQLiteSyncReservedKeyWordsFixture()
        {
            var builder = new SQLiteConnectionStringBuilder { DataSource = ClientSQLiteFilePath };
            this.ClientSQLiteConnectionString = builder.ConnectionString;

            if (File.Exists(ClientSQLiteFilePath))
                File.Delete(ClientSQLiteFilePath);

            // create databases
            helperDb.CreateDatabase(serverDbName);
            // create table
            helperDb.ExecuteScript(serverDbName, createTableScript);

            // insert table
            helperDb.ExecuteScript(serverDbName, datas);
        }
        public void Dispose()
        {
            helperDb.DeleteDatabase(serverDbName);

            GC.Collect();
            GC.WaitForPendingFinalizers();

            if (File.Exists(ClientSQLiteFilePath))
                File.Delete(ClientSQLiteFilePath);
        }

    }


    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    public class SQLiteSyncReservedKeyWordsTests : IClassFixture<SQLiteSyncReservedKeyWordsFixture>
    {
        SqlSyncProvider serverProvider;
        SQLiteSyncProvider clientProvider;
        SQLiteSyncReservedKeyWordsFixture fixture;
        SyncAgent agent;

        public SQLiteSyncReservedKeyWordsTests(SQLiteSyncReservedKeyWordsFixture fixture)
        {
            this.fixture = fixture;

            serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
            clientProvider = new SQLiteSyncProvider(fixture.ClientSQLiteFilePath);
            var simpleConfiguration = new SyncConfiguration(fixture.Tables);

            agent = new SyncAgent(clientProvider, serverProvider, simpleConfiguration);
        }

        [Fact, TestPriority(0)]
        public async Task Initialize()
        {
            var session = await agent.SynchronizeAsync();

            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(2)]
        public async Task SyncNoRows(SyncConfiguration conf)
        {
            agent.Configuration = conf;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(3)]
        public async Task InsertFromServer(SyncConfiguration conf)
        {
            var insertRowScript =
            $@"
                INSERT [Sql] ([SqlId], [File], [Read], [From], [To], 
                        [Select], [Array], [String]) 
                VALUES (newid(), N'File Info', N'Read on!', 'spertus@microsoft.com', 'an.to@free.fr', 
                        'Select', 'Array', 'String')";

            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                using (var sqlCmd = new SqlCommand(insertRowScript, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            agent.Configuration = conf;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(4)]
        public async Task InsertFromClient(SyncConfiguration conf)
        {
            Guid newId = Guid.NewGuid();

            var insertRowScript =
            $@"
                INSERT INTO [Sql] ([SqlId], [File], [Read], [From], [To], 
                        [Select], [Array], [String]) 
                VALUES ('{newId.ToString()}', 'File Info', 'Read on!', 'spertus@microsoft.com', 'an.to@free.fr', 
                        'Select', 'Array', 'String')";

            using (var sqlConnection = new SQLiteConnection(fixture.ClientSQLiteConnectionString))
            {
                using (var sqlCmd = new SQLiteCommand(insertRowScript, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            agent.Configuration = conf;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
        }

  
        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(6)]
        public async Task UpdateFromServer(SyncConfiguration conf)
        {
            string title = $"Update from server at {DateTime.Now.Ticks.ToString()}";
            var updateRowScript =
                $@" Declare @id uniqueidentifier;
                    Select top 1 @id = SqlId from [Sql];
                    Update [Sql] Set [File] = '{title}' Where SqlId = @id";

            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                using (var sqlCmd = new SqlCommand(updateRowScript, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            agent.Configuration = conf;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }


    }


}
