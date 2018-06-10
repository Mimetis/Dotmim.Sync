using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Test.SqlUtils;
using System;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Xunit;
using System.IO;
using Microsoft.Data.Sqlite;
using Dotmim.Sync.Sqlite;

namespace Dotmim.Sync.Tests
{

    public class SqliteSyncReservedKeyWordsFixture : IDisposable
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
        public String ClientSqliteConnectionString { get; set; }
        public string ClientSqliteFilePath => Path.Combine(Directory.GetCurrentDirectory(), "sqliteReservedKeyWords.db");

        public SqliteSyncReservedKeyWordsFixture()
        {
            var builder = new SqliteConnectionStringBuilder { DataSource = ClientSqliteFilePath };
            this.ClientSqliteConnectionString = builder.ConnectionString;

            if (File.Exists(ClientSqliteFilePath))
                File.Delete(ClientSqliteFilePath);

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

            if (File.Exists(ClientSqliteFilePath))
                File.Delete(ClientSqliteFilePath);
        }

    }


    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    public class SqliteSyncReservedKeyWordsTests : IClassFixture<SqliteSyncReservedKeyWordsFixture>
    {
        SqlSyncProvider serverProvider;
        SqliteSyncProvider clientProvider;
        SqliteSyncReservedKeyWordsFixture fixture;
        SyncAgent agent;

        public SqliteSyncReservedKeyWordsTests(SqliteSyncReservedKeyWordsFixture fixture)
        {
            this.fixture = fixture;

            serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
            clientProvider = new SqliteSyncProvider(fixture.ClientSqliteFilePath);

            agent = new SyncAgent(clientProvider, serverProvider, fixture.Tables);
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
            agent.Configuration.DownloadBatchSizeInKB = conf.DownloadBatchSizeInKB;
            agent.Configuration.UseBulkOperations = conf.UseBulkOperations;
            agent.Configuration.SerializationFormat = conf.SerializationFormat;
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
            agent.Configuration.DownloadBatchSizeInKB = conf.DownloadBatchSizeInKB;
            agent.Configuration.UseBulkOperations = conf.UseBulkOperations;
            agent.Configuration.SerializationFormat = conf.SerializationFormat;
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
                VALUES (@id, 'File Info', 'Read on!', 'spertus@microsoft.com', 'an.to@free.fr', 
                        'Select', 'Array', 'String')";

            using (var sqlConnection = new SqliteConnection(fixture.ClientSqliteConnectionString))
            {
                using (var sqlCmd = new SqliteCommand(insertRowScript, sqlConnection))
                {
                    sqlCmd.Parameters.AddWithValue("@id", newId);

                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            agent.Configuration.DownloadBatchSizeInKB = conf.DownloadBatchSizeInKB;
            agent.Configuration.UseBulkOperations = conf.UseBulkOperations;
            agent.Configuration.SerializationFormat = conf.SerializationFormat;
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
            agent.Configuration.DownloadBatchSizeInKB = conf.DownloadBatchSizeInKB;
            agent.Configuration.UseBulkOperations = conf.UseBulkOperations;
            agent.Configuration.SerializationFormat = conf.SerializationFormat;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }


    }


}
