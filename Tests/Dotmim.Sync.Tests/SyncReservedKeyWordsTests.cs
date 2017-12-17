using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Test.SqlUtils;
using System;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Tests
{

    public class SyncReservedKeyWordsFixture : IDisposable
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
            end
            if (not exists (select * from sys.tables where name = 'Log'))
            begin
            CREATE TABLE [dbo].[Log](
                [Oid] [uniqueidentifier] ROWGUIDCOL NOT NULL,
                [TimeStamp] [datetime] NULL,
                [Operation] nvarchar(50) NULL,
                [ErrorDescription] nvarchar(50) NULL,
                [OptimisticLockField] [int] NULL,
                [GCRecord] [int] NULL,
                CONSTRAINT [PK_Log] PRIMARY KEY CLUSTERED ( [Oid] ASC ))
            end";

        private string datas =
        $@"
            INSERT INTO [Sql] ([SqlId], [File], [Read], [From], [To], 
                    [Select], [Array], [String]) 
            VALUES (newid(), N'File Info', N'Read on!', 'spertus@microsoft.com', 'an.to@free.fr', 
                    'Select', 'Array', 'String');
            INSERT INTO [Log] ([Oid], [TimeStamp], [Operation], [ErrorDescription], [OptimisticLockField], [GCRecord] )
            VALUES (newid(), getdate(), 'Operation', 'Error', 1, 1);";

        private HelperDB helperDb = new HelperDB();
        private string serverDbName = "Test_ReservedKeyWord_Server";
        private string client1DbName = "Test_ReservedKeyWord_Client";

        public string[] Tables => new string[] { "Sql", };

        public String ServerConnectionString => HelperDB.GetDatabaseConnectionString(serverDbName);
        public String Client1ConnectionString => HelperDB.GetDatabaseConnectionString(client1DbName);

        public SyncReservedKeyWordsFixture()
        {
            // create databases
            helperDb.CreateDatabase(serverDbName);
            helperDb.CreateDatabase(client1DbName);

            // create table
            helperDb.ExecuteScript(serverDbName, createTableScript);

            // insert table
            helperDb.ExecuteScript(serverDbName, datas);
        }
        public void Dispose()
        {
            helperDb.DeleteDatabase(serverDbName);
            helperDb.DeleteDatabase(client1DbName);
        }

    }


    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    public class SyncReservedKeyWordsTests : IClassFixture<SyncReservedKeyWordsFixture>
    {
        SqlSyncProvider serverProvider;
        SqlSyncProvider clientProvider;
        SyncReservedKeyWordsFixture fixture;
        SyncAgent agent;

        public SyncReservedKeyWordsTests(SyncReservedKeyWordsFixture fixture)
        {
            this.fixture = fixture;

            serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
            clientProvider = new SqlSyncProvider(fixture.Client1ConnectionString);
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

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(1)]
        public async Task SyncNoRows(SyncConfiguration conf)
        {
            agent.Configuration = conf;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(2)]
        public async Task InsertFromServer(SyncConfiguration conf)
        {
            var insertRowScript =
            $@"
                INSERT [Sql] ([SqlId], [File], [Read], [From], [To], 
                        [Select], [Array], [String]) 
                VALUES (newid(), N'File Info', N'Read on!', 'spertus@microsoft.com', 'an.to@free.fr', 
                        'Select', 'Array', 'String');
                INSERT INTO [Log] ([Oid], [TimeStamp], [Operation], [ErrorDescription], [OptimisticLockField], [GCRecord] )
                VALUES (newid(), getdate(), 'Operation', 'Error', 1, 1);";

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

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(3)]
        public async Task InsertFromClient(SyncConfiguration conf)
        {
            var insertRowScript =
            $@"
                INSERT [Sql] ([SqlId], [File], [Read], [From], [To], 
                        [Select], [Array], [String]) 
                VALUES (newid(), N'File Info', N'Read on!', 'spertus@microsoft.com', 'an.to@free.fr', 
                        'Select', 'Array', 'String');";

            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
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

            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(4)]
        public async Task UpdateFromClient(SyncConfiguration conf)
        {
            string title = $"Update from client at {DateTime.Now.Ticks.ToString()}";

            var updateRowScript =
            $@" Declare @id uniqueidentifier;
                Select top 1 @id = SqlId from [Sql];
                Update [Sql] Set [File] = '{title}' Where SqlId = @id";

            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
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

            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(5)]
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

        [Fact, TestPriority(6)]
        public async Task ExceptionOnReservedKeysWords()
        {
            serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
            clientProvider = new SqlSyncProvider(fixture.Client1ConnectionString);
            agent = new SyncAgent(clientProvider, serverProvider, new String[] { "Log" });

            var ex = await Assert.ThrowsAsync<SyncException>(async () => await agent.SynchronizeAsync());

            Assert.Equal(SyncExceptionType.NotSupported, ex.ExceptionType);
        }
    }


}
