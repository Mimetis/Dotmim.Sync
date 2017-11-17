using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Test.SqlUtils;
using Dotmim.Sync.SqlServer;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using System.Diagnostics;
using Dotmim.Sync.Data;
using System.IO;
using Microsoft.Data.Sqlite;
using Dotmim.Sync.Sqlite;
using System.ComponentModel;

namespace Dotmim.Sync.Test
{

    public class SqliteSyncAllColumnsFixture : IDisposable
    {
        private string createTableScript =
        $@"
        if (not exists (select * from sys.tables where name = 'AllColumns'))
        begin
            CREATE TABLE [dbo].[AllColumns](
	            [ClientID] [uniqueidentifier] NOT NULL,
	            [CBinary] [binary](50) NULL,
	            [CBigInt] [bigint] NULL,
	            [CBit] [bit] NULL,
	            [CChar10] [char](10) NULL,
	            [CDate] [date] NULL,
	            [CDateTime] [datetime] NULL,
	            [CDateTime2] [datetime2](7) NULL,
	            [CDateTimeOffset] [datetimeoffset](7) NULL,
	            [CDecimal64] [decimal](6, 4) NULL,
	            [CFloat] [float] NULL,
	            [CInt] [int] NULL,
	            [CMoney] [money] NULL,
	            [CNChar10] [nchar](10) NULL,
	            [CNumeric64] [numeric](6, 4) NULL,
	            [CNVarchar50] [nvarchar](50) NULL,
	            [CNVarcharMax] [nvarchar](max) NULL,
	            [CReal] [real] NULL,
	            [CSmallDateTime] [smalldatetime] NULL,
	            [CSmallInt] [smallint] NULL,
	            [CSmallMoney] [smallmoney] NULL,
	            [CSqlVariant] [sql_variant] NULL,
	            [CTime7] [time](7) NULL,
	            [CTimeStamp] [timestamp] NULL,
	            [CTinyint] [tinyint] NULL,
	            [CUniqueIdentifier] [uniqueidentifier] NULL,
	            [CVarbinary50] [varbinary](50) NULL,
	            [CVarbinaryMax] [varbinary](max) NULL,
	            [CVarchar50] [varchar](50) NULL,
	            [CVarcharMax] [varchar](max) NULL,
	            [CXml] [xml] NULL,
                CONSTRAINT [PK_AllColumns] PRIMARY KEY CLUSTERED ( [ClientID] ASC))                
            end;";

        private string datas =
        $@"
            INSERT INTO [dbo].[AllColumns]
                    ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                    ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                    ,[CNChar10],[CNumeric64],[CNVarchar50],[CNVarcharMax],[CReal]
                    ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                    ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                    ,[CVarchar50],[CVarcharMax],[CXml])
            VALUES
                    (newId(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
                    ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234
                    ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
                    ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
                    ,'<root><client name=''Doe''>inner Doe client</client></root>')
            INSERT INTO [dbo].[AllColumns]
                    ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                    ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                    ,[CNChar10],[CNumeric64],[CNVarchar50],[CNVarcharMax],[CReal]
                    ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                    ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                    ,[CVarchar50],[CVarcharMax],[CXml])
            VALUES
                    (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
                    ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234
                    ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
                    ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
                    ,'<root><client name=''Doe''>inner Doe client</client></root>')
            INSERT INTO [dbo].[AllColumns]
                    ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                    ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                    ,[CNChar10],[CNumeric64],[CNVarchar50],[CNVarcharMax],[CReal]
                    ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                    ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                    ,[CVarchar50],[CVarcharMax],[CXml])
            VALUES
                    (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
                    ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234
                    ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
                    ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
                    ,'<root><client name=''Doe''>inner Doe client</client></root>')
            INSERT INTO [dbo].[AllColumns]
                    ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                    ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                    ,[CNChar10],[CNumeric64],[CNVarchar50],[CNVarcharMax],[CReal]
                    ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                    ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                    ,[CVarchar50],[CVarcharMax],[CXml])
            VALUES
                    (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
                    ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234
                    ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
                    ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
                    ,'<root><client name=''Doe''>inner Doe client</client></root>')
            INSERT INTO [dbo].[AllColumns]
                    ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                    ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                    ,[CNChar10],[CNumeric64],[CNVarchar50],[CNVarcharMax],[CReal]
                    ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                    ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                    ,[CVarchar50],[CVarcharMax],[CXml])
            VALUES
                    (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
                    ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234
                    ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
                    ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
                    ,'<root><client name=''Doe''>inner Doe client</client></root>')
            INSERT INTO [dbo].[AllColumns]
                    ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                    ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                    ,[CNChar10],[CNumeric64],[CNVarchar50],[CNVarcharMax],[CReal]
                    ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                    ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                    ,[CVarchar50],[CVarcharMax],[CXml])
            VALUES
                    (newId(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
                    ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234
                    ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
                    ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
                    ,'<root><client name=''Doe''>inner Doe client</client></root>')
            INSERT INTO [dbo].[AllColumns]
                    ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                    ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                    ,[CNChar10],[CNumeric64],[CNVarchar50],[CNVarcharMax],[CReal]
                    ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                    ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                    ,[CVarchar50],[CVarcharMax],[CXml])
            VALUES
                    (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
                    ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234
                    ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
                    ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
                    ,'<root><client name=''Doe''>inner Doe client</client></root>')
            INSERT INTO [dbo].[AllColumns]
                    ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                    ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                    ,[CNChar10],[CNumeric64],[CNVarchar50],[CNVarcharMax],[CReal]
                    ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                    ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                    ,[CVarchar50],[CVarcharMax],[CXml])
            VALUES
                    (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
                    ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234
                    ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
                    ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
                    ,'<root><client name=''Doe''>inner Doe client</client></root>')
            INSERT INTO [dbo].[AllColumns]
                    ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                    ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                    ,[CNChar10],[CNumeric64],[CNVarchar50],[CNVarcharMax],[CReal]
                    ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                    ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                    ,[CVarchar50],[CVarcharMax],[CXml])
            VALUES
                    (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
                    ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234
                    ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
                    ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
                    ,'<root><client name=''Doe''>inner Doe client</client></root>')
            INSERT INTO [dbo].[AllColumns]
                    ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                    ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                    ,[CNChar10],[CNumeric64],[CNVarchar50],[CNVarcharMax],[CReal]
                    ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                    ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                    ,[CVarchar50],[CVarcharMax],[CXml])
            VALUES
                    (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
                    ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234
                    ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
                    ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
                    ,'<root><client name=''Doe''>inner Doe client</client></root>')
        ";

        private HelperDB helperDb = new HelperDB();
        private string serverDbName = "Test_SqliteAllColumns_Server";

        public String ServerConnectionString => HelperDB.GetDatabaseConnectionString(serverDbName);
        public String ClientSqliteConnectionString { get; set; }
        public string ClientSqliteFilePath => Path.Combine(Directory.GetCurrentDirectory(), "sqliteAllColumnsTmpDb.db");
        public SyncAgent Agent { get; set; }

        public SqliteSyncAllColumnsFixture()
        {
            var builder = new SqliteConnectionStringBuilder { DataSource = ClientSqliteFilePath };
            this.ClientSqliteConnectionString = builder.ConnectionString;

            GC.Collect();
            GC.WaitForPendingFinalizers();

            if (File.Exists(ClientSqliteFilePath))
                File.Delete(ClientSqliteFilePath);

            // create databases
            helperDb.CreateDatabase(serverDbName);

            // create table
            helperDb.ExecuteScript(serverDbName, createTableScript);

            // insert table
            helperDb.ExecuteScript(serverDbName, datas);

            var serverProvider = new SqlSyncProvider(ServerConnectionString);
            var clientProvider = new SqliteSyncProvider(ClientSqliteFilePath);
            var simpleConfiguration = new SyncConfiguration(new[] { "AllColumns" });

            Agent = new SyncAgent(clientProvider, serverProvider, simpleConfiguration);
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
    public class SqliteSyncAllColumnsTests : IClassFixture<SqliteSyncAllColumnsFixture>
    {
        SqliteSyncAllColumnsFixture fixture;
        SyncAgent agent;
        //SyncConfiguration configuration;
        public SqliteSyncAllColumnsTests(SqliteSyncAllColumnsFixture fixture)
        {
            this.fixture = fixture;
            this.agent = fixture.Agent;
        }

        [Fact, TestPriority(1)]
        public async Task InitializeAndSync()
        {
            var session = await agent.SynchronizeAsync();

            Debug.WriteLine(session.TotalChangesDownloaded);

            Assert.Equal(10, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);

        }

        [Fact, TestPriority(2)]
        public async Task NoRows()
        {
            var session = await agent.SynchronizeAsync();

            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }

        [Fact, TestPriority(3)]
        public async Task OneRowFromServer()
        {
            var insertRowScript =
            $@"
                INSERT INTO [dbo].[AllColumns]
                        ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                        ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                        ,[CNChar10],[CNumeric64],[CNVarchar50],[CNVarcharMax],[CReal]
                        ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                        ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                        ,[CVarchar50],[CVarcharMax],[CXml])
                VALUES
                        (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
                        ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234
                        ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
                        ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
                        ,'<root><client name=''Doe''>inner Doe client</client></root>')
            ";
            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                using (var sqlCmd = new SqlCommand(insertRowScript, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            var session = await agent.SynchronizeAsync();

            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }

        [Fact, TestPriority(4)]
        public async Task OneRowFromClient()
        {
            Guid newId = Guid.NewGuid();

            var insertRowScript =
            $@"INSERT INTO [AllColumns]
                    ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                    ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                    ,[CNChar10],[CNumeric64],[CNVarchar50],[CNVarcharMax],[CReal]
                    ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                    ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                    ,[CVarchar50],[CVarcharMax],[CXml])
            VALUES
                    (@id,@bin,10000000000000,1,'char10',date('now'),datetime('now'),datetime('now')
                    ,datetime('now'),23.1234,12.123,1,3148.29,'char10',23.1234
                    ,'nvarchar(50)','nvarchar(max)',12.34,datetime('now'),12,3148.29
                    ,datetime('now'),time('now'),1,@id,@bin,@bin,'varchar(50)','varchar(max)'
                    ,'<root><client name=''Doe''>inner Doe client</client></root>')";


            using (var sqlConnection = new SqliteConnection(fixture.ClientSqliteConnectionString))
            {
                using (var sqlCmd = new SqliteCommand(insertRowScript, sqlConnection))
                {
                    sqlCmd.Parameters.AddWithValue("@id", newId);
                    sqlCmd.Parameters.AddWithValue("@bin", Encoding.UTF8.GetBytes("123444"));

                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            var session = await agent.SynchronizeAsync();

            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
        }

        [Fact, TestPriority(5)]
        public async Task UpdateRowFromServer()
        {
            var insertRowScript =
                $@"
                Declare @id uniqueidentifier;
                select top 1 @id = ClientID from AllColumns;
                Update [AllColumns] Set [CNVarchar50] = 'Updated Row' Where ClientID = @id";

            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                using (var sqlCmd = new SqlCommand(insertRowScript, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            var session = await agent.SynchronizeAsync();

            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }

        [Fact, TestPriority(6)]
        public async Task UpdateRowFromClient()
        {
            Guid newId = Guid.NewGuid();

            var insertRowScript =
            $@"INSERT INTO [AllColumns]
                    ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                    ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                    ,[CNChar10],[CNumeric64],[CNVarchar50],[CNVarcharMax],[CReal]
                    ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                    ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                    ,[CVarchar50],[CVarcharMax],[CXml])
            VALUES
                    (@id,@bin,12345,1,'char10',date('now'),datetime('now'),datetime('now')
                    ,datetime('now'),23.1234,12.123,1,3148.29,'char10',23.1234
                    ,'nvarchar(50)','nvarchar(max)',12.34,datetime('now'),12,3148.29
                    ,datetime('now'),time('now'),1,@id,@bin,@bin,'varchar(50)','varchar(max)'
                    ,'<root><client name=''Doe''>inner Doe client</client></root>')";

            using (var sqlConnection = new SqliteConnection(fixture.ClientSqliteConnectionString))
            {
                using (var sqlCmd = new SqliteCommand(insertRowScript, sqlConnection))
                {
                    sqlCmd.Parameters.AddWithValue("@id", newId);
                    sqlCmd.Parameters.AddWithValue("@bin", Encoding.UTF8.GetBytes("123444"));

                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            var session = await agent.SynchronizeAsync();

            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);

            var updateRowScript =
            $@"Update [AllColumns] Set [CNVarchar50] = 'Updated Row' Where ClientID = @id";

            using (var sqlConnection = new SqliteConnection(fixture.ClientSqliteConnectionString))
            {
                using (var sqlCmd = new SqliteCommand(updateRowScript, sqlConnection))
                {
                    sqlCmd.Parameters.AddWithValue("@id", newId);

                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            session = await agent.SynchronizeAsync();

            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
        }



    }
}
