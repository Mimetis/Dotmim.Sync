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
using System.Data.Common;

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
	            [CNumeric103] [numeric](10, 3) NULL,
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
                    ,[CNChar10],[CNumeric64],[CNumeric103],[CNVarchar50],[CNVarcharMax],[CReal]
                    ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                    ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                    ,[CVarchar50],[CVarcharMax],[CXml])
            VALUES
                    (newId(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
                    ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234, 1.783
                    ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
                    ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
                    ,'<root><client name=''Doe''>inner Doe client</client></root>')
            INSERT INTO [dbo].[AllColumns]
                    ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                    ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                    ,[CNChar10],[CNumeric64],[CNumeric103],[CNVarchar50],[CNVarcharMax],[CReal]
                    ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                    ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                    ,[CVarchar50],[CVarcharMax],[CXml])
            VALUES
                    (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
                    ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234, 1.783
                    ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
                    ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
                    ,'<root><client name=''Doe''>inner Doe client</client></root>')
            INSERT INTO [dbo].[AllColumns]
                    ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                    ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                    ,[CNChar10],[CNumeric64],[CNumeric103],[CNVarchar50],[CNVarcharMax],[CReal]
                    ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                    ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                    ,[CVarchar50],[CVarcharMax],[CXml])
            VALUES
                    (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
                    ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234, 1.783
                    ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
                    ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
                    ,'<root><client name=''Doe''>inner Doe client</client></root>')
            INSERT INTO [dbo].[AllColumns]
                    ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                    ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                    ,[CNChar10],[CNumeric64],[CNumeric103],[CNVarchar50],[CNVarcharMax],[CReal]
                    ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                    ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                    ,[CVarchar50],[CVarcharMax],[CXml])
            VALUES
                    (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
                    ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234, 1.783
                    ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
                    ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
                    ,'<root><client name=''Doe''>inner Doe client</client></root>')
            INSERT INTO [dbo].[AllColumns]
                    ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                    ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                    ,[CNChar10],[CNumeric64],[CNumeric103],[CNVarchar50],[CNVarcharMax],[CReal]
                    ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                    ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                    ,[CVarchar50],[CVarcharMax],[CXml])
            VALUES
                    (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
                    ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234, 1.783
                    ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
                    ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
                    ,'<root><client name=''Doe''>inner Doe client</client></root>')
            INSERT INTO [dbo].[AllColumns]
                    ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                    ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                    ,[CNChar10],[CNumeric64],[CNumeric103],[CNVarchar50],[CNVarcharMax],[CReal]
                    ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                    ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                    ,[CVarchar50],[CVarcharMax],[CXml])
            VALUES
                    (newId(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
                    ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234, 1.783
                    ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
                    ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
                    ,'<root><client name=''Doe''>inner Doe client</client></root>')
            INSERT INTO [dbo].[AllColumns]
                    ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                    ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                    ,[CNChar10],[CNumeric64],[CNumeric103],[CNVarchar50],[CNVarcharMax],[CReal]
                    ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                    ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                    ,[CVarchar50],[CVarcharMax],[CXml])
            VALUES
                    (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
                    ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234, 1.783
                    ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
                    ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
                    ,'<root><client name=''Doe''>inner Doe client</client></root>')
            INSERT INTO [dbo].[AllColumns]
                    ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                    ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                    ,[CNChar10],[CNumeric64],[CNumeric103],[CNVarchar50],[CNVarcharMax],[CReal]
                    ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                    ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                    ,[CVarchar50],[CVarcharMax],[CXml])
            VALUES
                    (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
                    ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234, 1.783
                    ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
                    ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
                    ,'<root><client name=''Doe''>inner Doe client</client></root>')
            INSERT INTO [dbo].[AllColumns]
                    ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                    ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                    ,[CNChar10],[CNumeric64],[CNumeric103],[CNVarchar50],[CNVarcharMax],[CReal]
                    ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                    ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                    ,[CVarchar50],[CVarcharMax],[CXml])
            VALUES
                    (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
                    ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234, 1.783
                    ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
                    ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
                    ,'<root><client name=''Doe''>inner Doe client</client></root>')
            INSERT INTO [dbo].[AllColumns]
                    ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                    ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                    ,[CNChar10],[CNumeric64],[CNumeric103],[CNVarchar50],[CNVarcharMax],[CReal]
                    ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                    ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                    ,[CVarchar50],[CVarcharMax],[CXml])
            VALUES
                    (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
                    ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234, 1.783
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

            Agent = new SyncAgent(clientProvider, serverProvider, new[] { "AllColumns" });
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
        private DateTime dateTimeNow = new DateTime(2010, 10, 01, 23, 10, 12, 400);
        private DateTime dateTimeNow2 = new DateTime(2010, 10, 01, 23, 10, 12, 900);
        private DateTime shortDateTimeNow = new DateTime(2010, 10, 01);
        private DateTimeOffset dateTimeOffset = DateTimeOffset.Now;
        private TimeSpan timespan = TimeSpan.FromMinutes(128);
        private byte[] byteArray = { 2, 4, 6, 8, 10, 12, 14, 16, 18, 20 };
        private byte[] byteArray50 = { 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20 };

        public SqliteSyncAllColumnsTests(SqliteSyncAllColumnsFixture fixture)
        {
            this.fixture = fixture;
            this.agent = fixture.Agent;
        }

        [Fact, TestPriority(1)]
        public async Task InitializeAndSync()
        {
            var session = await agent.SynchronizeAsync();

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

        private Guid InsertARow(string connectionString)
        {
            var clientId = Guid.NewGuid();


            var insertRowScript =
            $@"
                INSERT INTO [dbo].[AllColumns]
                        ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                        ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                        ,[CNChar10],[CNumeric64],[CNumeric103],[CNVarchar50],[CNVarcharMax],[CReal]
                        ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                        ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                        ,[CVarchar50],[CVarcharMax],[CXml])
                VALUES
                        (@clientId,@byteArray50,10000000000000,1,'char10',@shortDateTimeNow
                        ,@dateTimeNow,@dateTimeNow2,@dateTimeOffset
                        ,23.1234,12.123,1,3148.29,'char10',23.1234, 1.783
                        ,'nvarchar(50)','nvarchar(max)',12.34,@shortDateTimeNow,12,3148.29
                        ,'variant',@timespan,1,NEWID(),@byteArray,@byteArray,'varchar(50)','varchar(max)'
                        ,'<root><client name=""Doe"">inner Doe client</client></root>')
            ";
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                using (var sqlCmd = new SqlCommand(insertRowScript, sqlConnection))
                {
                    sqlCmd.Parameters.AddWithValue("@clientId", clientId);
                    sqlCmd.Parameters.AddWithValue("@shortDateTimeNow", shortDateTimeNow);
                    sqlCmd.Parameters.AddWithValue("@dateTimeOffset", dateTimeOffset);
                    sqlCmd.Parameters.AddWithValue("@timespan", timespan);
                    sqlCmd.Parameters.AddWithValue("@dateTimeNow", dateTimeNow);
                    sqlCmd.Parameters.AddWithValue("@dateTimeNow2", dateTimeNow2);
                    sqlCmd.Parameters.AddWithValue("@byteArray", byteArray);
                    sqlCmd.Parameters.AddWithValue("@byteArray50", byteArray50);

                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            return clientId;
        }


        [Fact, TestPriority(3)]
        public async Task OneRowFromServer()
        {

            var clientId = InsertARow(fixture.ServerConnectionString);

            var session = await agent.SynchronizeAsync();

            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);

            // check values
            using (var sqlConnection = new SqliteConnection(fixture.ClientSqliteConnectionString))
            {
                using (var sqlCmd = new SqliteCommand($"Select * from AllColumns where ClientId = @clientId", sqlConnection))
                {
                    sqlCmd.Parameters.AddWithValue("@clientId", clientId);
                    sqlConnection.Open();

                    using (var dbReader = sqlCmd.ExecuteReader())
                    {

                        dbReader.Read();
                        AssertReader(clientId, dbReader);
                    }


                    sqlConnection.Close();
                }
            }
        }

        private void AssertReader(Guid clientId, DbDataReader dbReader)
        {
            Assert.Equal(clientId, new Guid((byte[])dbReader["ClientID"]));

            var dbBytes = (Byte[])dbReader["CBinary"];
            Assert.Equal(byteArray50.Length, dbBytes.Length);

            for (int i = 0; i < byteArray50.Length; i++)
                Assert.Equal(byteArray50[i], dbBytes[i]);

            Assert.Equal(10000000000000, (long)dbReader["CBigInt"]);
            Assert.Equal(true, Convert.ToBoolean(dbReader["CBit"]));
            Assert.Equal("char10    ", (string)dbReader["CChar10"]);
            Assert.Equal(shortDateTimeNow, DateTime.Parse((string)dbReader["CDate"]));
            Assert.Equal(dateTimeNow, DateTime.Parse((string)dbReader["CDateTime"]));
            Assert.Equal(dateTimeNow2, DateTime.Parse((string)dbReader["CDateTime2"]));
            Assert.Equal(dateTimeOffset, DateTimeOffset.Parse((string)dbReader["CDateTimeOffset"]));
            Assert.Equal((Decimal)23.1234, Convert.ToDecimal(dbReader["CDecimal64"]));
            Assert.Equal(12.123, dbReader["CFloat"]);
            Assert.Equal(1, Convert.ToInt32(dbReader["CInt"]));
            Assert.Equal((Decimal)3148.29, Convert.ToDecimal(dbReader["CMoney"]));
            Assert.Equal("char10    ", (string)dbReader["CNChar10"]);
            Assert.Equal((Decimal)23.1234, Convert.ToDecimal(dbReader["CNumeric64"]));
            Assert.Equal((Decimal)1.783, Convert.ToDecimal(dbReader["CNumeric103"]));
            Assert.Equal("nvarchar(50)", dbReader["CNVarchar50"]);
            Assert.Equal("nvarchar(max)", dbReader["CNVarcharMax"]);
            Assert.Equal((float)12.34, Convert.ToSingle(dbReader["CReal"]));
            Assert.Equal(shortDateTimeNow, DateTime.Parse((string)dbReader["CSmallDateTime"]));
            Assert.Equal((Int16)12, Convert.ToInt16(dbReader["CSmallInt"]));
            Assert.Equal((decimal)3148.29, Convert.ToDecimal(dbReader["CSmallMoney"]));
            Assert.Equal("variant", (String)dbReader["CSqlVariant"]);
            Assert.Equal(timespan, TimeSpan.Parse((String)dbReader["CTime7"]));
            Assert.Equal((byte)1, Convert.ToByte(dbReader["CTinyint"]));

            var dbBytes2 = (Byte[])dbReader["CVarbinary50"];
            Assert.Equal(byteArray.Length, dbBytes2.Length);
            for (int i = 0; i < byteArray.Length; i++)
                Assert.Equal(byteArray[i], dbBytes2[i]);

            var dbBytes3 = (Byte[])dbReader["CVarbinaryMax"];
            Assert.Equal(byteArray.Length, dbBytes3.Length);
            for (int i = 0; i < byteArray.Length; i++)
                Assert.Equal(byteArray[i], dbBytes3[i]);


            Assert.Equal("varchar(50)", (string)dbReader["CVarchar50"]);
            Assert.Equal("varchar(max)", (String)dbReader["CVarcharMax"]);
            Assert.Equal(@"<root><client name=""Doe"">inner Doe client</client></root>", (String)dbReader["CXml"]);
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
