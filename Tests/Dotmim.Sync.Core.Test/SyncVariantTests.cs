using Dotmim.Sync.Core.Test.Misc;
using Dotmim.Sync.Core.Test.SqlUtils;
using Dotmim.Sync.SqlServer;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Core.Test
{

    public class SyncVariantFixture : IDisposable
    {
        private string createTableScript =
        $@"
        if (not exists (select * from sys.tables where name = 'VariantTable'))
        begin
            CREATE TABLE [dbo].[VariantTable](
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
                CONSTRAINT [PK_VariantTable] PRIMARY KEY CLUSTERED ( [ClientID] ASC))                
            end;";

        private string datas =
        $@"
            INSERT INTO [dbo].[VariantTable]
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
            INSERT INTO [dbo].[VariantTable]
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
            INSERT INTO [dbo].[VariantTable]
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
            INSERT INTO [dbo].[VariantTable]
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
            INSERT INTO [dbo].[VariantTable]
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
        private string serverDbName = "Test_Variant_Server";
        private string client1DbName = "Test_Variant_Client";

        public String ServerConnectionString => helperDb.GetDatabaseConnectionString(serverDbName);
        public String Client1ConnectionString => helperDb.GetDatabaseConnectionString(client1DbName);

        public SyncVariantFixture()
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

    [Collection("Sync")]
    [TestCaseOrderer("Dotmim.Sync.Core.Test.Misc.PriorityOrderer", "Dotmim.Sync.Core.Test")]
    public class SyncVariantTests : IClassFixture<SyncVariantFixture>
    {
        SyncVariantFixture fixture;
        SyncAgent agent;
        ServiceConfiguration configuration;
        public SyncVariantTests(SyncVariantFixture fixture)
        {
            this.fixture = fixture;

            SqlSyncProvider serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
            SqlSyncProvider clientProvider = new SqlSyncProvider(fixture.Client1ConnectionString);

            configuration = new ServiceConfiguration(new[] { "VariantTable" });
            agent = new SyncAgent(clientProvider, serverProvider, configuration);
        }

        [Fact, TestPriority(1)]
        public async Task InitializeAndSync()
        {
            var session = await agent.SynchronizeAsync();

            Assert.Equal(5, session.TotalChangesDownloaded);
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

            var session = await agent.SynchronizeAsync();

            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }

        [Fact, TestPriority(4)]
        public async Task OneRowFromClient()
        {
            var insertRowScript =
            $@"
            INSERT INTO [dbo].[VariantTable]
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

            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
            {
                using (var sqlCmd = new SqlCommand(insertRowScript, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            var session = await agent.SynchronizeAsync();

            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
        }

        [Fact, TestPriority(4)]
        public async Task UpdateRowFromServer()
        {
            var insertRowScript =
                $@"
                Declare @id uniqueidentifier;
                select top 1 @id = ClientID from VariantTable;
                Update [VariantTable] Set [CNVarchar50] = 'Updated Row' Where ClientID = @id";

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

        [Fact, TestPriority(5)]
        public async Task UpdateRowFromClient()
        {
            var insertRowScript =
                $@"
                Declare @id uniqueidentifier;
                select top 1 @id = ClientID from VariantTable;
                Update [VariantTable] Set [CNVarchar50] = 'Updated Row' Where ClientID = @id";
            
            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
            {
                using (var sqlCmd = new SqlCommand(insertRowScript, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            var session = await agent.SynchronizeAsync();

            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
        }



    }
}
