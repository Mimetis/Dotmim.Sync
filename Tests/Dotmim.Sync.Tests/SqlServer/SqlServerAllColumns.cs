//using Dotmim.Sync.Data;
//using Dotmim.Sync.SqlServer;
//using Dotmim.Sync.Tests.Core;
//using Dotmim.Sync.Tests.Misc;
//using System;
//using System.Data.SqlClient;
//using System.Linq;
//using System.Threading.Tasks;
//using Xunit;

//namespace Dotmim.Sync.Tests.SqlServer
//{
//    public class SyncAllColumnsFixture : IDisposable
//    {
//        private string createTableScript =
//        $@"
//            if (not exists (select * from sys.tables where name = 'AllColumns'))
//            begin
//                CREATE TABLE [dbo].[AllColumns](
//    	            [ClientID] [uniqueidentifier] NOT NULL,
//    	            [CBinary] [binary](50) NULL,
//    	            [CBigInt] [bigint] NULL,
//    	            [CBit] [bit] NULL,
//    	            [CChar10] [char](10) NULL,
//    	            [CDate] [date] NULL,
//    	            [CDateTime] [datetime] NULL,
//    	            [CDateTime2] [datetime2](7) NULL,
//    	            [CDateTimeOffset] [datetimeoffset](7) NULL,
//    	            [CDecimal64] [decimal](6, 4) NULL,
//    	            [CFloat] [float] NULL,
//    	            [CInt] [int] NULL,
//    	            [CMoney] [money] NULL,
//    	            [CNChar10] [nchar](10) NULL,
//    	            [CNumeric64] [numeric](6, 4) NULL,
//    	            [CNumeric103] [numeric](10, 3) NULL,
//    	            [CNVarchar50] [nvarchar](50) NULL,
//    	            [CNVarcharMax] [nvarchar](max) NULL,
//    	            [CReal] [real] NULL,
//    	            [CSmallDateTime] [smalldatetime] NULL,
//    	            [CSmallInt] [smallint] NULL,
//    	            [CSmallMoney] [smallmoney] NULL,
//    	            [CSqlVariant] [sql_variant] NULL,
//    	            [CTime7] [time](7) NULL,
//    	            [CTimeStamp] [timestamp] NULL,
//    	            [CTinyint] [tinyint] NULL,
//    	            [CUniqueIdentifier] [uniqueidentifier] NULL,
//    	            [CVarbinary50] [varbinary](50) NULL,
//    	            [CVarbinaryMax] [varbinary](max) NULL,
//    	            [CVarchar50] [varchar](50) NULL,
//    	            [CVarcharMax] [varchar](max) NULL,
//    	            [CXml] [xml] NULL,
//                    [C White Space Column] [nvarchar](50) NULL,
//                    CONSTRAINT [PK_AllColumns] PRIMARY KEY CLUSTERED ( [ClientID] ASC))                
//                end;";

//        private string datas =
//        $@"
//                INSERT INTO [dbo].[AllColumns]
//                        ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
//                        ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
//                        ,[CNChar10],[CNumeric64],[CNumeric103],[CNVarchar50],[CNVarcharMax],[CReal]
//                        ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
//                        ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
//                        ,[CVarchar50],[CVarcharMax],[CXml], [C White Space Column])
//                VALUES
//                        (newId(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
//                        ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234, 1.783
//                        ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
//                        ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
//                        ,'<root><client name=''Doe''>inner Doe client</client></root>', 'White Space value 1')
//                INSERT INTO [dbo].[AllColumns]
//                        ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
//                        ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
//                        ,[CNChar10],[CNumeric64],[CNumeric103],[CNVarchar50],[CNVarcharMax],[CReal]
//                        ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
//                        ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
//                        ,[CVarchar50],[CVarcharMax],[CXml], [C White Space Column])
//                VALUES
//                        (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
//                        ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234, 1.783
//                        ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
//                        ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
//                        ,'<root><client name=''Doe''>inner Doe client</client></root>', 'White Space value 2')
//                INSERT INTO [dbo].[AllColumns]
//                        ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
//                        ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
//                        ,[CNChar10],[CNumeric64],[CNumeric103],[CNVarchar50],[CNVarcharMax],[CReal]
//                        ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
//                        ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
//                        ,[CVarchar50],[CVarcharMax],[CXml])
//                VALUES
//                        (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
//                        ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234, 1.783
//                        ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
//                        ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
//                        ,'<root><client name=''Doe''>inner Doe client</client></root>')
//                INSERT INTO [dbo].[AllColumns]
//                        ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
//                        ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
//                        ,[CNChar10],[CNumeric64],[CNumeric103],[CNVarchar50],[CNVarcharMax],[CReal]
//                        ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
//                        ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
//                        ,[CVarchar50],[CVarcharMax],[CXml])
//                VALUES
//                        (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
//                        ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234, 1.783
//                        ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
//                        ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
//                        ,'<root><client name=''Doe''>inner Doe client</client></root>')
//                INSERT INTO [dbo].[AllColumns]
//                        ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
//                        ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
//                        ,[CNChar10],[CNumeric64],[CNumeric103],[CNVarchar50],[CNVarcharMax],[CReal]
//                        ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
//                        ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
//                        ,[CVarchar50],[CVarcharMax],[CXml])
//                VALUES
//                        (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
//                        ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234, 1.783
//                        ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
//                        ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
//                        ,'<root><client name=''Doe''>inner Doe client</client></root>')
//                INSERT INTO [dbo].[AllColumns]
//                        ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
//                        ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
//                        ,[CNChar10],[CNumeric64],[CNumeric103],[CNVarchar50],[CNVarcharMax],[CReal]
//                        ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
//                        ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
//                        ,[CVarchar50],[CVarcharMax],[CXml])
//                VALUES
//                        (newId(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
//                        ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234, 1.783
//                        ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
//                        ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
//                        ,'<root><client name=''Doe''>inner Doe client</client></root>')
//                INSERT INTO [dbo].[AllColumns]
//                        ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
//                        ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
//                        ,[CNChar10],[CNumeric64],[CNumeric103],[CNVarchar50],[CNVarcharMax],[CReal]
//                        ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
//                        ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
//                        ,[CVarchar50],[CVarcharMax],[CXml])
//                VALUES
//                        (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
//                        ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234, 1.783
//                        ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
//                        ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
//                        ,'<root><client name=''Doe''>inner Doe client</client></root>')
//                INSERT INTO [dbo].[AllColumns]
//                        ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
//                        ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
//                        ,[CNChar10],[CNumeric64],[CNumeric103],[CNVarchar50],[CNVarcharMax],[CReal]
//                        ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
//                        ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
//                        ,[CVarchar50],[CVarcharMax],[CXml])
//                VALUES
//                        (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
//                        ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234, 1.783
//                        ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
//                        ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
//                        ,'<root><client name=''Doe''>inner Doe client</client></root>')
//                INSERT INTO [dbo].[AllColumns]
//                        ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
//                        ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
//                        ,[CNChar10],[CNumeric64],[CNumeric103],[CNVarchar50],[CNVarcharMax],[CReal]
//                        ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
//                        ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
//                        ,[CVarchar50],[CVarcharMax],[CXml])
//                VALUES
//                        (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
//                        ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234, 1.783
//                        ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
//                        ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
//                        ,'<root><client name=''Doe''>inner Doe client</client></root>')
//                INSERT INTO [dbo].[AllColumns]
//                        ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
//                        ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
//                        ,[CNChar10],[CNumeric64],[CNumeric103],[CNVarchar50],[CNVarcharMax],[CReal]
//                        ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
//                        ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
//                        ,[CVarchar50],[CVarcharMax],[CXml])
//                VALUES
//                        (NEWID(),12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
//                        ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234, 1.783
//                        ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
//                        ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
//                        ,'<root><client name=''Doe''>inner Doe client</client></root>')
//            ";


//        private string serverDbName = "Test_AllColumns_Server";
//        private string client1DbName = "Test_AllColumns_Client";

//        public String ServerConnectionString => HelperDB.GetConnectionString(ProviderType.Sql, serverDbName);
//        public String Client1ConnectionString => HelperDB.GetConnectionString(ProviderType.Sql, client1DbName);

//        public SyncAllColumnsFixture()
//        {
//            // create databases
//            HelperDB.CreateDatabase(ProviderType.Sql, serverDbName);
//            HelperDB.CreateDatabase(ProviderType.Sql, client1DbName);

//            // create table
//            HelperDB.ExecuteSqlScript(serverDbName, createTableScript);

//            // insert table
//            HelperDB.ExecuteSqlScript(serverDbName, datas);
//        }
//        public void Dispose()
//        {
//            HelperDB.DropDatabase(ProviderType.Sql, serverDbName);
//            HelperDB.DropDatabase(ProviderType.Sql, client1DbName);
//        }

//    }

//    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
//    public class SyncAllColumnsTests : IClassFixture<SyncAllColumnsFixture>
//    {
//        private SyncAllColumnsFixture fixture;
//        private SyncAgent agent;
//        private DateTime dateTimeNow = new DateTime(2010, 10, 01, 23, 10, 12, 400);
//        private DateTime dateTimeNow2 = new DateTime(2010, 10, 01, 23, 10, 12, 900);
//        private DateTime shortDateTimeNow = new DateTime(2010, 10, 01);
//        private DateTimeOffset dateTimeOffset = DateTimeOffset.Now;
//        private TimeSpan timespan = TimeSpan.FromMinutes(128);
//        private byte[] byteArray = { 2, 4, 6, 8, 10, 12, 14, 16, 18, 20 };
//        private byte[] byteArray50 = { 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20 };


//        public SyncAllColumnsTests(SyncAllColumnsFixture fixture)
//        {
//            this.fixture = fixture;

//            var serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
//            var clientProvider = new SqlSyncProvider(fixture.Client1ConnectionString);

//            agent = new SyncAgent(clientProvider, serverProvider, new[] { "AllColumns" });
//        }

//        [Fact, TestPriority(1)]
//        public async Task InitializeAndSync()
//        {
//            var session = await agent.SynchronizeAsync();

//            Assert.Equal(10, session.TotalChangesDownloaded);
//            Assert.Equal(0, session.TotalChangesUploaded);

//            DmTable dmColumnsListServer;
//            DmTable dmColumnsListClient;

//            // check if all types are correct
//            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
//            {
//                sqlConnection.Open();
//                dmColumnsListServer = SqlManagementUtils.ColumnsForTable(sqlConnection, null, "AllColumns", null);
//                sqlConnection.Close();
//            }
//            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
//            {
//                sqlConnection.Open();
//                dmColumnsListClient = SqlManagementUtils.ColumnsForTable(sqlConnection, null, "AllColumns", null);
//                sqlConnection.Close();
//            }

//            // check if all columns are replicated
//            Assert.Equal(dmColumnsListServer.Rows.Count, dmColumnsListClient.Rows.Count);

//            // check if all types are correct
//            foreach (var serverRow in dmColumnsListServer.Rows.OrderBy(r => (int)r["column_id"]))
//            {
//                var name = serverRow["name"].ToString();
//                var ordinal = (int)serverRow["column_id"];
//                var typeString = serverRow["type"].ToString();
//                var maxLength = (Int16)serverRow["max_length"];
//                var precision = (byte)serverRow["precision"];
//                var scale = (byte)serverRow["scale"];
//                var isNullable = (bool)serverRow["is_nullable"];
//                var isIdentity = (bool)serverRow["is_identity"];

//                var clientRow = dmColumnsListClient.Rows.FirstOrDefault(cr => (int)cr["column_id"] == ordinal);

//                Assert.NotNull(clientRow);

//                // exception on numeric, check if it could be decimal
//                var cTypeString = clientRow["type"].ToString();
//                if (typeString.ToLowerInvariant() == "numeric" && cTypeString.ToLowerInvariant() == "decimal")
//                    cTypeString = "numeric";

//                Assert.Equal(name, clientRow["name"].ToString());
//                Assert.Equal(ordinal, (int)clientRow["column_id"]);
//                Assert.Equal(typeString, cTypeString);
//                Assert.Equal(maxLength, (Int16)clientRow["max_length"]);
//                Assert.Equal(precision, (byte)clientRow["precision"]);
//                Assert.Equal(scale, (byte)clientRow["scale"]);
//                Assert.Equal(isNullable, (bool)clientRow["is_nullable"]);
//                Assert.Equal(isIdentity, (bool)clientRow["is_identity"]);

//            }

//        }

//        private Guid InsertARow(string connectionString)
//        {
//            var clientId = Guid.NewGuid();


//            var insertRowScript =
//            $@"
//                    INSERT INTO [dbo].[AllColumns]
//                            ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
//                            ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
//                            ,[CNChar10],[CNumeric64],[CNumeric103],[CNVarchar50],[CNVarcharMax],[CReal]
//                            ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
//                            ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
//                            ,[CVarchar50],[CVarcharMax],[CXml])
//                    VALUES
//                            (@clientId,@byteArray50,10000000000000,1,'char10',@shortDateTimeNow
//                            ,@dateTimeNow,@dateTimeNow2,@dateTimeOffset
//                            ,23.1234,12.123,1,3148.29,'char10',23.1234, 1.783
//                            ,'nvarchar(50)','nvarchar(max)',12.34,@shortDateTimeNow,12,3148.29
//                            ,'variant',@timespan,1,NEWID(),@byteArray,@byteArray,'varchar(50)','varchar(max)'
//                            ,'<root><client name=""Doe"">inner Doe client</client></root>')
//                ";
//            using (var sqlConnection = new SqlConnection(connectionString))
//            {
//                using (var sqlCmd = new SqlCommand(insertRowScript, sqlConnection))
//                {
//                    sqlCmd.Parameters.AddWithValue("@clientId", clientId);
//                    sqlCmd.Parameters.AddWithValue("@shortDateTimeNow", shortDateTimeNow);
//                    sqlCmd.Parameters.AddWithValue("@dateTimeOffset", dateTimeOffset);
//                    sqlCmd.Parameters.AddWithValue("@timespan", timespan);
//                    sqlCmd.Parameters.AddWithValue("@dateTimeNow", dateTimeNow);
//                    sqlCmd.Parameters.AddWithValue("@dateTimeNow2", dateTimeNow2);
//                    sqlCmd.Parameters.AddWithValue("@byteArray", byteArray);
//                    sqlCmd.Parameters.AddWithValue("@byteArray50", byteArray50);

//                    sqlConnection.Open();
//                    sqlCmd.ExecuteNonQuery();
//                    sqlConnection.Close();
//                }
//            }

//            return clientId;
//        }

//        [Fact, TestPriority(3)]
//        public async Task OneRowFromServer()
//        {

//            var clientId = InsertARow(fixture.ServerConnectionString);

//            var session = await agent.SynchronizeAsync();

//            Assert.Equal(1, session.TotalChangesDownloaded);
//            Assert.Equal(0, session.TotalChangesUploaded);

//            // check values
//            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
//            {
//                using (var sqlCmd = new SqlCommand($"Select * from AllColumns where ClientId = '{clientId.ToString()}'", sqlConnection))
//                {
//                    sqlConnection.Open();

//                    using (var dbReader = sqlCmd.ExecuteReader())
//                    {

//                        dbReader.Read();
//                        AssertReader(clientId, dbReader);
//                    }


//                    sqlConnection.Close();
//                }
//            }
//        }

//        private void AssertReader(Guid clientId, SqlDataReader dbReader)
//        {
//            Assert.Equal(clientId, (Guid)dbReader["ClientID"]);

//            var dbBytes = (Byte[])dbReader["CBinary"];
//            Assert.Equal(byteArray50.Length, dbBytes.Length);

//            for (int i = 0; i < byteArray50.Length; i++)
//                Assert.Equal(byteArray50[i], dbBytes[i]);

//            Assert.Equal(10000000000000, (long)dbReader["CBigInt"]);
//            Assert.True((bool)dbReader["CBit"]);
//            Assert.Equal("char10    ", (string)dbReader["CChar10"]);
//            Assert.Equal(shortDateTimeNow, (DateTime)dbReader["CDate"]);
//            Assert.Equal(dateTimeNow, (DateTime)dbReader["CDateTime"]);
//            Assert.Equal(dateTimeNow2, (DateTime)dbReader["CDateTime2"]);
//            Assert.Equal(dateTimeOffset, (DateTimeOffset)dbReader["CDateTimeOffset"]);
//            Assert.Equal((decimal)23.1234, dbReader["CDecimal64"]);
//            Assert.Equal(12.123, dbReader["CFloat"]);
//            Assert.Equal(1, dbReader["CInt"]);
//            Assert.Equal((decimal)3148.29, (decimal)dbReader["CMoney"]);
//            Assert.Equal("char10    ", dbReader["CNChar10"]);
//            Assert.Equal((decimal)23.1234, (decimal)dbReader["CNumeric64"]);
//            Assert.Equal((decimal)1.783, (decimal)dbReader["CNumeric103"]);
//            Assert.Equal("nvarchar(50)", dbReader["CNVarchar50"]);
//            Assert.Equal("nvarchar(max)", dbReader["CNVarcharMax"]);
//            Assert.Equal((float)12.34, (float)dbReader["CReal"]);
//            Assert.Equal(shortDateTimeNow, (DateTime)dbReader["CSmallDateTime"]);
//            Assert.Equal((short)12, (short)dbReader["CSmallInt"]);
//            Assert.Equal((decimal)3148.29, (decimal)dbReader["CSmallMoney"]);
//            Assert.Equal("variant", (string)dbReader["CSqlVariant"]);
//            Assert.Equal(timespan, (TimeSpan)dbReader["CTime7"]);
//            Assert.Equal((byte)1, (byte)dbReader["CTinyint"]);

//            var dbBytes2 = (byte[])dbReader["CVarbinary50"];
//            Assert.Equal(byteArray.Length, dbBytes2.Length);
//            for (int i = 0; i < byteArray.Length; i++)
//                Assert.Equal(byteArray[i], dbBytes2[i]);

//            var dbBytes3 = (byte[])dbReader["CVarbinaryMax"];
//            Assert.Equal(byteArray.Length, dbBytes3.Length);
//            for (int i = 0; i < byteArray.Length; i++)
//                Assert.Equal(byteArray[i], dbBytes3[i]);


//            Assert.Equal("varchar(50)", (string)dbReader["CVarchar50"]);
//            Assert.Equal("varchar(max)", (string)dbReader["CVarcharMax"]);
//            Assert.Equal(@"<root><client name=""Doe"">inner Doe client</client></root>", (String)dbReader["CXml"]);
//        }

//        [Fact, TestPriority(4)]
//        public async Task OneRowFromClient()
//        {
//            var clientId = InsertARow(fixture.Client1ConnectionString);
//            var session = await agent.SynchronizeAsync();

//            Assert.Equal(0, session.TotalChangesDownloaded);
//            Assert.Equal(1, session.TotalChangesUploaded);

//            // check values
//            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
//            {
//                using (var sqlCmd = new SqlCommand($"Select * from AllColumns where ClientId = '{clientId.ToString()}'", sqlConnection))
//                {
//                    sqlConnection.Open();
//                    using (var dbReader = sqlCmd.ExecuteReader())
//                    {
//                        dbReader.Read();
//                        AssertReader(clientId, dbReader);
//                    }
//                    sqlConnection.Close();
//                }
//            }
//        }

//        [Fact, TestPriority(4)]
//        public async Task UpdateRowFromServer()
//        {
//            var insertRowScript =
//                $@" Declare @id uniqueidentifier;
//                    select top 1 @id = ClientID from AllColumns;
//                    Update [AllColumns] Set [CNVarchar50] = 'Updated Row' Where ClientID = @id";

//            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
//            {
//                using (var sqlCmd = new SqlCommand(insertRowScript, sqlConnection))
//                {
//                    sqlConnection.Open();
//                    sqlCmd.ExecuteNonQuery();
//                    sqlConnection.Close();
//                }
//            }

//            var session = await agent.SynchronizeAsync();

//            Assert.Equal(1, session.TotalChangesDownloaded);
//            Assert.Equal(0, session.TotalChangesUploaded);
//        }

//        [Fact, TestPriority(5)]
//        public async Task UpdateRowFromClient()
//        {
//            var insertRowScript =
//                $@"
//                    Declare @id uniqueidentifier;
//                    select top 1 @id = ClientID from AllColumns;
//                    Update [AllColumns] Set [CNVarchar50] = 'Updated Row' Where ClientID = @id";

//            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
//            {
//                using (var sqlCmd = new SqlCommand(insertRowScript, sqlConnection))
//                {
//                    sqlConnection.Open();
//                    sqlCmd.ExecuteNonQuery();
//                    sqlConnection.Close();
//                }
//            }

//            var session = await agent.SynchronizeAsync();

//            Assert.Equal(0, session.TotalChangesDownloaded);
//            Assert.Equal(1, session.TotalChangesUploaded);
//        }



//    }

//}
