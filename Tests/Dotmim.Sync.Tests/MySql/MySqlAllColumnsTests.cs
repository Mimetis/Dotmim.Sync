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
using System.Data.SqlTypes;
using Dotmim.Sync.MySql;
using MySql.Data.MySqlClient;
using System.Data.Common;

namespace Dotmim.Sync.Test.MySql
{

    public class MySqlAllColumnsFixture : IDisposable
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
        private string serverDbName = "Test_AllColumns_MySql";
        private string client1DbName = "testallcolumnsmysql";

        public String ServerConnectionString => HelperDB.GetDatabaseConnectionString(serverDbName);
        public String ClientMySqlConnectionString => HelperDB.GetMySqlDatabaseConnectionString(client1DbName);

        public MySqlAllColumnsFixture()
        {
            // create databases
            helperDb.CreateDatabase(serverDbName);
            helperDb.CreateMySqlDatabase(client1DbName);

            // create table
            helperDb.ExecuteScript(serverDbName, createTableScript);

            // insert table
            helperDb.ExecuteScript(serverDbName, datas);
        }
        public void Dispose()
        {
            helperDb.DeleteDatabase(serverDbName);
            //helperDb.DropMySqlDatabase(client1DbName);
        }

    }

    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    public class MySqlAllColumnsTests : IClassFixture<MySqlAllColumnsFixture>
    {
        private MySqlAllColumnsFixture fixture;
        private SyncAgent agent;
        private SyncConfiguration configuration;
        private DateTime dateTimeNow = new DateTime(2010, 10, 01, 23, 10, 12, 400);
        private DateTime dateTimeNow2 = new DateTime(2010, 10, 01, 23, 10, 12, 900);
        private DateTime shortDateTimeNow = new DateTime(2010, 10, 01);
        private DateTimeOffset dateTimeOffset = DateTimeOffset.Now;
        private TimeSpan timespan = TimeSpan.FromMinutes(128);
        private byte[] byteArray = { 2, 4, 6, 8, 10, 12, 14, 16, 18, 20 };
        private byte[] byteArray50 = { 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20 };


        public MySqlAllColumnsTests(MySqlAllColumnsFixture fixture)
        {
            this.fixture = fixture;

            SqlSyncProvider serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
            MySqlSyncProvider clientProvider = new MySqlSyncProvider(fixture.ClientMySqlConnectionString);

            agent = new SyncAgent(clientProvider, serverProvider, new[] { "AllColumns" });

        }

        [Fact, TestPriority(1)]
        public async Task InitializeAndSync()
        {
            var session = await agent.SynchronizeAsync();

            Assert.Equal(10, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);

            DmTable dmColumnsListServer;
            DmTable dmColumnsListClient;

            // check if all types are correct
            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                sqlConnection.Open();
                dmColumnsListServer = SqlManagementUtils.ColumnsForTable(sqlConnection, null, "AllColumns");
                sqlConnection.Close();
            }
            using (var mysqlConnection = new MySqlConnection(fixture.ClientMySqlConnectionString))
            {
                mysqlConnection.Open();
                dmColumnsListClient = MySqlManagementUtils.ColumnsForTable(mysqlConnection, null, "allcolumns");
                mysqlConnection.Close();
            }

            // check if all columns are replicated
            Assert.Equal(dmColumnsListServer.Rows.Count, dmColumnsListClient.Rows.Count);

            // check if all types are correct
            foreach (var serverRow in dmColumnsListServer.Rows.OrderBy(r => (int)r["column_id"]))
            {
                var name = serverRow["name"].ToString();
                var ordinal = (int)serverRow["column_id"];
                var typeString = serverRow["type"].ToString();
                var maxLength = (Int16)serverRow["max_length"];
                var precision = (byte)serverRow["precision"];
                var scale = (byte)serverRow["scale"];
                var isNullable = (bool)serverRow["is_nullable"];
                var isIdentity = (bool)serverRow["is_identity"];

                var clientRow = dmColumnsListClient.Rows.FirstOrDefault(cr => Convert.ToInt32(cr["ordinal_position"]) == ordinal);

                Assert.NotNull(clientRow);

                Assert.Equal(name.ToLowerInvariant(), clientRow["COLUMN_NAME"].ToString());
                Assert.Equal(ordinal, Convert.ToInt32(clientRow["ordinal_position"]));

                // TODO: Types can't be the same. Maybe check the types equality between providers ?
                //Assert.Equal(typeString, cTypeString);

                var maxLengthLong = clientRow["character_octet_length"] != DBNull.Value ? Convert.ToInt64(clientRow["character_octet_length"]) : 0;
                var maxLengthMySql = maxLengthLong > Int32.MaxValue ? Int32.MaxValue : (Int32)maxLengthLong;


                var mySqlPrecision = clientRow["numeric_precision"] != DBNull.Value ? Convert.ToByte(clientRow["numeric_precision"]) : (byte)0;
                var mySqlScale = clientRow["numeric_scale"] != DBNull.Value ? Convert.ToByte(clientRow["numeric_scale"]) : (byte)0;
                var mySqlAllowDBNull = (String)clientRow["is_nullable"] == "NO" ? false : true;
                var mySqlAutoIncrement = clientRow["extra"] != DBNull.Value ? ((string)clientRow["extra"]).Contains("auto increment") : false;

                // TODO : For some type (like GUID) length can't be the same
                //Assert.Equal(maxLength, maxLengthMySql);

                // for type like bit transformed to tinyint, precision can differ
                //Assert.Equal(precision, mySqlPrecision);
                //Assert.Equal(scale, mySqlScale);

                Assert.Equal(isNullable, mySqlAllowDBNull);
                Assert.Equal(isIdentity, mySqlAutoIncrement);

            }

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
            using (var sqlConnection = new MySqlConnection(fixture.ClientMySqlConnectionString))
            {
                using (var sqlCmd = new MySqlCommand($"Select * from allcolumns where clientid = '{clientId.ToString()}'", sqlConnection))
                {
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
            Assert.Equal(clientId, Guid.Parse((String)dbReader["ClientID"]));

            var dbBytes = (Byte[])dbReader["CBinary"];
            Assert.Equal(byteArray50.Length, dbBytes.Length);

            for (int i = 0; i < byteArray50.Length; i++)
                Assert.Equal(byteArray50[i], dbBytes[i]);

            Assert.Equal(10000000000000, (long)dbReader["CBigInt"]);
            Assert.Equal(true, Convert.ToBoolean(dbReader["CBit"]));
            Assert.Equal("char10    ", (string)dbReader["CChar10"]);
            Assert.Equal(shortDateTimeNow, (DateTime)dbReader["CDate"]);
            Assert.Equal(dateTimeNow, (DateTime)dbReader["CDateTime"]);
            Assert.Equal(dateTimeNow2, (DateTime)dbReader["CDateTime2"]);
            Assert.Equal(dateTimeOffset, (DateTimeOffset)dbReader["CDateTimeOffset"]);
            Assert.Equal((Decimal)23.1234, dbReader["CDecimal64"]);
            Assert.Equal(12.123, dbReader["CFloat"]);
            Assert.Equal(1, dbReader["CInt"]);
            Assert.Equal((Decimal)3148.29, (Decimal)dbReader["CMoney"]);
            Assert.Equal("char10    ", dbReader["CNChar10"]);
            Assert.Equal((Decimal)23.1234, (Decimal)dbReader["CNumeric64"]);
            Assert.Equal((Decimal)1.783, (Decimal)dbReader["CNumeric103"]);
            Assert.Equal("nvarchar(50)", dbReader["CNVarchar50"]);
            Assert.Equal("nvarchar(max)", dbReader["CNVarcharMax"]);
            Assert.Equal((float)12.34, (float)dbReader["CReal"]);
            Assert.Equal(shortDateTimeNow, (DateTime)dbReader["CSmallDateTime"]);
            Assert.Equal((Int16)12, (Int16)dbReader["CSmallInt"]);
            Assert.Equal((decimal)3148.29, (decimal)dbReader["CSmallMoney"]);
            Assert.Equal("variant", (String)dbReader["CSqlVariant"]);
            Assert.Equal(timespan, (TimeSpan)dbReader["CTime7"]);
            Assert.Equal((byte)1, (byte)dbReader["CTinyint"]);

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

        //[Fact, TestPriority(4)]
        //public async Task OneRowFromClient()
        //{


        //    var session = await agent.SynchronizeAsync();

        //    Assert.Equal(0, session.TotalChangesDownloaded);
        //    Assert.Equal(1, session.TotalChangesUploaded);

        //    // check values
        //    using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
        //    {
        //        using (var sqlCmd = new SqlCommand($"Select * from AllColumns where ClientId = '{clientId.ToString()}'", sqlConnection))
        //        {
        //            sqlConnection.Open();

        //            using (var dbReader = sqlCmd.ExecuteReader())
        //            {

        //                dbReader.Read();
        //                AssertReader(clientId, dbReader);
        //            }


        //            sqlConnection.Close();
        //        }
        //    }
        //}

        [Fact, TestPriority(4)]
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

        //[Fact, TestPriority(5)]
        //public async Task UpdateRowFromClient()
        //{
        //    var insertRowScript =
        //        $@"
        //        Declare @id uniqueidentifier;
        //        select top 1 @id = ClientID from AllColumns;
        //        Update [AllColumns] Set [CNVarchar50] = 'Updated Row' Where ClientID = @id";

        //    using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
        //    {
        //        using (var sqlCmd = new SqlCommand(insertRowScript, sqlConnection))
        //        {
        //            sqlConnection.Open();
        //            sqlCmd.ExecuteNonQuery();
        //            sqlConnection.Close();
        //        }
        //    }

        //    var session = await agent.SynchronizeAsync();

        //    Assert.Equal(0, session.TotalChangesDownloaded);
        //    Assert.Equal(1, session.TotalChangesUploaded);
        //}



    }
}
