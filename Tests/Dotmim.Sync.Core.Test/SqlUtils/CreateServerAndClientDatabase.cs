using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.Core.Test.SqlUtils
{
    public class CreateServerAndClientDatabase : IDisposable
    {
        private Dictionary<string, string> tablesScript = new Dictionary<string, string>();
        private Dictionary<string, string> datasScript = new Dictionary<string, string>();
        internal Dictionary<string, string> insertOneRowScript = new Dictionary<string, string>();
        internal Dictionary<string, string> deleteOneRowScript = new Dictionary<string, string>();
        internal Dictionary<string, string> updateOneRowScript = new Dictionary<string, string>();
        public List<PairDatabases> PairDatabases { get; set; } = new List<PairDatabases>();

        public static String GetDatabaseConnectionString(string dbName) => $@"Data Source=(localdb)\MSSQLLocalDB; Initial Catalog={dbName}; Integrated Security=true;";

        public CreateServerAndClientDatabase()
        {
            PairDatabases.Add(new PairDatabases("SimpleSync", "TEST_SSimpleDB", "TEST_CSimpleDB", "ServiceTickets"));
            PairDatabases.Add(new PairDatabases("TwoTablesSync", "TEST_STwoTableseDB", "TEST_CTwoTablesDB", new List<string>(new string[] { "Customers", "ServiceTickets" })));
            PairDatabases.Add(new PairDatabases("VariantSync", "TEST_CVariantDB", "TEST_SVariantDB", "TableVariant"));
            PairDatabases.Add(new PairDatabases("AllColumnsSync", "TEST_CAllColumnsDB", "TEST_SAllColumnsDB", "TableAllColumns"));

            // Adding scripts for create tables and datas
            AddSingleTableScript("SimpleSync");
            AddTwoTableScript("TwoTablesSync");
            AddAllColumnsAvailabeTableScript("AllColumnsSync");
            AddSqlVariantTableScript("VariantSync");

        }

        public void GenerateDatabase(string dbName, bool recreateDb = true)
        {
            SqlConnection masterConnection = null;
            SqlCommand cmdDb = null;
            masterConnection = new SqlConnection(GetDatabaseConnectionString("master"));

            masterConnection.Open();
            cmdDb = new SqlCommand(GetCreationDBScript(dbName, recreateDb), masterConnection);
            cmdDb.ExecuteNonQuery();
            masterConnection.Close();

        }

        public void GenerateDatabasesAndTables(PairDatabases db, Boolean recreateDb = true, Boolean withSchemaOnClient = false, Boolean withDatasOnServer = true)
        {
            SqlConnection connection = null;
            SqlConnection masterConnection = null;
            SqlCommand cmdDb = null;
            try
            {
                masterConnection = new SqlConnection(GetDatabaseConnectionString("master"));

                masterConnection.Open();
                if (db.ServerDatabase != null)
                {
                    cmdDb = new SqlCommand(GetCreationDBScript(db.ServerDatabase, recreateDb), masterConnection);
                    cmdDb.ExecuteNonQuery();
                }

                if (db.ClientDatabase != null)
                {
                    cmdDb = new SqlCommand(GetCreationDBScript(db.ClientDatabase, recreateDb), masterConnection);
                    cmdDb.ExecuteNonQuery();
                }

                masterConnection.Close();
                if (db.ServerDatabase != null)
                {

                    connection = new SqlConnection(GetDatabaseConnectionString(db.ServerDatabase));
                    connection.Open();
                    cmdDb = new SqlCommand(tablesScript[db.Key], connection);
                    cmdDb.ExecuteNonQuery();
                }

                if (withDatasOnServer)
                {
                    cmdDb = new SqlCommand(datasScript[db.Key], connection);
                    cmdDb.ExecuteNonQuery();
                }
                connection.Close();

                if (withSchemaOnClient)
                {
                    connection = new SqlConnection(GetDatabaseConnectionString(db.ClientDatabase));
                    connection.Open();
                    cmdDb = new SqlCommand(tablesScript[db.Key], connection);
                    cmdDb.ExecuteNonQuery();
                    connection.Close();
                }

            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                if (connection.State != ConnectionState.Closed)
                    connection.Close();
                if (masterConnection.State != ConnectionState.Closed)
                    masterConnection.Close();
            }
        }


        public void AddTwoTableScript(string key)
        {
            string tableCustomers = PairDatabases.First(c => c.Key == key).Tables[0];
            string tableServiceTickets = PairDatabases.First(c => c.Key == key).Tables[1];

            string id = Guid.NewGuid().ToString();

            var tblScript =
                $@"
                    if (not exists (select * from sys.tables where name = '{tableServiceTickets}'))
                    begin
                        CREATE TABLE [{tableServiceTickets}](
	                    [ServiceTicketID] [uniqueidentifier] NOT NULL,
	                    [Title] [nvarchar](max) NOT NULL,
	                    [Description] [nvarchar](max) NULL,
	                    [StatusValue] [int] NOT NULL,
	                    [EscalationLevel] [int] NOT NULL,
	                    [Opened] [datetime] NULL,
	                    [Closed] [datetime] NULL,
	                    [CustomerID] [int] NULL,
                        CONSTRAINT [PK_{tableServiceTickets}] PRIMARY KEY CLUSTERED ( [ServiceTicketID] ASC ));
                    end;
                    if (not exists (select * from sys.tables where name = '{tableCustomers}'))
                    begin
                        CREATE TABLE [{tableCustomers}](
	                    [CustomerID] [int] NOT NULL,
	                    [FirstName] [nvarchar](max) NOT NULL,
	                    [LastName] [nvarchar](max) NULL
                        CONSTRAINT [PK_{tableCustomers}] PRIMARY KEY CLUSTERED ( [CustomerID] ASC ));
                    end;
                    if (not exists (select * from sys.foreign_keys where name = 'FK_{tableServiceTickets}_{tableCustomers}'))
                    begin
                        ALTER TABLE {tableServiceTickets} ADD CONSTRAINT FK_{tableServiceTickets}_{tableCustomers} 
                        FOREIGN KEY ( CustomerID ) 
                        REFERENCES {tableCustomers} ( CustomerID ) 
                    end
                ";
            var datas =
                    $@"

                        INSERT [{tableCustomers}] ([CustomerID], [FirstName], [LastName]) VALUES (1, N'John', N'Doe');

                        INSERT [{tableServiceTickets}] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (N'{id}', N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
                        INSERT [{tableServiceTickets}] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
                        INSERT [{tableServiceTickets}] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
                        INSERT [{tableServiceTickets}] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
                        INSERT [{tableServiceTickets}] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
                 ";


            if (!tablesScript.ContainsKey(key))
                tablesScript.Add(key, tblScript);

            if (!datasScript.ContainsKey(key))
                datasScript.Add(key, datas);

        }


        public void AddSingleTableScript(string key)
        {
            string tableName = PairDatabases.First(c => c.Key == key).Tables[0];

            string id = Guid.NewGuid().ToString();

            var tblScript =
                $@"
                    if (not exists (select * from sys.tables where name = '{tableName}'))
                    begin
                        CREATE TABLE [{tableName}](
	                    [ServiceTicketID] [uniqueidentifier] NOT NULL,
	                    [Title] [nvarchar](max) NOT NULL,
	                    [Description] [nvarchar](max) NULL,
	                    [StatusValue] [int] NOT NULL,
	                    [EscalationLevel] [int] NOT NULL,
	                    [Opened] [datetime] NULL,
	                    [Closed] [datetime] NULL,
	                    [CustomerID] [int] NULL,
                        CONSTRAINT [PK_{tableName}] PRIMARY KEY CLUSTERED ( [ServiceTicketID] ASC ));
                    end";
            var datas =
                    $@"
                        INSERT [{tableName}] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (N'{id}', N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
                        INSERT [{tableName}] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
                        INSERT [{tableName}] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
                        INSERT [{tableName}] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
                        INSERT [{tableName}] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
                 ";

            var insertRowScript =
                    $@"
                        INSERT [{tableName}] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), N'Insert One Row', N'Description Insert One Row', 1, 0, getdate(), NULL, 1)
                    ";

            var updateRowScript =
                    $@"Update [{tableName}] Set [Title] = 'Updated Row' Where ServiceTicketId = '{id}'";

            var deleteRowScript =
                    $@"Delete from  [{tableName}] Where ServiceTicketId = '{id}'";

            if (!tablesScript.ContainsKey(key))
                tablesScript.Add(key, tblScript);

            if (!datasScript.ContainsKey(key))
                datasScript.Add(key, datas);

            if (!insertOneRowScript.ContainsKey(key))
                insertOneRowScript.Add(key, insertRowScript);

            if (!updateOneRowScript.ContainsKey(key))
                updateOneRowScript.Add(key, updateRowScript);

            if (!deleteOneRowScript.ContainsKey(key))
                deleteOneRowScript.Add(key, deleteRowScript);
        }
        public void AddAllColumnsAvailabeTableScript(string key)
        {
            string tableName = PairDatabases.First(c => c.Key == key).Tables[0];
            string id = Guid.NewGuid().ToString();

            var tblScript =
                $@"
                    if (not exists (select * from sys.tables where name = '{tableName}'))
                    begin
                        CREATE TABLE [dbo].[{tableName}](
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
                         CONSTRAINT [PK_{tableName}] PRIMARY KEY CLUSTERED ( [ClientID] ASC))                
                     end;";
            var datas =
                    $@"
                     INSERT INTO [dbo].[{tableName}]
                               ([ClientID] ,[CBinary],[CBigInt],[CBit],[CChar10],[CDate],[CDateTime]
                               ,[CDateTime2],[CDateTimeOffset],[CDecimal64],[CFloat],[CInt],[CMoney]
                               ,[CNChar10],[CNumeric64],[CNVarchar50],[CNVarcharMax],[CReal]
                               ,[CSmallDateTime],[CSmallInt],[CSmallMoney],[CSqlVariant],[CTime7]
                               ,[CTinyint],[CUniqueIdentifier],[CVarbinary50],[CVarbinaryMax]
                               ,[CVarchar50],[CVarcharMax],[CXml])
                     VALUES
                               ('{id}',12345,10000000000000,1,'char10',GETDATE(),GETDATE(),GETDATE()
                               ,GETDATE(),23.1234,12.123,1,3148.29,'char10',23.1234
                               ,'nvarchar(50)','nvarchar(max)',12.34,GETDATE(),12,3148.29
                               ,GETDATE(),GETDATE(),1,NEWID(),123456,123456,'varchar(50)','varchar(max)'
                               ,'<root><client name=''Doe''>inner Doe client</client></root>')
                     INSERT INTO [dbo].[{tableName}]
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
                     INSERT INTO [dbo].[{tableName}]
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
                     INSERT INTO [dbo].[{tableName}]
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
                     INSERT INTO [dbo].[{tableName}]
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

            var insertRowScript =
                    $@"
                     INSERT INTO [dbo].[{tableName}]
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

            var updateRowScript =
                    $@"Update [{tableName}] Set [CNVarchar50] = 'Updated Row' Where ClientID = '{id}'";

            var deleteRowScript =
                    $@"Delete from  [{tableName}] Where ClientID = '{id}'";

            if (!tablesScript.ContainsKey(key))
                tablesScript.Add(key, tblScript);

            if (!datasScript.ContainsKey(key))
                datasScript.Add(key, datas);

            if (!insertOneRowScript.ContainsKey(key))
                insertOneRowScript.Add(key, insertRowScript);

            if (!updateOneRowScript.ContainsKey(key))
                updateOneRowScript.Add(key, updateRowScript);

            if (!deleteOneRowScript.ContainsKey(key))
                deleteOneRowScript.Add(key, deleteRowScript);
        }
        public void AddSqlVariantTableScript(string key)
        {
            string tableName = PairDatabases.First(c => c.Key == key).Tables[0];
            string id = Guid.NewGuid().ToString();
            var tblScript =
                $@"
                    if (not exists (select * from sys.tables where name = '{tableName}'))
                    begin
                        CREATE TABLE [dbo].[{tableName}](
	                    [ClientID] [uniqueidentifier] NOT NULL,
	                    [Value] [sql_variant] NULL,
                        CONSTRAINT [PK_{tableName}] PRIMARY KEY CLUSTERED ( [ClientID] ASC ));
                    end";
            var datas =
                    $@"

                        INSERT INTO [dbo].[{tableName}] ([ClientID] ,[Value])
                        VALUES ('{id}' ,getdate())

                        INSERT INTO [dbo].[{tableName}] ([ClientID] ,[Value])
                        VALUES (newid(),'varchar text')

                        INSERT INTO [dbo].[{tableName}] ([ClientID] ,[Value])
                        VALUES (newid() , 12)

                        INSERT INTO [dbo].[{tableName}] ([ClientID] ,[Value])
                        VALUES (newid() ,45.1234)

                        INSERT INTO [dbo].[{tableName}] ([ClientID] ,[Value])
                        VALUES (newid() , CONVERT(bigint, 120000))
                 ";

            var insertRowScript =
                    $@"
                        INSERT INTO [dbo].[{tableName}] ([ClientID] ,[Value])
                        VALUES (newid() , 12)
                    ";

            var updateRowScript =
                    $@"Update [{tableName}] Set [Value] = 'Updated Row' Where ClientID = '{id}'";

            var deleteRowScript =
                    $@"Delete from  [{tableName}] Where ClientID = '{id}'";

            if (!tablesScript.ContainsKey(key))
                tablesScript.Add(key, tblScript);

            if (!datasScript.ContainsKey(key))
                datasScript.Add(key, datas);

            if (!insertOneRowScript.ContainsKey(key))
                insertOneRowScript.Add(key, insertRowScript);

            if (!updateOneRowScript.ContainsKey(key))
                updateOneRowScript.Add(key, updateRowScript);

            if (!deleteOneRowScript.ContainsKey(key))
                deleteOneRowScript.Add(key, deleteRowScript);
        }

        private string GetCreationDBScript(string dbName, Boolean recreateDb = true)
        {
            if (recreateDb)
                return $@"if (exists (Select * from sys.databases where name = '{dbName}'))
                    begin
	                    alter database [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	                    drop database {dbName}
                    end
                    Create database {dbName}";
            else
                return $@"if not (exists (Select * from sys.databases where name = '{dbName}')) 
                          Create database {dbName}";

        }

        private string GetDeleteDatabaseScript(string dbName)
        {
            return $@"if (exists (Select * from sys.databases where name = '{dbName}'))
                    begin
	                    alter database [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	                    drop database {dbName}
                    end";
        }

        public void DeleteDatabases(PairDatabases db)
        {
            SqlConnection masterConnection = null;
            try
            {
                masterConnection = new SqlConnection(GetDatabaseConnectionString("master"));
                masterConnection.Open();
                var cmdDb = new SqlCommand(GetDeleteDatabaseScript(db.ServerDatabase), masterConnection);
                cmdDb.ExecuteNonQuery();
                cmdDb = new SqlCommand(GetDeleteDatabaseScript(db.ClientDatabase), masterConnection);
                cmdDb.ExecuteNonQuery();
                masterConnection.Close();

            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                if (masterConnection.State != ConnectionState.Closed)
                    masterConnection.Close();
            }

        }

        public void Dispose()
        {
            //this.DeleteAllDatabases();
        }
    }

    public struct PairDatabases
    {
        public string Key { get; set; }
        public string ServerDatabase { get; set; }
        public string ServerConnectionString
        {
            get
            {
                return CreateServerAndClientDatabase.GetDatabaseConnectionString(ServerDatabase);
            }
        }

        public string ClientConnectionString
        {
            get
            {
                return CreateServerAndClientDatabase.GetDatabaseConnectionString(ClientDatabase);
            }
        }
        public string ClientDatabase { get; set; }
        public List<String> Tables { get; set; }
        public PairDatabases(string key, string serverDatabase, string clientDatabase, List<string> tables)
        {
            this.Key = key;
            this.ServerDatabase = serverDatabase;
            this.ClientDatabase = clientDatabase;
            this.Tables = tables;
        }
        public PairDatabases(string key, string serverDatabase, string clientDatabase, string table)
        {
            this.Key = key;
            this.ServerDatabase = serverDatabase;
            this.ClientDatabase = clientDatabase;
            this.Tables = new List<string>();
            this.Tables.Add(table);
        }


    }
}
