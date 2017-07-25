using Dotmim.Sync.Core.Enumerations;
using Dotmim.Sync.Core.Proxy;
using Dotmim.Sync.SqlServer;
using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Core.Test
{
    public class SyncSqlAllColumnsAvailables : IDisposable
    {
    
        const string serverDBName = "ServerDBAllColumns";
        const string clientDBName = "ClientDBAllColumns";
        const string tableName = "AllColumnsTable";

        public String ServerDatabaseString { get; set; } = $@"Data Source=(localdb)\MSSQLLocalDB; Initial Catalog={serverDBName}; Integrated Security=true;";
        public String ClientDatabaseString { get; set; } = $@"Data Source=(localdb)\MSSQLLocalDB; Initial Catalog={clientDBName}; Integrated Security=true;";

        private SqlConnection serverConnection;
        private SqlConnection clientConnection;
        private SqlConnection masterConnection;

        public SyncSqlAllColumnsAvailables()
        {
            masterConnection = new SqlConnection(@"Data Source=(localdb)\MSSQLLocalDB; Initial Catalog=master; Integrated Security=true;");
            serverConnection = new SqlConnection(ServerDatabaseString);
            clientConnection = new SqlConnection(ClientDatabaseString);

            try
            {
                masterConnection.Open();
                var cmd = new SqlCommand(GetCreationDBScript(clientDBName), masterConnection);
                cmd.ExecuteNonQuery();
                cmd = new SqlCommand(GetCreationDBScript(serverDBName), masterConnection);
                cmd.ExecuteNonQuery();

                serverConnection.Open();
                cmd = new SqlCommand(GetSingleTableScript(), serverConnection);
                cmd.ExecuteNonQuery();
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                if (masterConnection.State != System.Data.ConnectionState.Closed)
                    masterConnection.Close();
                if (serverConnection.State != System.Data.ConnectionState.Closed)
                    serverConnection.Close();
            }

        }


        [Fact]
        public async Task Table_With_All_Columns()
        {
            SqlSyncProvider serverProvider = new SqlSyncProvider(ServerDatabaseString);
            SqlSyncProvider clientProvider = new SqlSyncProvider(ClientDatabaseString);

            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new[] { tableName });

            var s = await agent.SynchronizeAsync();

            Assert.Equal(1, s.TotalChangesDownloaded);
        }

        [Fact]
        public async Task Table_With_All_Columns_With_Kestrell()
        {
            using (var server = new KestrellTestServer())
            {
                var clientHandler = new ResponseDelegate(async baseAdress => {
                    var proxyProvider = new WebProxyClientProvider(new Uri(baseAdress), SerializationFormat.Json);
                    var clientProvider = new SqlSyncProvider(ClientDatabaseString);

                    SyncAgent agent = new SyncAgent(clientProvider, proxyProvider);
                    var s = await agent.SynchronizeAsync();
                    Assert.Equal(1, s.TotalChangesDownloaded);

                });

                var serverHandler = new RequestDelegate(async context =>
                {
                    // Create the internal provider
                    SqlSyncProvider serverProvider = new SqlSyncProvider(ServerDatabaseString);
                    ServiceConfiguration configuration = new ServiceConfiguration(new string[] { tableName });
                    serverProvider.SetConfiguration(configuration);

                    // Create the proxy provider
                    WebProxyServerProvider proxyServerProvider = new WebProxyServerProvider(serverProvider, SerializationFormat.Json);

                    try
                    {
                        await proxyServerProvider.HandleRequestAsync(context);
                    }
                    catch (WebSyncException webSyncException)
                    {
                        Console.WriteLine("Proxy Server WebSyncException : " + webSyncException.Message);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Proxy Server Exception : " + e.Message);
                        throw e;
                    }
                });

                await server.Run(serverHandler, clientHandler);
            };
        }


        public string GetSingleTableScript()
        {
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
                     end;
                     INSERT INTO [dbo].[{tableName}]
                               ([ClientID]
                               ,[CBinary]
                               ,[CBigInt]
                               ,[CBit]
                               ,[CChar10]
                               ,[CDate]
                               ,[CDateTime]
                               ,[CDateTime2]
                               ,[CDateTimeOffset]
                               ,[CDecimal64]
                               ,[CFloat]
                               ,[CInt]
                               ,[CMoney]
                               ,[CNChar10]
                               ,[CNumeric64]
                               ,[CNVarchar50]
                               ,[CNVarcharMax]
                               ,[CReal]
                               ,[CSmallDateTime]
                               ,[CSmallInt]
                               ,[CSmallMoney]
                               ,[CSqlVariant]
                               ,[CTime7]
                               ,[CTinyint]
                               ,[CUniqueIdentifier]
                               ,[CVarbinary50]
                               ,[CVarbinaryMax]
                               ,[CVarchar50]
                               ,[CVarcharMax]
                               ,[CXml])
                         VALUES
                               (NEWID()
                               ,12345
                               ,10000000000000
                               ,1
                               ,'char10'
                               ,GETDATE()
                               ,GETDATE()
                               ,GETDATE()
                               ,GETDATE()
                               ,23.1234
                               ,12.123
                               ,1
                               ,3148.29
                               ,'char10'
                               ,23.1234
                               ,'nvarchar(50)'
                               ,'nvarchar(max)'
                               ,12.34
                               ,GETDATE()
                               ,12
                               ,3148.29
                               ,GETDATE()
                               ,GETDATE()
                               ,1
                               ,NEWID()
                               ,123456
                               ,123456
                               ,'varchar(50)'
                               ,'varchar(max)'
                               ,'<root><client name=''Doe''>inner Doe client</client></root>'
	                    )";

            return tblScript;
        }

        public string GetCreationDBScript(string dbName)
        {
            var createDbScript =
                    $@"if (exists (Select * from sys.databases where name = '{dbName}'))
                    begin
	                    alter database [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	                    drop database {dbName}
                    end
                    Create database {dbName}";

            return createDbScript;
        }

        public string GetDeleteDatabaseScript(string dbName)
        {
            return $@"if (exists (Select * from sys.databases where name = '{dbName}'))
                    begin
	                    alter database [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	                    drop database {dbName}
                    end";
        }


        public void Dispose()
        {
            try
            {
                masterConnection.Open();
                var cmd = new SqlCommand(GetDeleteDatabaseScript(clientDBName), masterConnection);
                cmd.ExecuteNonQuery();
                cmd = new SqlCommand(GetDeleteDatabaseScript(serverDBName), masterConnection);
                cmd.ExecuteNonQuery();

            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                if (masterConnection.State != System.Data.ConnectionState.Closed)
                    masterConnection.Close();
            }

        }
    }
}
