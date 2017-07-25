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
    public class SyncSqlVariant : IDisposable
    {
        const string serverDBName = "ServerDBVariant";
        const string clientDBName = "ClientDBVariant";
        const string tableName = "TblVariant";

        public String ServerDatabaseString { get; set; } = $@"Data Source=(localdb)\MSSQLLocalDB; Initial Catalog={serverDBName}; Integrated Security=true;";
        public String ClientDatabaseString { get; set; } = $@"Data Source=(localdb)\MSSQLLocalDB; Initial Catalog={clientDBName}; Integrated Security=true;";

        private SqlConnection serverConnection;
        private SqlConnection clientConnection;
        private SqlConnection masterConnection;

        public SyncSqlVariant()
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
        public async Task Table_With_Sql_Variant()
        {
            SqlSyncProvider serverProvider = new SqlSyncProvider(ServerDatabaseString);
            SqlSyncProvider clientProvider = new SqlSyncProvider(ClientDatabaseString);

            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new[] { tableName });

            var s = await agent.SynchronizeAsync();

            Assert.Equal(3, s.TotalChangesDownloaded);
        }

        [Fact]
        public async Task Table_With_Sql_Variant_With_Kestrell()
        {
            using (var server = new KestrellTestServer())
            {
                var clientHandler = new ResponseDelegate(async baseAdress => {
                    var proxyProvider = new WebProxyClientProvider(new Uri(baseAdress), SerializationFormat.Json);
                    var clientProvider = new SqlSyncProvider(ClientDatabaseString);

                    SyncAgent agent = new SyncAgent(clientProvider, proxyProvider);
                    var s = await agent.SynchronizeAsync();
                    Assert.Equal(3, s.TotalChangesDownloaded);

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
	                    [Value] [sql_variant] NULL,
                        CONSTRAINT [PK_{tableName}] PRIMARY KEY CLUSTERED ( [ClientID] ASC ));

                        INSERT INTO [dbo].[{tableName}] ([ClientID] ,[Value])
                        VALUES ('A6D3A194-2D4E-4BA9-8524-23585613E70A' ,getdate())

                        INSERT INTO [dbo].[{tableName}] ([ClientID] ,[Value])
                        VALUES ('B6D3A194-2D4E-4BA9-8524-23585613E70A' ,'varchar text')

                        INSERT INTO [dbo].[{tableName}] ([ClientID] ,[Value])
                        VALUES ('E6D3A194-2D4E-4BA9-8524-23585613E70A' , 12)
                    end
                 ";

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

            serverConnection.Dispose();
            masterConnection.Dispose();
        }
    }
}
