using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Web;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Test.SqlUtils;
using Microsoft.AspNetCore.Http;
using System;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Test
{
    public class SyncVariantFixture : IDisposable
    {
        public string serverDbName => "Test_Variant_Server";
        public string client1DbName => "Test_Variant_Client";
        public string[] Tables => new string[] { "VariantTable" };

        private string createTableScript =
        $@"
        if (not exists (select * from sys.tables where name = 'VariantTable'))
        begin
            CREATE TABLE [dbo].[VariantTable](
	        [ClientID] [uniqueidentifier] NOT NULL,
	        [Value] [sql_variant] NULL,
            CONSTRAINT [PK_VariantTable] PRIMARY KEY CLUSTERED ( [ClientID] ASC ));
        end";

        private string datas =
        $@"
        INSERT INTO [dbo].[VariantTable] ([ClientID] ,[Value])
        VALUES (newid() ,getdate())

        INSERT INTO [dbo].[VariantTable] ([ClientID] ,[Value])
        VALUES (newid(),'varchar text')

        INSERT INTO [dbo].[VariantTable] ([ClientID] ,[Value])
        VALUES (newid() , 12)

        INSERT INTO [dbo].[VariantTable] ([ClientID] ,[Value])
        VALUES (newid() ,45.1234)

        INSERT INTO [dbo].[VariantTable] ([ClientID] ,[Value])
        VALUES (newid() , CONVERT(bigint, 120000))
        ";

        private HelperDB helperDb = new HelperDB();

        public String ServerConnectionString => HelperDB.GetDatabaseConnectionString(serverDbName);
        public String Client1ConnectionString => HelperDB.GetDatabaseConnectionString(client1DbName);

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

    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    public class SyncVariantTests : IClassFixture<SyncVariantFixture>
    {
        SyncVariantFixture fixture;
        SqlSyncProvider serverProvider;
        SqlSyncProvider clientProvider;
        SyncAgent agent;

        public SyncVariantTests(SyncVariantFixture fixture)
        {
            this.fixture = fixture;

            serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
            clientProvider = new SqlSyncProvider(fixture.Client1ConnectionString);

            agent = new SyncAgent(clientProvider, serverProvider, fixture.Tables);
        }

        //[Fact, TestPriority(1)]
        //public async Task Initialize()
        //{
        //    var session = await agent.SynchronizeAsync();

        //    Assert.Equal(5, session.TotalChangesDownloaded);
        //    Assert.Equal(0, session.TotalChangesUploaded);
        //}

        [Fact, TestPriority(1)]
        public async Task SyncThroughHttp()
        {
            using (var server = new KestrellTestServer())
            {
                var serverHandler = new RequestDelegate(async context =>
                {
                    SqlSyncProvider serverProvider = new SqlSyncProvider(this.fixture.ServerConnectionString);
                    SyncConfiguration configuration = new SyncConfiguration(this.fixture.Tables);
                    configuration.DownloadBatchSizeInKB = 500;

                    WebProxyServerProvider proxyServerProvider = new WebProxyServerProvider(serverProvider);
                    proxyServerProvider.Configuration = configuration;

                    await proxyServerProvider.HandleRequestAsync(context);
                });

                var clientHandler = new ResponseDelegate(async (serviceUri) =>
                {
                    var proxyProvider = new WebProxyClientProvider(new Uri(serviceUri));
                    var clientProvider = new SqlSyncProvider(this.fixture.Client1ConnectionString);

                    SyncAgent agent = new SyncAgent(clientProvider, proxyProvider);
                    var session = await agent.SynchronizeAsync();

                    Assert.Equal(5, session.TotalChangesDownloaded);
                    Assert.Equal(0, session.TotalChangesUploaded);
                });

                await server.Run(serverHandler, clientHandler);

            }

        }

    }
}
