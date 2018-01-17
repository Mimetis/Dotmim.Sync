using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Test.SqlUtils;
using Dotmim.Sync.Tests.Misc;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Tests
{

    public class SyncCrossReferencesFixture : IDisposable
    {
        private string createTableScript =
        $@" CREATE TABLE [dbo].[Client](
	            [ClientId] [uniqueidentifier] NOT NULL,
	            [EmployeeId] [uniqueidentifier] NULL,
	            [FirstName] [nvarchar](50) NOT NULL,
            CONSTRAINT [PK_Client] PRIMARY KEY CLUSTERED ([ClientId] ASC) )         
            CREATE TABLE [dbo].[Employee](
	            [EmployeeId] [uniqueidentifier] NOT NULL,
	            [ClientId] [uniqueidentifier] NULL,
	            [FirstName] [nvarchar](50) NOT NULL,
            CONSTRAINT [PK_Employee] PRIMARY KEY CLUSTERED ([EmployeeId] ASC) )         
            ALTER TABLE [dbo].[Client]  WITH CHECK ADD  CONSTRAINT [FK_Client_Employee] FOREIGN KEY([EmployeeId])
            REFERENCES [dbo].[Employee] ([EmployeeId])
            ALTER TABLE [dbo].[Client] CHECK CONSTRAINT [FK_Client_Employee]
            ALTER TABLE [dbo].[Employee]  WITH CHECK ADD  CONSTRAINT [FK_Employee_Client] FOREIGN KEY([ClientId])
            REFERENCES [dbo].[Client] ([ClientId])
            ALTER TABLE [dbo].[Employee] CHECK CONSTRAINT [FK_Employee_Client]";

        private string datas =
        $@"
            INSERT [dbo].[Client] ([ClientId], [FirstName]) VALUES (N'0505c3cc-c6ee-4184-a6e5-2106bd1f9690', N'Sébastien')
            INSERT [dbo].[Employee] ([EmployeeId], [ClientId], [FirstName]) VALUES (N'52ef3ab9-d3e3-40fe-b81a-140eff5c9002', N'0505c3cc-c6ee-4184-a6e5-2106bd1f9690', N'Joe')
            UPDATE [dbo].[Client] Set [EmployeeId] = N'52ef3ab9-d3e3-40fe-b81a-140eff5c9002' WHERE ClientId = N'0505c3cc-c6ee-4184-a6e5-2106bd1f9690'
         ";

        private HelperDB helperDb = new HelperDB();
        private string serverDbName = "Test_CrossReferencesServer";
        private string client1DbName = "Test_CrossRererencesClient";

        public string[] Tables => new string[] { "Client", "Employee" };

        public String ServerConnectionString => HelperDB.GetDatabaseConnectionString(serverDbName);
        public String Client1ConnectionString => HelperDB.GetDatabaseConnectionString(client1DbName);

        public SyncCrossReferencesFixture()
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
    public class SyncCrossReferencesTests : IClassFixture<SyncCrossReferencesFixture>
    {
        SqlSyncProvider serverProvider;
        SqlSyncProvider clientProvider;
        SyncCrossReferencesFixture fixture;
        SyncAgent agent;

        public SyncCrossReferencesTests(SyncCrossReferencesFixture fixture)
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
            var ex = await Assert.ThrowsAsync<SyncException>(async () => await agent.SynchronizeAsync());

            Assert.IsType(typeof(SyncException), ex);
        }
    }
}
