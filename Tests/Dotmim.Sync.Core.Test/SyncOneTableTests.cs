using Dotmim.Sync.Core.Test.Misc;
using Dotmim.Sync.Core.Test.SqlUtils;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Xunit;
namespace Dotmim.Sync.Core.Test
{
    [Collection("Sync")]
    [TestCaseOrderer("Dotmim.Sync.Core.Test.Misc.PriorityOrderer", "Dotmim.Sync.Core.Test")]
    public class SyncOneTableTests : IClassFixture<CreateServerAndClientDatabase>
    {
        CreateServerAndClientDatabase fixture;
        List<PairDatabases> lstPairs;
        public SyncOneTableTests(CreateServerAndClientDatabase fixture)
        {
            this.fixture = fixture;
            lstPairs = this.fixture.PairDatabases.Where(pd => pd.Key == "SimpleSync" 
                || pd.Key == "VariantSync" 
                || pd.Key == "AllColumnsSync").ToList();
        }

        [Fact, TestPriority(1)]
        public async Task Enable_Sync()
        {
            foreach (var db in lstPairs)
                fixture.GenerateDatabasesAndTables(db, true, false, true);

            foreach (var db in lstPairs)
            {
                SqlSyncProvider serverProvider = new SqlSyncProvider(db.ServerConnectionString);
                SqlSyncProvider clientProvider = new SqlSyncProvider(db.ClientConnectionString);

                ServiceConfiguration configuration = new ServiceConfiguration(db.Tables.ToArray());

                SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);

                var session = await agent.SynchronizeAsync();

                Assert.Equal(5, session.TotalChangesDownloaded);
                Assert.Equal(0, session.TotalChangesUploaded);

            }

        }

        [Fact, TestPriority(2)]
        public async Task No_Rows()
        {

            foreach (var db in lstPairs)
            {
                SqlSyncProvider serverProvider = new SqlSyncProvider(db.ServerConnectionString);
                SqlSyncProvider clientProvider = new SqlSyncProvider(db.ClientConnectionString);

                ServiceConfiguration configuration = new ServiceConfiguration(db.Tables.ToArray());

                SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);

                var session = await agent.SynchronizeAsync();

                Assert.Equal(0, session.TotalChangesDownloaded);
                Assert.Equal(0, session.TotalChangesUploaded);

            }
        }

        [Fact, TestPriority(2)]
        public async Task One_Row_From_Server()
        {
            foreach (var db in lstPairs)
            {
                using (var sqlConnection = new SqlConnection(db.ServerConnectionString))
                {
                    using (var sqlCmd = new SqlCommand(fixture.insertOneRowScript[db.Key], sqlConnection))
                    {
                        sqlConnection.Open();
                        sqlCmd.ExecuteNonQuery();
                        sqlConnection.Close();
                    }
                }

                SqlSyncProvider serverProvider = new SqlSyncProvider(db.ServerConnectionString);
                SqlSyncProvider clientProvider = new SqlSyncProvider(db.ClientConnectionString);

                ServiceConfiguration configuration = new ServiceConfiguration(db.Tables.ToArray());

                SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);

                var session = await agent.SynchronizeAsync();

                Assert.Equal(1, session.TotalChangesDownloaded);
                Assert.Equal(0, session.TotalChangesUploaded);
            }
        }

        [Fact, TestPriority(2)]
        public async Task One_Row_From_Client()
        {
            foreach (var db in lstPairs)
            {
                using (var sqlConnection = new SqlConnection(db.ClientConnectionString))
                {
                    using (var sqlCmd = new SqlCommand(fixture.insertOneRowScript[db.Key], sqlConnection))
                    {
                        sqlConnection.Open();
                        sqlCmd.ExecuteNonQuery();
                        sqlConnection.Close();
                    }
                }

                SqlSyncProvider serverProvider = new SqlSyncProvider(db.ServerConnectionString);
                SqlSyncProvider clientProvider = new SqlSyncProvider(db.ClientConnectionString);

                ServiceConfiguration configuration = new ServiceConfiguration(db.Tables.ToArray());

                SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);

                var session = await agent.SynchronizeAsync();

                Assert.Equal(0, session.TotalChangesDownloaded);
                Assert.Equal(1, session.TotalChangesUploaded);
            }
        }

    }
}
