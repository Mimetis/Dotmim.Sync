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
    public class SyncTwoTablesTests : IClassFixture<CreateServerAndClientDatabase>
    {
        CreateServerAndClientDatabase fixture;
        PairDatabases db;
        public SyncTwoTablesTests(CreateServerAndClientDatabase fixture)
        {
            this.fixture = fixture;
            db = this.fixture.PairDatabases.First(pd => pd.Key == "TwoTablesSync");
        }

        [Fact, TestPriority(1)]
        public async Task Enable_Sync()
        {
            // recreate databases, don't create client schema, add datas on server
            fixture.GenerateDatabasesAndTables(db, true, false, true);

            SqlSyncProvider serverProvider = new SqlSyncProvider(db.ServerConnectionString);
            SqlSyncProvider clientProvider = new SqlSyncProvider(db.ClientConnectionString);

            ServiceConfiguration configuration = new ServiceConfiguration(db.Tables.ToArray());

            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);

            var session = await agent.SynchronizeAsync();

            Assert.Equal(6, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);



        }

        [Fact, TestPriority(2)]
        public async Task Cascade_Delete_From_Server()
        {
            using (var sqlConnection = new SqlConnection(db.ServerConnectionString))
            {
                var script = $@"Delete from ServiceTickets; Delete from Customers";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
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

            Assert.Equal(6, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }
    }
}
