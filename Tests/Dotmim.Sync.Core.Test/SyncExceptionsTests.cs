using Dotmim.Sync.Core.Test.SqlUtils;
using Dotmim.Sync.SqlServer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Core.Test
{
    [Collection("Sync")]
    public class SyncExceptionsTests : IClassFixture<CreateServerAndClientDatabase>
    {
        CreateServerAndClientDatabase fixture;

        // making tests only on the simple database
        PairDatabases simpleDb;
        public SyncExceptionsTests(CreateServerAndClientDatabase fixture)
        {
            this.fixture = fixture;

            simpleDb = fixture.PairDatabases.First(pd => pd.Key == "SimpleSync");

            fixture.GenerateDatabasesAndTables(simpleDb, false, false, false);
        }

        [Fact]
        public async Task Bad_Server_Connection()
        {
            SqlSyncProvider serverProvider = new SqlSyncProvider(@"Data Source=(localdb)\MSSQLLocalDB; Initial Catalog=WrongDB; Integrated Security=true;");
            SqlSyncProvider clientProvider = new SqlSyncProvider(simpleDb.ClientConnectionString);

            ServiceConfiguration configuration = new ServiceConfiguration(new string[] { "ServiceTickets" });

            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);

            var ex = await Assert.ThrowsAsync<SyncException>(async () =>
            {
                var session = await agent.SynchronizeAsync();
            });

            Assert.Equal(SyncExceptionType.DataStore, ex.ExceptionType);
        }
    }
}
