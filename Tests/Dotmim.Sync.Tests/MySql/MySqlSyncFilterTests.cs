using Dotmim.Sync.MySql;
using System;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Dotmim.Sync.Filter;
using Xunit;
using Dotmim.Sync.Test.SqlUtils;
using Dotmim.Sync.Tests.Misc;
using MySql.Data.MySqlClient;

namespace Dotmim.Sync.Test
{
    public class MySqlSyncFilterFixture : IDisposable
    {
        public string createTableScript =
        $@"Create table ServiceTickets(
	            ServiceTicketID varchar(36) NOT NULL,
	            Title nvarchar(1024) NOT NULL,
	            Description nvarchar(1024) NULL,
	            StatusValue int NOT NULL,
	            EscalationLevel int NOT NULL,
	            Opened datetime NULL,
	            Closed datetime NULL,
	            CustomerID int NULL,
                PRIMARY KEY (ServiceTicketID)
             )";

        public string datas =
        $@"
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('8388f45f-20c0-4b8e-ad4e-f8144e8f9dfd', 'Titre 3', 'Description 3', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('989c2c38-2a3a-454a-8ca4-8890a66bbc2d', 'Titre 4', 'Description 4', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('63ab2c9b-72eb-44db-a3be-b50d862e7bc7', 'Titre Client 1', 'Description Client 1', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('de4b70c9-49a8-4dd3-98da-f77894893acb', 'Titre 6', 'Description 6', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('1433dd00-1721-4cfc-8496-aeaa8f4ccba9', 'Titre 7', 'Description 7', 1, 0, NULL, NULL, 10)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('56211e1d-04c7-489b-8b0f-211eb08275b7', 'Titre 3', 'Description 3', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('3b7903ef-a29c-4541-a4da-9bda012e06e5', 'Titre 4', 'Description 4', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('8aa950b0-c852-4658-9720-03ea10f698ce', 'Titre Client 1', 'Description Client 1', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('a1b73f7c-c524-4cd6-8d49-9eeabad93ce1', 'Titre 6', 'Description 6', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('e0814f8a-982f-4972-86e3-7711a0fb8b47', 'Titre 7', 'Description 7', 1, 0, NULL, NULL, 10)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('0bbacb6e-f0db-4be9-a803-d01ef010f96a', 'Titre 3', 'Description 3', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('52a243e8-315f-4f30-9567-4604631bf627', 'Titre 4', 'Description 4', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('cceac854-e453-4793-8973-dc2bba770a83', 'Titre Client 1', 'Description Client 1', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('bd233396-0312-4afc-8cd1-d90eef7e127c', 'Titre 6', 'Description 6', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('86595c1b-6414-4897-a5e8-a2acdfa9cb86', 'Titre 7', 'Description 7', 1, 0, NULL, NULL, 10)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('ed982acc-de7a-4e0b-86b8-9ac582bb1e27', 'Titre 3', 'Description 3', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('5dde2029-c33a-4303-b9f7-30d0cb47e35f', 'Titre 4', 'Description 4', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('ab3c4e7b-fb0e-4490-b625-cbaf12ef058e', 'Titre Client 1', 'Description Client 1', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('ea757327-c94f-41a6-a03f-fd714ab9d64e', 'Titre 6', 'Description 6', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('b9481c8b-5686-4453-8863-26cd0e1278ec', 'Titre 7', 'Description 7', 1, 0, NULL, NULL, 10)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('4344714f-7ed4-42a7-8449-ba13e506add1', 'Titre 3', 'Description 3', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('31641838-a36a-4bb7-bf30-8fc96c5105a5', 'Titre 4', 'Description 4', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('3c0ff4d1-5607-452a-9020-daeddba5b1be', 'Titre Client 1', 'Description Client 1', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('9754a4b8-980c-4058-9b1b-99e825c2d665', 'Titre 6', 'Description 6', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('4e33bbdc-6714-4e65-bccf-75e867a400cd', 'Titre 7', 'Description 7', 1, 0, NULL, NULL, 10)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('e1e3db9c-e5e6-40e3-bb90-22def923b5c3', 'Titre 3', 'Description 3', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('05d187b4-6044-4d1d-ab5e-d940843edd8e', 'Titre 4', 'Description 4', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('2f208af7-c48b-4522-8c50-cec07b2b7aca', 'Titre Client 1', 'Description Client 1', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('2f58ce6d-56b6-425f-9875-5002d2182bd5', 'Titre 6', 'Description 6', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('8349aee4-8028-4b20-9858-7fa945f3b774', 'Titre 7', 'Description 7', 1, 0, NULL, NULL, 10)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('a09d7141-9d8d-48af-9bba-d7ccf5f3a07e', 'Titre 3', 'Description 3', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('86d8c7d9-a625-489d-8522-e8ac57eaca17', 'Titre 4', 'Description 4', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('1bd07799-fd85-4100-9c14-9901d3f3a5e0', 'Titre Client 1', 'Description Client 1', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('220f0d43-c985-41ad-9696-fa42e3b87168', 'Titre 6', 'Description 6', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('bd97d144-9962-4121-aaf5-863e2499a853', 'Titre 7', 'Description 7', 1, 0, NULL, NULL, 10)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('ae4b007a-57bf-4714-a01d-33ab925731f4', 'Titre 3', 'Description 3', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('238135e1-ab4a-4765-abe6-d296e11fafa0', 'Titre 4', 'Description 4', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('fed0a17f-af87-4708-a233-279ef260a33b', 'Titre Client 1', 'Description Client 1', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('35755138-0279-47ea-9842-68f5c0827811', 'Titre 6', 'Description 6', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('de3fda93-d87b-4235-9ad9-6cb800fdbfca', 'Titre 7', 'Description 7', 1, 0, NULL, NULL, 10)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('ba61bb2f-83b7-4afb-b257-bf4c86e58ae0', 'Titre 3', 'Description 3', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('4929d777-0626-4808-b48d-886035323021', 'Titre 4', 'Description 4', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('6d73d6c2-e14c-4285-ba09-ef43f156de9c', 'Titre Client 1', 'Description Client 1', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('88474938-abc4-4c74-821e-10563b9d50f2', 'Titre 6', 'Description 6', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('581e49bb-7704-406e-8ad4-3d265153101a', 'Titre 7', 'Description 7', 1, 0, NULL, NULL, 10)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('ffa94a52-f131-40c1-9c20-d901cf6b018d', 'Titre 3', 'Description 3', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('eaea01bf-f593-49df-a7c5-177a5cbea3ea', 'Titre 4', 'Description 4', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('87927129-8551-4dec-885f-fd065c584b5e', 'Titre Client 1', 'Description Client 1', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('32630cb5-5af3-42a4-bae5-a9c706ba3ef8', 'Titre 6', 'Description 6', 1, 0, NULL, NULL, 1)
            INSERT INTO ServiceTickets (`ServiceTicketID`, `Title`, `Description`, `StatusValue`, `EscalationLevel`, `Opened`, `Closed`, `CustomerID`) VALUES ('b884feb5-e4ab-4927-95d1-d5e921348459', 'Titre 7', 'Description 7', 1, 0, NULL, NULL, 10)
          ";

        public HelperDB helperDb = new HelperDB();
        public string serverDbName = "Test_MySql_SyncFilter_Server";
        public string client1DbName = "test_mysql_SyncFilter_client01";
        public string client2DbName = "test_mysql_SyncFilter_client02";
        public string client3DbName = "test_mysql_SyncFilter_client03";

        public string[] Tables => new string[] { "ServiceTickets" };

        public String ServerConnectionString => HelperDB.GetMySqlDatabaseConnectionString(serverDbName);

        public String Client1ConnectionString => HelperDB.GetMySqlDatabaseConnectionString(client1DbName);
        public String Client2ConnectionString => HelperDB.GetMySqlDatabaseConnectionString(client2DbName);
        public String Client3ConnectionString => HelperDB.GetMySqlDatabaseConnectionString(client3DbName);

        public MySqlSyncFilterFixture()
        {
            // create databases
            helperDb.CreateMySqlDatabase(serverDbName);
            // create table
            helperDb.ExecuteMySqlScript(serverDbName, createTableScript);
            // insert table
            foreach (var line in datas.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries).Where(l => !string.IsNullOrWhiteSpace(l)))
                helperDb.ExecuteMySqlScript(serverDbName, line);

            helperDb.CreateMySqlDatabase(client1DbName);
            helperDb.CreateMySqlDatabase(client2DbName);
            helperDb.CreateMySqlDatabase(client3DbName);
        }

        public void Dispose()
        {
            //helperDb.DropMySqlDatabase(serverDbName);
            helperDb.DropMySqlDatabase(client1DbName);
            helperDb.DropMySqlDatabase(client2DbName);
            helperDb.DropMySqlDatabase(client3DbName);
        }
    }

    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    public class MySqlSyncFilterTests : IClassFixture<MySqlSyncFilterFixture>
    {
        MySqlSyncFilterFixture fixture;

        public MySqlSyncFilterTests(MySqlSyncFilterFixture fixture)
        {
            this.fixture = fixture;
        }

        [Fact, TestPriority(1)]
        public async Task Initialize()
        {
            var serverProvider = new MySqlSyncProvider(fixture.ServerConnectionString);
            var clientProvider = new MySqlSyncProvider(fixture.Client1ConnectionString);

            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new[] { "ServiceTickets" });
            agent.Configuration.Filters.Add(new FilterClause("ServiceTickets", "CustomerID"));
            agent.Parameters.Add("ServiceTickets", "CustomerID", 1);

            var session = await agent.SynchronizeAsync();

            // Only 4 lines should be downloaded
            Assert.Equal(40, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);

        }

        [Fact, TestPriority(2)]
        public async Task AddFilterTableAndNormalTable()
        {
            var serverProvider = new MySqlSyncProvider(fixture.ServerConnectionString);
            var cli2Provider = new MySqlSyncProvider(fixture.Client2ConnectionString);
            var cli3Provider = new MySqlSyncProvider(fixture.Client3ConnectionString);


            // first agent with parameter for filter
            SyncAgent agent2 = new SyncAgent(cli2Provider, serverProvider, new[] { "ServiceTickets" });
            agent2.Configuration.Filters.Add(new FilterClause("ServiceTickets", "CustomerID"));
            agent2.Parameters.Add("ServiceTickets", "CustomerID", 1);

            // second agent with no parameter
            SyncAgent agent3 = new SyncAgent(cli3Provider, serverProvider, new[] { "ServiceTickets" });
            agent3.Configuration.Filters.Add(new FilterClause("ServiceTickets", "CustomerID"));

            var session2 = await agent2.SynchronizeAsync();
            var session3 = await agent3.SynchronizeAsync();

            // Only 4 lines should be downloaded
            Assert.Equal(40, session2.TotalChangesDownloaded);
            Assert.Equal(50, session3.TotalChangesDownloaded);

            session2 = await agent2.SynchronizeAsync();
            session3 = await agent3.SynchronizeAsync();

            Assert.Equal(0, session2.TotalChangesDownloaded);
            Assert.Equal(0, session3.TotalChangesDownloaded);

            // Update one server row outside filter
            using (var sqlConnection = new MySqlConnection(fixture.ServerConnectionString))
            {
                var textUpdateOneRow = "Update ServiceTickets set Opened=CURDATE() where CustomerID=10";

                using (var sqlCmd = new MySqlCommand(textUpdateOneRow, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            session2 = await agent2.SynchronizeAsync();
            session3 = await agent3.SynchronizeAsync();

            Assert.Equal(0, session2.TotalChangesDownloaded);
            Assert.Equal(10, session3.TotalChangesDownloaded);
        }

        [Fact, TestPriority(3)]
        public async Task UpdateNoFilter()
        {
            // Update one server row outside filter
            using (var sqlConnection = new MySqlConnection(fixture.ServerConnectionString))
            {
                var textUpdateOneRow = "Update ServiceTickets set Opened=CURDATE() where CustomerID=10";

                using (var sqlCmd = new MySqlCommand(textUpdateOneRow, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            var serverProvider = new MySqlSyncProvider(fixture.ServerConnectionString);
            var cli1Provider = new MySqlSyncProvider(fixture.Client2ConnectionString);
            var cli2Provider = new MySqlSyncProvider(fixture.Client3ConnectionString);

            // first agent with parameter for filter
            SyncAgent agent1 = new SyncAgent(cli1Provider, serverProvider, new[] { "ServiceTickets" });
            agent1.Configuration.Filters.Add(new FilterClause("ServiceTickets", "CustomerID"));
            agent1.Parameters.Add("ServiceTickets", "CustomerID", 1);

            // second agent with no parameter
            SyncAgent agent2 = new SyncAgent(cli2Provider, serverProvider, new[] { "ServiceTickets" });
            agent2.Configuration.Filters.Add(new FilterClause("ServiceTickets", "CustomerID"));


            var session1 = await agent1.SynchronizeAsync();
            var session2 = await agent2.SynchronizeAsync();

            Assert.Equal(0, session1.TotalChangesDownloaded);
            Assert.Equal(10, session2.TotalChangesDownloaded);
        }

        [Fact, TestPriority(4)]
        public async Task UpdateFilter()
        {
            // Update one server row outside filter
            using (var sqlConnection = new MySqlConnection(fixture.ServerConnectionString))
            {
                var textUpdateOneRow = "Update ServiceTickets set Opened=CURDATE() where CustomerID=1";

                using (var sqlCmd = new MySqlCommand(textUpdateOneRow, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            var serverProvider = new MySqlSyncProvider(fixture.ServerConnectionString);
            var cli1Provider = new MySqlSyncProvider(fixture.Client2ConnectionString);
            var cli2Provider = new MySqlSyncProvider(fixture.Client3ConnectionString);




            // first agent with parameter for filter
            SyncAgent agent1 = new SyncAgent(cli1Provider, serverProvider, new[] { "ServiceTickets" });
            agent1.Configuration.Filters.Add(new FilterClause("ServiceTickets", "CustomerID"));
            agent1.Parameters.Add("ServiceTickets", "CustomerID", 1);

            // second agent with no parameter
            SyncAgent agent2 = new SyncAgent(cli2Provider, serverProvider, new[] { "ServiceTickets" });
            agent2.Configuration.Filters.Add(new FilterClause("ServiceTickets", "CustomerID"));


            var session1 = await agent1.SynchronizeAsync();
            var session2 = await agent2.SynchronizeAsync();

            Assert.Equal(40, session1.TotalChangesDownloaded);
            Assert.Equal(40, session2.TotalChangesDownloaded);
        }

        [Fact, TestPriority(5)]
        public async Task DeleteNoFilter()
        {
            // Update one server row outside filter
            using (var sqlConnection = new MySqlConnection(fixture.ServerConnectionString))
            {
                var textUpdateOneRow = "Delete ServiceTickets Where CustomerID=10";

                using (var sqlCmd = new MySqlCommand(textUpdateOneRow, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            var serverProvider = new MySqlSyncProvider(fixture.ServerConnectionString);
            var cli1Provider = new MySqlSyncProvider(fixture.Client2ConnectionString);
            var cli2Provider = new MySqlSyncProvider(fixture.Client3ConnectionString);

            
            // first agent with parameter for filter
            SyncAgent agent1 = new SyncAgent(cli1Provider, serverProvider, new[] { "ServiceTickets" });
            // Add a filter
            agent1.Configuration.Filters.Add(new FilterClause("ServiceTickets", "CustomerID"));
            agent1.Parameters.Add("ServiceTickets", "CustomerID", 1);

            // second agent with no parameter
            SyncAgent agent2 = new SyncAgent(cli2Provider, serverProvider, new[] { "ServiceTickets" });
            agent2.Configuration.Filters.Add(new FilterClause("ServiceTickets", "CustomerID"));


            var session1 = await agent1.SynchronizeAsync();
            var session2 = await agent2.SynchronizeAsync();

            Assert.Equal(0, session1.TotalChangesDownloaded);
            Assert.Equal(10, session2.TotalChangesDownloaded);
        }
    }
}
