﻿using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Test.SqlUtils;
using System;
using System.Data.SqlClient;
using Microsoft.Data.Sqlite;
using System.IO;
using System.Threading.Tasks;
using Xunit;
using System.Data;

namespace Dotmim.Sync.Test
{
    public class SqliteSyncSimpleFixture : IDisposable
    {
        private string createTableScript =
        $@"if (not exists (select * from sys.tables where name = 'ServiceTickets'))
            begin
                CREATE TABLE [ServiceTickets](
	            [ServiceTicketID] [uniqueidentifier] NOT NULL,
	            [Title] [nvarchar](max) NOT NULL,
	            [Description] [nvarchar](max) NULL,
	            [StatusValue] [int] NOT NULL,
	            [EscalationLevel] [int] NOT NULL,
	            [Opened] [datetime] NULL,
	            [Closed] [datetime] NULL,
	            [CustomerID] [int] NULL,
                CONSTRAINT [PK_ServiceTickets] PRIMARY KEY CLUSTERED ( [ServiceTicketID] ASC ));
            end";

        private string datas =
        $@"
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 3', 'Description 3', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 4', 'Description 4', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre Client 1', 'Description Client 1', 1, 0, CAST('2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 6', 'Description 6', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 7', 'Description 7', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 3', 'Description 3', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 4', 'Description 4', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre Client 1', 'Description Client 1', 1, 0, CAST('2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 6', 'Description 6', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 7', 'Description 7', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 3', 'Description 3', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 4', 'Description 4', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre Client 1', 'Description Client 1', 1, 0, CAST('2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 6', 'Description 6', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 7', 'Description 7', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 3', 'Description 3', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 4', 'Description 4', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre Client 1', 'Description Client 1', 1, 0, CAST('2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 6', 'Description 6', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 7', 'Description 7', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 3', 'Description 3', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 4', 'Description 4', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre Client 1', 'Description Client 1', 1, 0, CAST('2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 6', 'Description 6', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 7', 'Description 7', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 3', 'Description 3', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 4', 'Description 4', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre Client 1', 'Description Client 1', 1, 0, CAST('2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 6', 'Description 6', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 7', 'Description 7', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 3', 'Description 3', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 4', 'Description 4', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre Client 1', 'Description Client 1', 1, 0, CAST('2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 6', 'Description 6', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 7', 'Description 7', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 3', 'Description 3', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 4', 'Description 4', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre Client 1', 'Description Client 1', 1, 0, CAST('2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 6', 'Description 6', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 7', 'Description 7', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 3', 'Description 3', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 4', 'Description 4', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre Client 1', 'Description Client 1', 1, 0, CAST('2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 6', 'Description 6', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 7', 'Description 7', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 3', 'Description 3', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 4', 'Description 4', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre Client 1', 'Description Client 1', 1, 0, CAST('2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 6', 'Description 6', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
            INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (newid(), 'Titre 7', 'Description 7', 1, 0, CAST('2016-07-29T16:36:41.733' AS DateTime), NULL, 10)
          ";

        private HelperDB helperDb = new HelperDB();
        private string serverDbName = "Test_Sqlite_Simple_Server";

        public string[] Tables => new string[] { "ServiceTickets" };

        public String ServerConnectionString => 
            HelperDB.GetDatabaseConnectionString(serverDbName);
        public SyncAgent Agent { get; set; }

        public String ClientSqliteConnectionString { get; set; }
        public string ClientSqliteFilePath => 
            Path.Combine(Directory.GetCurrentDirectory(), "sqliteTmpDb.db");

        public SqliteSyncSimpleFixture()
        {

            var builder = new SqliteConnectionStringBuilder {
                DataSource = ClientSqliteFilePath };
            this.ClientSqliteConnectionString = builder.ConnectionString;

            if (File.Exists(ClientSqliteFilePath))
                File.Delete(ClientSqliteFilePath);

            // create databases
            helperDb.CreateDatabase(serverDbName);
            // create table
            helperDb.ExecuteScript(serverDbName, createTableScript);
            // insert table
            helperDb.ExecuteScript(serverDbName, datas);

            var serverProvider = new SqlSyncProvider(ServerConnectionString);
            var clientProvider = new SqliteSyncProvider(ClientSqliteFilePath);

            //SqliteConnection c = new SqliteConnection($"Data Source=fabrikamde.db");
            //c.StateChange += (_, e) =>
            //{
            //    if (e.CurrentState == ConnectionState.Open)
            //    {
            //        var cmd = c.CreateCommand();
            //        cmd.CommandText = "PRAGMA journal_mode=WAL;";
            //        var i = cmd.ExecuteScalar();
            //    }
            //};

            //c.Open();

            //c.Close();


            //var totoClientProvider = new SqliteSyncProvider(
            //    @"Data Source=D:\database\myData.db;Journal Mode=WAL;Page Size=8192");

            //SqliteConnectionStringBuilder sqliteConnectionStringBuilder = new SqliteConnectionStringBuilder();


            Agent = new SyncAgent(clientProvider, serverProvider, Tables);

        }
        public void Dispose()
        {
            helperDb.DeleteDatabase(serverDbName);

            GC.Collect();
            GC.WaitForPendingFinalizers();

            if (File.Exists(ClientSqliteFilePath))
                File.Delete(ClientSqliteFilePath);
        }

    }

    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    public class SqliteSyncTests : IClassFixture<SqliteSyncSimpleFixture>
    {
        SqliteSyncSimpleFixture fixture;
        SyncAgent agent;

        public SqliteSyncTests(SqliteSyncSimpleFixture fixture)
        {
            this.fixture = fixture;
            this.agent = fixture.Agent;
        }

        [Fact, TestPriority(1)]
        public async Task Initialize()
        {
            var session = await agent.SynchronizeAsync();

            Assert.Equal(50, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(2)]
        public async Task SyncNoRows(SyncConfiguration conf)
        {
            agent.Configuration.DownloadBatchSizeInKB = conf.DownloadBatchSizeInKB;
            agent.Configuration.UseBulkOperations = conf.UseBulkOperations;
            agent.Configuration.SerializationFormat = conf.SerializationFormat;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(3)]
        public async Task InsertFromServer(SyncConfiguration conf)
        {
            var insertRowScript =
            $@"INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                VALUES (newid(), 'Insert One Row', 'Description Insert One Row', 1, 0, getdate(), NULL, 1)";

            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                using (var sqlCmd = new SqlCommand(insertRowScript, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            agent.Configuration.DownloadBatchSizeInKB = conf.DownloadBatchSizeInKB;
            agent.Configuration.UseBulkOperations = conf.UseBulkOperations;
            agent.Configuration.SerializationFormat = conf.SerializationFormat;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(4)]
        public async Task InsertFromClient(SyncConfiguration conf)
        {
            Guid newId = Guid.NewGuid();

            var insertRowScript =
            $@"INSERT INTO [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                VALUES (@id, 'Insert One Row in Sqlite client', 'Description Insert One Row', 1, 0, datetime('now'), NULL, 1)";

            int nbRowsInserted = 0;

            using (var sqlConnection = new SqliteConnection(fixture.ClientSqliteConnectionString))
            {
                using (var sqlCmd = new SqliteCommand(insertRowScript, sqlConnection))
                {
                    sqlCmd.Parameters.AddWithValue("@id", newId);

                    sqlConnection.Open();
                    nbRowsInserted = sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            if (nbRowsInserted < 0)
                throw new Exception("Row not inserted");

            agent.Configuration.DownloadBatchSizeInKB = conf.DownloadBatchSizeInKB;
            agent.Configuration.UseBulkOperations = conf.UseBulkOperations;
            agent.Configuration.SerializationFormat = conf.SerializationFormat;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(5)]
        public async Task UpdateFromClient(SyncConfiguration conf)
        {
            Guid newId = Guid.NewGuid();

            var insertRowScript =
            $@"INSERT INTO [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                VALUES (@id, 'Insert One Row in Sqlite client', 'Description Insert One Row', 1, 0, datetime('now'), NULL, 1)";

            using (var sqlConnection = new SqliteConnection(fixture.ClientSqliteConnectionString))
            {
                using (var sqlCmd = new SqliteCommand(insertRowScript, sqlConnection))
                {
                    sqlCmd.Parameters.AddWithValue("@id", newId);

                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            agent.Configuration.DownloadBatchSizeInKB = conf.DownloadBatchSizeInKB;
            agent.Configuration.UseBulkOperations = conf.UseBulkOperations;
            agent.Configuration.SerializationFormat = conf.SerializationFormat;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);

            var updateRowScript =
            $@" Update [ServiceTickets] Set [Title] = 'Updated from Sqlite Client side !' 
                    Where ServiceTicketId = @id";

            using (var sqlConnection = new SqliteConnection(fixture.ClientSqliteConnectionString))
            {
                using (var sqlCmd = new SqliteCommand(updateRowScript, sqlConnection))
                {
                    sqlCmd.Parameters.AddWithValue("@id", newId);

                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            agent.Configuration.DownloadBatchSizeInKB = conf.DownloadBatchSizeInKB;
            agent.Configuration.UseBulkOperations = conf.UseBulkOperations;
            agent.Configuration.SerializationFormat = conf.SerializationFormat;
            agent.Configuration.Add(fixture.Tables);
            session = await agent.SynchronizeAsync();

            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(6)]
        public async Task UpdateFromServer(SyncConfiguration conf)
        {
            var updateRowScript =
            $@" Declare @id uniqueidentifier;
                Select top 1 @id = ServiceTicketID from ServiceTickets;
                Update [ServiceTickets] Set [Title] = 'Updated from server' Where ServiceTicketId = @id";

            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                using (var sqlCmd = new SqlCommand(updateRowScript, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            agent.Configuration.DownloadBatchSizeInKB = conf.DownloadBatchSizeInKB;
            agent.Configuration.UseBulkOperations = conf.UseBulkOperations;
            agent.Configuration.SerializationFormat = conf.SerializationFormat;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(7)]
        public async Task DeleteFromServer(SyncConfiguration conf)
        {
            var updateRowScript =
            $@" Declare @id uniqueidentifier;
                Select top 1 @id = ServiceTicketID from ServiceTickets;
                Delete From [ServiceTickets] Where ServiceTicketId = @id";

            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                using (var sqlCmd = new SqlCommand(updateRowScript, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            agent.Configuration.DownloadBatchSizeInKB = conf.DownloadBatchSizeInKB;
            agent.Configuration.UseBulkOperations = conf.UseBulkOperations;
            agent.Configuration.SerializationFormat = conf.SerializationFormat;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(8)]
        public async Task DeleteFromClient(SyncConfiguration conf)
        {
            long count;
            var selectcount = $@"Select count(*) From [ServiceTickets]";
            var updateRowScript = $@"Delete From [ServiceTickets]";

            using (var sqlConnection = new SqliteConnection(fixture.ClientSqliteConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCmd = new SqliteCommand(selectcount, sqlConnection))
                    count = (long)sqlCmd.ExecuteScalar();
                using (var sqlCmd = new SqliteCommand(updateRowScript, sqlConnection))
                    sqlCmd.ExecuteNonQuery();
                sqlConnection.Close();
            }

            agent.Configuration.DownloadBatchSizeInKB = conf.DownloadBatchSizeInKB;
            agent.Configuration.UseBulkOperations = conf.UseBulkOperations;
            agent.Configuration.SerializationFormat = conf.SerializationFormat;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(count, session.TotalChangesUploaded);

            // check all rows deleted on server side
            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCmd = new SqlCommand(selectcount, sqlConnection))
                    count = (int)sqlCmd.ExecuteScalar();
            }
            Assert.Equal(0, count);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(9)]
        public async Task ConflictInsertInsertServerWins(SyncConfiguration conf)
        {
            Guid insertConflictId = Guid.NewGuid();

            using (var sqlConnection = new SqliteConnection(fixture.ClientSqliteConnectionString))
            {
                var script = $@"INSERT INTO [ServiceTickets] 
                            ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                            VALUES 
                            ( @id, 'Conflict Line Client', 'Description client', 1, 0, datetime('now'), NULL, 1)";

                using (var sqlCmd = new SqliteCommand(script, sqlConnection))
                {
                    sqlCmd.Parameters.AddWithValue("@id", insertConflictId);

                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                var script = $@"INSERT [ServiceTickets] 
                            ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                            VALUES 
                            (@id, 'Conflict Line Server', 'Description client', 1, 0, getdate(), NULL, 1)";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlCmd.Parameters.AddWithValue("@id", insertConflictId);

                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            agent.Configuration.DownloadBatchSizeInKB = conf.DownloadBatchSizeInKB;
            agent.Configuration.UseBulkOperations = conf.UseBulkOperations;
            agent.Configuration.SerializationFormat = conf.SerializationFormat;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            // check statistics
            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
            Assert.Equal(1, session.TotalSyncConflicts);

            string expectedRes = string.Empty;
            using (var sqlConnection = new SqliteConnection(fixture.ClientSqliteConnectionString))
            {
                var script = $@"Select Title from [ServiceTickets] 
                                Where ServiceTicketID=@id";

                using (var sqlCmd = new SqliteCommand(script, sqlConnection))
                {
                    sqlCmd.Parameters.AddWithValue("@id", insertConflictId);

                    sqlConnection.Open();
                    expectedRes = sqlCmd.ExecuteScalar() as string;
                    sqlConnection.Close();
                }
            }

            // check good title on client
            Assert.Equal("Conflict Line Server", expectedRes);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(10)]
        public async Task ConflictUpdateUpdateServerWins(SyncConfiguration conf)
        {
            Guid updateConflictId = Guid.NewGuid();
            using (var sqlConnection = new SqliteConnection(fixture.ClientSqliteConnectionString))
            {
                var script = $@"INSERT INTO [ServiceTickets] 
                            ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                            VALUES 
                            (@id, 'Line Client', 'Description client', 1, 0, datetime('now'), NULL, 1)";

                using (var sqlCmd = new SqliteCommand(script, sqlConnection))
                {
                    sqlCmd.Parameters.AddWithValue("@id", updateConflictId);

                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            agent.Configuration.DownloadBatchSizeInKB = conf.DownloadBatchSizeInKB;
            agent.Configuration.UseBulkOperations = conf.UseBulkOperations;
            agent.Configuration.SerializationFormat = conf.SerializationFormat;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            //just check, even if it's not the real test :)
            // check statistics
            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
            Assert.Equal(0, session.TotalSyncConflicts);


            using (var sqlConnection = new SqliteConnection(fixture.ClientSqliteConnectionString))
            {
                var script = $@"Update [ServiceTickets] 
                                Set Title = 'Updated from Client'
                                Where ServiceTicketId = @id";

                using (var sqlCmd = new SqliteCommand(script, sqlConnection))
                {
                    sqlCmd.Parameters.AddWithValue("@id", updateConflictId);

                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                var script = $@"Update [ServiceTickets] 
                                Set Title = 'Updated from Server'
                                Where ServiceTicketId = @id";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlCmd.Parameters.AddWithValue("@id", updateConflictId);

                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            session = await agent.SynchronizeAsync();

            // check statistics
            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
            Assert.Equal(1, session.TotalSyncConflicts);

            string expectedRes = string.Empty;
            using (var sqlConnection = new SqliteConnection(fixture.ClientSqliteConnectionString))
            {
                var script = $@"Select Title from [ServiceTickets] 
                            Where ServiceTicketID=@id";

                using (var sqlCmd = new SqliteCommand(script, sqlConnection))
                {
                    sqlCmd.Parameters.AddWithValue("@id", updateConflictId);

                    sqlConnection.Open();
                    expectedRes = sqlCmd.ExecuteScalar() as string;
                    sqlConnection.Close();
                }
            }

            // check good title on client
            Assert.Equal("Updated from Server", expectedRes);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(11)]
        public async Task ConflictUpdateUpdateClientWins(SyncConfiguration conf)
        {
            var id = Guid.NewGuid().ToString();

            using (var sqlConnection = new SqliteConnection(fixture.ClientSqliteConnectionString))
            {
                var script = $@"INSERT INTO [ServiceTickets] 
                            ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                            VALUES 
                            (@id, 'Line for conflict', 'Description client', 1, 0, datetime('now'), NULL, 1)";

                using (var sqlCmd = new SqliteCommand(script, sqlConnection))
                {
                    sqlCmd.Parameters.AddWithValue("@id", id);

                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            agent.Configuration.DownloadBatchSizeInKB = conf.DownloadBatchSizeInKB;
            agent.Configuration.UseBulkOperations = conf.UseBulkOperations;
            agent.Configuration.SerializationFormat = conf.SerializationFormat;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            //just check, even if it's not the real test :)
            // check statistics
            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
            Assert.Equal(0, session.TotalSyncConflicts);


            using (var sqlConnection = new SqliteConnection(fixture.ClientSqliteConnectionString))
            {
                var script = $@"Update [ServiceTickets] 
                                Set Title = 'Updated from Client'
                                Where ServiceTicketId = @id";

                using (var sqlCmd = new SqliteCommand(script, sqlConnection))
                {
                    sqlCmd.Parameters.AddWithValue("@id", id);

                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                var script = $@"Update [ServiceTickets] 
                                Set Title = 'Updated from Server'
                                Where ServiceTicketId = '{id}'";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            agent.ApplyChangedFailed += (s, args) => args.Action = ConflictAction.ClientWins;
           
            await Assert.RaisesAsync<ApplyChangeFailedEventArgs>(
                h => agent.ApplyChangedFailed += h,
                h => agent.ApplyChangedFailed -= h, async () =>
                {
                    session = await agent.SynchronizeAsync();
                });

            // check statistics
            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
            Assert.Equal(1, session.TotalSyncConflicts);

            string expectedRes = string.Empty;
            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                var script = $@"Select Title from [ServiceTickets] Where ServiceTicketID='{id}'";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    expectedRes = sqlCmd.ExecuteScalar() as string;
                    sqlConnection.Close();
                }
            }

            // check good title on client
            Assert.Equal("Updated from Client", expectedRes);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(12)]
        public async Task ConflictInsertInsertConfigurationClientWins(SyncConfiguration conf)
        {

            Guid id = Guid.NewGuid();

            using (var sqlConnection = new SqliteConnection(fixture.ClientSqliteConnectionString))
            {
                var script = $@"INSERT INTO [ServiceTickets] 
                            ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                            VALUES 
                            (@id, 'Conflict Line Client', 'Description client', 1, 0, datetime('now'), NULL, 1)";

                using (var sqlCmd = new SqliteCommand(script, sqlConnection))
                {
                    sqlCmd.Parameters.AddWithValue("@id", id);

                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                var script = $@"INSERT [ServiceTickets] 
                            ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) 
                            VALUES 
                            ('{id.ToString()}', 'Conflict Line Server', 'Description client', 1, 0, getdate(), NULL, 1)";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            agent.Configuration.DownloadBatchSizeInKB = conf.DownloadBatchSizeInKB;
            agent.Configuration.UseBulkOperations = conf.UseBulkOperations;
            agent.Configuration.SerializationFormat = conf.SerializationFormat;
            agent.Configuration.Add(fixture.Tables);
            agent.Configuration.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;
            var session = await agent.SynchronizeAsync();

            // check statistics
            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
            Assert.Equal(1, session.TotalSyncConflicts);

            string expectedRes = string.Empty;
            using (var sqlConnection = new SqlConnection(fixture.ServerConnectionString))
            {
                var script = $@"Select Title from [ServiceTickets] Where ServiceTicketID='{id.ToString()}'";

                using (var sqlCmd = new SqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    expectedRes = sqlCmd.ExecuteScalar() as string;
                    sqlConnection.Close();
                }
            }

            // check good title on client
            Assert.Equal("Conflict Line Client", expectedRes);
        }

    }
}
