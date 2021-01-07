using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests.UnitTests
{
    public partial class SqlServerHttpTestsSqlVariant : IDisposable
    {

        // Current test running
        private ITest test;
        private Stopwatch stopwatch;
        public ITestOutputHelper Output { get; }
        public WebServerOrchestrator WebServerOrchestrator { get; }

        private KestrellTestServer kestrell;

        public string ServiceUri { get; }
        public string ServerDatabaseName { get; }

        public SqlServerHttpTestsSqlVariant(ITestOutputHelper output)
        {

            // Getting the test running
            this.Output = output;
            var type = output.GetType();
            var testMember = type.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
            this.test = (ITest)testMember.GetValue(output);

            // Since we are creating a lot of databases
            // each database will have its own pool
            // Droping database will not clear the pool associated
            // So clear the pools on every start of a new test
            SqlConnection.ClearAllPools();

            // get the server provider (and db created) without seed
            this.ServerDatabaseName = HelperDatabase.GetRandomName("http_sv_");
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, ServerDatabaseName);

            var serverProvider = new SqlSyncProvider(cs);

            // create web remote orchestrator
            this.WebServerOrchestrator = new WebServerOrchestrator(serverProvider, new SyncOptions(), new WebServerOptions(), new SyncSetup());

            // Create a kestrell server
            this.kestrell = new KestrellTestServer(this.WebServerOrchestrator);

            // start server and get uri
            this.ServiceUri = this.kestrell.Run();


            this.stopwatch = Stopwatch.StartNew();
        }


        [Fact, TestPriority(1)]
        public virtual async Task Variant_Server_Insert()
        {
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, ServerDatabaseName, true);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.Add("Product");

            var dbNameClient = HelperDatabase.GetRandomName("tcp_cli_variant_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbNameClient, true);
            var clientCs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbNameClient);
            var clientProvider = new SqlSyncProvider(clientCs);

            var createProducTable = @"CREATE TABLE [dbo].[Product](
	                                    [ProductID] [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
	                                    [Name] [nvarchar](50) NOT NULL,
	                                    [ThumbNailPhoto] [sql_variant] NULL);
                                      INSERT INTO [Product] VALUES ('GEAR 02', 'Twelve');
                                      INSERT INTO [Product] VALUES ('GEAR 01', 12);
                                      ";

            await HelperDatabase.ExecuteScriptAsync(ProviderType.Sql, this.ServerDatabaseName, createProducTable);

            var agent = new SyncAgent(clientProvider, new WebClientOrchestrator(this.ServiceUri));

            var s = await agent.SynchronizeAsync();

            Assert.Equal(2, s.TotalChangesDownloaded);
            Assert.Equal(0, s.TotalChangesUploaded);
        }

        [Fact, TestPriority(1)]
        public virtual async Task Variant_Client_Insert()
        {
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, ServerDatabaseName, true);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.Add("Product");

            var dbNameClient = HelperDatabase.GetRandomName("tcp_cli_variant_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbNameClient, true);
            var clientCs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbNameClient);
            var clientProvider = new SqlSyncProvider(clientCs);

            var createProducTable = @"CREATE TABLE [dbo].[Product](
	                                    [ProductID] [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
	                                    [Name] [nvarchar](50) NOT NULL,
	                                    [ThumbNailPhoto] [sql_variant] NULL);
                                      INSERT INTO [Product] VALUES ('GEAR 02', 'Twelve');
                                      INSERT INTO [Product] VALUES ('GEAR 01', 12);
                                      ";

            await HelperDatabase.ExecuteScriptAsync(ProviderType.Sql, this.ServerDatabaseName, createProducTable);

            var agent = new SyncAgent(clientProvider, new WebClientOrchestrator(this.ServiceUri));

            var s = await agent.SynchronizeAsync();

            Assert.Equal(2, s.TotalChangesDownloaded);
            Assert.Equal(0, s.TotalChangesUploaded);

            var insScript = "INSERT INTO [Product] VALUES ('GEAR 03', N'Three');";

            await HelperDatabase.ExecuteScriptAsync(ProviderType.Sql, dbNameClient, insScript);

            s = await agent.SynchronizeAsync();

            Assert.Equal(0, s.TotalChangesDownloaded);
            Assert.Equal(1, s.TotalChangesUploaded);
        }

        [Fact, TestPriority(2)]
        public virtual async Task Variant_Update()
        {
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, ServerDatabaseName, true);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.Add("Product");

            var dbNameClient = HelperDatabase.GetRandomName("tcp_cli_variant_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbNameClient, true);
            var clientCs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbNameClient);
            var clientProvider = new SqlSyncProvider(clientCs);

            var createProducTable = @"CREATE TABLE [dbo].[Product](
	                                    [ProductID] [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
	                                    [Name] [nvarchar](50) NOT NULL,
	                                    [ThumbNailPhoto] [sql_variant] NULL);
                                      INSERT INTO [Product] VALUES ('GEAR 02', 'Twelve');
                                      INSERT INTO [Product] VALUES ('GEAR 01', 12);
                                      ";

            await HelperDatabase.ExecuteScriptAsync(ProviderType.Sql, this.ServerDatabaseName, createProducTable);

            var agent = new SyncAgent(clientProvider, new WebClientOrchestrator(this.ServiceUri));

            var s = await agent.SynchronizeAsync();

            var updateScript = "UPDATE Product Set ThumbNailPhoto = 12 where ProductID=1";

            await HelperDatabase.ExecuteScriptAsync(ProviderType.Sql, this.ServerDatabaseName, updateScript);

            s = await agent.SynchronizeAsync();

            Assert.Equal(1, s.TotalChangesDownloaded);
            Assert.Equal(0, s.TotalChangesUploaded);
        }

        [Fact, TestPriority(3)]
        public virtual async Task Variant_Delete()
        {
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, ServerDatabaseName, true);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.Add("Product");

            var dbNameClient = HelperDatabase.GetRandomName("tcp_cli_variant_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbNameClient, true);
            var clientCs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbNameClient);
            var clientProvider = new SqlSyncProvider(clientCs);

            var createProducTable = @"CREATE TABLE [dbo].[Product](
	                                    [ProductID] [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
	                                    [Name] [nvarchar](50) NOT NULL,
	                                    [ThumbNailPhoto] [sql_variant] NULL);
                                      INSERT INTO [Product] VALUES ('GEAR 02', 'Twelve');
                                      INSERT INTO [Product] VALUES ('GEAR 01', 12);
                                      ";

            await HelperDatabase.ExecuteScriptAsync(ProviderType.Sql, this.ServerDatabaseName, createProducTable);

            var agent = new SyncAgent(clientProvider, new WebClientOrchestrator(this.ServiceUri));

            var s = await agent.SynchronizeAsync();

            var updateScript = "DELETE Product where ProductID=1";

            await HelperDatabase.ExecuteScriptAsync(ProviderType.Sql, this.ServerDatabaseName, updateScript);

            s = await agent.SynchronizeAsync();

            Assert.Equal(1, s.TotalChangesDownloaded);
            Assert.Equal(0, s.TotalChangesUploaded);
        }

        [Fact, TestPriority(1)]
        public virtual async Task Variant_Snapshot_Server_Insert()
        {
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, ServerDatabaseName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, ServerDatabaseName);
            var serverProvider = new SqlSyncProvider(cs);

            // Create table
            var createProducTable = @"CREATE TABLE [dbo].[Product](
	                                    [ProductID] [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
	                                    [Name] [nvarchar](50) NOT NULL,
	                                    [ThumbNailPhoto] [sql_variant] NULL);
                                      INSERT INTO [Product] VALUES ('GEAR 02', 'Twelve');
                                      INSERT INTO [Product] VALUES ('GEAR 01', 12);
                                      ";

            await HelperDatabase.ExecuteScriptAsync(ProviderType.Sql, this.ServerDatabaseName, createProducTable);

            // snapshot directory options
            var snapshotDirctoryName = HelperDatabase.GetRandomName();
            var snapshotDirectory = Path.Combine(Environment.CurrentDirectory, snapshotDirctoryName);

            var options = new SyncOptions
            {
                SnapshotsDirectory = snapshotDirectory,
                BatchSize = 200
            };

            var setup = new SyncSetup(new string[] { "Product" });

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);

            var bi = await remoteOrchestrator.CreateSnapshotAsync();

            // configure server orchestrator
            this.WebServerOrchestrator.Options = options;
            this.WebServerOrchestrator.Setup = setup;


            var dbNameClient = HelperDatabase.GetRandomName("tcp_cli_variant_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbNameClient, true);
            var clientCs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbNameClient);
            var clientProvider = new SqlSyncProvider(clientCs);

            var agent = new SyncAgent(clientProvider, new WebClientOrchestrator(this.ServiceUri));

            var s = await agent.SynchronizeAsync();

            Assert.Equal(2, s.TotalChangesDownloaded);
            Assert.Equal(0, s.TotalChangesUploaded);
        }

        public void Dispose()
        {
            this.stopwatch.Stop();

            var str = $"{test.TestCase.DisplayName} : {this.stopwatch.Elapsed.Minutes}:{this.stopwatch.Elapsed.Seconds}.{this.stopwatch.Elapsed.Milliseconds}";
            Console.WriteLine(str);
            Debug.WriteLine(str);
        }
    }
}
