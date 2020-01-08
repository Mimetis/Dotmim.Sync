using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Test.Misc;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.Sqlite;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests.V2
{
    public abstract class HttpTests : IClassFixture<HelperProvider>, IDisposable
    {
        private Stopwatch stopwatch;

        /// <summary>
        /// Gets the sync tables involved in the tests
        /// </summary>
        public abstract string[] Tables { get; }

        /// <summary>
        /// Gets the clients type we want to tests
        /// </summary>
        public abstract List<ProviderType> ClientsType { get; }

        /// <summary>
        /// Gets the server type we want to test
        /// </summary>
        public abstract ProviderType ServerType { get; }

        /// <summary>
        /// Gets if fiddler is in use
        /// </summary>
        public abstract bool UseFiddler { get; }

        /// <summary>
        /// Get the server rows count
        /// </summary>
        public abstract int GetServerDatabaseRowsCount((string DatabaseName, ProviderType ProviderType, IOrchestrator Orchestrator) t);


        // abstract fixture used to run the tests
        protected readonly HelperProvider fixture;

        // Current test running
        private ITest test;
        private KestrellTestServer kestrell;

        /// <summary>
        /// Gets the remote orchestrator and its database name
        /// </summary>
        public (string DatabaseName, ProviderType ProviderType, WebServerOrchestrator WebServerOrchestrator) Server { get; private set; }

        /// <summary>
        /// Gets the dictionary of all local orchestrators with database name as key
        /// </summary>
        public List<(string DatabaseName, ProviderType ProviderType, LocalOrchestrator LocalOrchestrator, WebClientOrchestrator WebClientOrchestrator)> Clients { get; set; }


        /// <summary>
        /// ctor
        /// </summary>
        public HttpTests(HelperProvider fixture, ITestOutputHelper output)
        {

            // Getting the test running
            var type = output.GetType();
            var testMember = type.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
            this.test = (ITest)testMember.GetValue(output);

            Console.WriteLine($"{test.DisplayName}");
            Debug.WriteLine(test.DisplayName);
            this.stopwatch = Stopwatch.StartNew();


            this.fixture = fixture;

            // get the server provider (and db created) without seed
            var serverDatabaseName = HelperDatabase.GetRandomName("sv_");

            // Create an empty server database
            HelperDatabase.CreateDatabaseAsync(this.ServerType, serverDatabaseName, true);

            // create remote orchestrator
            var webServerOrchestrator = this.fixture.CreateOrchestrator<WebServerOrchestrator>(this.ServerType, serverDatabaseName);

            // public property
            this.Server = (serverDatabaseName, this.ServerType, webServerOrchestrator);

            // Create a kestrell server
            this.kestrell = new KestrellTestServer(this.Server, this.UseFiddler);

            // start server and get uri
            var serviceUri = this.kestrell.Run();

            // Get all clients providers
            Clients = new List<(string, ProviderType, LocalOrchestrator, WebClientOrchestrator)>(this.ClientsType.Count);

            // Generate Client database
            foreach (var clientType in this.ClientsType)
            {
                var dbCliName = HelperDatabase.GetRandomName("cli_");
                // create local orchestratpr
                var localOrchestrator = this.fixture.CreateOrchestrator<LocalOrchestrator>(clientType, dbCliName);

                // create local proxy client
                var webclientOrchestrator = new WebClientOrchestrator(serviceUri);

                // create database
                HelperDatabase.CreateDatabaseAsync(clientType, dbCliName, true);

                this.Clients.Add((dbCliName, clientType, localOrchestrator, webclientOrchestrator));
            }

        }

        /// <summary>
        /// Drop all databases used for the tests
        /// </summary>
        public void Dispose()
        {
            HelperDatabase.DropDatabase(this.ServerType, Server.DatabaseName);

            foreach (var client in Clients)
                HelperDatabase.DropDatabase(client.ProviderType, client.DatabaseName);

            this.kestrell.Dispose();

            this.stopwatch.Stop();

            var str = $"{this.stopwatch.Elapsed.Minutes}:{this.stopwatch.Elapsed.Seconds}.{this.stopwatch.Elapsed.Milliseconds}";
            Console.WriteLine(str);
            Debug.WriteLine(str);

        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public virtual async Task RowsCount(SyncOptions options)
        {
            // create a server db and seed it
            this.fixture.EnsureDatabaseSchemaAndSeed(this.Server, true, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            this.Server.WebServerOrchestrator.Setup = new SyncSetup(Tables);

            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator, null, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
            }
        }

        [Fact]
        public virtual async Task SchemaIsCreated()
        {
            // create a server db without seed
            this.fixture.EnsureDatabaseSchemaAndSeed(Server, true, false);

            // configure server orchestrator
            this.Server.WebServerOrchestrator.Setup = new SyncSetup(Tables);

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
            }


        }


        /// <summary>
        /// Check a bad connection should raise correct error
        /// </summary>
        [Fact]
        public async Task BadConnection_FromServer_ShouldRaiseError()
        {
            // configure server orchestrator
            this.Server.WebServerOrchestrator.Setup = new SyncSetup(Tables);

            // change the remote orchestrator connection string
            Server.WebServerOrchestrator.Provider.ConnectionString = $@"Server=unknown;Database=unknown;UID=sa;PWD=unknown";

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync();

                });

                Assert.Equal(SyncExceptionSide.ServerSide, se.Side);

            }
        }


    }
}
