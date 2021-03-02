using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Threading.Tasks;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Web.Server;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests
{

    //[TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    public abstract class HttpTestsBase : IClassFixture<HelperProvider>, IDisposable
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
        /// Service Uri provided by kestrell when starts
        /// </summary>
        public string ServiceUri { get; private set; }

        /// <summary>
        /// Gets the Web Server Orchestrator used for the tests
        /// </summary>
        public WebServerOrchestrator WebServerOrchestrator { get; private set; }

        /// <summary>
        /// Get the server rows count
        /// </summary>
        public abstract int GetServerDatabaseRowsCount((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) t);

        /// <summary>
        /// Create a provider
        /// </summary>
        public abstract CoreProvider CreateProvider(ProviderType providerType, string dbName);

        /// <summary>
        /// Create database, seed it, with or without schema
        /// </summary>
        public abstract Task EnsureDatabaseSchemaAndSeedAsync((string DatabaseName,
            ProviderType ProviderType, CoreProvider Provider) t, bool useSeeding = false, bool useFallbackSchema = false);


        /// <summary>
        /// Create an empty database
        /// </summary>
        public abstract Task CreateDatabaseAsync(ProviderType providerType, string dbName, bool recreateDb = true);


        // abstract fixture used to run the tests
        protected readonly HelperProvider fixture;

        // Current test running
        private ITest test;
        private KestrellTestServer kestrell;


        /// <summary>
        /// Gets the remote orchestrator and its database name
        /// </summary>
        public (string DatabaseName, ProviderType ProviderType, CoreProvider Provider) Server { get; private set; }

        /// <summary>
        /// Gets the dictionary of all local orchestrators with database name as key
        /// </summary>
        public List<(string DatabaseName, ProviderType ProviderType, CoreProvider Provider)> Clients { get; set; }

        /// <summary>
        /// Gets a bool indicating if we should generate the schema for tables
        /// </summary>
        public bool UseFallbackSchema => ServerType == ProviderType.Sql;

        /// <summary>
        /// ctor
        /// </summary>
        public HttpTestsBase(HelperProvider fixture, ITestOutputHelper output)
        {

            // Getting the test running
            var type = output.GetType();
            var testMember = type.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
            this.test = (ITest)testMember.GetValue(output);

            this.stopwatch = Stopwatch.StartNew();

            this.fixture = fixture;

            // Since we are creating a lot of databases
            // each database will have its own pool
            // Droping database will not clear the pool associated
            // So clear the pools on every start of a new test
            SqlConnection.ClearAllPools();
            MySqlConnection.ClearAllPools();

            // get the server provider (and db created) without seed
            var serverDatabaseName = HelperDatabase.GetRandomName("http_sv_");

            var serverProvider = this.CreateProvider(this.ServerType, serverDatabaseName);

            // create web remote orchestrator
            this.WebServerOrchestrator = new WebServerOrchestrator(serverProvider, new SyncOptions(), new WebServerOptions(), new SyncSetup());

            // public property
            this.Server = (serverDatabaseName, this.ServerType, serverProvider);

            // Create a kestrell server
            this.kestrell = new KestrellTestServer(this.WebServerOrchestrator, this.UseFiddler);

            // start server and get uri
            this.ServiceUri = this.kestrell.Run();

            // Get all clients providers
            Clients = new List<(string, ProviderType, CoreProvider)>(this.ClientsType.Count);

            // Generate Client database
            foreach (var clientType in this.ClientsType)
            {
                var dbCliName = HelperDatabase.GetRandomName("http_cli_");
                var localProvider = this.CreateProvider(clientType, dbCliName);
                this.Clients.Add((dbCliName, clientType, localProvider));
            }

        }

        /// <summary>
        /// Drop all databases used for the tests
        /// </summary>
        public void Dispose()
        {
            HelperDatabase.DropDatabase(this.ServerType, Server.DatabaseName);

            foreach (var client in Clients)
            {
                HelperDatabase.DropDatabase(client.ProviderType, client.DatabaseName);
            }

            this.kestrell.Dispose();

            this.stopwatch.Stop();

            var str = $"{test.TestCase.DisplayName} : {this.stopwatch.Elapsed.Minutes}:{this.stopwatch.Elapsed.Seconds}.{this.stopwatch.Elapsed.Milliseconds}";
            Console.WriteLine(str);
            Debug.WriteLine(str);

        }
    }

}
