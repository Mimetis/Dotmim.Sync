using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Filter;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests
{
    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]

    public abstract class TcpFilterTests : IClassFixture<HelperProvider>, IDisposable
    {
        private Stopwatch stopwatch;

        /// <summary>
        /// Gets the sync filtered tables involved in the tests
        /// </summary>
        public abstract SyncSetup FilterSetup { get; }

        /// <summary>
        /// Gets the filter parameter value
        /// </summary>
        public abstract List<SyncParameter> FilterParameters { get; }

        /// <summary>
        /// Gets the clients type we want to tests
        /// </summary>
        public abstract List<ProviderType> ClientsType { get; }

        /// <summary>
        /// Gets the server type we want to test
        /// </summary>
        public abstract ProviderType ServerType { get; }

        /// <summary>
        /// Get the server rows count
        /// </summary>
        public abstract int GetServerDatabaseRowsCount((string DatabaseName, ProviderType ProviderType, IOrchestrator Orchestrator) t);


        // abstract fixture used to run the tests
        protected readonly HelperProvider fixture;

        // Current test running
        private ITest test;

        /// <summary>
        /// Gets the remote orchestrator and its database name
        /// </summary>
        public (string DatabaseName, ProviderType ProviderType, RemoteOrchestrator RemoteOrchestrator) Server { get; private set; }

        /// <summary>
        /// Gets the dictionary of all local orchestrators with database name as key
        /// </summary>
        public List<(string DatabaseName, ProviderType ProviderType, LocalOrchestrator LocalOrchestrator)> Clients { get; set; }

        /// <summary>
        /// Gets a bool indicating if we should generate the schema for tables
        /// </summary>
        public bool UseFallbackSchema => ServerType == ProviderType.Sql;

        public ITestOutputHelper Output { get; }

        /// <summary>
        /// For each test, Create a server database and some clients databases, depending on ProviderType provided in concrete class
        /// </summary>
        public TcpFilterTests(HelperProvider fixture, ITestOutputHelper output)
        {

            // Getting the test running
            this.Output = output;
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
            var serverDatabaseName = HelperDatabase.GetRandomName("sv_");

            // create remote orchestrator
            var remoteOrchestrator = this.fixture.CreateOrchestrator<RemoteOrchestrator>(this.ServerType, serverDatabaseName);

            this.Server = (serverDatabaseName, this.ServerType, remoteOrchestrator);

            // Get all clients providers
            Clients = new List<(string DatabaseName, ProviderType ProviderType, LocalOrchestrator LocalOrhcestrator)>(this.ClientsType.Count);

            // Generate Client database
            foreach (var clientType in this.ClientsType)
            {
                var dbCliName = HelperDatabase.GetRandomName("cli_");
                var localOrchestrator = this.fixture.CreateOrchestrator<LocalOrchestrator>(clientType, dbCliName);

                HelperDatabase.CreateDatabaseAsync(clientType, dbCliName, true);

                this.Clients.Add((dbCliName, clientType, localOrchestrator));
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

            this.stopwatch.Stop();

            var str = $"{test.TestCase.DisplayName} : {this.stopwatch.Elapsed.Minutes}:{this.stopwatch.Elapsed.Seconds}.{this.stopwatch.Elapsed.Milliseconds}";
            Console.WriteLine(str);
            Debug.WriteLine(str);

        }

        [Fact, TestPriority(1)]
        public virtual async Task SchemaIsCreated()
        {
            // create a server db without seed
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false);

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator, this.FilterSetup);
                agent.Parameters.AddRange(this.FilterParameters);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);

                // Check we have the correct columns replicated
                using (var c = client.LocalOrchestrator.Provider.CreateConnection())
                {
                    await c.OpenAsync();

                    foreach (var setupTable in FilterSetup.Tables)
                    {
                        var tableClientManagerFactory = client.LocalOrchestrator.Provider.GetTableManagerFactory(setupTable.TableName, setupTable.SchemaName);
                        var tableClientManager = tableClientManagerFactory.CreateManagerTable(c);
                        var clientColumns = tableClientManager.GetColumns();

                        // Check we have the same columns count
                        if (setupTable.Columns.Count == 0)
                        {
                            using (var serverConnection = this.Server.RemoteOrchestrator.Provider.CreateConnection())
                            {
                                serverConnection.Open();
                                var tableServerManagerFactory = this.Server.RemoteOrchestrator.Provider.GetTableManagerFactory(setupTable.TableName, setupTable.SchemaName);
                                var tableServerManager = tableServerManagerFactory.CreateManagerTable(serverConnection);
                                var serverColumns = tableClientManager.GetColumns();

                                serverConnection.Close();

                                Assert.Equal(serverColumns.Count(), clientColumns.Count());

                                // Check we have the same columns names
                                foreach (var serverColumn in serverColumns)
                                    Assert.Contains(clientColumns, (col) => col.ColumnName == serverColumn.ColumnName);
                            }
                        }
                        else
                        {
                            Assert.Equal(setupTable.Columns.Count, clientColumns.Count());

                            // Check we have the same columns names
                            foreach (var setupColumn in setupTable.Columns)
                                Assert.Contains(clientColumns, (col) => col.ColumnName == setupColumn);
                        }
                    }
                    c.Close();

                }
            }
        }




        [Theory, TestPriority(2)]
        [ClassData(typeof(SyncOptionsData))]
        public virtual async Task RowsCount(SyncOptions options)
        {
            // create a server db and seed it
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator, 
                                          this.FilterSetup, options);
                agent.Parameters.AddRange(this.FilterParameters);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
            }
        }
    }
}
