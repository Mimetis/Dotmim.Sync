using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Test.Misc;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Tests.V2
{
    public abstract class HttpTests : IClassFixture<HelperProvider>
    {
        /// <summary>
        /// Gets the sync tables involved in the tests
        /// </summary>
        public abstract string[] Tables { get; }

        /// <summary>
        /// Gets the clients type we want to tests
        /// </summary>
        public abstract ProviderType ClientsType { get; }

        /// <summary>
        /// Gets the server type we want to test
        /// </summary>
        public abstract ProviderType ServerType { get; }

        /// <summary>
        /// Get the server rows count
        /// </summary>
        public abstract int GetServerDatabaseRowsCount(Core.ProviderType providerType, CoreProvider provider);


        // abstract fixture used to run the tests
        protected readonly HelperProvider fixture;

        /// <summary>
        /// ctor
        /// </summary>
        public HttpTests(HelperProvider fixture) => this.fixture = fixture;

        [Fact]
        public virtual async Task RowsCount()
        {
            //// get classical orchestrators with seeding on server
            //var orc = await this.fixture.CreateProviders(this.ServerType, this.ClientsType, true, true);

            //// Get count of rows
            //var rowsCount = this.GetServerDatabaseRowsCount(ServerType, orc.RemoteOrchestrator.Provider);

            //// set tables
            //orc.WebServerOrchestrator.Setup = new SyncSetup(this.Tables);

            //// Create a kestrell server
            //var server = new KestrellTestServer(orc.WebServerOrchestrator);

            //// start server and get uri
            //var serviceUri = server.Run();

            //// Execute a sync on all clients and check results
            //foreach (var clientOrchestrator in orc.LocalOrchestrators)
            //{
            //    // Create my web client proxy orchestrator
            //    orc.WebClientOrchestrator.ServiceUri = serviceUri;

            //    var agent = new SyncAgent(clientOrchestrator, orc.WebClientOrchestrator);

            //    var s = await agent.SynchronizeAsync();

            //    Assert.Equal(rowsCount, s.TotalChangesDownloaded);
            //    Assert.Equal(0, s.TotalChangesUploaded);
            //}

            //// clean db
            //this.fixture.DropDatabases(orc);
        }

        [Fact]
        public virtual async Task SchemaIsCreated()
        {
            //// get classical orchestrators
            //var orc = await this.fixture.CreateProviders(this.ServerType, this.ClientsType, true, false);

            //// set tables
            //orc.WebServerOrchestrator.Setup = new SyncSetup(this.Tables);

            //// Create a kestrell server
            //var server = new KestrellTestServer(orc.WebServerOrchestrator);

            //// start server and get uri
            //var serviceUri = server.Run();

            //// Execute a sync on all clients and check results
            //foreach (var clientOrchestrator in orc.LocalOrchestrators)
            //{
            //    // Create my web client proxy orchestrator
            //    orc.WebClientOrchestrator.ServiceUri = serviceUri;

            //    var agent = new SyncAgent(clientOrchestrator, orc.WebClientOrchestrator);

            //    var s = await agent.SynchronizeAsync();

            //    Assert.Equal(0, s.TotalChangesDownloaded);
            //    Assert.Equal(0, s.TotalChangesUploaded);
            //}

            //// stop server
            //await server.StopAsync();

            //// clean db
            //this.fixture.DropDatabases(orc);

        }

    }
}
