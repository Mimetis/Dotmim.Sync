using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class SyncAgentTests
    {

        public SyncAgentTests()
        {

        }

        private void CheckConstructor(SyncAgent agent)
        {
            Assert.Equal(SyncSessionState.Ready, agent.SessionState);
            Assert.Null(agent.Schema);
            Assert.NotNull(agent.LocalOrchestrator);
            Assert.NotNull(agent.RemoteOrchestrator);
            Assert.NotNull(agent.LocalOrchestrator.Options);
            Assert.NotNull(agent.RemoteOrchestrator.Options);
            Assert.NotNull(agent.LocalOrchestrator.Setup);
            Assert.NotNull(agent.RemoteOrchestrator.Setup);
            Assert.Same(agent.LocalOrchestrator.Options, agent.RemoteOrchestrator.Options);
            Assert.Same(agent.LocalOrchestrator.Setup, agent.RemoteOrchestrator.Setup);

        }

        [Fact]
        public void SyncAgent_FirstConstructor_LocalOrchestrator_ShouldMatch_RemoteOrchestrator()
        {
            var clientProvider = new SqlSyncProvider();
            var serverProvider = new SqlSyncProvider();
            var tables = new string[] { "Customer" };

            var agent = new SyncAgent(clientProvider, serverProvider, tables);

            this.CheckConstructor(agent);

            Assert.Equal(SyncOptions.DefaultScopeName, agent.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.LocalOrchestrator.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.RemoteOrchestrator.ScopeName);
            Assert.Single(agent.LocalOrchestrator.Setup.Tables);
            Assert.Single(agent.RemoteOrchestrator.Setup.Tables);
            Assert.Equal("Customer", agent.LocalOrchestrator.Setup.Tables[0].TableName);
            Assert.Equal("Customer", agent.RemoteOrchestrator.Setup.Tables[0].TableName);
        }

        [Fact]
        public void SyncAgent_FirstConstructor_SetupTables_ShouldBe_Empty_When_TablesArgIsNull()
        {
            var clientProvider = new SqlSyncProvider();
            var serverProvider = new SqlSyncProvider();
            string[] tables = null;

            var agent = new SyncAgent(clientProvider, serverProvider, tables);

            this.CheckConstructor(agent);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.LocalOrchestrator.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.RemoteOrchestrator.ScopeName);
            Assert.Empty(agent.LocalOrchestrator.Setup.Tables);
            Assert.Empty(agent.RemoteOrchestrator.Setup.Tables);
        }


        [Fact]
        public void SyncAgent_FirstConstructor_LocalOrchestrator_ShouldMatch_RemoteOrchestrator_When_ScopeNameIsDefined()
        {
            var clientProvider = new SqlSyncProvider();
            var serverProvider = new SqlSyncProvider();
            var tables = new string[] { "Customer" };

            var agent = new SyncAgent(clientProvider, serverProvider, tables, "CustomerScope");

            this.CheckConstructor(agent);

            Assert.Equal("CustomerScope", agent.ScopeName);
            Assert.Equal("CustomerScope", agent.LocalOrchestrator.ScopeName);
            Assert.Equal("CustomerScope", agent.RemoteOrchestrator.ScopeName);
            Assert.Single(agent.LocalOrchestrator.Setup.Tables);
            Assert.Single(agent.RemoteOrchestrator.Setup.Tables);
            Assert.Equal("Customer", agent.LocalOrchestrator.Setup.Tables[0].TableName);
            Assert.Equal("Customer", agent.RemoteOrchestrator.Setup.Tables[0].TableName);
        }

        [Fact]
        public void SyncAgent_SecondConstructor_LocalOrchestrator_ShouldMatch_RemoteOrchestrator()
        {
            var clientProvider = new SqlSyncProvider();
            var serverProvider = new SqlSyncProvider();

            var agent = new SyncAgent(clientProvider, serverProvider);

            this.CheckConstructor(agent);

            Assert.Equal(SyncOptions.DefaultScopeName, agent.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.LocalOrchestrator.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.RemoteOrchestrator.ScopeName);
            Assert.Empty(agent.LocalOrchestrator.Setup.Tables);
            Assert.Empty(agent.RemoteOrchestrator.Setup.Tables);
        }

        [Fact]
        public void SyncAgent_SecondConstructor_LocalOrchestrator_ShouldMatch_RemoteOrchestrator_When_ScopeNameIsDefined()
        {
            var clientProvider = new SqlSyncProvider();
            var serverProvider = new SqlSyncProvider();

            var agent = new SyncAgent(clientProvider, serverProvider, "CustomerScope");

            this.CheckConstructor(agent);

            Assert.Equal("CustomerScope", agent.ScopeName);
            Assert.Equal("CustomerScope", agent.LocalOrchestrator.ScopeName);
            Assert.Equal("CustomerScope", agent.RemoteOrchestrator.ScopeName);
            Assert.Empty(agent.LocalOrchestrator.Setup.Tables);
            Assert.Empty(agent.RemoteOrchestrator.Setup.Tables);
        }

        [Fact]
        public void SyncAgent_ThirdConstructor_LocalOrchestrator_ShouldMatch_RemoteOrchestrator()
        {
            var clientProvider = new SqlSyncProvider();
            var serverProvider = new SqlSyncProvider();
            var options = new SyncOptions();
            var tables = new string[] { "Customer" };

            var agent = new SyncAgent(clientProvider, serverProvider, options, tables);

            this.CheckConstructor(agent);
            Assert.Same(options, agent.LocalOrchestrator.Options);
            Assert.Same(options, agent.RemoteOrchestrator.Options);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.LocalOrchestrator.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.RemoteOrchestrator.ScopeName);
            Assert.Single(agent.LocalOrchestrator.Setup.Tables);
            Assert.Single(agent.RemoteOrchestrator.Setup.Tables);
            Assert.Equal("Customer", agent.LocalOrchestrator.Setup.Tables[0].TableName);
            Assert.Equal("Customer", agent.RemoteOrchestrator.Setup.Tables[0].TableName);
        }

        [Fact]
        public void SyncAgent_FourthConstructor_LocalOrchestrator_ShouldMatch_RemoteOrchestrator()
        {
            var clientProvider = new SqlSyncProvider();
            var serverProvider = new SqlSyncProvider();
            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "Customer" });

            var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

            this.CheckConstructor(agent);
            Assert.Same(options, agent.LocalOrchestrator.Options);
            Assert.Same(options, agent.RemoteOrchestrator.Options);
            Assert.Same(setup, agent.LocalOrchestrator.Setup);
            Assert.Same(setup, agent.RemoteOrchestrator.Setup);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.LocalOrchestrator.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.RemoteOrchestrator.ScopeName);
            Assert.Single(agent.LocalOrchestrator.Setup.Tables);
            Assert.Single(agent.RemoteOrchestrator.Setup.Tables);
            Assert.Equal("Customer", agent.LocalOrchestrator.Setup.Tables[0].TableName);
            Assert.Equal("Customer", agent.RemoteOrchestrator.Setup.Tables[0].TableName);
        }

        [Fact]
        public void SyncAgent_FourthConstructor_LocalOrchestrator_ShouldMatch_RemoteOrchestrator_When_TablesArgIsNull()
        {
            var clientProvider = new SqlSyncProvider();
            var serverProvider = new SqlSyncProvider();
            var options = new SyncOptions();
            var setup = new SyncSetup();

            var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

            this.CheckConstructor(agent);
            Assert.Same(options, agent.LocalOrchestrator.Options);
            Assert.Same(options, agent.RemoteOrchestrator.Options);
            Assert.Same(setup, agent.LocalOrchestrator.Setup);
            Assert.Same(setup, agent.RemoteOrchestrator.Setup);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.LocalOrchestrator.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.RemoteOrchestrator.ScopeName);
            Assert.Empty(agent.LocalOrchestrator.Setup.Tables);
            Assert.Empty(agent.RemoteOrchestrator.Setup.Tables);
        }


        [Fact]
        public void SyncAgent_FifthConstructor_LocalOrchestrator_ShouldMatch_RemoteOrchestrator()
        {
            var clientProvider = new SqlSyncProvider();

            // this options and setup will be overriden by the constructor
            var remoteOptions = new SyncOptions();
            var remoteSetup = new SyncSetup(new string[] { "Product", "ProductCategory" });
            
            var remoteOrchestrator = new RemoteOrchestrator(new SqlSyncProvider(), remoteOptions, remoteSetup);

            var agent = new SyncAgent(clientProvider, remoteOrchestrator, new string[] { "Customer" });

            this.CheckConstructor(agent);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.LocalOrchestrator.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.RemoteOrchestrator.ScopeName);
            Assert.Single(agent.LocalOrchestrator.Setup.Tables);
            Assert.Single(agent.RemoteOrchestrator.Setup.Tables);
            Assert.Equal("Customer", agent.LocalOrchestrator.Setup.Tables[0].TableName);
            Assert.Equal("Customer", agent.RemoteOrchestrator.Setup.Tables[0].TableName);

        }

        [Fact]
        public void SyncAgent_FifthConstructor_LocalOrchestrator_ShouldMatch_WebClientOrchestrator()
        {
            var clientProvider = new SqlSyncProvider();
            var remoteOrchestrator = new WebClientOrchestrator("http://localhost/api");

            var agent = new SyncAgent(clientProvider, remoteOrchestrator, new string[] { "Customer" });

            this.CheckConstructor(agent);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.LocalOrchestrator.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.RemoteOrchestrator.ScopeName);
            Assert.Single(agent.LocalOrchestrator.Setup.Tables);
            Assert.Single(agent.RemoteOrchestrator.Setup.Tables);
            Assert.Equal("Customer", agent.LocalOrchestrator.Setup.Tables[0].TableName);
            Assert.Equal("Customer", agent.RemoteOrchestrator.Setup.Tables[0].TableName);

        }

        [Fact]
        public void SyncAgent_FifthConstructor_LocalOrchestrator_ShouldMatch_RemoteOrchestrator_With_ScopeNameDefined()
        {
            var clientProvider = new SqlSyncProvider();
            // this options and setup will be overriden by the constructor
            var remoteOptions = new SyncOptions();
            var remoteSetup = new SyncSetup(new string[] { "Product", "ProductCategory" });
            var remoteOrchestrator = new RemoteOrchestrator(new SqlSyncProvider(), remoteOptions, remoteSetup);

            var agent = new SyncAgent(clientProvider, remoteOrchestrator, new string[] { "Customer" }, "CustomerScope");

            this.CheckConstructor(agent);
            Assert.Equal("CustomerScope", agent.ScopeName);
            Assert.Equal("CustomerScope", agent.LocalOrchestrator.ScopeName);
            Assert.Equal("CustomerScope", agent.RemoteOrchestrator.ScopeName);
            Assert.Single(agent.LocalOrchestrator.Setup.Tables);
            Assert.Single(agent.RemoteOrchestrator.Setup.Tables);
            Assert.Equal("Customer", agent.LocalOrchestrator.Setup.Tables[0].TableName);
            Assert.Equal("Customer", agent.RemoteOrchestrator.Setup.Tables[0].TableName);

        }

        [Fact]
        public void SyncAgent_FifthConstructor_LocalOrchestrator_ShouldMatch_WebClientOrchestrator_With_ScopeNameDefined()
        {
            var clientProvider = new SqlSyncProvider();
            var remoteOrchestrator = new WebClientOrchestrator("http://localhost/api");

            var agent = new SyncAgent(clientProvider, remoteOrchestrator, new string[] { "Customer" }, "CustomerScope");

            this.CheckConstructor(agent);
            Assert.Equal("CustomerScope", agent.ScopeName);
            Assert.Equal("CustomerScope", agent.LocalOrchestrator.ScopeName);
            Assert.Equal("CustomerScope", agent.RemoteOrchestrator.ScopeName);
            Assert.Single(agent.LocalOrchestrator.Setup.Tables);
            Assert.Single(agent.RemoteOrchestrator.Setup.Tables);
            Assert.Equal("Customer", agent.LocalOrchestrator.Setup.Tables[0].TableName);
            Assert.Equal("Customer", agent.RemoteOrchestrator.Setup.Tables[0].TableName);

        }


        [Fact]
        public void SyncAgent_SixthConstructor_LocalOrchestrator_ShouldMatch_RemoteOrchestrator()
        {
            var clientProvider = new SqlSyncProvider();

            // this options and setup will be overriden by the constructor
            var remoteOptions = new SyncOptions();
            var remoteSetup = new SyncSetup(new string[] { "Product", "ProductCategory" });

            var remoteOrchestrator = new RemoteOrchestrator(new SqlSyncProvider(), remoteOptions, remoteSetup);

            var agent = new SyncAgent(clientProvider, remoteOrchestrator);

            this.CheckConstructor(agent);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.LocalOrchestrator.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.RemoteOrchestrator.ScopeName);
            Assert.Empty(agent.LocalOrchestrator.Setup.Tables);
            Assert.Empty(agent.LocalOrchestrator.Setup.Tables);

        }

        [Fact]
        public void SyncAgent_SixthConstructor_LocalOrchestrator_ShouldMatch_WebClientOrchestrator()
        {
            var clientProvider = new SqlSyncProvider();
            var remoteOrchestrator = new WebClientOrchestrator("http://localhost/api");

            var agent = new SyncAgent(clientProvider, remoteOrchestrator);

            this.CheckConstructor(agent);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.LocalOrchestrator.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.RemoteOrchestrator.ScopeName);
            Assert.Empty(agent.LocalOrchestrator.Setup.Tables);
            Assert.Empty(agent.LocalOrchestrator.Setup.Tables);

        }

        [Fact]
        public void SyncAgent_SixthConstructor_LocalOrchestrator_ShouldMatch_RemoteOrchestrator_With_ScopeNameDefined()
        {
            var clientProvider = new SqlSyncProvider();
            // this options and setup will be overriden by the constructor
            var remoteOptions = new SyncOptions();
            var remoteSetup = new SyncSetup(new string[] { "Product", "ProductCategory" });
            var remoteOrchestrator = new RemoteOrchestrator(new SqlSyncProvider(), remoteOptions, remoteSetup);

            var agent = new SyncAgent(clientProvider, remoteOrchestrator, "CustomerScope");

            this.CheckConstructor(agent);
            Assert.Equal("CustomerScope", agent.ScopeName);
            Assert.Equal("CustomerScope", agent.LocalOrchestrator.ScopeName);
            Assert.Equal("CustomerScope", agent.RemoteOrchestrator.ScopeName);
            Assert.Empty(agent.LocalOrchestrator.Setup.Tables);
            Assert.Empty(agent.LocalOrchestrator.Setup.Tables);

        }

        [Fact]
        public void SyncAgent_SixthConstructor_LocalOrchestrator_ShouldMatch_WebClientOrchestrator_With_ScopeNameDefined()
        {
            var clientProvider = new SqlSyncProvider();
            var remoteOrchestrator = new WebClientOrchestrator("http://localhost/api");

            var agent = new SyncAgent(clientProvider, remoteOrchestrator, "CustomerScope");

            this.CheckConstructor(agent);
            Assert.Equal("CustomerScope", agent.ScopeName);
            Assert.Equal("CustomerScope", agent.LocalOrchestrator.ScopeName);
            Assert.Equal("CustomerScope", agent.RemoteOrchestrator.ScopeName);
            Assert.Empty(agent.LocalOrchestrator.Setup.Tables);
            Assert.Empty(agent.LocalOrchestrator.Setup.Tables);

        }



        [Fact]
        public void SyncAgent_SeventhConstructor_LocalOrchestrator_ShouldMatch_RemoteOrchestrator()
        {
            var clientProvider = new SqlSyncProvider();
            var options = new SyncOptions();

            // this options and setup will be overriden by the constructor
            var remoteOptions = new SyncOptions();
            var remoteSetup = new SyncSetup(new string[] { "Product", "ProductCategory" });
           
            var remoteOrchestrator = new RemoteOrchestrator(new SqlSyncProvider(), remoteOptions, remoteSetup);

            var agent = new SyncAgent(clientProvider, remoteOrchestrator, options, new string[] { "Customer" });

            this.CheckConstructor(agent);
            Assert.Same(options, agent.LocalOrchestrator.Options);
            Assert.Same(options, agent.RemoteOrchestrator.Options);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.LocalOrchestrator.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.RemoteOrchestrator.ScopeName);
            Assert.Single(agent.LocalOrchestrator.Setup.Tables);
            Assert.Single(agent.RemoteOrchestrator.Setup.Tables);
            Assert.Equal("Customer", agent.LocalOrchestrator.Setup.Tables[0].TableName);
            Assert.Equal("Customer", agent.RemoteOrchestrator.Setup.Tables[0].TableName);

        }

        [Fact]
        public void SyncAgent_SeventhConstructor_LocalOrchestrator_ShouldMatch_WebClientOrchestrator()
        {
            var clientProvider = new SqlSyncProvider();
            var options = new SyncOptions();

            var remoteOrchestrator = new WebClientOrchestrator("http://localhost/api");

            var agent = new SyncAgent(clientProvider, remoteOrchestrator, options, new string[] { "Customer" });

            this.CheckConstructor(agent);
            Assert.Same(options, agent.LocalOrchestrator.Options);
            Assert.Same(options, agent.RemoteOrchestrator.Options);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.LocalOrchestrator.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.RemoteOrchestrator.ScopeName);
            Assert.Single(agent.LocalOrchestrator.Setup.Tables);
            Assert.Single(agent.RemoteOrchestrator.Setup.Tables);
            Assert.Equal("Customer", agent.LocalOrchestrator.Setup.Tables[0].TableName);
            Assert.Equal("Customer", agent.RemoteOrchestrator.Setup.Tables[0].TableName);

        }



        [Fact]
        public void SyncAgent_SeventhConstructor_LocalOrchestrator_ShouldMatch_RemoteOrchestrator_With_ScopeNameDefined()
        {
            var clientProvider = new SqlSyncProvider();
            var options = new SyncOptions();

            // this options and setup will be overriden by the constructor
            var remoteOptions = new SyncOptions();
            var remoteSetup = new SyncSetup(new string[] { "Product", "ProductCategory" });

            var remoteOrchestrator = new RemoteOrchestrator(new SqlSyncProvider(), remoteOptions, remoteSetup);

            var agent = new SyncAgent(clientProvider, remoteOrchestrator, options, new string[] { "Customer" }, "CustomerScope");

            this.CheckConstructor(agent);
            Assert.Same(options, agent.LocalOrchestrator.Options);
            Assert.Same(options, agent.RemoteOrchestrator.Options);
            Assert.Equal("CustomerScope", agent.ScopeName);
            Assert.Equal("CustomerScope", agent.LocalOrchestrator.ScopeName);
            Assert.Equal("CustomerScope", agent.RemoteOrchestrator.ScopeName);
            Assert.Single(agent.LocalOrchestrator.Setup.Tables);
            Assert.Single(agent.RemoteOrchestrator.Setup.Tables);
            Assert.Equal("Customer", agent.LocalOrchestrator.Setup.Tables[0].TableName);
            Assert.Equal("Customer", agent.RemoteOrchestrator.Setup.Tables[0].TableName);

        }

        [Fact]
        public void SyncAgent_SeventhConstructor_LocalOrchestrator_ShouldMatch_WebClientOrchestrator_With_ScopeNameDefined()
        {
            var clientProvider = new SqlSyncProvider();
            var options = new SyncOptions();

            var remoteOrchestrator = new WebClientOrchestrator("http://localhost/api");

            var agent = new SyncAgent(clientProvider, remoteOrchestrator, options, new string[] { "Customer" }, "CustomerScope");

            this.CheckConstructor(agent);
            Assert.Same(options, agent.LocalOrchestrator.Options);
            Assert.Same(options, agent.RemoteOrchestrator.Options);
            Assert.Equal("CustomerScope", agent.ScopeName);
            Assert.Equal("CustomerScope", agent.LocalOrchestrator.ScopeName);
            Assert.Equal("CustomerScope", agent.RemoteOrchestrator.ScopeName);
            Assert.Single(agent.LocalOrchestrator.Setup.Tables);
            Assert.Single(agent.RemoteOrchestrator.Setup.Tables);
            Assert.Equal("Customer", agent.LocalOrchestrator.Setup.Tables[0].TableName);
            Assert.Equal("Customer", agent.RemoteOrchestrator.Setup.Tables[0].TableName);

        }



        [Fact]
        public void SyncAgent_EighthConstructor_LocalOrchestrator_ShouldMatch_RemoteOrchestrator()
        {
            var clientProvider = new SqlSyncProvider();
            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "Customer" });

            // this options and setup will be overriden by the constructor
            var remoteOptions = new SyncOptions();
            var remoteSetup = new SyncSetup(new string[] { "Product", "ProductCategory" });

            var remoteOrchestrator = new RemoteOrchestrator(new SqlSyncProvider(), remoteOptions, remoteSetup);

            var agent = new SyncAgent(clientProvider, remoteOrchestrator, options, setup);

            this.CheckConstructor(agent);
            Assert.Same(options, agent.LocalOrchestrator.Options);
            Assert.Same(options, agent.RemoteOrchestrator.Options);
            Assert.Same(setup, agent.LocalOrchestrator.Setup);
            Assert.Same(setup, agent.RemoteOrchestrator.Setup);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.LocalOrchestrator.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.RemoteOrchestrator.ScopeName);
            Assert.Single(agent.LocalOrchestrator.Setup.Tables);
            Assert.Single(agent.RemoteOrchestrator.Setup.Tables);
            Assert.Equal("Customer", agent.LocalOrchestrator.Setup.Tables[0].TableName);
            Assert.Equal("Customer", agent.RemoteOrchestrator.Setup.Tables[0].TableName);

        }

        [Fact]
        public void SyncAgent_EighthConstructor_LocalOrchestrator_ShouldMatch_WebClientOrchestrator()
        {
            var clientProvider = new SqlSyncProvider();
            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "Customer" });

            var remoteOrchestrator = new WebClientOrchestrator("http://localhost/api");

            var agent = new SyncAgent(clientProvider, remoteOrchestrator, options, setup);

            this.CheckConstructor(agent);
            Assert.Same(options, agent.LocalOrchestrator.Options);
            Assert.Same(options, agent.RemoteOrchestrator.Options);
            Assert.Same(setup, agent.LocalOrchestrator.Setup);
            Assert.Same(setup, agent.RemoteOrchestrator.Setup);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.LocalOrchestrator.ScopeName);
            Assert.Equal(SyncOptions.DefaultScopeName, agent.RemoteOrchestrator.ScopeName);
            Assert.Single(agent.LocalOrchestrator.Setup.Tables);
            Assert.Single(agent.RemoteOrchestrator.Setup.Tables);
            Assert.Equal("Customer", agent.LocalOrchestrator.Setup.Tables[0].TableName);
            Assert.Equal("Customer", agent.RemoteOrchestrator.Setup.Tables[0].TableName);

        }



        [Fact]
        public void SyncAgent_EighthConstructor_LocalOrchestrator_ShouldMatch_RemoteOrchestrator_With_ScopeNameDefined()
        {
            var clientProvider = new SqlSyncProvider();
            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "Customer" });

            // this options and setup will be overriden by the constructor
            var remoteOptions = new SyncOptions();
            var remoteSetup = new SyncSetup(new string[] { "Product", "ProductCategory" });

            var remoteOrchestrator = new RemoteOrchestrator(new SqlSyncProvider(), remoteOptions, remoteSetup);

            var agent = new SyncAgent(clientProvider, remoteOrchestrator, options, setup, "CustomerScope");

            this.CheckConstructor(agent);
            Assert.Same(options, agent.LocalOrchestrator.Options);
            Assert.Same(options, agent.RemoteOrchestrator.Options);
            Assert.Same(setup, agent.LocalOrchestrator.Setup);
            Assert.Same(setup, agent.RemoteOrchestrator.Setup);
            Assert.Equal("CustomerScope", agent.ScopeName);
            Assert.Equal("CustomerScope", agent.LocalOrchestrator.ScopeName);
            Assert.Equal("CustomerScope", agent.RemoteOrchestrator.ScopeName);
            Assert.Single(agent.LocalOrchestrator.Setup.Tables);
            Assert.Single(agent.RemoteOrchestrator.Setup.Tables);
            Assert.Equal("Customer", agent.LocalOrchestrator.Setup.Tables[0].TableName);
            Assert.Equal("Customer", agent.RemoteOrchestrator.Setup.Tables[0].TableName);

        }

        [Fact]
        public void SyncAgent_EighthConstructor_LocalOrchestrator_ShouldMatch_WebClientOrchestrator_With_ScopeNameDefined()
        {
            var clientProvider = new SqlSyncProvider();
            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "Customer" });

            var remoteOrchestrator = new WebClientOrchestrator("http://localhost/api");

            var agent = new SyncAgent(clientProvider, remoteOrchestrator, options, setup, "CustomerScope");

            this.CheckConstructor(agent);
            Assert.Same(options, agent.LocalOrchestrator.Options);
            Assert.Same(options, agent.RemoteOrchestrator.Options);
            Assert.Same(setup, agent.LocalOrchestrator.Setup);
            Assert.Same(setup, agent.RemoteOrchestrator.Setup);
            Assert.Equal("CustomerScope", agent.ScopeName);
            Assert.Equal("CustomerScope", agent.LocalOrchestrator.ScopeName);
            Assert.Equal("CustomerScope", agent.RemoteOrchestrator.ScopeName);
            Assert.Single(agent.LocalOrchestrator.Setup.Tables);
            Assert.Single(agent.RemoteOrchestrator.Setup.Tables);
            Assert.Equal("Customer", agent.LocalOrchestrator.Setup.Tables[0].TableName);
            Assert.Equal("Customer", agent.RemoteOrchestrator.Setup.Tables[0].TableName);

        }




    }
}
