using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Tests.UnitTests
{
    public partial class LocalOrchestratorTests
    {

        public string[] Tables => new string[]
         {
            "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product", "Employee", "Customer", "Address", "CustomerAddress", "EmployeeAddress",
            "SalesLT.SalesOrderHeader", "SalesLT.SalesOrderDetail", "dbo.Sql", "Posts", "Tags", "PostTag",
            "PricesList", "PricesListCategory", "PricesListDetail"
         };

        public LocalOrchestratorTests()
        {

        }

        [Fact]
        public void LocalOrchestrator_Constructor()
        {
            var provider = new MockProvider();
            var options = new SyncOptions();
            var setup = new SyncSetup();
            var localOrchestrator = new LocalOrchestrator(provider, options, setup);

            Assert.NotNull(localOrchestrator.Options);
            Assert.Same(options, localOrchestrator.Options);

            Assert.NotNull(localOrchestrator.Provider);
            Assert.Same(provider, localOrchestrator.Provider);

            Assert.NotNull(localOrchestrator.Setup);
            Assert.Same(setup, localOrchestrator.Setup);

            Assert.NotNull(provider.Orchestrator);
            Assert.Same(provider.Orchestrator, localOrchestrator);

        }

        [Fact]
        public void LocalOrchestrator_ShouldFail_When_Args_AreNull()
        {
            var provider = new MockProvider();
            var options = new SyncOptions();
            var setup = new SyncSetup();

            Assert.Throws<ArgumentNullException>(() => new LocalOrchestrator(null, null, null));
            Assert.Throws<ArgumentNullException>(() => new LocalOrchestrator(provider, null, null));
            Assert.Throws<ArgumentNullException>(() => new LocalOrchestrator(provider, options, null));
            Assert.Throws<ArgumentNullException>(() => new LocalOrchestrator(null, options, setup));
            Assert.Throws<ArgumentNullException>(() => new LocalOrchestrator(null, null, setup));
        }


        [Fact]
        public void LocalOrchestrator_GetContext_ShouldBeInitialized()
        {
            var options = new SyncOptions();
            var setup = new SyncSetup();
            var provider = new SqlSyncProvider();

            var localOrchestrator = new LocalOrchestrator(provider, options, setup, "scope1");

           var ctx = localOrchestrator.GetContext();

            Assert.Equal(SyncStage.None, ctx.SyncStage);
            Assert.Equal(localOrchestrator.ScopeName, ctx.ScopeName);
            Assert.Equal(SyncType.Normal, ctx.SyncType);
            Assert.Equal(SyncWay.None, ctx.SyncWay);
            Assert.Null(ctx.Parameters);
        }


        [Fact]
        public async Task LocalOrchestrator_BeginSession_ShouldIncrement_SyncStage()
        {
            var options = new SyncOptions();
            var setup = new SyncSetup();
            var provider = new SqlSyncProvider();
            var onSessionBegin = false;


            var localOrchestrator = new LocalOrchestrator(provider, options, setup);
            var ctx = localOrchestrator.GetContext();

            localOrchestrator.OnSessionBegin(args =>
            {
                Assert.Equal(SyncStage.BeginSession, args.Context.SyncStage);
                Assert.IsType<SessionBeginArgs>(args);
                Assert.Null(args.Connection);
                Assert.Null(args.Transaction);
                onSessionBegin = true;
            });

            await localOrchestrator.BeginSessionAsync();

            Assert.Equal(SyncStage.BeginSession, ctx.SyncStage);
            Assert.True(onSessionBegin);
        }
        [Fact]
        public async Task LocalOrchestrator_EndSession_ShouldIncrement_SyncStage()
        {
            var options = new SyncOptions();
            var setup = new SyncSetup();
            var provider = new SqlSyncProvider();
            var onSessionEnd = false;

            var localOrchestrator = new LocalOrchestrator(provider, options, setup);
            var ctx = localOrchestrator.GetContext();

            localOrchestrator.OnSessionEnd(args =>
            {
                Assert.Equal(SyncStage.EndSession, args.Context.SyncStage);
                Assert.IsType<SessionEndArgs>(args);
                Assert.Null(args.Connection);
                Assert.Null(args.Transaction);
                onSessionEnd = true;
            });

            await localOrchestrator.EndSessionAsync();

            Assert.Equal(SyncStage.EndSession, ctx.SyncStage);
            Assert.True(onSessionEnd);
        }

    }
}
