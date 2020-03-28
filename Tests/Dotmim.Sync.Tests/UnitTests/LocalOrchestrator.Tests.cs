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
        public async Task BaseOrchestrator_BeginSession_ShouldIncrement_SyncStage()
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
        public async Task BaseOrchestrator_EndSession_ShouldIncrement_SyncStage()
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
