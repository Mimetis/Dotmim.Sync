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
    public partial class BaseOrchestratorTests
    {

        public string[] Tables => new string[]
        {
            "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product", "Employee", "Customer", "Address", "CustomerAddress", "EmployeeAddress",
            "SalesLT.SalesOrderHeader", "SalesLT.SalesOrderDetail", "dbo.Sql", "Posts", "Tags", "PostTag",
            "PricesList", "PricesListCategory", "PricesListDetail"
        };


        [Fact]
        public void BaseOrchestrator_Constructor()
        {
            var provider = new MockProvider();
            var options = new SyncOptions();
            var setup = new SyncSetup();
            var orchestrator = new MockBaseClientOrchestrator(provider, options, setup);

            Assert.NotNull(orchestrator.Options);
            Assert.Same(options, orchestrator.Options);

            Assert.NotNull(orchestrator.Provider);
            Assert.Same(provider, orchestrator.Provider);

            Assert.NotNull(orchestrator.Setup);
            Assert.Same(setup, orchestrator.Setup);

            Assert.NotNull(provider.Orchestrator);
            Assert.Same(provider.Orchestrator, orchestrator);

        }

        [Fact]
        public void BaseOrchestrator_ShouldFail_When_Args_AreNull()
        {
            var provider = new MockProvider();
            var options = new SyncOptions();
            var setup = new SyncSetup();

            Assert.Throws<ArgumentNullException>(() => new MockBaseClientOrchestrator(null, null, null));
            Assert.Throws<ArgumentNullException>(() => new MockBaseClientOrchestrator(provider, null, null));
            Assert.Throws<ArgumentNullException>(() => new MockBaseClientOrchestrator(provider, options, null));
            Assert.Throws<ArgumentNullException>(() => new MockBaseClientOrchestrator(null, options, setup));
            Assert.Throws<ArgumentNullException>(() => new MockBaseClientOrchestrator(null, null, setup));
        }


        [Fact]
        public void BaseOrchestrator_GetContext_ShouldBeInitialized()
        {
            var options = new SyncOptions();
            var setup = new SyncSetup();
            var provider = new SqlSyncProvider();

            var localOrchestrator = new MockBaseClientOrchestrator(provider, options, setup, "scope1");

            var ctx = localOrchestrator.GetContext();

            Assert.Equal(SyncStage.None, ctx.SyncStage);
            Assert.Equal(localOrchestrator.ScopeName, ctx.ScopeName);
            Assert.Equal(SyncType.Normal, ctx.SyncType);
            Assert.Equal(SyncWay.None, ctx.SyncWay);
            Assert.Null(ctx.Parameters);
        }


        [Fact]
        public async Task BaseOrchestrator_GetSchema_ShouldNot_Fail_If_NoTables_In_Setup()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);

            var options = new SyncOptions();
            var setup = new SyncSetup();
            var provider = new SqlSyncProvider(cs);

            var orchestrator = new MockBaseServerOrchestrator(provider, options, setup);

            var schema = await orchestrator.GetSchemaAsync();

            Assert.NotNull(schema);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);

        }

    }
}
