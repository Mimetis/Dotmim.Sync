using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests.UnitTests
{
    public partial class LocalOrchestratorTests : IDisposable
    {

        public string[] Tables => new string[]
         {
            "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product", "Employee", "Customer", "Address", "CustomerAddress", "EmployeeAddress",
            "SalesLT.SalesOrderHeader", "SalesLT.SalesOrderDetail", "Posts", "Tags", "PostTag",
            "PricesList", "PricesListCategory", "PricesListDetail"
         };

        // Current test running
        private ITest test;
        private Stopwatch stopwatch;
        public ITestOutputHelper Output { get; }

        public LocalOrchestratorTests(ITestOutputHelper output)
        {

            // Getting the test running
            this.Output = output;
            var type = output.GetType();
            var testMember = type.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
            this.test = (ITest)testMember.GetValue(output);
            this.stopwatch = Stopwatch.StartNew();
        }

        public void Dispose()
        {

            this.stopwatch.Stop();

            var str = $"{test.TestCase.DisplayName} : {this.stopwatch.Elapsed.Minutes}:{this.stopwatch.Elapsed.Seconds}.{this.stopwatch.Elapsed.Milliseconds}";
            Console.WriteLine(str);
            Debug.WriteLine(str);

        }


        [Fact]
        public async Task LocalOrchestrator_BeginSession_ShouldIncrement_SyncStage()
        {
            var options = new SyncOptions();
            var setup = new SyncSetup();
            var provider = new SqlSyncProvider();
            var onSessionBegin = false;


            var localOrchestrator = new LocalOrchestrator(provider, options);
            var ctx = localOrchestrator.GetContext(SyncOptions.DefaultScopeName);

            localOrchestrator.OnSessionBegin(args =>
            {
                Assert.Equal(SyncStage.BeginSession, args.Context.SyncStage);
                Assert.IsType<SessionBeginArgs>(args);
                Assert.NotNull(args.Connection);
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

            var localOrchestrator = new LocalOrchestrator(provider, options);
            var ctx = localOrchestrator.GetContext(SyncOptions.DefaultScopeName);

            localOrchestrator.OnSessionEnd(args =>
            {
                Assert.Equal(SyncStage.EndSession, args.Context.SyncStage);
                Assert.IsType<SessionEndArgs>(args);
                Assert.NotNull(args.Connection);
                Assert.Null(args.Transaction);
                onSessionEnd = true;
            });

            await localOrchestrator.EndSessionAsync(SyncOptions.DefaultScopeName);

            Assert.Equal(SyncStage.EndSession, ctx.SyncStage);
            Assert.True(onSessionEnd);
        }

    }
}
