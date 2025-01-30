﻿using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Fixtures;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests.UnitTests
{
    public abstract partial class LocalOrchestratorTests : DatabaseTest, IClassFixture<DatabaseServerFixture>, IDisposable
    {
        private CoreProvider serverProvider;
        private CoreProvider clientProvider;
        private IEnumerable<CoreProvider> clientsProvider;
        private SyncSetup setup;
        private SyncOptions options;

        public LocalOrchestratorTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
            serverProvider = GetServerProvider();
            clientsProvider = GetClientProviders();
            clientProvider = clientsProvider.First();
            setup = GetSetup();
            options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

        }

        [Fact]
        public async Task LocalOrchestrator_BeginSession_ShouldIncrement_SyncStage()
        {
            var options = new SyncOptions();
            var setup = new SyncSetup();
            var provider = new SqlSyncProvider();
            var onSessionBegin = false;


            var localOrchestrator = new LocalOrchestrator(provider, options);

            localOrchestrator.OnSessionBegin(args =>
            {
                Assert.Equal(SyncStage.BeginSession, args.Context.SyncStage);
                Assert.IsType<SessionBeginArgs>(args);
                Assert.NotNull(args.Connection);
                Assert.Null(args.Transaction);
                onSessionBegin = true;
            });

            await localOrchestrator.BeginSessionAsync();
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

            localOrchestrator.OnSessionEnd(args =>
            {
                Assert.Equal(SyncStage.EndSession, args.Context.SyncStage);
                Assert.IsType<SessionEndArgs>(args);
                Assert.NotNull(args.Connection);
                Assert.Null(args.Transaction);
                onSessionEnd = true;
            });

            await localOrchestrator.EndSessionAsync(new SyncResult(), SyncOptions.DefaultScopeName);

            Assert.True(onSessionEnd);
        }

        [Fact]
        public void LocalOrchestrator_Constructor()
        {
            var provider = new SqlSyncProvider();
            var options = new SyncOptions();
            var orchestrator = new LocalOrchestrator(provider, options);

            Assert.NotNull(orchestrator.Options);
            Assert.Same(options, orchestrator.Options);

            Assert.NotNull(orchestrator.Provider);
            Assert.Same(provider, orchestrator.Provider);

            Assert.NotNull(provider.Orchestrator);
            Assert.Same(provider.Orchestrator, orchestrator);

        }

        [Fact]
        public void LocalOrchestrator_ShouldFail_When_Args_AreNull()
        {
            var provider = new SqlSyncProvider();
            var options = new SyncOptions();
            var setup = new SyncSetup();

            var ex1 = Assert.Throws<SyncException>(() => new LocalOrchestrator(null, options));
            Assert.Equal("MissingProviderException", ex1.TypeName);

            var ex3 = Assert.Throws<SyncException>(() => new LocalOrchestrator(provider, null));
            Assert.Equal("ArgumentNullException", ex3.TypeName);
        }

    }
}
