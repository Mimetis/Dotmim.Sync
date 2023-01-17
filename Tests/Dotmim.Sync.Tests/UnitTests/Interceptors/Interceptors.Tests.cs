using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Fixtures;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests.UnitTests
{
    public abstract partial class InterceptorsTests : DatabaseTest, IClassFixture<DatabaseServerFixture>, IDisposable
    {
        private CoreProvider serverProvider;
        private CoreProvider clientProvider;
        private IEnumerable<CoreProvider> clientsProvider;
        private SyncSetup setup;
        private SyncOptions options;

        public InterceptorsTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
            serverProvider = GetServerProvider();
            clientsProvider = GetClientProviders();
            clientProvider = clientsProvider.First();
            setup = GetSetup();
            options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

        }

    }
}
