using Dotmim.Sync.Sqlite;
using Dotmim.Sync.Tests.Core;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Tests.Sqlite
{
    /// <summary>
    /// Fixture used to test the SqlSyncProvider
    /// </summary>
    public class SqliteFixture : ProviderFixture
    {
        public override ProviderType ProviderType => ProviderType.Sqlite;

        public override CoreProvider NewServerProvider(string connectionString)
        {
            return new SqliteSyncProvider(connectionString);
        }

    }
}
