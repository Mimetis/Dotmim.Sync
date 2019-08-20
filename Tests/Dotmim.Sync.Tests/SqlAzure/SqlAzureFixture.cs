using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Tests.SqlAzure
{
    /// <summary>
    /// Fixture used to test the SqlSyncProvider
    /// </summary>
    public class SqlAzureFixture : ProviderFixture
    {
        public override ProviderType ProviderType => ProviderType.SqlAzure;

        public override CoreProvider NewServerProvider(string connectionString)
        {
            return new SqlSyncProvider(connectionString);
        }

    }
}
