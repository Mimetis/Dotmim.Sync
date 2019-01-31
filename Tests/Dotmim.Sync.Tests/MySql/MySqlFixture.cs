using Dotmim.Sync.MySql;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Tests.MySql
{
    /// <summary>
    /// Fixture used to test the SqlSyncProvider
    /// </summary>
    public class MySqlFixture : ProviderFixture
    {
        public override ProviderType ProviderType => ProviderType.MySql;

   
        public override CoreProvider NewServerProvider(string connectionString)
        {
            return new MySqlSyncProvider(connectionString);
        }
        
    }
}
