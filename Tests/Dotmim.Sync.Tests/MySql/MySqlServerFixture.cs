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
    public class MySqlServerFixture : ProviderFixture<CoreProvider>
    {
        public override string DatabaseName => "mysqladventureworks";

        public override ProviderType ProviderType => ProviderType.MySql;

        public override CoreProvider ServerProvider => new MySqlSyncProvider(
            HelperDB.GetMySqlDatabaseConnectionString(DatabaseName));

        public override bool EnableSqlServerClientOnTcp => false;
        public override bool EnableSqlServerClientOnHttp => false;
        public override bool EnableMySqlClientOnTcp => true;
        public override bool EnableMySqlClientOnHttp => false;
        public override bool EnableSqliteClientOnTcp => false;
        public override bool EnableSqliteClientOnHttp => false;

        // for debugging
        //public override bool DeleteAllDatabasesOnDispose => false;

        public override void ServerDatabaseEnsureCreated()
        {
            using (AdventureWorksContext ctx =
                new AdventureWorksContext(ProviderType, HelperDB.GetConnectionString(ProviderType, DatabaseName)))
            {
                ctx.Database.EnsureDeleted();
                ctx.Database.EnsureCreated();
            }
        }

        public override void ServerDatabaseEnsureDeleted()
        {
            using (AdventureWorksContext ctx =
                new AdventureWorksContext(ProviderType, HelperDB.GetConnectionString(ProviderType, DatabaseName)))
            {
                ctx.Database.EnsureDeleted();
            }
        }
    }
}
