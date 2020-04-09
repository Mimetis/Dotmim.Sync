using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.SqlServer.ChangeTracking.Builders
{
    public class SqlChangeTrackingScopeBuilder : DbScopeBuilder
    {
        public override IDbScopeInfoBuilder CreateScopeInfoBuilder(string scopeTableName, DbConnection connection, DbTransaction transaction = null) 
            => new SqlChangeTrackingScopeInfoBuilder(scopeTableName, connection, transaction);
    }
}
