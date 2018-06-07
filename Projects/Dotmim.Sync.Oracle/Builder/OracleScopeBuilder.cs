using Dotmim.Sync.Builders;
using System;
using System.Data.Common;

namespace Dotmim.Sync.Oracle.Scope
{
    public class OracleScopeBuilder : DbScopeBuilder
    {
        public OracleScopeBuilder()
        {
        }

        public override IDbScopeInfoBuilder CreateScopeInfoBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new OracleScopeInfoBuilder(connection, transaction);
        }
    }
}
