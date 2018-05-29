using Dotmim.Sync.Builders;
using System;
using System.Data.Common;

namespace Dotmim.Sync.Oracle.Scope
{
    public class OracleScopeBuilder : DbScopeBuilder
    {
        public override IDbScopeInfoBuilder CreateScopeInfoBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            throw new NotImplementedException();
        }
    }
}
