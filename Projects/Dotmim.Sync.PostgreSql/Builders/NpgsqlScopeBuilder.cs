using Dotmim.Sync.Builders;
using System.Data.Common;

namespace Dotmim.Sync.Postgres.Scope
{
    public class NpgsqlScopeBuilder : DbScopeBuilder
    {
        

        public override IDbScopeInfoBuilder CreateScopeInfoBuilder(string scopeTableName)
        {
            return new NpgsqlScopeInfoBuilder(scopeTableName);
        }
    }
}
