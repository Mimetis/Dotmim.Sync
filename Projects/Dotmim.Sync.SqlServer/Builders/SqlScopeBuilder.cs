using Dotmim.Sync.Builders;
using System.Data.Common;

namespace Dotmim.Sync.SqlServer.Scope
{
    public class SqlScopeBuilder : DbScopeBuilder
    {
        public override IDbScopeInfoBuilder CreateScopeInfoBuilder(string scopeTableName) => new SqlScopeInfoBuilder(scopeTableName);
    }
}
