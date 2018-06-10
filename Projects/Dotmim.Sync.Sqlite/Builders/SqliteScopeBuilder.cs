using Dotmim.Sync.Builders;
using System.Data.Common;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteScopeBuilder : DbScopeBuilder
    {
        

        public override IDbScopeInfoBuilder CreateScopeInfoBuilder(string scopeTableName, DbConnection connection, DbTransaction transaction = null)
        {
            return new SqliteScopeInfoBuilder(scopeTableName, connection, transaction);
        }
    }
}
