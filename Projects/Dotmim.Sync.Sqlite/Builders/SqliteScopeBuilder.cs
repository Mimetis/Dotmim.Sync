using Dotmim.Sync.Builders;
using System.Data.Common;

namespace Dotmim.Sync.SQLite
{
    public class SQLiteScopeBuilder : DbScopeBuilder
    {
        

        public override IDbScopeInfoBuilder CreateScopeInfoBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new SQLiteScopeInfoBuilder(connection, transaction);
        }
    }
}
