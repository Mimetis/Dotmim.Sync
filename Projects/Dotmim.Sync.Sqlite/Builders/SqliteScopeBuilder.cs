using Dotmim.Sync.Builders;
using System.Data.Common;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteScopeBuilder : DbScopeBuilder
    {
        

        public override IDbScopeInfoBuilder CreateScopeInfoBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new SqliteScopeInfoBuilder(connection, transaction);
        }
    }
}
