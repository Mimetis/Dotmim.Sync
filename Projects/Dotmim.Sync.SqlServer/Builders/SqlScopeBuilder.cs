using Dotmim.Sync.Builders;
using System.Data.Common;

namespace Dotmim.Sync.SqlServer.Scope
{
    public class SqlScopeBuilder : DbScopeBuilder
    {
        

        public override IDbScopeInfoBuilder CreateScopeInfoBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return (IDbScopeInfoBuilder)(new SqlScopeInfoBuilder(connection, transaction));
        }
    }
}
