using Dotmim.Sync.Builders;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteScopeBuilder : DbScopeBuilder
    {
        public SqliteScopeBuilder(string scopeInfoTableName) : base(scopeInfoTableName)
        {
        }

        public override Task<DbCommand> GetAllScopesCommandAsync(DbScopeType scopeType, string scopeName, DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
        public override Task<DbCommand> GetCreateScopeInfoTableCommandAsync(DbScopeType scopeType, DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
        public override Task<DbCommand> GetDropScopeInfoTableCommandAsync(DbScopeType scopeType, DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
        public override Task<DbCommand> GetExistsScopeInfoTableCommandAsync(DbScopeType scopeType, DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
        public override Task<DbCommand> GetLocalTimestampCommandAsync(DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
        public override Task<DbCommand> GetUpsertScopeInfoCommandAsync(DbScopeType scopeType, object scopeInfo, DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
    }
}
