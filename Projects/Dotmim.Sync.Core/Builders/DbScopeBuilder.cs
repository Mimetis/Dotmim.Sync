using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Builders
{
    public abstract class DbScopeBuilder 
    {
        public ParserName ScopeInfoTableName { get; }

        public DbScopeBuilder(string scopeInfoTableName) => this.ScopeInfoTableName = ParserName.Parse(scopeInfoTableName);

        public abstract Task<DbCommand> GetExistsScopeInfoTableCommandAsync(DbScopeType scopeType, DbConnection connection, DbTransaction transaction);
        public abstract Task<DbCommand> GetCreateScopeInfoTableCommandAsync(DbScopeType scopeType, DbConnection connection, DbTransaction transaction);
        public abstract Task<DbCommand> GetAllScopesCommandAsync(DbScopeType scopeType, string scopeName, DbConnection connection, DbTransaction transaction);
        public abstract Task<DbCommand> GetUpsertScopeInfoCommandAsync(DbScopeType scopeType, object scopeInfo, DbConnection connection, DbTransaction transaction);
        public abstract Task<DbCommand> GetLocalTimestampCommandAsync(DbConnection connection, DbTransaction transaction);
        public abstract Task<DbCommand> GetDropScopeInfoTableCommandAsync(DbScopeType scopeType, DbConnection connection, DbTransaction transaction);
    }
}
