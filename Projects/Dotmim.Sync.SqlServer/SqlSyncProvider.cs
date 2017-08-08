using Dotmim.Sync.Builders;
using Dotmim.Sync.Cache;
using Dotmim.Sync.Data;
using Dotmim.Sync.Manager;
using Dotmim.Sync.SqlServer.Builders;
using Dotmim.Sync.SqlServer.Manager;
using Dotmim.Sync.SqlServer.Scope;
using System.Data.Common;
using System.Data.SqlClient;

namespace Dotmim.Sync.SqlServer
{
    public class SqlSyncProvider : CoreProvider
    {
        ICache cacheManager;

        public SqlSyncProvider() : base()
        { }

        public SqlSyncProvider(string connectionString) : base()
        {
            this.ConnectionString = connectionString;
        }

        public override ICache CacheManager
        {
            get
            {
                if (cacheManager == null)
                    cacheManager = new InMemoryCache();

                return cacheManager;
            }
            set
            {
                cacheManager = value;

            }
        }

        /// <summary>
        /// Sql server support bulk operations through Table Value parameter
        /// </summary>
        public override bool SupportBulkOperations => true;

        /// <summary>
        /// Sql Server supports to be a server side provider
        /// </summary>
        public override bool CanBeServerProvider => true;
        


        public override DbConnection CreateConnection() => new SqlConnection(this.ConnectionString);
        public override DbScopeBuilder GetScopeBuilder() => new SqlScopeBuilder();
        public override DbBuilder GetDatabaseBuilder(DmTable tableDescription, DbBuilderOption options = DbBuilderOption.UseExistingSchema) => new SqlBuilder(tableDescription, options);
        public override DbManager GetDbManager(string tableName) => new SqlManager(tableName);

    }
}
