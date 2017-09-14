using Dotmim.Sync.Builders;
using Dotmim.Sync.Cache;
using Dotmim.Sync.Data;
using Dotmim.Sync.Manager;
using System.Data.Common;
using Npgsql;


namespace Dotmim.Sync.MySql
{

    public class PostgreSqlyncProvider : CoreProvider
    {
        ICache cacheManager;


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
        /// MySql does not support Bulk operations
        /// </summary>
        public override bool SupportBulkOperations => false;

        /// <summary>
        /// MySql can be a server side provider
        /// </summary>
        public override bool CanBeServerProvider => true;

        public PostgreSqlyncProvider() : base()
        {
        }
        public PostgreSqlyncProvider(string connectionString) : base()
        {
            this.ConnectionString = connectionString;
        }

        public override DbConnection CreateConnection() => new NpgsqlConnection(this.ConnectionString);

        public override DbBuilder GetDatabaseBuilder(DmTable tableDescription, DbBuilderOption options = DbBuilderOption.UseExistingSchema) => new MySqlBuilder(tableDescription, options);

        public override DbManager GetDbManager(string tableName) => new PostgreSqlManager(tableName);

        public override DbScopeBuilder GetScopeBuilder() => new PostgreSqlScopeBuilder();
    }
}
