using Dotmim.Sync.Builders;
using Dotmim.Sync.Cache;
using Dotmim.Sync.Data;
using Dotmim.Sync.Manager;
using System.Data.Common;
using MySql.Data.MySqlClient;


namespace Dotmim.Sync.MySql
{

    public class MySqlSyncProvider : CoreProvider
    {
        ICache cacheManager;
        IDbMetadata dbMetadata;


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


        /// <summary>
        /// Gets or Sets the MySql Metadata object, provided to validate the MySql Columns issued from MySql
        /// </summary>
        public override IDbMetadata Metadata
        {
            get
            {
                if (dbMetadata == null)
                    dbMetadata = new MySqlMetadata();

                return dbMetadata;
            }
            set
            {
                dbMetadata = value;

            }
        }

        public MySqlSyncProvider() : base()
        {
        }
        public MySqlSyncProvider(string connectionString) : base()
        {
            this.ConnectionString = connectionString;
        }

        public override DbConnection CreateConnection() => new MySqlConnection(this.ConnectionString);

        public override DbBuilder GetDatabaseBuilder(DmTable tableDescription, DbBuilderOption options = DbBuilderOption.UseExistingSchema) => new MySqlBuilder(tableDescription, options);

        public override DbManager GetDbManager(string tableName) => new MySqlManager(tableName);

        public override DbScopeBuilder GetScopeBuilder() => new MySqlScopeBuilder();
    }
}
