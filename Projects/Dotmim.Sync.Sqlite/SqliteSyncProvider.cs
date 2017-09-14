using Dotmim.Sync.Builders;
using Dotmim.Sync.Cache;
using Dotmim.Sync.Data;
using Dotmim.Sync.Manager;
using System;
using System.Data.Common;
using System.Data.SQLite;


namespace Dotmim.Sync.SQLite
{

    public class SQLiteSyncProvider : CoreProvider
    {
        private ICache cacheManager;
        private string filePath;
        private DbMetadata dbMetadata;
        private static String providerType;

        public override DbMetadata Metadata
        {
            get
            {
                if (dbMetadata == null)
                    dbMetadata = new SQLiteDbMetadata();

                return dbMetadata;
            }
            set
            {
                dbMetadata = value;

            }
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
        /// SQLite does not support Bulk operations
        /// </summary>
        public override bool SupportBulkOperations => false;

        /// <summary>
        /// SQLIte does not support to be a server side.
        /// Reason 1 : Can't easily insert / update batch with handling conflict
        /// Reason 2 : Can't filter rows (based on filterclause)
        /// </summary>
        public override bool CanBeServerProvider => false;

        public override string ProviderTypeName
        {
            get
            {
                return ProviderType;
            }
        }

        public static string ProviderType
        {
            get
            {
                if (!string.IsNullOrEmpty(providerType))
                    return providerType;

                Type type = typeof(SQLiteSyncProvider);
                providerType = $"{type.Name}, {type.ToString()}";

                return providerType;
            }

        }
        public SQLiteSyncProvider() : base()
        {
        }
        public SQLiteSyncProvider(string filePath) : base()
        {
            this.filePath = filePath;
            var builder = new SQLiteConnectionStringBuilder { DataSource = filePath };

            // prefer to store guid in text
            builder.BinaryGUID = false;

            this.ConnectionString = builder.ConnectionString;
        }

        public override DbConnection CreateConnection() => new SQLiteConnection(this.ConnectionString);

        public override DbBuilder GetDatabaseBuilder(DmTable tableDescription, DbBuilderOption options = DbBuilderOption.UseExistingSchema) => new SQLiteBuilder(tableDescription, options);

        public override DbManager GetDbManager(string tableName) => new SQLiteManager(tableName);

        public override DbScopeBuilder GetScopeBuilder() => new SQLiteScopeBuilder();
    }
}
