using Dotmim.Sync.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dotmim.Sync.Core.Batch;
using System.Data.Common;
using Dotmim.Sync.Core.Log;
using Dotmim.Sync.Core.Scope;
using Dotmim.Sync.Data;
using Dotmim.Sync.Core.Builders;
using Dotmim.Sync.Core.Manager;
using Dotmim.Sync.Core.Cache;
using System.Data.SQLite;


namespace Dotmim.Sync.SQLite
{

    public class SQLiteSyncProvider : CoreProvider
    {
        ICache cacheManager;
        string connectionString;
        private string filePath;

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

        public SQLiteSyncProvider() : base()
        {
        }
        public SQLiteSyncProvider(string filePath) : base()
        {
            this.filePath = filePath;
            var builder = new SQLiteConnectionStringBuilder { DataSource = filePath };

            // prefer to store guid in text
            builder.BinaryGUID = false;


            this.connectionString = builder.ConnectionString;
        }

        public override DbConnection CreateConnection() => new SQLiteConnection(this.connectionString);

        public override DbBuilder GetDatabaseBuilder(DmTable tableDescription, DbBuilderOption options = DbBuilderOption.UseExistingSchema) => new SQLiteBuilder(tableDescription, options);

        public override DbManager GetDbManager(string tableName) => new SQLiteManager(tableName);

        public override DbScopeBuilder GetScopeBuilder() => new SQLiteScopeBuilder();
    }
}
