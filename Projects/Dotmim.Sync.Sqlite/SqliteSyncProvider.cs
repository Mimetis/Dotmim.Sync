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

        public SQLiteSyncProvider(string connectionString) : base()
        {
            this.connectionString = connectionString;
        }

        public override DbConnection CreateConnection() => new SQLiteConnection(this.connectionString);
        
        public override DbBuilder GetDatabaseBuilder(DmTable tableDescription, DbBuilderOption options = DbBuilderOption.UseExistingSchema) => new SQLiteBuilder(tableDescription, options);

        public override DbManager GetDbManager(string tableName) => new SQLiteManager(tableName);

        public override DbScopeBuilder GetScopeBuilder() => new SQLiteScopeBuilder();
    }
}
