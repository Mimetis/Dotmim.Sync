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
using Dotmim.Sync.SqlServer.Scope;

namespace Dotmim.Sync.Sqlite
{

    public class SqliteSyncProvider : CoreProvider
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

        public SqliteSyncProvider(string connectionString) : base()
        {
            this.connectionString = connectionString;
        }


        public override DbConnection CreateConnection()
        {
            return new SQLiteConnection(this.connectionString);
        }

        public override DbBuilder GetDatabaseBuilder(DmTable tableDescription, DbBuilderOption options = DbBuilderOption.UseExistingSchema)
        {
            throw new NotImplementedException();
        }

        public override DbManager GetDbManager(string tableName)
        {
            throw new NotImplementedException();
        }

        public override DbScopeBuilder GetScopeBuilder() => new SQLiteScopeBuilder();
    }
}
