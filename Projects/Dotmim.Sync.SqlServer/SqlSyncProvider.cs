using Dotmim.Sync.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dotmim.Sync.Core.Batch;
using Dotmim.Sync.SqlServer.Batch;
using Dotmim.Sync.SqlServer.Builders;
using System.Data.Common;
using System.Data.SqlClient;
using Dotmim.Sync.Core.Log;
using Dotmim.Sync.Core.Scope;
using Dotmim.Sync.Data;
using Dotmim.Sync.Core.Builders;
using Dotmim.Sync.SqlServer.Scope;
using Dotmim.Sync.Core.Manager;
using Dotmim.Sync.SqlServer.Manager;
using Dotmim.Sync.Core.Cache;

namespace Dotmim.Sync.SqlServer
{
    public class SqlSyncProvider : CoreProvider
    {
        string connectionString;
  
        /// <summary>
        /// Get the sql connection used to access the server side database
        /// </summary>
        public override DbConnection CreateConnection()
        {
            return new SqlConnection(this.connectionString);
        }


        public SqlSyncProvider(string connectionString) : base()
        {
            this.connectionString = connectionString;
        }

        private ICache cacheManager;
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

        public override DbScopeBuilder GetScopeBuilder()
        {
            return new SqlScopeBuilder();
        }

        public override DbBuilder GetDatabaseBuilder(DmTable tableDescription, DbBuilderOption options = DbBuilderOption.UseExistingSchema)
        {
            return new SqlBuilder(tableDescription, options);
        }

        public override DbManager GetDbManager(string tableName)
        {
            return new SqlManager(tableName);
        }

    }
}
