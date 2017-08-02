using Dotmim.Sync.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dotmim.Sync.Core.Batch;
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
        ICache cacheManager;

        public SqlSyncProvider(string connectionString) : base()
        {
            this.connectionString = connectionString;
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
        


        public override DbConnection CreateConnection() => new SqlConnection(this.connectionString);
        public override DbScopeBuilder GetScopeBuilder() => new SqlScopeBuilder();
        public override DbBuilder GetDatabaseBuilder(DmTable tableDescription, DbBuilderOption options = DbBuilderOption.UseExistingSchema) => new SqlBuilder(tableDescription, options);
        public override DbManager GetDbManager(string tableName) => new SqlManager(tableName);

    }
}
