using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using Dotmim.Sync.SqlServer.Builders;
using Dotmim.Sync.SqlServer.ChangeTracking.Builders;
using Dotmim.Sync.SqlServer.Manager;
using Dotmim.Sync.SqlServer.Scope;
using System;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer
{
    public class SqlSyncChangeTrackingProvider : SqlSyncProvider
    {
        static String providerType;

        public SqlSyncChangeTrackingProvider() : base()
        {
        }

        public SqlSyncChangeTrackingProvider(string connectionString) : base()
        {
            this.ConnectionString = connectionString;
        }

        public SqlSyncChangeTrackingProvider(SqlConnectionStringBuilder builder) : base()
        {
            if (String.IsNullOrEmpty(builder.ConnectionString))
                throw new Exception("You have to provide parameters to the Sql builder to be able to construct a valid connection string.");

            this.ConnectionString = builder.ConnectionString;
        }

        public static new string ProviderType
        {
            get
            {
                if (!string.IsNullOrEmpty(providerType))
                    return providerType;

                Type type = typeof(SqlSyncChangeTrackingProvider);
                providerType = $"{type.Name}, {type}";

                return providerType;
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
        public override DbScopeBuilder GetScopeBuilder(string scopeInfoTableName) => new SqlChangeTrackingScopeBuilder(scopeInfoTableName);

        public override DbTableBuilder GetTableBuilder(SyncTable tableDescription, SyncSetup setup)
        {
            var (tableName, trackingName) = GetParsers(tableDescription, setup);

            var tableBuilder = new SqlChangeTrackingTableBuilder(tableDescription, tableName, trackingName, setup);

            return tableBuilder;
        }

        public override DbBuilder GetDatabaseBuilder() => new SqlChangeTrackingBuilder();


    }
}
