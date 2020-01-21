using Dotmim.Sync.Builders;
using Dotmim.Sync.Cache;
using Dotmim.Sync.Data;
using Dotmim.Sync.Manager;
using Dotmim.Sync.SqlServer.Builders;
using Dotmim.Sync.SqlServer.ChangeTracking.Builders;
using Dotmim.Sync.SqlServer.Manager;
using Dotmim.Sync.SqlServer.Scope;
using System;
using System.Data.Common;
using System.Data.SqlClient;

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

                Type type = typeof(SqlSyncProvider);
                providerType = $"{type.Name}, {type.ToString()}";

                return providerType;
            }

        }

        /// <summary>
        /// this provider supports change tracking(
        /// </summary>
        public override bool UseChangeTracking => true;

        /// <summary>
        /// Sql server support bulk operations through Table Value parameter
        /// </summary>
        public override bool SupportBulkOperations => true;

        /// <summary>
        /// Sql Server supports to be a server side provider
        /// </summary>
        public override bool CanBeServerProvider => true;
     
        public override DbConnection CreateConnection() => new SqlConnection(this.ConnectionString);
        public override DbScopeBuilder GetScopeBuilder() => new SqlChangeTrackingScopeBuilder();
        public override DbTableBuilder GetTableBuilder(SyncTable tableDescription) => new SqlChangeTrackingBuilder(tableDescription);
        public override DbTableManagerFactory GetTableManagerFactory(string tableName, string schemaName) => new SqlManager(tableName, schemaName);

    }
}
