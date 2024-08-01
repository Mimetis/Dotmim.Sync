using Dotmim.Sync.Builders;
using Dotmim.Sync.SqlServer.Builders;
using Dotmim.Sync.SqlServer.ChangeTracking.Builders;
using Microsoft.Data.SqlClient;
using System;
using System.Data.Common;

namespace Dotmim.Sync.SqlServer
{
    public class SqlSyncChangeTrackingProvider : SqlSyncProvider
    {
        private static string providerType;

        public SqlSyncChangeTrackingProvider()
            : base() { }

        public SqlSyncChangeTrackingProvider(string connectionString)
            : base(connectionString)
        {
        }

        public SqlSyncChangeTrackingProvider(SqlConnectionStringBuilder builder)
            : base(builder)
        {
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

        public override string GetProviderTypeName() => ProviderType;

        private static string shortProviderType;

        public static new string ShortProviderType
        {
            get
            {
                if (!string.IsNullOrEmpty(shortProviderType))
                    return shortProviderType;

                var type = typeof(SqlSyncChangeTrackingProvider);
                shortProviderType = type.Name;

                return shortProviderType;
            }
        }

        public override DbScopeBuilder GetScopeBuilder(string scopeInfoTableName) => new SqlChangeTrackingScopeBuilder(scopeInfoTableName);

        public override DbTableBuilder GetTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
            => new SqlChangeTrackingTableBuilder(tableDescription, tableName, trackingTableName, setup, scopeName);

        public override DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
            => new SqlChangeTrackingSyncAdapter(tableDescription, tableName, trackingTableName, setup, scopeName, this.UseBulkOperations);

        public override DbDatabaseBuilder GetDatabaseBuilder() => new SqlChangeTrackingDatabaseBuilder();
    }
}