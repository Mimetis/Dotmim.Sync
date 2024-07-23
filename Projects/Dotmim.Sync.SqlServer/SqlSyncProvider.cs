using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using Dotmim.Sync.SqlServer.Builders;
using Dotmim.Sync.SqlServer.Manager;
using Dotmim.Sync.SqlServer.Scope;
using Microsoft.Data.SqlClient;
using System;
using System.Data.Common;

namespace Dotmim.Sync.SqlServer
{
    public class SqlSyncProvider : CoreProvider
    {
        private static string providerType;
        private DbMetadata dbMetadata;
        private SqlConnectionStringBuilder builder;

        public SqlSyncProvider()
            : base() { }

        public static string ProviderType
        {
            get
            {
                if (!string.IsNullOrEmpty(providerType))
                    return providerType;

                var type = typeof(SqlSyncProvider);
                providerType = $"{type.Name}, {type}";

                return providerType;
            }
        }

        public override string ConnectionString
        {
            get => this.builder == null || string.IsNullOrEmpty(this.builder.ConnectionString) ? null : this.builder.ConnectionString;
            set
            {
                this.builder = string.IsNullOrEmpty(value) ? null : new SqlConnectionStringBuilder(value);
                this.SupportsMultipleActiveResultSets = this.builder != null && this.builder.MultipleActiveResultSets;
            }
        }

        public override DbConnection CreateConnection() => new SqlConnection(this.ConnectionString);

        public SqlSyncProvider(string connectionString)
            : base() => this.ConnectionString = connectionString;

        public SqlSyncProvider(SqlConnectionStringBuilder builder)
            : base()
        {
            if (builder == null || string.IsNullOrEmpty(builder.ConnectionString))
                throw new Exception("You have to provide parameters to the Sql builder to be able to construct a valid connection string.");

            this.builder = builder;
            this.SupportsMultipleActiveResultSets = builder.MultipleActiveResultSets;
        }

        public override string GetProviderTypeName() => ProviderType;

        public override string DefaultSchemaName => "dbo";

        public override ConstraintsLevelAction ConstraintsLevelAction => ConstraintsLevelAction.OnSessionLevel;

        private static string shortProviderType;

        public override string GetShortProviderTypeName() => ShortProviderType;

        public static string ShortProviderType
        {
            get
            {
                if (!string.IsNullOrEmpty(shortProviderType))
                    return shortProviderType;

                var type = typeof(SqlSyncProvider);
                shortProviderType = type.Name;

                return shortProviderType;
            }
        }

        public override string GetDatabaseName()
        {
            if (this.builder != null && !string.IsNullOrEmpty(this.builder.InitialCatalog))
                return this.builder.InitialCatalog;

            return string.Empty;
        }

        /// <summary>
        /// Gets or sets the Metadata object which parse Sql server types.
        /// </summary>
        public override DbMetadata GetMetadata()
        {
            this.dbMetadata ??= new SqlDbMetadata();

            return this.dbMetadata;
        }

        /// <summary>
        /// Gets a chance to make a retry connection.
        /// </summary>
        public override bool ShouldRetryOn(Exception exception)
        {
            Exception ex = exception;
            while (ex != null)
            {
                if (ex is SqlException)
                    return SqlServerTransientExceptionDetector.ShouldRetryOn(ex);
                else
                    ex = ex.InnerException;
            }

            return false;
        }

        public override void EnsureSyncException(SyncException syncException)
        {
            if (this.builder != null && !string.IsNullOrEmpty(this.builder.ConnectionString))
            {
                syncException.DataSource = this.builder.DataSource;
                syncException.InitialCatalog = this.builder.InitialCatalog;
            }

            // Can add more info from SqlException
            if (syncException.InnerException is not SqlException sqlException)
                return;

            syncException.Number = sqlException.Number;

            return;
        }

        /// <summary>
        /// Gets a value indicating whether sql Server supports to be a server side provider.
        /// </summary>
        public override bool CanBeServerProvider => true;

        public override (ParserName TableName, ParserName TrackingName) GetParsers(SyncTable tableDescription, SyncSetup setup)
        {
            var originalTableName = ParserName.Parse(tableDescription);

            var pref = setup?.TrackingTablesPrefix;
            var suf = setup?.TrackingTablesSuffix;

            // be sure, at least, we have a suffix if we have empty values.
            // othewise, we have the same name for both table and tracking table
            if (string.IsNullOrEmpty(pref) && string.IsNullOrEmpty(suf))
                suf = "_tracking";

            var trakingTableNameString = $"{pref}{originalTableName.ObjectName}{suf}";

            if (!string.IsNullOrEmpty(originalTableName.SchemaName))
                trakingTableNameString = $"{originalTableName.SchemaName}.{trakingTableNameString}";

            var trackingTableName = ParserName.Parse(trakingTableNameString);

            return (originalTableName, trackingTableName);
        }

        public override DbScopeBuilder GetScopeBuilder(string scopeInfoTableName) => new SqlScopeBuilder(scopeInfoTableName);

        /// <summary>
        /// Get the table builder. Table builder builds table, stored procedures and triggers.
        /// </summary>
        public override DbTableBuilder GetTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
        => new SqlTableBuilder(tableDescription, tableName, trackingTableName, setup, scopeName);

        public override DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
            => new SqlSyncAdapter(tableDescription, tableName, trackingTableName, setup, scopeName, this.UseBulkOperations);

        public override DbBuilder GetDatabaseBuilder() => new SqlBuilder();
    }
}