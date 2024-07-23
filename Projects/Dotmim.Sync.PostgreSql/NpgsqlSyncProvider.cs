using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using Dotmim.Sync.PostgreSql.Builders;
using Dotmim.Sync.PostgreSql.Scope;
using Npgsql;
using System;
using System.Data.Common;

namespace Dotmim.Sync.PostgreSql
{
    public class NpgsqlSyncProvider : CoreProvider
    {
        private static string providerType;
        private static string shortProviderType;
        private NpgsqlConnectionStringBuilder builder;
        private NpgsqlDbMetadata dbMetadata;
        internal const string NPGSQLPREFIXPARAMETER = "in_";

        public NpgsqlSyncProvider()
            : base() { }

        public static string ShortProviderType
        {
            get
            {
                if (!string.IsNullOrEmpty(shortProviderType))
                    return shortProviderType;

                var type = typeof(NpgsqlSyncProvider);
                shortProviderType = type.Name;

                return shortProviderType;
            }
        }

        public override string ConnectionString
        {
            get => this.builder == null || string.IsNullOrEmpty(this.builder.ConnectionString) ? null : this.builder.ConnectionString;
            set => this.builder = string.IsNullOrEmpty(value) ? null : new NpgsqlConnectionStringBuilder(value);
        }

        public NpgsqlSyncProvider(string connectionString)
            : base() => this.ConnectionString = connectionString;

        public NpgsqlSyncProvider(NpgsqlConnectionStringBuilder builder)
            : base()
        {
            if (builder == null || string.IsNullOrEmpty(builder.ConnectionString))
                throw new Exception("You have to provide parameters to the Npgsql builder to be able to construct a valid connection string.");

            this.builder = builder;
        }

        public static string ProviderType
        {
            get
            {
                if (!string.IsNullOrEmpty(providerType))
                    return providerType;

                var type = typeof(NpgsqlSyncProvider);
                providerType = $"{type.Name}, {type}";

                return providerType;
            }
        }

        public override ConstraintsLevelAction ConstraintsLevelAction => ConstraintsLevelAction.OnTableLevel;

        public override bool CanBeServerProvider => true;

        public override string DefaultSchemaName => "public";

        public override DbConnection CreateConnection() => new NpgsqlConnection(this.ConnectionString);

        /// <summary>
        /// Gets a chance to make a retry if the error is a transient error.
        /// </summary>
        public override bool ShouldRetryOn(Exception exception)
        {
            Exception ex = exception;
            while (ex != null)
            {
                if (ex is NpgsqlException npgsqlException)
                    return npgsqlException.IsTransient;
                else
                    ex = ex.InnerException;
            }

            return false;
        }

        public override DbBuilder GetDatabaseBuilder() => new NpgsqlBuilder();

        public override string GetDatabaseName()
        {
            if (this.builder != null && !string.IsNullOrEmpty(this.builder.Database))
                return this.builder.Database;

            return string.Empty;
        }

        public override DbMetadata GetMetadata()
        {
            this.dbMetadata ??= new NpgsqlDbMetadata();

            return this.dbMetadata;
        }

        public override (ParserName TableName, ParserName TrackingName) GetParsers(SyncTable tableDescription, SyncSetup setup = null)
        {
            var originalTableName = ParserName.Parse(tableDescription, "\"");

            var pref = setup?.TrackingTablesPrefix;
            var suf = setup?.TrackingTablesSuffix;

            // be sure, at least, we have a suffix if we have empty values.
            // othewise, we have the same name for both table and tracking table
            if (string.IsNullOrEmpty(pref) && string.IsNullOrEmpty(suf))
                suf = "_tracking";

            var trakingTableNameString = $"{pref}{originalTableName.ObjectName}{suf}";

            if (!string.IsNullOrEmpty(originalTableName.SchemaName))
                trakingTableNameString = $"{originalTableName.SchemaName}.{trakingTableNameString}";

            var trackingTableName = ParserName.Parse(trakingTableNameString, "\"");

            return (originalTableName, trackingTableName);
        }

        public override string GetProviderTypeName() => ProviderType;

        public override DbScopeBuilder GetScopeBuilder(string scopeInfoTableName) => new NpgsqlScopeBuilder(scopeInfoTableName);

        public override string GetShortProviderTypeName() => ShortProviderType;

        public override DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
                => new NpgsqlSyncAdapter(tableDescription, tableName, trackingTableName, setup, scopeName, this.UseBulkOperations);

        public override DbTableBuilder GetTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
                    => new NpgsqlTableBuilder(tableDescription, tableName, trackingTableName, setup, scopeName);
    }
}