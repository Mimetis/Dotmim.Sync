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
    /// <summary>
    /// Represents a Npgsql provider that can handle every PostgreSQL database for sync.
    /// </summary>
    public class NpgsqlSyncProvider : CoreProvider
    {
        private static string providerType;
        private static string shortProviderType;
        private NpgsqlConnectionStringBuilder builder;
        private NpgsqlDbMetadata dbMetadata;
        internal const string NPGSQLPREFIXPARAMETER = "in_";

        /// <summary>
        /// Initializes a new instance of the <see cref="NpgsqlSyncProvider"/> class.
        /// </summary>
        public NpgsqlSyncProvider()
            : base() { }

        /// <summary>
        /// Gets the Npgsql short name.
        /// </summary>
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

        /// <summary>
        /// Gets the Npgsql provider type.
        /// </summary>
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

        /// <inheritdoc />
        public override string ConnectionString
        {
            get => this.builder == null || string.IsNullOrEmpty(this.builder.ConnectionString) ? null : this.builder.ConnectionString;
            set => this.builder = string.IsNullOrEmpty(value) ? null : new NpgsqlConnectionStringBuilder(value);
        }

        /// <inheritdoc  cref="NpgsqlSyncProvider"/>
        public NpgsqlSyncProvider(string connectionString)
            : base() => this.ConnectionString = connectionString;

        /// <inheritdoc  cref="NpgsqlSyncProvider"/>
        public NpgsqlSyncProvider(NpgsqlConnectionStringBuilder builder)
            : base()
        {
            if (builder == null || string.IsNullOrEmpty(builder.ConnectionString))
                throw new Exception("You have to provide parameters to the Npgsql builder to be able to construct a valid connection string.");

            this.builder = builder;
        }

        /// <inheritdoc/>
        public override ConstraintsLevelAction ConstraintsLevelAction => ConstraintsLevelAction.OnTableLevel;

        /// <inheritdoc/>
        public override bool CanBeServerProvider => true;

        /// <inheritdoc/>
        public override string DefaultSchemaName => "public";

        /// <inheritdoc/>
        public override DbConnection CreateConnection() => new NpgsqlConnection(this.ConnectionString);

        /// <inheritdoc/>
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

        /// <inheritdoc/>
        public override DbDatabaseBuilder GetDatabaseBuilder() => new NpgsqlDatabaseBuilder();

        /// <inheritdoc/>
        public override string GetDatabaseName()
            => this.builder != null && !string.IsNullOrEmpty(this.builder.Database) ? this.builder.Database : string.Empty;

        /// <inheritdoc/>
        public override DbMetadata GetMetadata()
        {
            this.dbMetadata ??= new NpgsqlDbMetadata();
            return this.dbMetadata;
        }

        /// <inheritdoc/>
        public override string GetProviderTypeName() => ProviderType;

        /// <inheritdoc/>
        public override DbScopeBuilder GetScopeBuilder(string scopeInfoTableName) => new NpgsqlScopeBuilder(scopeInfoTableName);

        /// <inheritdoc/>
        public override string GetShortProviderTypeName() => ShortProviderType;

        /// <inheritdoc/>
        public override DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, ScopeInfo scopeInfo)
                => new NpgsqlSyncAdapter(tableDescription, scopeInfo, this.UseBulkOperations);
    }
}