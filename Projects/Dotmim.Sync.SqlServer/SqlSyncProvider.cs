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
    /// <summary>
    /// SqlSyncProvider provider for Sql Server.
    /// </summary>
    public class SqlSyncProvider : CoreProvider
    {
        private static string shortProviderType;
        private static string providerType;
        private DbMetadata dbMetadata;
        private SqlConnectionStringBuilder builder;

        /// <inheritdoc cref="SqlSyncProvider" />
        public SqlSyncProvider()
            : base() { }

        /// <summary>
        /// Gets the provider type.
        /// </summary>
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

        /// <summary>
        /// Gets the short provider type.
        /// </summary>
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

        /// <summary>
        /// Gets or sets the sql connection string.
        /// </summary>
        public override string ConnectionString
        {
            get => this.builder == null || string.IsNullOrEmpty(this.builder.ConnectionString) ? null : this.builder.ConnectionString;
            set
            {
                this.builder = string.IsNullOrEmpty(value) ? null : new SqlConnectionStringBuilder(value);
                this.SupportsMultipleActiveResultSets = this.builder != null && this.builder.MultipleActiveResultSets;
            }
        }

        /// <inheritdoc cref="SqlSyncProvider"/>
        public SqlSyncProvider(string connectionString)
            : base() => this.ConnectionString = connectionString;

        /// <inheritdoc cref="SqlSyncProvider"/>
        public SqlSyncProvider(SqlConnectionStringBuilder builder)
            : base()
        {
            if (builder == null || string.IsNullOrEmpty(builder.ConnectionString))
                throw new Exception("You have to provide parameters to the Sql builder to be able to construct a valid connection string.");

            this.builder = builder;
            this.SupportsMultipleActiveResultSets = builder.MultipleActiveResultSets;
        }

        /// <inheritdoc/>
        public override DbConnection CreateConnection() => new SqlConnection(this.ConnectionString);

        /// <inheritdoc/>
        public override string GetProviderTypeName() => ProviderType;

        /// <inheritdoc/>
        public override string DefaultSchemaName => "dbo";

        /// <inheritdoc/>
        public override ConstraintsLevelAction ConstraintsLevelAction => ConstraintsLevelAction.OnSessionLevel;

        /// <inheritdoc/>
        public override string GetShortProviderTypeName() => ShortProviderType;

        /// <inheritdoc/>
        public override string GetDatabaseName()
        {
            return this.builder != null && !string.IsNullOrEmpty(this.builder.InitialCatalog) ? this.builder.InitialCatalog : string.Empty;
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

        /// <inheritdoc/>
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

        /// <inheritdoc/>
        public override DbScopeBuilder GetScopeBuilder(string scopeInfoTableName) => new SqlScopeBuilder(scopeInfoTableName);

        /// <inheritdoc/>
        public override DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, ScopeInfo scopeInfo)
            => new SqlSyncAdapter(tableDescription, scopeInfo, this.UseBulkOperations);

        /// <inheritdoc/>
        public override DbDatabaseBuilder GetDatabaseBuilder() => new SqlDatabaseBuilder();
    }
}