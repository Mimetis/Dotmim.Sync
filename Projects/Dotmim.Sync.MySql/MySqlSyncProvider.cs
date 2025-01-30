using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using System.Data.Common;
#if NET6_0 || NET8_0_OR_GREATER
using MySqlConnector;
#elif NETSTANDARD
using MySql.Data.MySqlClient;
#endif
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.MySql.Builders;
using System;

namespace Dotmim.Sync.MySql
{

    /// <summary>
    /// MySql Sync Provider.
    /// </summary>
    public class MySqlSyncProvider : CoreProvider
    {
        private static string providerType;
        private static string shortProviderType;
        private DbMetadata dbMetadata;
        private MySqlConnectionStringBuilder builder;

        /// <inheritdoc />
        public override string GetProviderTypeName() => ProviderType;

        /// <inheritdoc />
        public override string ConnectionString
        {
            get => this.builder == null || string.IsNullOrEmpty(this.builder.ConnectionString) ? null : this.builder.ConnectionString;
            set
            {
                this.builder = string.IsNullOrEmpty(value) ? null : new MySqlConnectionStringBuilder(value);

                // Set the default behavior to use Found rows and not Affected rows !
                this.builder.UseAffectedRows = false;
                this.builder.AllowUserVariables = true;
            }
        }

        /// <inheritdoc cref="MySqlSyncProvider"/>
        public MySqlSyncProvider(string connectionString)
            : base() => this.ConnectionString = connectionString;

        /// <inheritdoc cref="MySqlSyncProvider" />
        public MySqlSyncProvider(MySqlConnectionStringBuilder builder)
            : base()
        {
            if (builder == null || string.IsNullOrEmpty(builder.ConnectionString))
                throw new Exception("You have to provide parameters to the MySql builder to be able to construct a valid connection string.");

            this.builder = builder;

            // Set the default behavior to use Found rows and not Affected rows !
            builder.UseAffectedRows = false;
            builder.AllowUserVariables = true;
        }

        /// <summary>
        /// Gets the provider type.
        /// </summary>
        public static string ProviderType
        {
            get
            {
                if (!string.IsNullOrEmpty(providerType))
                    return providerType;

                var type = typeof(MySqlSyncProvider);
                providerType = $"{type.Name}, {type}";

                return providerType;
            }
        }

        /// <inheritdoc />
        public override string GetShortProviderTypeName() => ShortProviderType;

        /// <summary>
        /// Gets the short name of the provider.
        /// </summary>
        public static string ShortProviderType
        {
            get
            {
                if (!string.IsNullOrEmpty(shortProviderType))
                    return shortProviderType;

                var type = typeof(MySqlSyncProvider);
                shortProviderType = type.Name;

                return shortProviderType;
            }
        }

        /// <inheritdoc />
        public override ConstraintsLevelAction ConstraintsLevelAction => ConstraintsLevelAction.OnTableLevel;

        /// <summary>
        /// Gets a value indicating whether mySql can be a server side provider.
        /// </summary>
        public override bool CanBeServerProvider => true;

        /// <summary>
        /// Gets or Sets the MySql Metadata object, provided to validate the MySql Columns issued from MySql.
        /// </summary>
        /// <summary>
        /// Gets or sets the Metadata object which parse Sql server types.
        /// </summary>
        public override DbMetadata GetMetadata()
        {
            this.dbMetadata ??= new MySqlDbMetadata();
            return this.dbMetadata;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlSyncProvider"/> class.
        /// </summary>
        public MySqlSyncProvider()
            : base()
        {
        }

        /// <summary>
        /// Gets a chance to make a retry if the error is a transient error.
        /// </summary>
        public override bool ShouldRetryOn(Exception exception)
        {
            Exception ex = exception;
            while (ex != null)
            {
                if (ex is MySqlException mySqlException)
                    return MySqlTransientExceptionDetector.ShouldRetryOn(mySqlException);
                else
                    ex = ex.InnerException;
            }

            return false;
        }

        /// <inheritdoc />
        public override string GetDatabaseName()
        {
            if (this.builder != null && !string.IsNullOrEmpty(this.builder.Database))
                return this.builder.Database;

            return string.Empty;
        }

        /// <inheritdoc />
        public override void EnsureSyncException(SyncException syncException)
        {
            if (this.builder != null && !string.IsNullOrEmpty(this.builder.ConnectionString))
            {
                syncException.DataSource = this.builder.Server;
                syncException.InitialCatalog = this.builder.Database;
            }

            if (syncException.InnerException is not MySqlException mySqlException)
                return;

            syncException.Number = mySqlException.Number;

            return;
        }

        /// <inheritdoc />
        public override DbConnection CreateConnection() => new MySqlConnection(this.ConnectionString);

        /// <inheritdoc />
        public override DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, ScopeInfo scopeInfo)
            => new MySqlSyncAdapter(tableDescription, scopeInfo);

        /// <inheritdoc />
        public override DbScopeBuilder GetScopeBuilder(string scopeInfoTableName)
            => new MySqlScopeInfoBuilder(scopeInfoTableName);

        /// <inheritdoc />
        public override DbDatabaseBuilder GetDatabaseBuilder()
            => new MySqlDatabaseBuilder();
    }
}