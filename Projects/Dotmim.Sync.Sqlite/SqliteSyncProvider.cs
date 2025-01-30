using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using Dotmim.Sync.Sqlite.Builders;
using Microsoft.Data.Sqlite;
using System;
using System.Data.Common;
using System.IO;

namespace Dotmim.Sync.Sqlite
{
    /// <summary>
    /// Sqlite sync provider.
    /// </summary>
    public class SqliteSyncProvider : CoreProvider
    {

        private static string shortProviderType;
        private static string providerType;
        private DbMetadata dbMetadata;
        private SqliteConnectionStringBuilder builder;

        /// <inheritdoc />
        public override DbMetadata GetMetadata()
        {
            this.dbMetadata ??= new SqliteDbMetadata();

            return this.dbMetadata;
        }

        /// <inheritdoc />
        public override string ConnectionString
        {
            get => this.builder == null || string.IsNullOrEmpty(this.builder.ConnectionString) ? null : this.builder.ConnectionString;
            set => this.builder = string.IsNullOrEmpty(value) ? null : new SqliteConnectionStringBuilder(value);
        }

        /// <inheritdoc />
        public override ConstraintsLevelAction ConstraintsLevelAction => ConstraintsLevelAction.OnDatabaseLevel;

        /// <summary>
        /// Gets a value indicating whether sQLIte does not support to be a server side.
        /// Reason 1 : Can't easily insert / update batch with handling conflict
        /// Reason 2 : Can't filter rows (based on filterclause).
        /// </summary>
        public override bool CanBeServerProvider => false;

        /// <summary>
        /// Gets or sets a value indicating whether SQL filters generation is disabled.
        /// When set to <c>true</c>, SQL filters will not be generated during command creation.
        /// </summary>
        /// <value>
        ///   <c>true</c> if SQL filters generation is disabled; otherwise, <c>false</c>.
        ///   The default value is <c>true</c>.
        /// </value>
        public bool DisableSqlFiltersGeneration { get; set; } = true;

        /// <inheritdoc />
        public override string GetProviderTypeName() => ProviderType;

        /// <summary>
        /// Gets the provider type.
        /// </summary>
        public static string ProviderType
        {
            get
            {
                if (!string.IsNullOrEmpty(providerType))
                    return providerType;

                Type type = typeof(SqliteSyncProvider);
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

                var type = typeof(SqliteSyncProvider);
                shortProviderType = type.Name;

                return shortProviderType;
            }
        }

        /// <inheritdoc />
        public override string GetShortProviderTypeName() => ShortProviderType;

        /// <inheritdoc cref="SqliteSyncProvider" />
        public SqliteSyncProvider()
            : base()
        {
        }

        /// <inheritdoc cref="SqliteSyncProvider"/>
        public SqliteSyncProvider(FileInfo fileInfo)
            : this() => this.builder = new SqliteConnectionStringBuilder { DataSource = fileInfo.FullName };

        /// <inheritdoc cref="SqliteSyncProvider"/>
        public SqliteSyncProvider(SqliteConnectionStringBuilder sqliteConnectionStringBuilder)
            : this()
        {
            if (string.IsNullOrEmpty(sqliteConnectionStringBuilder.DataSource))
                throw new Exception("You have to provide at least a DataSource property to be able to connect to your SQlite database.");

            this.builder = sqliteConnectionStringBuilder;
        }

        /// <inheritdoc cref="SqliteSyncProvider"/>
        public SqliteSyncProvider(string filePath)
            : this()
        {
            if (filePath.StartsWith("data source", SyncGlobalization.DataSourceStringComparison))
            {
                this.builder = new SqliteConnectionStringBuilder(filePath);
            }
            else
            {
                var fileInfo = new FileInfo(filePath);

                if (!Directory.Exists(fileInfo.Directory.FullName))
                    throw new Exception($"filePath directory {fileInfo.Directory.FullName} does not exists.");

                if (string.IsNullOrEmpty(fileInfo.Name))
                    throw new Exception($"Sqlite database file path needs a file name");

                this.builder = new SqliteConnectionStringBuilder { DataSource = filePath };
            }
        }

        /// <summary>
        /// Gets the file path extracted from the connection string.
        /// </summary>
        public string FilePath
        {
            get
            {
                try
                {
                    return new FileInfo(this.builder.DataSource).FullName;
                }
                catch (Exception)
                {

                    return null;
                }
            }
        }

        /// <summary>
        /// Gets a chance to make a retry if the error is a transient error.
        /// </summary>
        public override bool ShouldRetryOn(Exception exception)
        {
            Exception ex = exception;
            while (ex != null)
            {
                if (ex is SqliteException sqliteException)
                    return SqliteTransientExceptionDetector.ShouldRetryOn(sqliteException);
                else
                    ex = ex.InnerException;
            }

            return false;
        }

        /// <inheritdoc/>
        public override string GetDatabaseName() => this.builder != null && !string.IsNullOrEmpty(this.builder.DataSource)
                ? new FileInfo(this.builder.DataSource).Name
                : string.Empty;

        /// <inheritdoc/>
        public override void EnsureSyncException(SyncException syncException)
        {
            if (this.builder != null)
                syncException.DataSource = this.builder.DataSource;

            if (syncException.InnerException is not SqliteException sqliteException)
                return;

            syncException.Number = sqliteException.SqliteErrorCode;

            return;
        }

        /// <inheritdoc/>
        public override DbConnection CreateConnection()
        {
            if (!this.builder.ForeignKeys.HasValue && this.Orchestrator != null)
                this.builder.ForeignKeys = !this.Orchestrator.Options.DisableConstraintsOnApplyChanges;

            var connectionString = this.builder.ToString();

            var sqliteConnection = new SqliteConnection(connectionString);

            return sqliteConnection;
        }

        /// <inheritdoc/>
        public override DbScopeBuilder GetScopeBuilder(string scopeInfoTableName) => new SqliteScopeBuilder(scopeInfoTableName);

        /// <inheritdoc/>
        public override DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, ScopeInfo scopeInfo)
            => new SqliteSyncAdapter(tableDescription, scopeInfo, this.DisableSqlFiltersGeneration);

        /// <inheritdoc/>
        public override DbDatabaseBuilder GetDatabaseBuilder() => new SQLiteDatabaseBuilder();
    }
}