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

    public class SqliteSyncProvider : CoreProvider
    {

        private DbMetadata dbMetadata;
        private static string providerType;
        private SqliteConnectionStringBuilder builder;

        public override DbMetadata GetMetadata()
        {
            if (this.dbMetadata == null)
                this.dbMetadata = new SqliteDbMetadata();

            return this.dbMetadata;
        }

        public override string ConnectionString
        {
            get => this.builder == null || string.IsNullOrEmpty(this.builder.ConnectionString) ? null : this.builder.ConnectionString;
            set => this.builder = string.IsNullOrEmpty(value) ? null : new SqliteConnectionStringBuilder(value);
        }

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

        public override string GetProviderTypeName() => ProviderType;

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

        private static string shortProviderType;

        public override string GetShortProviderTypeName() => ShortProviderType;

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

        public SqliteSyncProvider()
            : base()
        {
        }

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

        public SqliteSyncProvider(FileInfo fileInfo)
            : this()
        {
            this.builder = new SqliteConnectionStringBuilder { DataSource = fileInfo.FullName };
        }

        public override string GetDatabaseName()
        {
            if (this.builder != null && !string.IsNullOrEmpty(this.builder.DataSource))
                return new FileInfo(this.builder.DataSource).Name;

            return string.Empty;
        }

        public SqliteSyncProvider(SqliteConnectionStringBuilder sqliteConnectionStringBuilder)
            : this()
        {
            if (string.IsNullOrEmpty(sqliteConnectionStringBuilder.DataSource))
                throw new Exception("You have to provide at least a DataSource property to be able to connect to your SQlite database.");

            this.builder = sqliteConnectionStringBuilder;
        }

        public override void EnsureSyncException(SyncException syncException)
        {
            if (this.builder != null)
                syncException.DataSource = this.builder.DataSource;

            if (syncException.InnerException is not SqliteException sqliteException)
                return;

            syncException.Number = sqliteException.SqliteErrorCode;

            return;
        }

        public override DbConnection CreateConnection()
        {
            if (!this.builder.ForeignKeys.HasValue && this.Orchestrator != null)
                this.builder.ForeignKeys = !this.Orchestrator.Options.DisableConstraintsOnApplyChanges;

            var connectionString = this.builder.ToString();

            var sqliteConnection = new SqliteConnection(connectionString);

            return sqliteConnection;
        }

        public override DbScopeBuilder GetScopeBuilder(string scopeInfoTableName) => new SqliteScopeBuilder(scopeInfoTableName);

        public override DbTableBuilder GetTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
        => new SqliteTableBuilder(tableDescription, tableName, trackingTableName, setup, scopeName, this.DisableSqlFiltersGeneration);

        public override DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
            => new SqliteSyncAdapter(tableDescription, tableName, trackingTableName, setup, scopeName, this.DisableSqlFiltersGeneration);

        public override DbBuilder GetDatabaseBuilder() => new SqliteBuilder();

        public override (ParserName TableName, ParserName TrackingName) GetParsers(SyncTable tableDescription, SyncSetup setup)
        {
            string tableAndPrefixName = tableDescription.TableName;
            var originalTableName = ParserName.Parse(tableDescription);

            var pref = setup != null && setup.TrackingTablesPrefix != null ? setup.TrackingTablesPrefix : string.Empty;
            var suf = setup != null && setup.TrackingTablesSuffix != null ? setup.TrackingTablesSuffix : string.Empty;

            // be sure, at least, we have a suffix if we have empty values.
            // othewise, we have the same name for both table and tracking table
            if (string.IsNullOrEmpty(pref) && string.IsNullOrEmpty(suf))
                suf = "_tracking";

            var trackingTableName = ParserName.Parse($"{pref}{tableAndPrefixName}{suf}");

            return (originalTableName, trackingTableName);
        }
    }
}