using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using System;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using System.IO;
using Dotmim.Sync.Sqlite.Builders;
using SQLitePCL;
using System.Reflection;
using Dotmim.Sync.Enumerations;

namespace Dotmim.Sync.Sqlite
{

    public class SqliteSyncProvider : CoreProvider
    {


        private DbMetadata dbMetadata;
        private static String providerType;
        private SqliteConnectionStringBuilder builder;

        public override DbMetadata GetMetadata()
        {
            if (dbMetadata == null)
                dbMetadata = new SqliteDbMetadata();

            return dbMetadata;
        }

        public override ConstraintsLevelAction ConstraintsLevelAction => ConstraintsLevelAction.OnDatabaseLevel;

        /// <summary>
        /// SQLIte does not support to be a server side.
        /// Reason 1 : Can't easily insert / update batch with handling conflict
        /// Reason 2 : Can't filter rows (based on filterclause)
        /// </summary>
        public override bool CanBeServerProvider => false;

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


        static string shortProviderType;
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
        public SqliteSyncProvider() : base()
        {
        }

        public override string ConnectionString
        {
            get
            {
                if (builder == null)
                    return null;

                return builder.ConnectionString;
            }
            set
            {
                this.builder = new SqliteConnectionStringBuilder(value);
            }
        }
        public SqliteSyncProvider(string filePath) : this()
        {
            if (filePath.ToLowerInvariant().StartsWith("data source"))
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

        public SqliteSyncProvider(FileInfo fileInfo) : this()
        {
            this.builder = new SqliteConnectionStringBuilder { DataSource = fileInfo.FullName };
        }

        public override string GetDatabaseName()
        {
            if (builder != null && !String.IsNullOrEmpty(builder.DataSource))
                return builder.DataSource;

            return string.Empty;
        }

        public SqliteSyncProvider(SqliteConnectionStringBuilder sqliteConnectionStringBuilder) : this()
        {
            if (string.IsNullOrEmpty(sqliteConnectionStringBuilder.DataSource))
                throw new Exception("You have to provide at least a DataSource property to be able to connect to your SQlite database.");

            this.builder = sqliteConnectionStringBuilder;
        }

        public override void EnsureSyncException(SyncException syncException)
        {

            if (builder != null)
                syncException.DataSource = builder.DataSource;

            var sqliteException = syncException.InnerException as SqliteException;

            if (sqliteException == null)
                return;

            syncException.Number = sqliteException.SqliteErrorCode;


            return;
        }

        public override DbConnection CreateConnection()
        {
            if (!builder.ForeignKeys.HasValue && this.Orchestrator != null)
                builder.ForeignKeys = !this.Orchestrator.Options.DisableConstraintsOnApplyChanges;

            var connectionString = builder.ToString();
            
            var sqliteConnection = new SqliteConnection(connectionString);

            return sqliteConnection;
        }

        public override DbScopeBuilder GetScopeBuilder(string scopeInfoTableName) => new SqliteScopeBuilder(scopeInfoTableName);

        public override DbTableBuilder GetTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
        => new SqliteTableBuilder(tableDescription, tableName, trackingTableName, setup, scopeName);

        public override DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
            => new SqliteSyncAdapter(tableDescription, tableName, trackingTableName, setup, scopeName);

        public override DbBuilder GetDatabaseBuilder() => new SqliteBuilder();

        public override (ParserName tableName, ParserName trackingName) GetParsers(SyncTable tableDescription, SyncSetup setup)
        {
            string tableAndPrefixName = tableDescription.TableName;
            var originalTableName = ParserName.Parse(tableDescription);

            var pref = setup.TrackingTablesPrefix != null ? setup.TrackingTablesPrefix : "";
            var suf = setup.TrackingTablesSuffix != null ? setup.TrackingTablesSuffix : "";

            // be sure, at least, we have a suffix if we have empty values. 
            // othewise, we have the same name for both table and tracking table
            if (string.IsNullOrEmpty(pref) && string.IsNullOrEmpty(suf))
                suf = "_tracking";

            var trackingTableName = ParserName.Parse($"{pref}{tableAndPrefixName}{suf}");

            return (originalTableName, trackingTableName);
        }
    }
}
