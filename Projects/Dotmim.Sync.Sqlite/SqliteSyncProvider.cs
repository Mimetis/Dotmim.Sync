using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using System;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using System.IO;
using Dotmim.Sync.Sqlite.Builders;

namespace Dotmim.Sync.Sqlite
{

    public class SqliteSyncProvider : CoreProvider
    {
        private string filePath;
        private DbMetadata dbMetadata;
        private static String providerType;

        public override DbMetadata Metadata
        {
            get
            {
                if (dbMetadata == null)
                    dbMetadata = new SqliteDbMetadata();

                return dbMetadata;
            }
            set
            {
                dbMetadata = value;

            }
        }
     

        /// <summary>
        /// Sqlite does not support Bulk operations
        /// </summary>
        public override bool SupportBulkOperations => false;

        /// <summary>
        /// SQLIte does not support to be a server side.
        /// Reason 1 : Can't easily insert / update batch with handling conflict
        /// Reason 2 : Can't filter rows (based on filterclause)
        /// </summary>
        public override bool CanBeServerProvider => false;

        public override string ProviderTypeName => ProviderType;

        public static string ProviderType
        {
            get
            {
                if (!string.IsNullOrEmpty(providerType))
                    return providerType;

                Type type = typeof(SqliteSyncProvider);
                providerType = $"{type.Name}, {type.ToString()}";

                return providerType;
            }

        }
        public SqliteSyncProvider() : base()
        {
        }

        /// <summary>
        /// Override Options for settings special PRAGMA stuff
        /// </summary>
        public override SyncOptions Options
        {
            get
            {
                return base.Options;
            }
            set
            {
                base.Options = value;

                // Affect options
                var builder = new SqliteConnectionStringBuilder(this.ConnectionString)
                {
                    // overriding connectionstring
                    ForeignKeys = !this.Options.DisableConstraintsOnApplyChanges
                };

                this.ConnectionString = builder.ToString();
            }
        }

        public SqliteSyncProvider(string filePath) : this()
        {
            this.filePath = filePath;
            var builder = new SqliteConnectionStringBuilder();

            if (filePath.ToLowerInvariant().StartsWith("data source"))
            {
                this.ConnectionString = filePath;
            }
            else
            {
                var fileInfo = new FileInfo(filePath);

                if (!Directory.Exists(fileInfo.Directory.FullName))
                    throw new Exception($"filePath directory {fileInfo.Directory.FullName} does not exists.");

                if (string.IsNullOrEmpty(fileInfo.Name))
                    throw new Exception($"Sqlite database file path needs a file name");

                builder.DataSource = filePath;

                this.ConnectionString = builder.ConnectionString;
            }

        }

        public SqliteSyncProvider(FileInfo fileInfo) : this()
        {
            this.filePath = fileInfo.FullName;
            var builder = new SqliteConnectionStringBuilder { DataSource = filePath };

            this.ConnectionString = builder.ConnectionString;
        }



        public SqliteSyncProvider(SqliteConnectionStringBuilder sqliteConnectionStringBuilder) : this()
        {
            if (String.IsNullOrEmpty(sqliteConnectionStringBuilder.DataSource))
                throw new Exception("You have to provide at least a DataSource property to be able to connect to your SQlite database.");

            this.filePath = sqliteConnectionStringBuilder.DataSource;

            this.ConnectionString = sqliteConnectionStringBuilder.ConnectionString;
        }

        public override void EnsureSyncException(SyncException syncException)
        {
            if (!string.IsNullOrEmpty(this.ConnectionString))
            {
                var builder = new SqliteConnectionStringBuilder(this.ConnectionString);

                syncException.DataSource = builder.DataSource;
            }

            var sqliteException = syncException.InnerException as SqliteException;

            if (sqliteException == null)
                return;

            syncException.Number = sqliteException.SqliteErrorCode;
            

            return;
        }


        public override DbConnection CreateConnection()
        {
            var sqliteConnection = new SqliteConnection(this.ConnectionString);
            return sqliteConnection;
        }



        public override DbTableBuilder GetTableBuilder(SyncTable tableDescription) => new SqliteTableBuilder(tableDescription);

        public override DbTableManagerFactory GetTableManagerFactory(string tableName, string schemaName) => new SqliteManager(tableName);

        public override DbScopeBuilder GetScopeBuilder() => new SqliteScopeBuilder();

        public override DbBuilder GetDatabaseBuilder() => new SqliteBuilder();
    }
}
