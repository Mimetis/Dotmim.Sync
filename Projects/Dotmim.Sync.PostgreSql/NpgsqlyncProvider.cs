using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using System.Data.Common;
using Npgsql;
using System;

namespace Dotmim.Sync.Postgres
{

    public class NpgsqlSyncProvider : CoreProvider
    {
        private DbMetadata dbMetadata;
        static string providerType;
        public NpgsqlSyncProvider() : base()
        { }

        public NpgsqlSyncProvider(string connectionString) : base() => this.ConnectionString = connectionString;

        public NpgsqlSyncProvider(NpgsqlConnectionStringBuilder builder) : base()
        {
            if (String.IsNullOrEmpty(builder.ConnectionString))
                throw new Exception("You have to provide parameters to the Sql builder to be able to construct a valid connection string.");

            this.ConnectionString = builder.ConnectionString;
        }

        public override string ProviderTypeName => ProviderType;

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



        /// <summary>
        /// Gets or sets the Metadata object which parse Sql server types
        /// </summary>
        public override DbMetadata Metadata
        {
            get
            {
                if (dbMetadata == null)
                    dbMetadata = new NpgsqlDbMetadata();

                return dbMetadata;
            }
            set
            {
                dbMetadata = value;

            }
        }

        /// <summary>
        /// Gets a chance to make a retry connection
        /// </summary>
        public override bool ShouldRetryOn(Exception exception)
        {
            if (exception is NpgsqlException)
                return ((NpgsqlException)exception).IsTransient;

            return true;
        }

        public override void EnsureSyncException(SyncException syncException)
        {
            if (!string.IsNullOrEmpty(this.ConnectionString))
            {
                var builder = new NpgsqlConnectionStringBuilder(this.ConnectionString);

                syncException.DataSource = builder.Host;
                syncException.InitialCatalog = builder.Database;
            }

            // Can add more info from SqlException
            var sqlException = syncException.InnerException as NpgsqlException;

            if (sqlException == null)
                return;

            syncException.Number = sqlException.ErrorCode;

            return;
        }

        /// <summary>
        /// Sql server support bulk operations through Table Value parameter
        /// </summary>
        public override bool SupportBulkOperations => true;

        /// <summary>
        /// Sql Server supports to be a server side provider
        /// </summary>
        public override bool CanBeServerProvider => true;

        public override DbConnection CreateConnection() => new NpgsqlConnection(this.ConnectionString);
        public override DbScopeBuilder GetScopeBuilder() => new NpgsqlScopeBuilder();
        public override DbTableBuilder GetTableBuilder(SyncTable tableDescription, SyncSetup setup) => new NpgsqlTableBuilder(tableDescription, setup);
        public override DbTableManagerFactory GetTableManagerFactory(string tableName, string schemaName) => new NpgsqlManager(tableName, schemaName);
        public override DbBuilder GetDatabaseBuilder() => new NpgsqlBuilder();

    }
}
