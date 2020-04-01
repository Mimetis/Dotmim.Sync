using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using Dotmim.Sync.SqlServer.Builders;
using Dotmim.Sync.SqlServer.Manager;
using Dotmim.Sync.SqlServer.Scope;
using System;
using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace Dotmim.Sync.SqlServer
{
    public class SqlSyncProvider : CoreProvider
    {
        private DbMetadata dbMetadata;
        static string providerType;
        public SqlSyncProvider() : base()
        { }

        public SqlSyncProvider(string connectionString) : base() => this.ConnectionString = connectionString;

        public SqlSyncProvider(SqlConnectionStringBuilder builder) : base()
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

                var type = typeof(SqlSyncProvider);
                providerType = $"{type.Name}, {type.ToString()}";

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
                    dbMetadata = new SqlDbMetadata();

                return dbMetadata;
            }
            set
            {
                dbMetadata = value;

            }
        }
    
        public override void EnsureSyncException(SyncException syncException)
        {
            if (!string.IsNullOrEmpty(this.ConnectionString))
            {
                var builder = new SqlConnectionStringBuilder(this.ConnectionString);

                syncException.DataSource = builder.DataSource;
                syncException.InitialCatalog = builder.InitialCatalog;
            }

            // Can add more info from SqlException
            var sqlException = syncException.InnerException as SqlException;

            if (sqlException == null)
                return;

            syncException.Number = sqlException.Number;

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
     
        public override DbConnection CreateConnection() => new SqlConnection(this.ConnectionString);
        public override DbScopeBuilder GetScopeBuilder() => new SqlScopeBuilder();
        public override DbTableBuilder GetTableBuilder(SyncTable tableDescription) => new SqlTableBuilder(tableDescription);
        public override DbTableManagerFactory GetTableManagerFactory(string tableName, string schemaName) => new SqlManager(tableName, schemaName);
        public override DbBuilder GetDatabaseBuilder() => new SqlBuilder();
       
    }
}
