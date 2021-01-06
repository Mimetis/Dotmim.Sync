using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using System.Data.Common;
using MySqlConnector;
using Dotmim.Sync.MySql.Builders;
using System;
using Dotmim.Sync.MySql;

namespace Dotmim.Sync.MariaDB
{
    public class MariaDBSyncProvider : CoreProvider
    {
        DbMetadata dbMetadata;
        static string providerType;

        public override string ProviderTypeName
        {
            get
            {
                return ProviderType;
            }
        }

        public static string ProviderType
        {
            get
            {
                if (!string.IsNullOrEmpty(providerType))
                    return providerType;

                var type = typeof(MariaDBSyncProvider);
                providerType = $"{type.Name}, {type.ToString()}";

                return providerType;
            }

        }

        /// <summary>
        /// MySql does not support Bulk operations
        /// </summary>
        public override bool SupportBulkOperations => false;

        /// <summary>
        /// MySql can be a server side provider
        /// </summary>
        public override bool CanBeServerProvider => true;


        /// <summary>
        /// Gets or Sets the MySql Metadata object, provided to validate the MySql Columns issued from MySql
        /// </summary>
        /// <summary>
        /// Gets or sets the Metadata object which parse Sql server types
        /// </summary>
        public override DbMetadata Metadata
        {
            get
            {
                if (dbMetadata == null)
                    dbMetadata = new MySqlDbMetadata();

                return dbMetadata;
            }
            set
            {
                dbMetadata = value;

            }
        }

        public MariaDBSyncProvider() : base()
        {
        }
        public MariaDBSyncProvider(string connectionString) : base()
        {

            var builder = new MySqlConnectionStringBuilder(connectionString);

            // Set the default behavior to use Found rows and not Affected rows !
            builder.UseAffectedRows = false;

            this.ConnectionString = builder.ConnectionString;
        }


        public MariaDBSyncProvider(MySqlConnectionStringBuilder builder) : base()
        {
            if (String.IsNullOrEmpty(builder.ConnectionString))
                throw new Exception("You have to provide parameters to the MySql builder to be able to construct a valid connection string.");

            // Set the default behavior to use Found rows and not Affected rows !
            builder.UseAffectedRows = false;

            this.ConnectionString = builder.ConnectionString;
        }

        public override void EnsureSyncException(SyncException syncException)
        {
            if (!string.IsNullOrEmpty(this.ConnectionString))
            {
                var builder = new MySqlConnectionStringBuilder(this.ConnectionString);

                syncException.DataSource = builder.Server;
                syncException.InitialCatalog = builder.Database;
            }

            var mySqlException = syncException.InnerException as MySqlException;

            if (mySqlException == null)
                return;

            syncException.Number = mySqlException.Number;

            return;
        }

        public override DbConnection CreateConnection() => new MySqlConnection(this.ConnectionString);

        public override DbTableBuilder GetTableBuilder(SyncTable tableDescription, SyncSetup setup)
        {
            var (tableName, trackingName) = GetParsers(tableDescription, setup);

            var tableBuilder = new MyTableSqlBuilder(tableDescription, tableName, trackingName, setup)
            {
                UseBulkProcedures = this.SupportBulkOperations,
                UseChangeTracking = this.UseChangeTracking
            };

            return tableBuilder;
        }
        public override DbScopeBuilder GetScopeBuilder(string scopeInfoTableName) => new MySqlScopeInfoBuilder(scopeInfoTableName);


        public override DbBuilder GetDatabaseBuilder() => new MySqlBuilder();
        public override DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, SyncSetup setup)
        {
            var (tableName, trackingName) = GetParsers(tableDescription, setup);
            var adapter = new MySqlSyncAdapter(tableDescription, tableName, trackingName, setup);
            return adapter;
        }
        public override (ParserName tableName, ParserName trackingName) GetParsers(SyncTable tableDescription, SyncSetup setup)
        {
            string tableAndPrefixName = tableDescription.TableName;

            var originalTableName = ParserName.Parse(tableDescription, "`");

            var pref = setup.TrackingTablesPrefix != null ? setup.TrackingTablesPrefix : "";
            var suf = setup.TrackingTablesSuffix != null ? setup.TrackingTablesSuffix : "";

            // be sure, at least, we have a suffix if we have empty values. 
            // othewise, we have the same name for both table and tracking table
            if (string.IsNullOrEmpty(pref) && string.IsNullOrEmpty(suf))
                suf = "_tracking";

            var trackingTableName = ParserName.Parse($"{pref}{tableAndPrefixName}{suf}", "`");

            return (originalTableName, trackingTableName);
        }

    }
}
