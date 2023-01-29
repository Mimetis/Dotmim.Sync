using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using System.Data.Common;
using Dotmim.Sync.Enumerations;
#if NET5_0 || NET6_0 || NET7_0 || NETCOREAPP3_1
using MySqlConnector;
#elif NETSTANDARD
using MySql.Data.MySqlClient;
using System.Reflection;
#endif

#if MARIADB
using Dotmim.Sync.MariaDB.Builders;
#elif MYSQL
using Dotmim.Sync.MySql.Builders;
#endif

using System;

namespace Dotmim.Sync.MariaDB
{
    public class MariaDBSyncProvider : CoreProvider
    {
        DbMetadata dbMetadata;
        private MySqlConnectionStringBuilder builder;

        static string providerType;
        public override string GetProviderTypeName() => ProviderType;

        public static string ProviderType
        {
            get
            {
                if (!string.IsNullOrEmpty(providerType))
                    return providerType;

                var type = typeof(MariaDBSyncProvider);
                providerType = $"{type.Name}, {type}";

                return providerType;
            }
        }
        public override ConstraintsLevelAction ConstraintsLevelAction => ConstraintsLevelAction.OnTableLevel;

        static string shortProviderType;
        public override string GetShortProviderTypeName() => ShortProviderType;
        public static string ShortProviderType
        {
            get
            {
                if (!string.IsNullOrEmpty(shortProviderType))
                    return shortProviderType;

                var type = typeof(MariaDBSyncProvider);
                shortProviderType = type.Name;

                return shortProviderType;
            }
        }

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
        public override DbMetadata GetMetadata()
        {
            if (dbMetadata == null)
                dbMetadata = new MySqlDbMetadata();

            return dbMetadata;
        }

        public MariaDBSyncProvider() : base()
        {
        }
        public MariaDBSyncProvider(string connectionString) : base()
        {

            this.builder = new MySqlConnectionStringBuilder(connectionString);

            // Set the default behavior to use Found rows and not Affected rows !
            builder.UseAffectedRows = false;

            this.ConnectionString = builder.ConnectionString;
        }
        public override string GetDatabaseName()
        {
            if (builder != null && !String.IsNullOrEmpty(builder.Database))
                return builder.Database;

            return string.Empty;

        }


        public MariaDBSyncProvider(MySqlConnectionStringBuilder builder) : base()
        {
            if (String.IsNullOrEmpty(builder.ConnectionString))
                throw new Exception("You have to provide parameters to the MySql builder to be able to construct a valid connection string.");

            this.builder = builder;

            // Set the default behavior to use Found rows and not Affected rows !
            this.builder.UseAffectedRows = false;

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

        public override bool ShouldRetryOn(Exception exception)
        {
            Exception ex = exception;
            while (ex != null)
            {
                if (ex is MySqlException)
                    return MySqlTransientExceptionDetector.ShouldRetryOn((MySqlException)ex);
                else
                    ex = ex.InnerException;
            }
            return false;
        }

        public override DbConnection CreateConnection() => new MySqlConnection(this.ConnectionString);
        public override DbTableBuilder GetTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
            => new MySqlTableBuilder(tableDescription, tableName, trackingTableName, setup, scopeName);

        public override DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
            => new MySqlSyncAdapter(tableDescription, tableName, trackingTableName, setup, scopeName);

        public override DbScopeBuilder GetScopeBuilder(string scopeInfoTableName) => new MySqlScopeInfoBuilder(scopeInfoTableName);
        public override DbBuilder GetDatabaseBuilder() => new MySqlBuilder();
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
