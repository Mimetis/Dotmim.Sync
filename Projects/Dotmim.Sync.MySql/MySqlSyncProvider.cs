using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using System.Data.Common;
#if NET5_0 || NET6_0 || NET7_0 || NETCOREAPP3_1
using MySqlConnector;
#elif NETSTANDARD 
using MySql.Data.MySqlClient;
#endif
using Dotmim.Sync.MySql.Builders;
using System;
using System.Reflection;
using Dotmim.Sync.Enumerations;

namespace Dotmim.Sync.MySql
{

    public class MySqlSyncProvider : CoreProvider
    {
        DbMetadata dbMetadata;
        static string providerType;
        private MySqlConnectionStringBuilder builder;
        public override string GetProviderTypeName() => ProviderType;

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

        static string shortProviderType;
        public override string GetShortProviderTypeName() => ShortProviderType;
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
        public override ConstraintsLevelAction ConstraintsLevelAction => ConstraintsLevelAction.OnTableLevel;
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

        public MySqlSyncProvider() : base()
        {
        }
        public MySqlSyncProvider(string connectionString) : base()
        {

            this.builder = new MySqlConnectionStringBuilder(connectionString);

            // Set the default behavior to use Found rows and not Affected rows !
            builder.UseAffectedRows = false;

            this.ConnectionString = builder.ConnectionString;
        }

        /// <summary>
        /// Gets a chance to make a retry if the error is a transient error
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

        public MySqlSyncProvider(MySqlConnectionStringBuilder builder) : base()
        {
            if (String.IsNullOrEmpty(builder.ConnectionString))
                throw new Exception("You have to provide parameters to the MySql builder to be able to construct a valid connection string.");

            this.builder = builder;

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

        public override void EnsureSyncException(SyncException syncException)
        {
            if (!string.IsNullOrEmpty(this.ConnectionString))
            {
                var builder = new MySqlConnectionStringBuilder(this.ConnectionString);

                syncException.DataSource = builder.Server;
                syncException.InitialCatalog = builder.Database;
            }

            if (syncException.InnerException is not MySqlException mySqlException)
                return;

            syncException.Number = mySqlException.Number;

            return;
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
