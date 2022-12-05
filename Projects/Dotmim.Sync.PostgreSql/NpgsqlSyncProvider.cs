using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using System.Data.Common;
using Npgsql;
using System;
using Dotmim.Sync.PostgreSql.Scope;
using Dotmim.Sync.PostgreSql.Builders;
using Npgsql.NameTranslation;

namespace Dotmim.Sync.PostgreSql
{
    public class NpgsqlSyncProvider : CoreProvider
    {
        static string providerType;
        static string shortProviderType;
        private NpgsqlConnectionStringBuilder builder;
        private NpgsqlDbMetadata dbMetadata;

        public NpgsqlSyncProvider() : base() { }
        public NpgsqlSyncProvider(string connectionString) : base()
        {
            this.ConnectionString = connectionString;

            if (!string.IsNullOrEmpty(this.ConnectionString))
            {
                this.builder = new NpgsqlConnectionStringBuilder(this.ConnectionString);
            }
        }
        public NpgsqlSyncProvider(NpgsqlConnectionStringBuilder builder) : base()
        {
            if (String.IsNullOrEmpty(builder.ConnectionString))
                throw new Exception("You have to provide parameters to the Npgsql builder to be able to construct a valid connection string.");

            this.builder = builder;
            this.ConnectionString = builder.ConnectionString;
        }
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

        public override bool CanBeServerProvider => true;

        public string ShortProviderType
        {
            get
            {
                if (!string.IsNullOrEmpty(shortProviderType))
                    return shortProviderType;

                var type = typeof(NpgsqlSyncProvider);
                shortProviderType = type.Name;

                return shortProviderType;
            }
        }

        public override DbConnection CreateConnection() {
            var connection = new NpgsqlConnection(this.ConnectionString);

            NpgsqlConnection.GlobalTypeMapper.MapComposite<Department>("address_BulkType", new NpgsqlNullNameTranslator());
            return connection;
            }

        public override DbBuilder GetDatabaseBuilder() => new NpgsqlBuilder();

        public override string GetDatabaseName()
        {
            if (builder != null && !String.IsNullOrEmpty(builder.Database))
                return builder.Database;

            return string.Empty;
        }

        public override DbMetadata GetMetadata()
        {
            if (dbMetadata == null)
                dbMetadata = new NpgsqlDbMetadata();

            return dbMetadata;
        }

        public override (ParserName tableName, ParserName trackingName) GetParsers(SyncTable tableDescription, SyncSetup setup = null)
        {
            var originalTableName = ParserName.Parse(tableDescription);

            var pref = setup?.TrackingTablesPrefix;
            var suf = setup?.TrackingTablesSuffix;

            // be sure, at least, we have a suffix if we have empty values. 
            // othewise, we have the same name for both table and tracking table
            if (string.IsNullOrEmpty(pref) && string.IsNullOrEmpty(suf))
                suf = "_tracking";

            var trakingTableNameString = $"{pref}{originalTableName.ObjectName}{suf}";

            if (!string.IsNullOrEmpty(originalTableName.SchemaName))
                trakingTableNameString = $"{originalTableName.SchemaName}.{trakingTableNameString}";

            var trackingTableName = ParserName.Parse(trakingTableNameString);

            return (originalTableName, trackingTableName);
        }

        public override string GetProviderTypeName() => ProviderType;
        public override DbScopeBuilder GetScopeBuilder(string scopeInfoTableName) => new NpgsqlScopeBuilder(scopeInfoTableName);

        public override string GetShortProviderTypeName() => ShortProviderType;
        public override DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
                => new NpgsqlSyncAdapter(tableDescription, tableName, trackingTableName, setup, scopeName, this.UseBulkOperations);

        public override DbTableBuilder GetTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
                    => new NpgsqlTableBuilder(tableDescription, tableName, trackingTableName, setup, scopeName);
    }

    internal class Department
    {
        public int Departmentid { get; set; }
        public string Name { get; set; }

        public string Mroupname { get; set; }
        public DateTime Modifieddate { get; set; }
    }
}
