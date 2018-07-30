using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.OracleClient;
using System.Text;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Cache;
using Dotmim.Sync.Data;
using Dotmim.Sync.Manager;
using Dotmim.Sync.Oracle.Builder;
using Dotmim.Sync.Oracle.Manager;
using Dotmim.Sync.Oracle.Scope;

namespace Dotmim.Sync.Oracle
{
    public class OracleSyncProvider : CoreProvider
    {
        ICache cacheManager;
        DbMetadata dbMetadata;
        static String providerType;

        #region Property
        public override DbMetadata Metadata
        {
            get
            {
                if (dbMetadata == null)
                    dbMetadata = new OracleDbMetadata();

                return dbMetadata;
            }
            set
            {
                dbMetadata = value;

            }
        }

        public override ICache CacheManager 
        {
            get
            {
                if (cacheManager == null)
                    cacheManager = new InMemoryCache();

                return cacheManager;
            }
            set
            {
                cacheManager = value;
            }
        }

        public override string ProviderTypeName => ProviderType;

        public override bool SupportBulkOperations => false;

        public override bool CanBeServerProvider => true;

        public static string ProviderType
        {
            get
            {
                if (!string.IsNullOrEmpty(providerType))
                    return providerType;

                Type type = typeof(OracleSyncProvider);
                providerType = $"{type.Name}, {type.ToString()}";

                return providerType;
            }

        }

        #endregion

        #region Ctor

        public OracleSyncProvider() : base() { }

        public OracleSyncProvider(string connectionString) : base()
        {
            this.ConnectionString = connectionString;
        }

        public OracleSyncProvider(OracleConnectionStringBuilder builder) : base()
        {
            if (String.IsNullOrEmpty(builder.ConnectionString))
                throw new Exception("You have to provide parameters to the Oracle builder to be able to construct a valid connection string.");

            this.ConnectionString = builder.ConnectionString;
        }

        #endregion

        public override DbConnection CreateConnection() => new OracleConnection(this.ConnectionString);

        public override DbBuilder GetDatabaseBuilder(DmTable tableDescription) => new OracleBuilder(tableDescription, SupportBulkOperations);

        public override DbManager GetDbManager(string tableName) => new OracleManager(tableName);

        public override DbScopeBuilder GetScopeBuilder() => new OracleScopeBuilder();
    }
}
