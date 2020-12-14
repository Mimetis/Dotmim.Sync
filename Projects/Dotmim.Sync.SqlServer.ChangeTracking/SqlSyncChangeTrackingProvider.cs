using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using Dotmim.Sync.SqlServer.Builders;
using Dotmim.Sync.SqlServer.ChangeTracking.Builders;
using Dotmim.Sync.SqlServer.Manager;
using Dotmim.Sync.SqlServer.Scope;
using System;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer
{
    public class SqlSyncChangeTrackingProvider : SqlSyncProvider
    {
        static String providerType;

        public SqlSyncChangeTrackingProvider() : base()
        {
        }

        public SqlSyncChangeTrackingProvider(string connectionString) : base()
        {
            this.ConnectionString = connectionString;
        }

        public SqlSyncChangeTrackingProvider(SqlConnectionStringBuilder builder) : base()
        {
            if (String.IsNullOrEmpty(builder.ConnectionString))
                throw new Exception("You have to provide parameters to the Sql builder to be able to construct a valid connection string.");

            this.ConnectionString = builder.ConnectionString;
        }

        public static new string ProviderType
        {
            get
            {
                if (!string.IsNullOrEmpty(providerType))
                    return providerType;

                Type type = typeof(SqlSyncProvider);
                providerType = $"{type.Name}, {type.ToString()}";

                return providerType;
            }

        }

        /// <summary>
        /// this provider supports change tracking(
        /// </summary>
        public override bool UseChangeTracking => true;

        /// <summary>
        /// Sql server support bulk operations through Table Value parameter
        /// </summary>
        public override bool SupportBulkOperations => true;

        /// <summary>
        /// Sql Server supports to be a server side provider
        /// </summary>
        public override bool CanBeServerProvider => true;


        ///// <summary>
        ///// Metadatas are handled by Change Tracking
        ///// So just do nothing here
        ///// </summary>
        //public override Task<(SyncContext, DatabaseMetadatasCleaned)> DeleteMetadatasAsync(SyncContext context, SyncSet schema, SyncSetup setup, long timestampLimit, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null) 
        //    => Task.FromResult((context, new DatabaseMetadatasCleaned()));

        public override DbConnection CreateConnection() => new SqlConnection(this.ConnectionString);
        public override DbScopeBuilder GetScopeBuilder() => new SqlChangeTrackingScopeBuilder();
        public override DbTableBuilder GetTableBuilder(SyncTable tableDescription, SyncSetup setup)
        {
            var (tableName, trackingName) = GetParsers(tableDescription, setup);

            var tableBuilder = new SqlChangeTrackingBuilder(tableDescription, tableName, trackingName, setup)
            {
                UseBulkProcedures = this.SupportBulkOperations,
                UseChangeTracking = this.UseChangeTracking,
                Filter = tableDescription.GetFilter()
            };

            return tableBuilder;
        }



    }
}
