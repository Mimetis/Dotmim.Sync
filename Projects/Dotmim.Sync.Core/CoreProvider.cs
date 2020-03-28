using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Core provider : should be implemented by any server / client provider
    /// </summary>
    public abstract partial class CoreProvider
    {
        /// <summary>
        /// Gets the reference to the orchestrator owner of this instance
        /// </summary>
        public IOrchestrator Orchestrator { get; internal set; }

        /// <summary>
        /// Connection is opened. this method is called before any interceptors
        /// </summary>
        public virtual void OnConnectionOpened(DbConnection connection) { }

        /// <summary>
        /// Connection is closed. this method is called after all interceptors
        /// </summary>
        public virtual void OnConnectionClosed(DbConnection connection) { }


        /// <summary>
        /// Gets or Sets options used during the sync
        /// </summary>
        public virtual SyncOptions Options { get; set; }

   
        /// <summary>
        /// Create a new instance of the implemented Connection provider
        /// </summary>
        public abstract DbConnection CreateConnection();


        /// <summary>
        /// Get a database builder helper;
        /// </summary>
        /// <returns></returns>
        public abstract DbBuilder GetDatabaseBuilder();

        /// <summary>
        /// Get a table builder helper. Need a complete table description (SchemaTable). Will then generate table, table tracking, stored proc and triggers
        /// </summary>
        /// <returns></returns>
        public abstract DbTableBuilder GetTableBuilder(SyncTable tableDescription);

        /// <summary>
        /// Get a table manager, which can get informations directly from data source
        /// </summary>
        public abstract DbTableManagerFactory GetTableManagerFactory(string tableName, string schemaName);

        /// <summary>
        /// Create a Scope Builder, which can create scope table, and scope config
        /// </summary>
        public abstract DbScopeBuilder GetScopeBuilder();

        /// <summary>
        /// Gets or sets the metadata resolver (validating the columns definition from the data store)
        /// </summary>
        public abstract DbMetadata Metadata { get; set; }

        /// <summary>
        /// Get the provider type name
        /// </summary>
        public abstract string ProviderTypeName { get; }

        /// <summary>
        /// Gets or sets the connection string used by the implemented provider
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Gets a boolean indicating if the provider can use bulk operations
        /// </summary>
        public abstract bool SupportBulkOperations { get; }

        /// <summary>
        /// Gets a boolean indicating if the provider can use change tracking
        /// </summary>
        public virtual bool UseChangeTracking { get; } = false;

        /// <summary>
        /// Gets a boolean indicating if the provider can be a server side provider
        /// </summary>
        public abstract bool CanBeServerProvider { get; }

 
        /// <summary>
        /// Let a chance to provider to enrich SyncExecption
        /// </summary>
        public virtual void EnsureSyncException(SyncException syncException) { }


        /// <summary>
        /// Read a scope info
        /// </summary>
        public virtual async Task<long> GetLocalTimestampAsync(SyncContext context,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            var scopeBuilder = this.GetScopeBuilder();
            
            // Create a scopeInfo builder based on default scope inf table, since we don't use it to retrieve local time stamp, even if scope info table
            // in client database is not the DefaultScopeInfoTableName
            var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(SyncOptions.DefaultScopeInfoTableName, connection, transaction);
            
            var localTime = await scopeInfoBuilder.GetLocalTimestampAsync().ConfigureAwait(false);
            
            return localTime;
        }

        /// <summary>
        /// TODO : Manage an outdated scope. Complicated on the server side since we don't store any informations
        /// </summary>
        internal virtual bool IsRemoteOutdated() =>
            //var lastCleanupTimeStamp = 0; // A établir comment récupérer la dernière date de clean up des metadatas
            //return (ScopeInfo.LastTimestamp < lastCleanupTimeStamp);
            false;
    }
}
