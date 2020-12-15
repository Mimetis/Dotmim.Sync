using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.Serialization;
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
        [JsonIgnore]
        [IgnoreDataMember]
        public BaseOrchestrator Orchestrator { get; internal set; }

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
        [JsonIgnore]
        [IgnoreDataMember]
        public virtual SyncOptions Options { get; set; }


        /// <summary>
        /// Create a new instance of the implemented Connection provider
        /// </summary>
        public abstract DbConnection CreateConnection();


        /// <summary>
        /// Get Database Builder which can create object at the database level
        /// </summary>
        /// <returns></returns>
        public abstract DbBuilder GetDatabaseBuilder();

        /// <summary>
        /// Get a table builder helper which can create object at the table level
        /// </summary>
        /// <returns></returns>
        public abstract DbTableBuilder GetTableBuilder(SyncTable tableDescription, SyncSetup setup);

        /// <summary>
        /// Get sync adapter which can executes all the commands needed for a complete sync
        /// </summary>
        public abstract DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, SyncSetup setup);

        /// <summary>
        /// Create a Scope Builder, which can create scope table, and scope config
        /// </summary>
        public abstract DbScopeBuilder GetScopeBuilder(string scopeInfoTableName);

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
        /// Get naming tables
        /// </summary>
        public abstract (ParserName tableName, ParserName trackingName) GetParsers(SyncTable tableDescription, SyncSetup setup);

        /// <summary>
        /// Let a chance to provider to enrich SyncExecption
        /// </summary>
        public virtual void EnsureSyncException(SyncException syncException) { }


        /// <summary>
        /// Let's a chance to retry on error if connection has been refused.
        /// </summary>
        public virtual bool ShouldRetryOn(Exception exception) => false;


    }
}
