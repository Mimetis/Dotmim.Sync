using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Core provider : should be implemented by any server / client provider.
    /// </summary>
    public abstract partial class CoreProvider
    {

        internal Action<DbConnection> onConnectionOpened = new(c => { });
        internal Action<DbConnection> onConnectionClosed = new(c => { });

        /// <summary>
        /// Gets the reference to the orchestrator owner of this instance.
        /// </summary>
        [JsonIgnore]
        [IgnoreDataMember]
        public BaseOrchestrator Orchestrator { get; internal set; }

        /// <summary>
        /// Connection is opened. this method is called before any Interceptors.
        /// </summary>
        public virtual void OnConnectionOpened(Action<DbConnection> onConnectionOpened) => this.onConnectionOpened = onConnectionOpened;

        /// <summary>
        /// Connection is closed. this method is called after all Interceptors.
        /// </summary>
        public virtual void OnConnectionClosed(Action<DbConnection> onConnectionClosed) => this.onConnectionClosed = onConnectionClosed;

        /// <summary>
        /// Create a new instance of the implemented Connection provider.
        /// </summary>
        public abstract DbConnection CreateConnection();

        /// <summary>
        /// Get Database Builder which can create object at the database level.
        /// </summary>
        /// <returns></returns>
        public abstract DbBuilder GetDatabaseBuilder();

        /// <summary>
        /// Get a table builder helper which can create object at the table level.
        /// </summary>
        /// <returns></returns>
        public abstract DbTableBuilder GetTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName);

        /// <summary>
        /// Get sync adapter which can executes all the commands needed for a complete sync.
        /// </summary>
        public abstract DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName);

        /// <summary>
        /// Create a Scope Builder, which can create scope table, and scope config.
        /// </summary>
        public abstract DbScopeBuilder GetScopeBuilder(string scopeInfoTableName);

        /// <summary>
        /// Gets or sets the metadata resolver (validating the columns definition from the data store).
        /// </summary>
        public abstract DbMetadata GetMetadata();

        /// <summary>
        /// Get the provider type name.
        /// </summary>
        public abstract string GetProviderTypeName();

        /// <summary>
        /// Get the provider type name.
        /// </summary>
        public abstract string GetShortProviderTypeName();

        /// <summary>
        /// Gets the database name if any.
        /// </summary>
        /// <returns></returns>
        public abstract string GetDatabaseName();

        /// <summary>
        /// Gets or sets the connection string used by the implemented provider.
        /// </summary>
        public abstract string ConnectionString { get; set; }

        /// <summary>
        /// Gets a value indicating whether gets a boolean indicating if the provider can be a server side provider.
        /// </summary>
        public abstract bool CanBeServerProvider { get; }

        /// <summary>
        /// Gets a value indicating on which level constraints disabling and enabling should be applied.
        /// </summary>
        public abstract ConstraintsLevelAction ConstraintsLevelAction { get; }

        /// <summary>
        /// Gets or sets the default isolation level used during transaction.
        /// </summary>
        public virtual IsolationLevel IsolationLevel { get; set; } = IsolationLevel.ReadCommitted;

        /// <summary>
        /// Gets or Sets the number of line for every batch bulk operations.
        /// </summary>
        public virtual int BulkBatchMaxLinesCount { get; set; } = 10000;

        /// <summary>
        /// Gets or sets a value indicating whether gets or Sets if the provider supports multi results sets on the same connection.
        /// </summary>
        public virtual bool SupportsMultipleActiveResultSets { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether gets or Sets if provider should use bulk operations for Insert / Update (only Sql Server).
        /// </summary>
        public virtual bool UseBulkOperations { get; set; } = true;

        /// <summary>
        /// Gets the default schema name ("dbo" for sql server, "public" for postgres or null for mysql).
        /// </summary>
        public virtual string DefaultSchemaName { get; }

        /// <summary>
        /// Gets additional options for the provider.
        /// </summary>
        public Dictionary<string, string> AdditionalProperties { get; } = new();

        /// <summary>
        /// Get naming tables.
        /// </summary>
        public abstract (ParserName TableName, ParserName TrackingName) GetParsers(SyncTable tableDescription, SyncSetup setup);

        /// <summary>
        /// Let a chance to provider to enrich SyncExecption.
        /// </summary>
        public virtual void EnsureSyncException(SyncException syncException) { }

        /// <summary>
        /// Let's a chance to retry on error if connection has been refused.
        /// </summary>
        public virtual bool ShouldRetryOn(Exception exception) => false;
    }
}