using Dotmim.Sync.Batch;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using Newtonsoft.Json;
using System;

namespace Dotmim.Sync.Messages
{
    /// <summary>
    /// Message exchanged during the Begin session sync stage
    /// </summary>
    [Serializable]
    public class MessageApplyChanges
    {
        [NonSerialized]
        private DmSet _schema;

        public MessageApplyChanges(ScopeInfo fromScope, DmSet schema, ConflictResolutionPolicy policy, bool disableConstraintsOnApplyChanges, bool useBulkOperations, bool cleanMetadatas, string scopeInfoTableName, BatchInfo changes, SerializationFormat serializationFormat)
        {
            this.FromScope = fromScope ?? throw new ArgumentNullException(nameof(fromScope));
            this.Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            this.Policy = policy;
            this.DisableConstraintsOnApplyChanges = disableConstraintsOnApplyChanges;
            this.UseBulkOperations = useBulkOperations;
            this.CleanMetadatas = cleanMetadatas;
            this.ScopeInfoTableName = scopeInfoTableName ?? throw new ArgumentNullException(nameof(scopeInfoTableName));
            this.Changes = changes ?? throw new ArgumentNullException(nameof(changes));
            this.SerializationFormat = serializationFormat;
        }

        /// <summary>
        /// Gets or Sets the scope info for the current sync
        /// </summary>
        public ScopeInfo FromScope { get; set; }

        /// <summary>
        /// Gets or Sets the schema used for this sync
        /// </summary>
        [JsonIgnore]
        public DmSet Schema { get => this._schema; set => this._schema = value; }

        /// <summary>
        /// Gets or Sets the current Conflict resolution policy
        /// </summary>
        public ConflictResolutionPolicy Policy { get; set; }

        /// <summary>
        /// Gets or Sets if we should disable all constraints on apply changes.
        /// </summary>
        public bool DisableConstraintsOnApplyChanges { get; set; }

        /// <summary>
        /// Gets or Sets if during appy changes, we are using bulk operations
        /// </summary>
        public bool UseBulkOperations { get; set; }

        /// <summary>
        /// Gets or Sets if we should cleaning tmp dir files after sync.
        /// </summary>
        public bool CleanMetadatas { get; set; }

        /// <summary>
        /// Gets ors Sets the Scope info table name used for the sync
        /// </summary>
        public string ScopeInfoTableName { get; set; }

        /// <summary>
        /// Gets or Sets the Batch Info used for this sync session
        /// </summary>
        public BatchInfo Changes { get; set; }

        /// <summary>
        /// Gets or Sets the Serialization format used during the sync
        /// </summary>
        public SerializationFormat SerializationFormat { get; set; }

    }
}
