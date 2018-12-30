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

        ///// <summary>
        ///// Gets or sets the boolean indicating if we can use bulk operations
        ///// </summary>
        //[JsonIgnore]
        //public bool UseBulkOperations { get; set; }

        ///// <summary>
        ///// Gets or Sets if we should cleaning tmp dir files after sync.
        ///// </summary>
        //[JsonIgnore]
        //public bool CleanMetadatas { get; set; }

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
