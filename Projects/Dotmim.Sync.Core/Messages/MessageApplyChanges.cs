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
    public class MessageApplyChanges
    {

        public MessageApplyChanges(Guid applyingScopeId, bool isNew, long lastTimestamp, SyncSchema schema, 
                                    ConflictResolutionPolicy policy, bool disableConstraintsOnApplyChanges, 
                                    bool useBulkOperations, bool cleanMetadatas, BatchInfo changes)
        {
            this.ApplyingScopeId = applyingScopeId;
            this.IsNew = isNew;
            this.LastTimestamp = lastTimestamp;
            this.Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            this.Policy = policy;
            this.DisableConstraintsOnApplyChanges = disableConstraintsOnApplyChanges;
            this.UseBulkOperations = useBulkOperations;
            this.CleanMetadatas = cleanMetadatas;
            this.Changes = changes ?? throw new ArgumentNullException(nameof(changes));
        }


        /// <summary>
        /// Gets or Sets the Scope Id that should be identified as the applier
        /// IE : When we apply lines on the client, we want to be sure that the Server scope id (ie Guid.Empty) is the one used as id.
        /// </summary>
        public Guid ApplyingScopeId { get; }
        
        /// <summary>
        /// Gets or Sets if the sync is a first sync. In this case, the last sync timestamp is ignored
        /// </summary>
        public bool IsNew { get; }

        /// <summary>
        /// Gets or Sets the last date timestamp from where we want rows
        /// </summary>
        public long LastTimestamp { get; }

        /// <summary>
        /// Gets or Sets the schema used for this sync
        /// </summary>
        public SyncSchema Schema { get; set; }

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
        /// Gets or Sets the Batch Info used for this sync session
        /// </summary>
        public BatchInfo Changes { get; set; }

    }
}
