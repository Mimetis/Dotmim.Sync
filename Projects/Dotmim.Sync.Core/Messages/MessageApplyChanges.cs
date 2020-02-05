﻿using Dotmim.Sync.Batch;

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

        /// <summary>
        /// Applying changes message.
        /// Be careful policy could be differente from the schema (especially on client side, it's the reverse one, by default)
        /// </summary>
        public MessageApplyChanges(Guid localScopeId, Guid senderScopeId,  bool isNew, long lastTimestamp, SyncSet schema, 
                                    ConflictResolutionPolicy policy, bool disableConstraintsOnApplyChanges, 
                                    bool useBulkOperations, bool cleanMetadatas, bool cleanFolder, BatchInfo changes)
        {
            this.LocalScopeId = localScopeId;
            this.SenderScopeId = senderScopeId;
            this.IsNew = isNew;
            this.LastTimestamp = lastTimestamp;
            this.Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            this.Policy = policy;
            this.DisableConstraintsOnApplyChanges = disableConstraintsOnApplyChanges;
            this.UseBulkOperations = useBulkOperations;
            this.CleanMetadatas = cleanMetadatas;
            this.CleanFolder = cleanFolder;
            this.Changes = changes ?? throw new ArgumentNullException(nameof(changes));
        }


        /// <summary>
        /// Gets the local Scope Id
        /// </summary>
        public Guid LocalScopeId { get; }

        /// <summary>
        /// Gets the sender Scope Id
        /// </summary>
        public Guid SenderScopeId { get; }


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
        public SyncSet Schema { get; set; }

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
        /// Gets or Sets if we should cleaning tracking table metadatas
        /// </summary>
        public bool CleanMetadatas { get; set; }

        /// <summary>
        /// Gets or Sets if we should cleaning tmp dir files after sync.
        /// </summary>
        public bool CleanFolder { get; set; }

        /// <summary>
        /// Gets or Sets the Batch Info used for this sync session
        /// </summary>
        public BatchInfo Changes { get; set; }

    }
}
