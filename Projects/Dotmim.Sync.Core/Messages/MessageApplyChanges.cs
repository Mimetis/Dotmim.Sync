using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using System;

namespace Dotmim.Sync
{
    /// <summary>
    /// Message exchanged during the Begin session sync stage.
    /// </summary>
    public class MessageApplyChanges
    {

        /// <summary>
        /// Initializes a new instance of the <see cref="MessageApplyChanges"/> class.
        /// Applying changes message.
        /// Be careful policy could be differente from the schema (especially on client side, it's the reverse one, by default).
        /// </summary>
        public MessageApplyChanges(Guid localScopeId, Guid senderScopeId, bool isNew, long? lastTimestamp, SyncSet schema,
                                    ConflictResolutionPolicy policy, bool snapshotApplied, string batchDirectory,
                                    BatchInfo changes, SyncSet failedRows, DatabaseChangesApplied changesApplied)
        {
            this.LocalScopeId = localScopeId;
            this.SenderScopeId = senderScopeId;
            this.IsNew = isNew;
            this.LastTimestamp = lastTimestamp;
            this.Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            this.Policy = policy;
            this.Changes = changes ?? throw new ArgumentNullException(nameof(changes));
            this.FailedRows = failedRows;
            this.ChangesApplied = changesApplied;
            this.SnapshoteApplied = snapshotApplied;
            this.BatchDirectory = batchDirectory;
        }

        /// <summary>
        /// Gets or sets the local Scope Id.
        /// </summary>
        public Guid LocalScopeId { get; set; }

        /// <summary>
        /// Gets or sets the sender Scope Id.
        /// </summary>
        public Guid SenderScopeId { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether gets if the sync is a first sync. In this case, the last sync timestamp is ignored.
        /// </summary>
        public bool IsNew { get; set; }

        /// <summary>
        /// Gets or sets the Last timestamp used to compare rows.
        /// </summary>
        public long? LastTimestamp { get; set; }

        /// <summary>
        /// Gets or Sets the schema used for this sync.
        /// </summary>
        public SyncSet Schema { get; set; }

        /// <summary>
        /// Gets or Sets the current Conflict resolution policy.
        /// </summary>
        public ConflictResolutionPolicy Policy { get; set; }

        /// <summary>
        /// Gets or Sets the batch info containing the changes to apply.
        /// </summary>
        public BatchInfo Changes { get; set; }

        /// <summary>
        /// Gets or Sets the failed rows set.
        /// </summary>
        public SyncSet FailedRows { get; set; }

        /// <summary>
        /// Gets or Sets the changes applied.
        /// </summary>
        public DatabaseChangesApplied ChangesApplied { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether gets or Sets if we have already applied a snapshot. So far, we don't reset the tables, even if we are in reinit mode.
        /// </summary>
        public bool SnapshoteApplied { get; set; }

        /// <summary>
        /// Gets or sets the batch directory.
        /// </summary>
        public string BatchDirectory { get; set; }
    }
}