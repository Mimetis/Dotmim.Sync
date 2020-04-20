

using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.Serialization;
using System.Linq;
using Dotmim.Sync.Batch;

namespace Dotmim.Sync
{

    /// <summary>
    /// Event args generated before getting changes on the target database
    /// </summary>
    public class DatabaseChangesSelectingArgs : ProgressArgs
    {
        public DatabaseChangesSelectingArgs(SyncContext context, MessageGetChangesBatch changesRequest, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.ChangesRequest = changesRequest;
        }

        public override string Message => $"[{Connection.Database}] getting changes ...";

        public MessageGetChangesBatch ChangesRequest { get; }
        public override int EventId => 13;
    }

    /// <summary>
    /// Event args generated before after getting changes on the target database
    /// </summary>
    public class DatabaseChangesSelectedArgs : ProgressArgs
    {
        public DatabaseChangesSelectedArgs(SyncContext context, long timestamp, BatchInfo clientBatchInfo, DatabaseChangesSelected changesSelected, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Timestamp = timestamp;
            this.ClientBatchInfo = clientBatchInfo;
            this.ChangesSelected = changesSelected;
        }

        public override string Message => $"[{Connection.Database}] upserts:{this.ChangesSelected.TotalChangesSelectedUpdates} deletes:{this.ChangesSelected.TotalChangesSelectedDeletes} total:{this.ChangesSelected.TotalChangesSelected}";

        public long Timestamp { get; }
        public BatchInfo ClientBatchInfo { get; }
        public DatabaseChangesSelected ChangesSelected { get; }
        public override int EventId => 14;
    }

    /// <summary>
    /// Event args generated before applying change on the target database
    /// </summary>
    public class DatabaseChangesApplyingArgs : ProgressArgs
    {
        public DatabaseChangesApplyingArgs(SyncContext context, MessageApplyChanges applyChanges, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.ApplyChanges = applyChanges;
        }

        public override string Message => $"[{Connection.Database}] applying changes...";

        /// <summary>
        /// All parameters that will be used to apply changes
        /// </summary>
        public MessageApplyChanges ApplyChanges { get; }
        public override int EventId => 15;
    }

    /// <summary>
    /// Event args generated after changes applied
    /// </summary>
    public class DatabaseChangesAppliedArgs : ProgressArgs
    {
        public DatabaseChangesAppliedArgs(SyncContext context, DatabaseChangesApplied changesApplied, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ChangesApplied = changesApplied;
        }

        public DatabaseChangesApplied ChangesApplied { get; set; }

        public override string Message => $"[{Connection.Database}] applied:{ChangesApplied.TotalAppliedChanges} resolved conflicts:{ChangesApplied.TotalResolvedConflicts}";

        public override int EventId => 16;
    }


}
