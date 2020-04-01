using Dotmim.Sync.Batch;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync
{
    /// <summary>
    /// Event args generated before applying a snapshot on the target database
    /// </summary>
    public class SnapshotApplyingArgs : ProgressArgs
    {
        public SnapshotApplyingArgs(SyncContext context) : base(context, null, null)
        {
        }

        public override string Message => $"Applying snapshot.";
    }


    /// <summary>
    /// Event args generated before applying a snapshot on the target database
    /// </summary>
    public class SnapshotAppliedArgs : ProgressArgs
    {
        public SnapshotAppliedArgs(SyncContext context) : base(context, null, null)
        {
        }

        public override string Message => $"Snapshot applied.";
    }


    /// <summary>
    /// Event args generated before creating a snapshot
    /// </summary>
    public class SnapshotCreatingArgs : ProgressArgs
    {
        public SnapshotCreatingArgs(SyncContext context, SyncSet schema, string snapshotDirectory, int batchSize, long timestamp, DbConnection connection, DbTransaction transaction) : base(context, connection, transaction)
        {
            this.Schema = schema;
            this.SnapshotDirectory = snapshotDirectory;
            this.BatchSize = batchSize;
            this.Timestamp = timestamp;
        }

        /// <summary>
        /// Gets the schema used to create the snapshot
        /// </summary>
        public SyncSet Schema { get; }

        /// <summary>
        /// Gets the directory used to store the snapshot
        /// </summary>
        public string SnapshotDirectory { get; }

        /// <summary>
        /// Gets the batchsize of each file
        /// </summary>
        public int BatchSize { get; }

        /// <summary>
        /// Gets the timestamp defining the timestamp limit to generate the snapshot
        /// </summary>
        public long Timestamp { get; }

        public override string Message => $"Creating snapshot.";
    }


    /// <summary>
    /// Event args generated before after a snapshot has been created
    /// </summary>
    public class SnapshotCreatedArgs : ProgressArgs
    {
        public SnapshotCreatedArgs(SyncContext context, SyncSet schema, BatchInfo batchInfo, DbConnection connection = null, DbTransaction transaction = null) : base(context, connection, transaction)
        {
            this.Schema = schema;
            this.BatchInfo = batchInfo;
        }

        public override string Message => $"Created snapshot.";

        /// <summary>
        /// Gets the schema used to create the snapshot
        /// </summary>
        public SyncSet Schema { get; }
        
        /// <summary>
        /// Gets the batch info summarizing the snapshot created
        /// </summary>
        public BatchInfo BatchInfo { get; }
    }
}
