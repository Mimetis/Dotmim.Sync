using Dotmim.Sync.Batch;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

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

        public override string Source => Connection.Database;
        public override string Message => $"Applying Snapshot.";
        public override int EventId => SyncEventsId.SnapshotApplying.Id;
    }


    /// <summary>
    /// Event args generated before applying a snapshot on the target database
    /// </summary>
    public class SnapshotAppliedArgs : ProgressArgs
    {
        public DatabaseChangesApplied ChangesApplied { get; set; }

        public SnapshotAppliedArgs(SyncContext context, DatabaseChangesApplied changesApplied) : base(context, null, null)
        {
            this.ChangesApplied = changesApplied;
        }

        public override string Source => "Snapshot";
        public override string Message => $"[Total] Applied:{ChangesApplied.TotalAppliedChanges}. Resolved Conflicts:{ChangesApplied.TotalResolvedConflicts}.";
        public override int EventId => SyncEventsId.SnapshotApplied.Id;
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

        public override string Source => Connection.Database;
        public override string Message => $"Creating Snapshot.";
        public override int EventId => SyncEventsId.SnapshotCreating.Id;
    }


    /// <summary>
    /// Event args generated before after a snapshot has been created
    /// </summary>
    public class SnapshotCreatedArgs : ProgressArgs
    {
        public SnapshotCreatedArgs(SyncContext context, BatchInfo batchInfo, DbConnection connection = null, DbTransaction transaction = null) : base(context, connection, transaction) 
            => this.BatchInfo = batchInfo;

        public override string Source => Connection.Database;
        public override string Message => $"Snapshot Created [{BatchInfo.GetDirectoryFullPath()}].";

        /// <summary>
        /// Gets the batch info summarizing the snapshot created
        /// </summary>
        public BatchInfo BatchInfo { get; }
        public override int EventId => SyncEventsId.SnapshotCreated.Id;
    }

    public static partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the orchestrator when creating a snapshot
        /// </summary>
        public static void OnSnapshotCreating(this BaseOrchestrator orchestrator, Action<SnapshotCreatingArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the orchestrator when creating a snapshot
        /// </summary>
        public static void OnSnapshotCreating(this BaseOrchestrator orchestrator, Func<SnapshotCreatingArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the orchestrator when a snapshot has been created
        /// </summary>
        public static void OnSnapshotCreated(this BaseOrchestrator orchestrator, Action<SnapshotCreatedArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the orchestrator when a snapshot has been created
        /// </summary>
        public static void OnSnapshotCreated(this BaseOrchestrator orchestrator, Func<SnapshotCreatedArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the orchestrator when applying a snapshot
        /// </summary>
        public static void OnSnapshotApplying(this BaseOrchestrator orchestrator, Action<SnapshotApplyingArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the orchestrator when applying a snapshot
        /// </summary>
        public static void OnSnapshotApplying(this BaseOrchestrator orchestrator, Func<SnapshotApplyingArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the orchestrator when a snapshot has been applied
        /// </summary>
        public static void OnSnapshotApplied(this BaseOrchestrator orchestrator, Action<SnapshotAppliedArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the orchestrator when a snapshot has been applied
        /// </summary>
        public static void OnSnapshotApplied(this BaseOrchestrator orchestrator, Func<SnapshotAppliedArgs, Task> action)
            => orchestrator.SetInterceptor(action);

    }

    public static partial class SyncEventsId
    {
        public static EventId SnapshotCreating => CreateEventId(10000, nameof(SnapshotCreating));
        public static EventId SnapshotCreated => CreateEventId(10050, nameof(SnapshotCreated));
        public static EventId SnapshotApplying => CreateEventId(10100, nameof(SnapshotApplying));
        public static EventId SnapshotApplied => CreateEventId(10150, nameof(SnapshotApplied));

    }
}
