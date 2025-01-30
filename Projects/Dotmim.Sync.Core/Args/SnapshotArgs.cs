using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Event args generated before applying a snapshot on the target database.
    /// </summary>
    public class SnapshotApplyingArgs : ProgressArgs
    {
        /// <inheritdoc cref="SnapshotApplyingArgs" />
        public SnapshotApplyingArgs(SyncContext context, DbConnection connection)
            : base(context, connection, null)
        {
        }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"Applying Snapshot.";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 10100;
    }

    /// <summary>
    /// Event args generated before applying a snapshot on the target database.
    /// </summary>
    public class SnapshotAppliedArgs : ProgressArgs
    {

        /// <inheritdoc cref="SnapshotAppliedArgs" />
        public SnapshotAppliedArgs(SyncContext context, DatabaseChangesApplied changesApplied)
            : base(context, null, null)
        {
            this.ChangesApplied = changesApplied;
        }

        /// <summary>
        /// Gets or sets the changes applied during the snapshot application.
        /// </summary>
        public DatabaseChangesApplied ChangesApplied { get; set; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => (this.ChangesApplied != null && this.ChangesApplied.TotalAppliedChanges > 0) || this.ChangesApplied.TotalAppliedChangesFailed > 0 || this.ChangesApplied.TotalResolvedConflicts > 0 ? SyncProgressLevel.Information : SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Source" />
        public override string Source => "Snapshot";

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message =>
            this.ChangesApplied == null
            ? $"DatabaseChangesApplied progress."
            : $"[Total] Applied:{this.ChangesApplied.TotalAppliedChanges}. Resolved Conflicts:{this.ChangesApplied.TotalResolvedConflicts}.";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 10150;
    }

    /// <summary>
    /// Event args generated before creating a snapshot.
    /// </summary>
    public class SnapshotCreatingArgs : ProgressArgs
    {
        /// <inheritdoc cref="SnapshotCreatingArgs" />
        public SnapshotCreatingArgs(SyncContext context, SyncSet schema, string snapshotDirectory, int batchSize, long timestamp, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.Schema = schema;
            this.SnapshotDirectory = snapshotDirectory;
            this.BatchSize = batchSize;
            this.Timestamp = timestamp;
        }

        /// <summary>
        /// Gets the schema used to create the snapshot.
        /// </summary>
        public SyncSet Schema { get; }

        /// <summary>
        /// Gets the directory used to store the snapshot.
        /// </summary>
        public string SnapshotDirectory { get; }

        /// <summary>
        /// Gets the batchsize of each file.
        /// </summary>
        public int BatchSize { get; }

        /// <summary>
        /// Gets the timestamp defining the timestamp limit to generate the snapshot.
        /// </summary>
        public long Timestamp { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"Creating Snapshot.";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 10000;
    }

    /// <summary>
    /// Event args generated before after a snapshot has been created.
    /// </summary>
    public class SnapshotCreatedArgs : ProgressArgs
    {
        /// <inheritdoc cref="SnapshotCreatedArgs" />
        public SnapshotCreatedArgs(SyncContext context, BatchInfo batchInfo, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
            => this.BatchInfo = batchInfo;

        /// <summary>
        /// Gets the batch info summarizing the snapshot created.
        /// </summary>
        public BatchInfo BatchInfo { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => this.BatchInfo != null && this.BatchInfo.RowsCount > 0 ? SyncProgressLevel.Information : SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => this.BatchInfo == null ? $"SnapshotCreatedArgs progress." : $"Snapshot Created [{this.BatchInfo.GetDirectoryFullPath()}].";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 10050;
    }

    /// <summary>
    /// Interceptors extensions.
    /// </summary>
    public partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the orchestrator when creating a snapshot.
        /// </summary>
        public static Guid OnSnapshotCreating(this BaseOrchestrator orchestrator, Action<SnapshotCreatingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the orchestrator when creating a snapshot.
        /// </summary>
        public static Guid OnSnapshotCreating(this BaseOrchestrator orchestrator, Func<SnapshotCreatingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the orchestrator when a snapshot has been created.
        /// </summary>
        public static Guid OnSnapshotCreated(this BaseOrchestrator orchestrator, Action<SnapshotCreatedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the orchestrator when a snapshot has been created.
        /// </summary>
        public static Guid OnSnapshotCreated(this BaseOrchestrator orchestrator, Func<SnapshotCreatedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the orchestrator when applying a snapshot.
        /// </summary>
        public static Guid OnSnapshotApplying(this BaseOrchestrator orchestrator, Action<SnapshotApplyingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the orchestrator when applying a snapshot.
        /// </summary>
        public static Guid OnSnapshotApplying(this BaseOrchestrator orchestrator, Func<SnapshotApplyingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the orchestrator when a snapshot has been applied.
        /// </summary>
        public static Guid OnSnapshotApplied(this BaseOrchestrator orchestrator, Action<SnapshotAppliedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the orchestrator when a snapshot has been applied.
        /// </summary>
        public static Guid OnSnapshotApplied(this BaseOrchestrator orchestrator, Func<SnapshotAppliedArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}