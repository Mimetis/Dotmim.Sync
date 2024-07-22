using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Event args generated before getting changes on the target database.
    /// </summary>
    public class DatabaseChangesSelectingArgs : ProgressArgs
    {
        /// <inheritdoc cref="DatabaseChangesSelectingArgs"/>
        public DatabaseChangesSelectingArgs(SyncContext context, string batchDirectory, int batchSize, bool isNew, long? fromLastTimestamp, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.BatchDirectory = batchDirectory;
            this.BatchSize = batchSize;
            this.IsNew = isNew;
            this.FromTimestamp = fromLastTimestamp;
        }

        /// <summary>
        /// Gets the batch directory where the changes will be stored.
        /// </summary>
        public string BatchDirectory { get; }

        /// <summary>
        /// Gets the batch size.
        /// </summary>
        public int BatchSize { get; }

        /// <summary>
        /// Gets a value indicating whether the current sync is a new sync or a reinit sync.
        /// </summary>
        public bool IsNew { get; }

        /// <summary>
        /// Gets the last timestamp from the last sync.
        /// </summary>
        public long? FromTimestamp { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"Getting Changes. [{this.BatchDirectory}]. Batch size:{this.BatchSize}. IsNew:{this.IsNew}.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 1000;
    }

    /// <summary>
    /// Event args generated before after getting changes on the target database.
    /// </summary>
    public class DatabaseChangesSelectedArgs : ProgressArgs
    {
        /// <inheritdoc cref="DatabaseChangesSelectedArgs"/>
        public DatabaseChangesSelectedArgs(SyncContext context, long? fromLastTimestamp, BatchInfo clientBatchInfo, DatabaseChangesSelected changesSelected, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.FromTimestamp = fromLastTimestamp;
            this.BatchInfo = clientBatchInfo;
            this.ChangesSelected = changesSelected;
        }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>/>
        public override SyncProgressLevel ProgressLevel => this.ChangesSelected != null && this.ChangesSelected.TotalChangesSelected > 0 ? SyncProgressLevel.Information : SyncProgressLevel.Debug;

        /// <summary>
        /// Gets the last timestamp from the caller.
        /// </summary>
        public long? FromTimestamp { get; }

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message =>
            this.ChangesSelected == null
            ? $"DatabaseChangesSelectedArgs progress."
            : $"[Total] Upserts:{this.ChangesSelected.TotalChangesSelectedUpdates}. Deletes:{this.ChangesSelected.TotalChangesSelectedDeletes}. Total:{this.ChangesSelected.TotalChangesSelected}. [{this.BatchInfo.DirectoryName}]";

        /// <summary>
        /// Gets get the batch info. Always null when raised from a call from GetEstimatedChangesCount.
        /// </summary>
        public BatchInfo BatchInfo { get; }

        /// <summary>
        /// Gets the changes selected.
        /// </summary>
        public DatabaseChangesSelected ChangesSelected { get; }

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 1050;
    }

    /// <summary>
    /// Event args generated before applying change on the target database.
    /// </summary>
    public class DatabaseChangesApplyingArgs : ProgressArgs
    {
        /// <inheritdoc cref="DatabaseChangesApplyingArgs"/>
        public DatabaseChangesApplyingArgs(SyncContext context, MessageApplyChanges applyChanges, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.ApplyChanges = applyChanges;
        }

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"Applying Changes. Total Changes To Apply: {this.ApplyChanges.Changes.RowsCount}";

        /// <summary>
        /// Gets all parameters that will be used to apply changes.
        /// </summary>
        public MessageApplyChanges ApplyChanges { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 1100;
    }

    /// <summary>
    /// Event args generated after changes applied.
    /// </summary>
    public class DatabaseChangesAppliedArgs : ProgressArgs
    {
        /// <inheritdoc cref="DatabaseChangesAppliedArgs"/>
        public DatabaseChangesAppliedArgs(SyncContext context, DatabaseChangesApplied changesApplied, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ChangesApplied = changesApplied;
        }

        /// <summary>
        /// Gets the changes applied.
        /// </summary>
        public DatabaseChangesApplied ChangesApplied { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => this.ChangesApplied != null && this.ChangesApplied.TotalAppliedChanges > 0 ? SyncProgressLevel.Information : SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message =>
            this.ChangesApplied == null
            ? "DatabaseChangesAppliedArgs progress."
            : $"[Total] Applied:{this.ChangesApplied.TotalAppliedChanges}. Conflicts:{this.ChangesApplied.TotalResolvedConflicts}.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 1150;
    }

    /// <summary>
    /// Interceptors extensions.
    /// </summary>
    public partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider action when changes are going to be applied on local database.
        /// <example>
        /// <code>
        /// localOrchestrator.OnDatabaseChangesApplying(args =>
        /// {
        ///   Console.WriteLine($"Directory: {args.ApplyChanges.Changes.DirectoryName}. " +
        ///     $"Number of files: {args.ApplyChanges.Changes.BatchPartsInfo?.Count()} ");
        ///
        ///   Console.WriteLine($"Total: {args.ApplyChanges.Changes.RowsCount} ");
        /// });
        /// </code>
        /// </example>
        /// </summary>
        public static Guid OnDatabaseChangesApplying(this BaseOrchestrator orchestrator, Action<DatabaseChangesApplyingArgs> func)
            => orchestrator.AddInterceptor(func);

        /// <inheritdoc cref="OnDatabaseChangesApplying(BaseOrchestrator, Action{DatabaseChangesApplyingArgs})"/>
        public static Guid OnDatabaseChangesApplying(this BaseOrchestrator orchestrator, Func<DatabaseChangesApplyingArgs, Task> func)
            => orchestrator.AddInterceptor(func);

        /// <summary>
        /// Intercept the provider action when changes are applied on each table defined in the configuration schema.
        /// </summary>
        public static Guid OnDatabaseChangesApplied(this BaseOrchestrator orchestrator, Action<DatabaseChangesAppliedArgs> func)
            => orchestrator.AddInterceptor(func);

        /// <summary>
        /// Intercept the provider action when changes are applied on each table defined in the configuration schema.
        /// </summary>
        public static Guid OnDatabaseChangesApplied(this BaseOrchestrator orchestrator, Func<DatabaseChangesAppliedArgs, Task> func)
            => orchestrator.AddInterceptor(func);

        /// <summary>
        /// Occurs when changes are going to be queried from the underline database.
        /// <example>
        /// <code>
        /// var localOrchestrator = new LocalOrchestrator(clientProvider);
        /// localOrchestrator.OnDatabaseChangesSelecting(args => {
        ///   Console.WriteLine($"Getting changes from local database:");
        ///   Console.WriteLine($"Batch directory: {args.BatchDirectory}. Batch size: {args.BatchSize}. Is first sync: {args.IsNew}");
        ///   Console.WriteLine($"From: {args.FromTimestamp}.");
        /// }
        /// </code>
        /// </example>
        /// </summary>
        public static Guid OnDatabaseChangesSelecting(this BaseOrchestrator orchestrator, Action<DatabaseChangesSelectingArgs> func)
            => orchestrator.AddInterceptor(func);

        /// <inheritdoc cref="OnDatabaseChangesSelecting(BaseOrchestrator, Action{DatabaseChangesSelectingArgs})"/>
        public static Guid OnDatabaseChangesSelecting(this BaseOrchestrator orchestrator, Func<DatabaseChangesSelectingArgs, Task> func)
            => orchestrator.AddInterceptor(func);

        /// <summary>
        /// Occurs when changes have been retrieved from the local database.
        /// <example>
        /// <code>
        /// var localOrchestrator = new LocalOrchestrator(clientProvider);
        /// localOrchestrator.OnDatabaseChangesSelected(args =>
        /// {
        ///   Console.WriteLine($"Directory: {args.BatchInfo.DirectoryName}. Number of files: {args.BatchInfo.BatchPartsInfo?.Count()} ");
        ///   Console.WriteLine($"Total: {args.ChangesSelected.TotalChangesSelected} " +
        ///             $"({args.ChangesSelected.TotalChangesSelectedUpdates}/{args.ChangesSelected.TotalChangesSelectedDeletes})");
        ///   foreach (var table in args.ChangesSelected.TableChangesSelected)
        ///           Console.WriteLine($"Table: {table.TableName}. Total: {table.TotalChanges} ({table.Upserts / table.Deletes}");
        /// });
        /// </code>
        /// </example>
        /// </summary>
        public static Guid OnDatabaseChangesSelected(this BaseOrchestrator orchestrator, Action<DatabaseChangesSelectedArgs> func)
            => orchestrator.AddInterceptor(func);

        /// <inheritdoc cref="OnDatabaseChangesSelected(BaseOrchestrator, Action{DatabaseChangesSelectedArgs})"/>
        public static Guid OnDatabaseChangesSelected(this BaseOrchestrator orchestrator, Func<DatabaseChangesSelectedArgs, Task> func)
            => orchestrator.AddInterceptor(func);
    }
}