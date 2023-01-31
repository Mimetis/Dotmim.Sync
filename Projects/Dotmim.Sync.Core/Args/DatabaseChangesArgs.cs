﻿

using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.Serialization;
using System.Linq;
using Dotmim.Sync.Batch;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Event args generated before getting changes on the target database
    /// </summary>
    public class DatabaseChangesSelectingArgs : ProgressArgs
    {
        public DatabaseChangesSelectingArgs(SyncContext context, string batchDirectory, int batchSize, bool isNew, long? fromLastTimestamp, long? toLastTimestamp, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.BatchDirectory = batchDirectory;
            this.BatchSize = batchSize;
            this.IsNew = isNew;
            this.FromTimestamp = fromLastTimestamp;
            this.ToTimestamp = toLastTimestamp;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override string Message => $"Getting Changes. [{BatchDirectory}]. Batch size:{BatchSize}. IsNew:{IsNew}.";
        public override int EventId => SyncEventsId.DatabaseChangesSelecting.Id;

        public string BatchDirectory { get; }
        public int BatchSize { get; }
        public bool IsNew { get; }
        public long? FromTimestamp { get; }
        public long? ToTimestamp { get; }
    }

    /// <summary>
    /// Event args generated before after getting changes on the target database
    /// </summary>
    public class DatabaseChangesSelectedArgs : ProgressArgs
    {
        public DatabaseChangesSelectedArgs(SyncContext context, long? fromLastTimestamp, long? toLastTimestamp, BatchInfo clientBatchInfo, DatabaseChangesSelected changesSelected, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.FromTimestamp = fromLastTimestamp;
            this.ToTimestamp = toLastTimestamp;
            this.BatchInfo = clientBatchInfo;
            this.ChangesSelected = changesSelected;
        }

        public override SyncProgressLevel ProgressLevel => this.ChangesSelected != null && this.ChangesSelected.TotalChangesSelected> 0 ? SyncProgressLevel.Information : SyncProgressLevel.Debug;
        public long? FromTimestamp { get; }
        public long? ToTimestamp { get; }


        public override string Message => 
            this.ChangesSelected == null
            ? $"DatabaseChangesSelectedArgs progress."
            : $"[Total] Upserts:{this.ChangesSelected.TotalChangesSelectedUpdates}. Deletes:{this.ChangesSelected.TotalChangesSelectedDeletes}. Total:{this.ChangesSelected.TotalChangesSelected}. [{this.BatchInfo.DirectoryName}]";

        /// <summary>
        /// Get the batch info. Always null when raised from a call from GetEstimatedChangesCount
        /// </summary>
        public BatchInfo BatchInfo { get; }
        public DatabaseChangesSelected ChangesSelected { get; }
        public override int EventId => SyncEventsId.DatabaseChangesSelected.Id;
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

        public override string Message => $"Applying Changes. Total Changes To Apply: {ApplyChanges.Changes.RowsCount}";

        /// <summary>
        /// All parameters that will be used to apply changes
        /// </summary>
        public MessageApplyChanges ApplyChanges { get; }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override int EventId => SyncEventsId.DatabaseChangesApplying.Id;
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
        public override SyncProgressLevel ProgressLevel => ChangesApplied != null && ChangesApplied.TotalAppliedChanges > 0 ? SyncProgressLevel.Information: SyncProgressLevel.Debug;
        public override string Message =>
            ChangesApplied == null 
            ? "DatabaseChangesAppliedArgs progress."
            : $"[Total] Applied:{ChangesApplied.TotalAppliedChanges}. Conflicts:{ChangesApplied.TotalResolvedConflicts}.";

        public override int EventId => SyncEventsId.DatabaseChangesApplied.Id;
    }

    public static partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider action when changes are going to be applied on local database
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
        /// Intercept the provider action when changes are applied on each table defined in the configuration schema
        /// </summary>
        public static Guid OnDatabaseChangesApplied(this BaseOrchestrator orchestrator, Action<DatabaseChangesAppliedArgs> func)
            => orchestrator.AddInterceptor(func);
        /// <summary>
        /// Intercept the provider action when changes are applied on each table defined in the configuration schema
        /// </summary>
        public static Guid OnDatabaseChangesApplied(this BaseOrchestrator orchestrator, Func<DatabaseChangesAppliedArgs, Task> func)
            => orchestrator.AddInterceptor(func);

        /// <summary>
        /// Occurs when changes are going to be queried from the underline database
        /// <example>
        /// <code>
        /// var localOrchestrator = new LocalOrchestrator(clientProvider);
        /// localOrchestrator.OnDatabaseChangesSelecting(args => {
        ///   Console.WriteLine($"Getting changes from local database:");
        ///   Console.WriteLine($"Batch directory: {args.BatchDirectory}. Batch size: {args.BatchSize}. Is first sync: {args.IsNew}");
        ///   Console.WriteLine($"From: {args.FromTimestamp}. To: {args.ToTimestamp}.");
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
        /// Occurs when changes have been retrieved from the local database
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

    public static partial class SyncEventsId
    {
        public static EventId DatabaseChangesSelecting => CreateEventId(1000, nameof(DatabaseChangesSelecting));
        public static EventId DatabaseChangesSelected => CreateEventId(1050, nameof(DatabaseChangesSelected));
        public static EventId DatabaseChangesApplying => CreateEventId(1100, nameof(DatabaseChangesApplying));
        public static EventId DatabaseChangesApplied => CreateEventId(1150, nameof(DatabaseChangesApplied));
    }
}
