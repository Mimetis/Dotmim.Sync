

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
        public DatabaseChangesSelectingArgs(SyncContext context, MessageGetChangesBatch changesRequest, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.ChangesRequest = changesRequest;
        }

        public override string Source => Connection.Database;
        public override string Message => $"Getting Changes.";

        public MessageGetChangesBatch ChangesRequest { get; }
        public override int EventId => SyncEventsId.DatabaseChangesSelecting.Id;
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
            this.BatchInfo = clientBatchInfo;
            this.ChangesSelected = changesSelected;
        }

        public override string Source => Connection.Database;
        public override string Message => $"[Total] Upserts:{this.ChangesSelected.TotalChangesSelectedUpdates}. Deletes:{this.ChangesSelected.TotalChangesSelectedDeletes}. Total:{this.ChangesSelected.TotalChangesSelected}";

        public long Timestamp { get; }

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

        public override string Source => Connection.Database;
        public override string Message => $"Applying Changes. Total Changes To Apply: {ApplyChanges.Changes.RowsCount}";

        /// <summary>
        /// All parameters that will be used to apply changes
        /// </summary>
        public MessageApplyChanges ApplyChanges { get; }


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

        public override string Source => Connection.Database;
        public override string Message => $"[Total] Applied:{ChangesApplied.TotalAppliedChanges}. Conflicts:{ChangesApplied.TotalResolvedConflicts}.";

        public override int EventId => SyncEventsId.DatabaseChangesApplied.Id;
    }

    public static partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider action when changes are going to be applied on each table defined in the configuration schema
        /// </summary>
        public static void OnDatabaseChangesApplying(this BaseOrchestrator orchestrator, Action<DatabaseChangesApplyingArgs> func)
            => orchestrator.SetInterceptor(func);
        /// <summary>
        /// Intercept the provider action when changes are going to be applied on each table defined in the configuration schema
        /// </summary>
        public static void OnDatabaseChangesApplying(this BaseOrchestrator orchestrator, Func<DatabaseChangesApplyingArgs, Task> func)
            => orchestrator.SetInterceptor(func);


        /// <summary>
        /// Intercept the provider action when changes are applied on each table defined in the configuration schema
        /// </summary>
        public static void OnDatabaseChangesApplied(this BaseOrchestrator orchestrator, Action<DatabaseChangesAppliedArgs> func)
            => orchestrator.SetInterceptor(func);
        /// <summary>
        /// Intercept the provider action when changes are applied on each table defined in the configuration schema
        /// </summary>
        public static void OnDatabaseChangesApplied(this BaseOrchestrator orchestrator, Func<DatabaseChangesAppliedArgs, Task> func)
            => orchestrator.SetInterceptor(func);


        /// <summary>
        /// Occurs when changes are going to be queried on the local database
        /// </summary>
        public static void OnDatabaseChangesSelecting(this BaseOrchestrator orchestrator, Action<DatabaseChangesSelectingArgs> func)
            => orchestrator.SetInterceptor(func);
        /// <summary>
        /// Occurs when changes are going to be queried on the local database
        /// </summary>
        public static void OnDatabaseChangesSelecting(this BaseOrchestrator orchestrator, Func<DatabaseChangesSelectingArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Occurs when changes have been retrieved from the local database
        /// </summary>
        public static void OnDatabaseChangesSelected(this BaseOrchestrator orchestrator, Action<DatabaseChangesSelectedArgs> func)
            => orchestrator.SetInterceptor(func);
        /// <summary>
        /// Occurs when changes have been retrieved from the local database
        /// </summary>
        public static void OnDatabaseChangesSelected(this BaseOrchestrator orchestrator, Func<DatabaseChangesSelectedArgs, Task> func)
            => orchestrator.SetInterceptor(func);

    }

    public static partial class SyncEventsId
    {
        public static EventId DatabaseChangesSelecting => CreateEventId(1000, nameof(DatabaseChangesSelecting));
        public static EventId DatabaseChangesSelected => CreateEventId(1050, nameof(DatabaseChangesSelected));
        public static EventId DatabaseChangesApplying => CreateEventId(1100, nameof(DatabaseChangesApplying));
        public static EventId DatabaseChangesApplied => CreateEventId(1150, nameof(DatabaseChangesApplied));
    }
}
