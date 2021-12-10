using Dotmim.Sync.Batch;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Contains statistics about selected changes from local provider
    /// </summary>
    public class TableChangesSelectedArgs : ProgressArgs
    {
        public TableChangesSelectedArgs(SyncContext context, List<BatchPartInfo> batchPartInfos, TableChangesSelected changesSelected, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.BatchPartInfos = batchPartInfos;
            this.TableChangesSelected = changesSelected;
        }

        /// <summary>
        /// Gets the SyncTable instances containing all changes selected.
        /// If you get this instance from a call from GetEstimatedChangesCount, this property is always null
        /// </summary>
        public List<BatchPartInfo> BatchPartInfos { get; }

        /// <summary>
        /// Gets the incremental summary of changes selected
        /// </summary>
        public TableChangesSelected TableChangesSelected { get; }

        public override string Source => Connection.Database;
        public override string Message => $"[{this.TableChangesSelected.TableName}] [Total] Upserts:{this.TableChangesSelected.Upserts}. Deletes:{this.TableChangesSelected.Deletes}. Total:{this.TableChangesSelected.TotalChanges}.";
        public override int EventId => SyncEventsId.TableChangesSelected.Id;
    }

    /// <summary>
    /// Raise before selecting changes will occur
    /// </summary>
    public class TableChangesSelectingArgs : ProgressArgs
    {

        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; set; }

        public TableChangesSelectingArgs(SyncContext context, SyncTable schemaTable, DbCommand command, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.Table = schemaTable;
            this.Command = command;
        }

        /// <summary>
        /// Gets the table from where the changes are going to be selected.
        /// </summary>
        public SyncTable Table { get; }

        public override string Source => Connection.Database;
        public override string Message => $"Getting Changes [{this.Table.GetFullName()}].";

        public override int EventId => SyncEventsId.TableChangesSelecting.Id;
    }

    /// <summary>
    /// Event args raised when a batch changes is applied on a datasource
    /// </summary>
    public class TableChangesBatchAppliedArgs : ProgressArgs
    {
        public TableChangesBatchAppliedArgs(SyncContext context, TableChangesApplied tableChangesApplied, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.TableChangesApplied = tableChangesApplied;
        }


        /// <summary>
        /// Table changes applied
        /// </summary>
        public TableChangesApplied TableChangesApplied { get; set; }

        public override string Source => Connection.Database;
        public override string Message => $"[{this.TableChangesApplied.TableName}] [{this.TableChangesApplied.State}] " +
                                          $"Applied:({this.TableChangesApplied.Applied}) Total:({this.TableChangesApplied.TotalAppliedCount}/{this.TableChangesApplied.TotalRowsCount}).";

        public override int EventId => SyncEventsId.TableChangesApplied.Id;
    }

    /// <summary>
    /// Event args before a batch changes is going to be applied on a datasource
    /// </summary>
    public class TableChangesBatchApplyingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; set; }

        public TableChangesBatchApplyingArgs(SyncContext context, BatchPartInfo part, DataRowState state, DbCommand command, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.State = state;
            this.Command = command;
            this.BatchPartInfo = part;
        }

        /// <summary>
        /// Gets the RowState of the applied rows
        /// </summary>
        public DataRowState State { get; }

        /// <summary>
        /// Gets the changes to be applied into the database
        /// </summary>
        public BatchPartInfo BatchPartInfo { get; }

        public override string Source => Connection.Database;
        public override string Message => $"Applying [{this.BatchPartInfo.FileName}] Batch file. State:{this.State}.";

        public override int EventId => SyncEventsId.TableChangesApplying.Id;
    }

    /// <summary>
    /// Event args raised when all changes for a table have been applied on a datasource
    /// </summary>
    public class TableChangesAppliedArgs : ProgressArgs
    {
        public TableChangesAppliedArgs(SyncContext context, TableChangesApplied tableChangesApplied, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.TableChangesApplied = tableChangesApplied;
        }

        public TableChangesApplied TableChangesApplied { get; set; }

        public override string Source => Connection.Database;
        public override string Message => $"[{this.TableChangesApplied.TableName}] Changes {this.TableChangesApplied.State} Applied:{this.TableChangesApplied.Applied}. Resolved Conflicts:{this.TableChangesApplied.ResolvedConflicts}.";
        public override int EventId => SyncEventsId.TableChangesApplied.Id;
    }


    /// <summary>
    /// Event args before a table changes is going to be applied on a datasource
    /// </summary>
    public class TableChangesApplyingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;

        public TableChangesApplyingArgs(SyncContext context, SyncTable schemaTable, DataRowState state, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.Table = schemaTable;
            this.State = state;
        }

        /// <summary>
        /// Gets the RowState of the applied rows
        /// </summary>
        public DataRowState State { get; }

        /// <summary>
        /// Gets the changes to be applied into the database
        /// </summary>
        public SyncTable Table { get; }

        public override string Source => Connection.Database;
        public override string Message => $"Applying Changes To {this.Table.GetFullName()}.";

        public override int EventId => SyncEventsId.TableChangesApplying.Id;
    }


    public static partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider action when changes are going to be selected on each table defined in the configuration schema
        /// </summary>
        public static void OnTableChangesSelecting(this BaseOrchestrator orchestrator, Action<TableChangesSelectingArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider action when changes are going to be selected on each table defined in the configuration schema
        /// </summary>
        public static void OnTableChangesSelecting(this BaseOrchestrator orchestrator, Func<TableChangesSelectingArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when changes are selected on each table defined in the configuration schema
        /// </summary>
        public static void OnTableChangesSelected(this BaseOrchestrator orchestrator, Action<TableChangesSelectedArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider action when changes are selected on each table defined in the configuration schema
        /// </summary>
        public static void OnTableChangesSelected(this BaseOrchestrator orchestrator, Func<TableChangesSelectedArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a table starts to apply changes
        /// </summary>
        public static void OnTableChangesApplying(this BaseOrchestrator orchestrator, Action<TableChangesApplyingArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a table starts to apply changes
        /// </summary>
        public static void OnTableChangesApplying(this BaseOrchestrator orchestrator, Func<TableChangesApplyingArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a batch changes is going to be applied on a table
        /// </summary>
        public static void OnTableChangesBatchApplying(this BaseOrchestrator orchestrator, Action<TableChangesBatchApplyingArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a batch changes is going to be applied on a table
        /// </summary>
        public static void OnTableChangesBatchApplying(this BaseOrchestrator orchestrator, Func<TableChangesBatchApplyingArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a batch changes is applied on a datasource table
        /// </summary>
        public static void OnTableChangesBatchApplied(this BaseOrchestrator orchestrator, Action<TableChangesBatchAppliedArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a batch changes is applied on a datasource table
        /// </summary>
        public static void OnTableChangesBatchApplied(this BaseOrchestrator orchestrator, Func<TableChangesBatchAppliedArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a all changes have been applied on a datasource table
        /// </summary>
        public static void OnTableChangesApplied(this BaseOrchestrator orchestrator, Action<TableChangesAppliedArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a all changes have been applied on a datasource table
        /// </summary>
        public static void OnTableChangesApplied(this BaseOrchestrator orchestrator, Func<TableChangesAppliedArgs, Task> action)
            => orchestrator.SetInterceptor(action);

    }

    public static partial class SyncEventsId
    {
        public static EventId TableChangesSelecting => CreateEventId(13000, nameof(TableChangesSelecting));
        public static EventId TableChangesSelected => CreateEventId(13050, nameof(TableChangesSelected));
        public static EventId TableChangesApplying => CreateEventId(13100, nameof(TableChangesApplying));
        public static EventId TableChangesApplied => CreateEventId(13150, nameof(TableChangesApplied));

    }
}
