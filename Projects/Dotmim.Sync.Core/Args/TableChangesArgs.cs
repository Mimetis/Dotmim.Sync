using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync
{

    /// <summary>
    /// Contains statistics about selected changes from local provider
    /// </summary>
    public class TableChangesSelectedArgs : ProgressArgs
    {
        public TableChangesSelectedArgs(SyncContext context, SyncTable changes, TableChangesSelected changesSelected, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.Changes = changes;
            this.TableChangesSelected = changesSelected;
        }

        /// <summary>
        /// Gets the SyncTable instances containing all changes selected.
        /// If you get this instance from a call from GetEstimatedChangesCount, this property is always null
        /// </summary>
        public SyncTable Changes { get; }

        /// <summary>
        /// Gets the incremental summary of changes selected
        /// </summary>
        public TableChangesSelected TableChangesSelected { get; }

        public override string Message => $"[{Connection.Database}] [{this.TableChangesSelected.TableName}] upserts:{this.TableChangesSelected.Upserts} deletes:{this.TableChangesSelected.Deletes} total:{this.TableChangesSelected.TotalChanges}";
        public override int EventId => SyncEventsId.TableChangesSelected.Id;
    }

    /// <summary>
    /// Raise before selecting changes will occur
    /// </summary>
    public class TableChangesSelectingArgs : ProgressArgs
    {

        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; }

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

        public override string Message => $"[{Connection.Database}] Getting changes [{this.Table.GetFullName()}]";

        public override int EventId => SyncEventsId.TableChangesSelecting.Id;
    }

    /// <summary>
    /// Event args raised to get Changes applied on a provider
    /// </summary>
    public class TableChangesAppliedArgs : ProgressArgs
    {
        public TableChangesAppliedArgs(SyncContext context, TableChangesApplied tableChangesApplied, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.TableChangesApplied = tableChangesApplied;
        }

        public TableChangesApplied TableChangesApplied { get; set; }

        public override string Message => $"[{Connection.Database}] [{this.TableChangesApplied.TableName}] {this.TableChangesApplied.State} applied:{this.TableChangesApplied.Applied} resolved conflicts:{this.TableChangesApplied.ResolvedConflicts}";
        public override int EventId => SyncEventsId.TableChangesApplied.Id;
    }

    /// <summary>
    /// Event args before Changes are applied on a provider
    /// </summary>
    public class TableChangesApplyingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; }

        public TableChangesApplyingArgs(SyncContext context, SyncTable changes, DataRowState state, DbCommand command, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.State = state;
            this.Command = command;
            this.Changes = changes;
        }

        /// <summary>
        /// Gets the RowState of the applied rows
        /// </summary>
        public DataRowState State { get; }

        /// <summary>
        /// Gets the changes to be applied into the database
        /// </summary>
        public SyncTable Changes { get; }

        public override string Message => $"{this.Changes.TableName} state:{this.State}";

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
        /// Intercept the provider action when changes are selected on each table defined in the configuration schema
        /// </summary>
        public static void OnTableChangesSelected(this BaseOrchestrator orchestrator, Action<TableChangesSelectedArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when changes are going to be applied on each table defined in the configuration schema
        /// </summary>
        public static void OnTableChangesApplying(this BaseOrchestrator orchestrator, Action<TableChangesApplyingArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when changes are applied on each table defined in the configuration schema
        /// </summary>
        public static void OnTableChangesApplied(this BaseOrchestrator orchestrator, Action<TableChangesAppliedArgs> action)
            => orchestrator.SetInterceptor(action);

    }

    public static partial class SyncEventsId
    {
        public static EventId TableChangesSelecting => CreateEventId(13000, nameof(TableChangesSelecting));
        public static EventId TableChangesSelected => CreateEventId(13100, nameof(TableChangesSelected));
        public static EventId TableChangesApplying => CreateEventId(13200, nameof(TableChangesApplying));
        public static EventId TableChangesApplied => CreateEventId(13300, nameof(TableChangesApplied));

    }
}
