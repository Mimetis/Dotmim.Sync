using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Event args raised when all changes for a table have been applied on a datasource.
    /// </summary>
    public class TableChangesAppliedArgs : ProgressArgs
    {
        /// <inheritdoc cref="TableChangesAppliedArgs"/>
        public TableChangesAppliedArgs(SyncContext context, TableChangesApplied tableChangesApplied, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction) => this.TableChangesApplied = tableChangesApplied;

        /// <summary>
        /// Gets or sets the changes applied on the datasource.
        /// </summary>
        public TableChangesApplied TableChangesApplied { get; set; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => this.TableChangesApplied != null && this.TableChangesApplied.Applied > 0 ? SyncProgressLevel.Information : SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message =>
            this.TableChangesApplied == null
            ? $"TableChangesAppliedArgs progress."
            : $"[{this.TableChangesApplied.TableName}] Changes {this.TableChangesApplied.State} Applied:{this.TableChangesApplied.Applied}. Resolved Conflicts:{this.TableChangesApplied.ResolvedConflicts}.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 13100;
    }

    /// <summary>
    /// Event args before a table changes is going to be applied on a datasource.
    /// </summary>
    public class TableChangesApplyingArgs : ProgressArgs
    {

        /// <inheritdoc cref="TableChangesApplyingArgs"/>
        public TableChangesApplyingArgs(SyncContext context, BatchInfo batchInfo, IEnumerable<BatchPartInfo> batchPartInfos, SyncTable schemaTable, SyncRowState state, DbCommand command, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.BatchInfo = batchInfo;
            this.BatchPartInfos = batchPartInfos;
            this.SchemaTable = schemaTable;
            this.State = state;
            this.Command = command;
        }

        /// <summary>
        /// Gets or sets a value indicating whether gets or sets if the changes should be canceled.
        /// </summary>
        public bool Cancel { get; set; }

        /// <summary>
        /// Gets the RowState of the applied rows.
        /// </summary>
        public SyncRowState State { get; }

        /// <summary>
        /// Gets or sets the command to be executed.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <summary>
        /// Gets or sets the batch info.
        /// </summary>
        public BatchInfo BatchInfo { get; set; }

        /// <summary>
        /// Gets the batch part infos.
        /// </summary>
        public IEnumerable<BatchPartInfo> BatchPartInfos { get; }

        /// <summary>
        /// Gets the table schema.
        /// </summary>
        public SyncTable SchemaTable { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"Applying Changes To {this.SchemaTable.GetFullName()}.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 13150;
    }

    /// <summary>
    /// Intercept the provider action when a all changes have been applied on a datasource table.
    /// </summary>
    public partial class InterceptorsExtensions
    {

        /// <summary>
        /// Occurs when a table is about to be applied on the local data source.
        /// <example>
        /// <code>
        /// localOrchestrator.OnTableChangesApplying(async args =>
        /// {
        ///     if (args.BatchPartInfos != null)
        ///     {
        ///         var syncTable = await localOrchestrator.LoadTableFromBatchInfoAsync(
        ///             args.BatchInfo, args.SchemaTable.ColumnName, args.SchemaTable.TableName, args.State);
        ///
        ///         if (syncTable != null "and" syncTable.HasRows)
        ///         {
        ///             Console.WriteLine($"Changes for table
        ///                     {args.SchemaTable.GetFullName()}. Rows:{syncTable.Rows.Count}");
        ///             foreach (var row in syncTable.Rows)
        ///                 Console.WriteLine(row);
        ///         }
        ///
        ///     }
        /// });
        /// </code>
        /// </example>
        /// </summary>
        public static Guid OnTableChangesApplying(this BaseOrchestrator orchestrator, Action<TableChangesApplyingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <inheritdoc cref="OnTableChangesApplying(BaseOrchestrator, Action{TableChangesApplyingArgs})"/>
        public static Guid OnTableChangesApplying(this BaseOrchestrator orchestrator, Func<TableChangesApplyingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a all changes have been applied on a datasource table.
        /// </summary>
        public static Guid OnTableChangesApplied(this BaseOrchestrator orchestrator, Action<TableChangesAppliedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a all changes have been applied on a datasource table.
        /// </summary>
        public static Guid OnTableChangesApplied(this BaseOrchestrator orchestrator, Func<TableChangesAppliedArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}