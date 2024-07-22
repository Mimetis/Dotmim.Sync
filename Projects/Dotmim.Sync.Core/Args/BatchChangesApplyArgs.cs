using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Event args raised when all changes from a batch, for a particular table, have been applied on a datasource.
    /// </summary>
    public class BatchChangesAppliedArgs : ProgressArgs
    {
        /// <inheritdoc cref="BatchChangesAppliedArgs" />
        public BatchChangesAppliedArgs(SyncContext context, BatchInfo batchInfo, BatchPartInfo batchPartInfo, SyncTable schemaTable, SyncRowState state, DbCommand command, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.BatchInfo = batchInfo;
            this.BatchPartInfo = batchPartInfo;
            this.SchemaTable = schemaTable;
            this.State = state;
            this.Command = command;
        }

        /// <summary>
        /// Gets the progress level info : SyncProgressLevel.Information if this.BatchPartInfo.RowsCount > 0 else SyncProgressLevel.Debug.
        /// </summary>
        public override SyncProgressLevel ProgressLevel => this.BatchPartInfo != null && this.BatchPartInfo.RowsCount > 0 ? SyncProgressLevel.Information : SyncProgressLevel.Debug;

        /// <summary>
        /// Gets the row state.
        /// </summary>
        public SyncRowState State { get; }

        /// <summary>
        /// Gets or sets the command used to apply the batch changes.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <summary>
        /// Gets or sets the batch info.
        /// </summary>
        public BatchInfo BatchInfo { get; set; }

        /// <summary>
        /// Gets the batch part info.
        /// </summary>
        public BatchPartInfo BatchPartInfo { get; }

        /// <summary>
        /// Gets the table schema.
        /// </summary>
        public SyncTable SchemaTable { get; }

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message =>
            this.BatchPartInfo == null
            ? $"[{this.SchemaTable.GetFullName()}] [{this.State}] BatchChangesAppliedArgs progress."
            : $"[{this.SchemaTable.GetFullName()}] [{this.State}] Batch {this.BatchPartInfo.FileName} ({this.BatchPartInfo.Index + 1}/{this.BatchInfo.BatchPartsInfo.Count}) Applied.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 13170;
    }

    /// <summary>
    /// Event args before a table changes from a batch, for a particular table, is going to be applied on a datasource.
    /// </summary>
    public class BatchChangesApplyingArgs : ProgressArgs
    {
        /// <inheritdoc cref="BatchChangesApplyingArgs" />
        public BatchChangesApplyingArgs(SyncContext context, BatchInfo batchInfo, BatchPartInfo batchPartInfo, SyncTable schemaTable, SyncRowState state, DbCommand command, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.BatchInfo = batchInfo;
            this.BatchPartInfo = batchPartInfo;
            this.SchemaTable = schemaTable;
            this.State = state;
            this.Command = command;
        }

        /// <summary>
        /// Gets or sets a value indicating whether gets or sets if the current batch changes applying should be canceled.
        /// </summary>
        public bool Cancel { get; set; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <summary>
        /// Gets the row state.
        /// </summary>
        public SyncRowState State { get; }

        /// <summary>
        /// Gets or sets the command used to apply the batch changes.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <summary>
        /// Gets or sets the batch info.
        /// </summary>
        public BatchInfo BatchInfo { get; set; }

        /// <summary>
        /// Gets the batch part info.
        /// </summary>
        public BatchPartInfo BatchPartInfo { get; }

        /// <summary>
        /// Gets the table schema.
        /// </summary>
        public SyncTable SchemaTable { get; }

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message =>
                        this.BatchPartInfo == null
            ? $"[{this.SchemaTable.GetFullName()}] [{this.State}] BatchChangesApplyingArgs progress."
            : $"[{this.SchemaTable.GetFullName()}] Applying Batch {this.BatchPartInfo.FileName} ({this.BatchPartInfo.Index + 1}/{this.BatchInfo.BatchPartsInfo.Count}).";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 13160;
    }

    /// <summary>
    /// Interceptors extensions.
    /// </summary>
    public partial class InterceptorsExtensions
    {

        /// <summary>
        /// Occurs when a batch for a particular table is about to be applied on the local data source.
        /// <example>
        /// <code>
        /// agent.LocalOrchestrator.OnBatchChangesApplying(async args =>
        /// {
        ///     if (args.BatchPartInfo != null)
        ///     {
        ///         Console.WriteLine($"FileName:{args.BatchPartInfo.FileName}. RowsCount:{args.BatchPartInfo.RowsCount} ");
        ///         Console.WriteLine($"Applying rows from this batch part info:");
        ///
        ///         var table = await agent.LocalOrchestrator.LoadTableFromBatchPartInfo(args.BatchInfo,
        ///                           args.BatchPartInfo, args.State, args.Connection, args.Transaction);
        ///
        ///         foreach (var row in table.Rows)
        ///             Console.WriteLine(row);
        ///
        ///     }
        /// });
        /// </code>
        /// </example>
        /// </summary>
        public static Guid OnBatchChangesApplying(this BaseOrchestrator orchestrator, Action<BatchChangesApplyingArgs> action)
                    => orchestrator.AddInterceptor(action);

        /// <inheritdoc cref="OnBatchChangesApplying(BaseOrchestrator, Action{BatchChangesApplyingArgs})"/>
        public static Guid OnBatchChangesApplying(this BaseOrchestrator orchestrator, Func<BatchChangesApplyingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Occurs when a batch for a particular table has been applied on the local data source.
        /// <example>
        /// <code>
        /// agent.LocalOrchestrator.OnBatchChangesApplied(async args =>
        /// {
        ///     if (args.BatchPartInfo != null)
        ///     {
        ///         Console.WriteLine($"FileName:{args.BatchPartInfo.FileName}. RowsCount:{args.BatchPartInfo.RowsCount} ");
        ///         Console.WriteLine($"Rows applied from this batch part info:");
        ///
        ///         var table = await agent.LocalOrchestrator.LoadTableFromBatchPartInfo(args.BatchInfo,
        ///                           args.BatchPartInfo, args.State, args.Connection, args.Transaction);
        ///
        ///         foreach (var row in table.Rows)
        ///             Console.WriteLine(row);
        ///
        ///     }
        /// });
        /// </code>
        /// </example>
        /// </summary>
        public static Guid OnBatchChangesApplied(this BaseOrchestrator orchestrator, Action<BatchChangesAppliedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <inheritdoc cref="OnBatchChangesApplied(BaseOrchestrator, Action{BatchChangesAppliedArgs})"/>
        public static Guid OnBatchChangesApplied(this BaseOrchestrator orchestrator, Func<BatchChangesAppliedArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}