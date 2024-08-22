using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Event args raised when a batch file is created and serialized locally, for a particular table.
    /// </summary>
    public class BatchChangesCreatedArgs : ProgressArgs
    {
        /// <inheritdoc cref="BatchChangesCreatedArgs"/>
        public BatchChangesCreatedArgs(SyncContext context, BatchPartInfo batchPartInfo, SyncTable syncTable, TableChangesSelected tableChangesSelected, SyncRowState state, DbConnection connection, DbTransaction transaction)
                    : base(context, connection, transaction)
        {
            this.BatchPartInfo = batchPartInfo;
            this.SchemaTable = syncTable;
            this.TableChangesSelected = tableChangesSelected;
            this.State = state;
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
        /// Gets the table changes selected.
        /// </summary>
        public TableChangesSelected TableChangesSelected { get; }

        /// <summary>
        /// Gets the table schema.
        /// </summary>
        public SyncTable SchemaTable { get; }

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message =>
            this.BatchPartInfo == null
            ? $"[{this.SchemaTable.GetFullName()}] [{this.State}] BatchChangesCreatedArgs progress."
            : $"[{this.SchemaTable.GetFullName()}] [{this.State}] Batch {this.BatchPartInfo.FileName} ({this.BatchPartInfo.Index + 1}) Created.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 13175;
    }

    /// <summary>
    /// Interceptors extensions.
    /// </summary>
    public partial class InterceptorsExtensions
    {

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
        ///         var table = await agent.LocalOrchestrator.LoadTableFromBatchPartInfoAsync(args.BatchInfo,
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
        public static Guid OnBatchChangesCreated(this BaseOrchestrator orchestrator, Action<BatchChangesCreatedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <inheritdoc cref="OnBatchChangesCreated(BaseOrchestrator, Action{BatchChangesCreatedArgs})"/>
        public static Guid OnBatchChangesCreated(this BaseOrchestrator orchestrator, Func<BatchChangesCreatedArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}