using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{



    /// <summary>
    /// Event args raised when a batch file is created and serialized locally, for a particular table.
    /// </summary>
    public class BatchChangesCreatedArgs : ProgressArgs
    {
        public BatchChangesCreatedArgs(SyncContext context, BatchPartInfo batchPartInfo, SyncTable syncTable, TableChangesSelected tableChangesSelected, SyncRowState state, DbConnection connection, DbTransaction transaction)
                    : base(context, connection, transaction)
        {
            this.BatchPartInfo = batchPartInfo;
            this.SchemaTable = syncTable;
            this.TableChangesSelected = tableChangesSelected;
            this.State = state;
        }

        public override SyncProgressLevel ProgressLevel => BatchPartInfo != null && BatchPartInfo.RowsCount > 0 ? SyncProgressLevel.Information : SyncProgressLevel.Debug;

        public SyncRowState State { get; }
        public DbCommand Command { get; set; }
        public BatchInfo BatchInfo { get; set; }
        public BatchPartInfo BatchPartInfo { get; }

        public TableChangesSelected TableChangesSelected { get; }

        /// <summary>
        /// Gets the table schema
        /// </summary>
        public SyncTable SchemaTable { get; }

        public override string Message =>
            BatchPartInfo == null 
            ? $"[{this.SchemaTable.GetFullName()}] [{this.State}] BatchChangesCreatedArgs progress."
            : $"[{this.SchemaTable.GetFullName()}] [{this.State}] Batch {BatchPartInfo.FileName} ({BatchPartInfo.Index + 1}) Created.";

        public override int EventId => SyncEventsId.BatchChangesCreated.Id;

    }

    public static partial class InterceptorsExtensions
    {

        /// <summary>
        /// Occurs when a batch for a particular table has been applied on the local data source
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

    public static partial class SyncEventsId
    {
        public static EventId BatchChangesCreated => CreateEventId(13175, nameof(BatchChangesCreated));

    }
}
