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
    /// Event args raised when all changes from a batch, for a particular table, have been applied on a datasource
    /// </summary>
    public class BatchChangesAppliedArgs : ProgressArgs
    {
        public BatchChangesAppliedArgs(SyncContext context, BatchInfo batchInfo, BatchPartInfo batchPartInfo, SyncTable schemaTable, SyncRowState state, DbCommand command, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.BatchInfo = batchInfo;
            this.BatchPartInfo = batchPartInfo;
            this.SchemaTable = schemaTable;
            this.State = state;
            this.Command = command;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Information;

        public SyncRowState State { get; }
        public DbCommand Command { get; set; }
        public BatchInfo BatchInfo { get; set; }
        public BatchPartInfo BatchPartInfo { get; }

        /// <summary>
        /// Gets the table schema
        /// </summary>
        public SyncTable SchemaTable { get; }

        public override string Source => Connection?.Database;
        public override string Message => $"[{this.SchemaTable.GetFullName()}] Batch {BatchPartInfo.FileName} ({BatchPartInfo.Index + 1}/{BatchInfo.BatchPartsInfo.Count}) Applied.";

        public override int EventId => SyncEventsId.BacthChangesApplied.Id;
    }


    /// <summary>
    /// Event args before a table changes from a batch, for a particular table, is going to be applied on a datasource
    /// </summary>
    public class BatchChangesApplyingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;

        public BatchChangesApplyingArgs(SyncContext context, BatchInfo batchInfo, BatchPartInfo batchPartInfo, SyncTable schemaTable, SyncRowState state, DbCommand command, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.BatchInfo = batchInfo;
            this.BatchPartInfo = batchPartInfo;
            this.SchemaTable = schemaTable;
            this.State = state;
            this.Command = command;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public SyncRowState State { get; }
        public DbCommand Command { get; set; }
        public BatchInfo BatchInfo { get; set; }
        public BatchPartInfo BatchPartInfo { get; }

        /// <summary>
        /// Gets the table schema
        /// </summary>
        public SyncTable SchemaTable { get; }

        public override string Source => Connection?.Database;
        public override string Message => $"[{this.SchemaTable.GetFullName()}] Applying Batch {BatchPartInfo.FileName} ({BatchPartInfo.Index + 1}/{BatchInfo.BatchPartsInfo.Count}).";

        public override int EventId => SyncEventsId.BacthChangesApplying.Id;
    }


    public static partial class InterceptorsExtensions
    {

        /// <summary>
        /// Occurs when a batch for a particular table is about to be applied on the local data source
        /// <example>
        /// <code>
        /// agent.LocalOrchestrator.OnBatchChangesApplying(async args =>
        /// {
        ///     if (args.BatchPartInfo != null)
        ///     {
        ///         Console.WriteLine($"FileName:{args.BatchPartInfo.FileName}. RowsCount:{args.BatchPartInfo.RowsCount} ");
        ///         Console.WriteLine($"Applying rows from this batch part info:");
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
        public static Guid OnBatchChangesApplying(this BaseOrchestrator orchestrator, Action<BatchChangesApplyingArgs> action)
                    => orchestrator.AddInterceptor(action);

        /// <inheritdoc cref="OnBatchChangesApplying(BaseOrchestrator, Action{BatchChangesApplyingArgs})"/>
        public static Guid OnBatchChangesApplying(this BaseOrchestrator orchestrator, Func<BatchChangesApplyingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

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
        public static Guid OnBatchChangesApplied(this BaseOrchestrator orchestrator, Action<BatchChangesAppliedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <inheritdoc cref="OnBatchChangesApplied(BaseOrchestrator, Action{BatchChangesAppliedArgs})"/>
        public static Guid OnBatchChangesApplied(this BaseOrchestrator orchestrator, Func<BatchChangesAppliedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

    }

    public static partial class SyncEventsId
    {
        public static EventId BacthChangesApplying => CreateEventId(13160, nameof(BacthChangesApplying));
        public static EventId BacthChangesApplied => CreateEventId(13170, nameof(BacthChangesApplied));

    }
}
