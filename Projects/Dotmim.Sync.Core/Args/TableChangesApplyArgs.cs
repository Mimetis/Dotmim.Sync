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
        public override SyncProgressLevel ProgressLevel => this.TableChangesApplied != null && this.TableChangesApplied.Applied > 0 ? SyncProgressLevel.Information : SyncProgressLevel.Debug;
        public override string Message =>
            this.TableChangesApplied == null 
            ? $"TableChangesAppliedArgs progress."
            : $"[{this.TableChangesApplied.TableName}] Changes {this.TableChangesApplied.State} Applied:{this.TableChangesApplied.Applied}. Resolved Conflicts:{this.TableChangesApplied.ResolvedConflicts}.";
        
        public override int EventId => SyncEventsId.TableChangesApplied.Id;
    }


    /// <summary>
    /// Event args before a table changes is going to be applied on a datasource
    /// </summary>
    public class TableChangesApplyingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;

        public TableChangesApplyingArgs(SyncContext context, BatchInfo batchInfo, IEnumerable<BatchPartInfo> batchPartInfos, SyncTable schemaTable, SyncRowState state, DbCommand command, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.BatchInfo = batchInfo;
            this.BatchPartInfos = batchPartInfos;
            this.SchemaTable = schemaTable;
            this.State = state;
            this.Command = command;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        /// <summary>
        /// Gets the RowState of the applied rows
        /// </summary>
        public SyncRowState State { get; }
        public DbCommand Command { get; set; }
        public BatchInfo BatchInfo { get; set; }
        public IEnumerable<BatchPartInfo> BatchPartInfos { get; }

        /// <summary>
        /// Gets the table schema
        /// </summary>
        public SyncTable SchemaTable { get; }

        public override string Message => $"Applying Changes To {this.SchemaTable.GetFullName()}.";

        public override int EventId => SyncEventsId.TableChangesApplying.Id;
    }


    public static partial class InterceptorsExtensions
    {

        /// <summary>
        /// Occurs when a table is about to be applied on the local data source
        /// <example>
        /// <code>
        /// localOrchestrator.OnTableChangesApplying(async args =>
        /// {
        ///     if (args.BatchPartInfos != null)
        ///     {
        ///         var syncTable = await localOrchestrator.LoadTableFromBatchInfoAsync(
        ///             args.BatchInfo, args.SchemaTable.TableName, args.SchemaTable.SchemaName, args.State);
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
        /// Intercept the provider action when a all changes have been applied on a datasource table
        /// </summary>
        public static Guid OnTableChangesApplied(this BaseOrchestrator orchestrator, Action<TableChangesAppliedArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a all changes have been applied on a datasource table
        /// </summary>
        public static Guid OnTableChangesApplied(this BaseOrchestrator orchestrator, Func<TableChangesAppliedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

    }

    public static partial class SyncEventsId
    {
        public static EventId TableChangesApplying => CreateEventId(13100, nameof(TableChangesApplying));
        public static EventId TableChangesApplied => CreateEventId(13150, nameof(TableChangesApplied));

    }
}
