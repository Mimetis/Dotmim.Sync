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
    /// Event args before a batch changes is going to be applied on a datasource
    /// </summary>
    public class RowsChangesApplyingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; set; }

        public RowsChangesApplyingArgs(SyncContext context, BatchInfo batchInfo, List<SyncRow> syncRows, SyncTable schemaTable, DataRowState state, DbCommand command, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.State = state;
            this.Command = command;
            this.BatchInfo = batchInfo;
            this.SyncRows = syncRows;
            this.SchemaTable = schemaTable;
        }

        /// <summary>
        /// Gets the RowState of the applied rows
        /// </summary>
        public DataRowState State { get; }
        public BatchInfo BatchInfo { get; }
        public List<SyncRow> SyncRows { get; }

        public SyncTable SchemaTable { get; }

        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override string Source => Connection.Database;
        public override string Message => $"Applying [{this.SchemaTable.GetFullName()}] batch rows. State:{this.State}. Count:{this.SyncRows.Count()}";

        public override int EventId => SyncEventsId.RowsChangesApplying.Id;
    }


    public static partial class InterceptorsExtensions
    {


        /// <summary>
        /// Intercept the provider action when a batch changes is going to be applied on a table
        /// </summary>
        public static Guid OnRowsChangesApplying(this BaseOrchestrator orchestrator, Action<RowsChangesApplyingArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a batch changes is going to be applied on a table
        /// </summary>
        public static Guid OnRowsChangesApplying(this BaseOrchestrator orchestrator, Func<RowsChangesApplyingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

      

    }

    public static partial class SyncEventsId
    {
        public static EventId RowsChangesApplying => CreateEventId(13100, nameof(RowsChangesApplying));

    }
}
