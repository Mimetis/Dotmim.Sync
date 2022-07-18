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
    /// Contains SyncRow selected to be written in the batchpart info
    /// </summary>
    public class RowsChangesSelectedArgs : ProgressArgs
    {
        public RowsChangesSelectedArgs(SyncContext context, SyncRow syncRow, SyncTable schemaTable, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.SyncRow = syncRow;
            this.SchemaTable = schemaTable;
        }
        public SyncRow SyncRow { get; set; }

        /// <summary>
        /// Gets the table schema
        /// </summary>
        public SyncTable SchemaTable { get; }

        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        public override string Source => Connection.Database;
        public override string Message => $"[{this.SchemaTable.GetFullName()}] [SyncRow] {SyncRow.ToString()}.";
        public override int EventId => SyncEventsId.TableChangesSelected.Id;
    }


   
    public static partial class InterceptorsExtensions
    {

        /// <summary>
        /// Intercept the provider action when a sync row is about to be serialized in a batch part info after have been selected from the data source
        /// </summary>
        public static Guid OnRowsChangesSelected(this BaseOrchestrator orchestrator, Action<RowsChangesSelectedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a sync row is about to be serialized in a batch part info after have been selected from the data source
        /// </summary>
        public static Guid OnTableChangesSelectedSyncRow(this BaseOrchestrator orchestrator, Func<RowsChangesSelectedArgs, Task> action)
            => orchestrator.AddInterceptor(action);


    }

    public static partial class SyncEventsId
    {
    }


}
