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
    /// Event args raised when a table is reset due to Reinitialize call
    /// </summary>
    public class TableResetAppliedArgs : ProgressArgs
    {
        public TableResetAppliedArgs(SyncContext context, SyncTable schemaTable, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.Table = schemaTable;
        }

        public SyncTable Table { get; set; }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Information;
        public override string Message => $"[{this.Table.GetFullName()}] reset.";
        public override int EventId => SyncEventsId.TableResetApplied.Id;
    }



    public static partial class InterceptorsExtensions
    {

        /// <summary>
        /// Intercept the provider action when a table is reset due to Reinitialize call
        /// </summary>
        public static Guid OnTableResetApplied(this BaseOrchestrator orchestrator, Action<TableChangesAppliedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a table is reset due to Reinitialize call
        /// </summary>
        public static Guid OnTableResetApplied(this BaseOrchestrator orchestrator, Func<TableChangesAppliedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

    }

    public static partial class SyncEventsId
    {
        public static EventId TableResetApplied => CreateEventId(13151, nameof(TableResetApplied));

    }
}
