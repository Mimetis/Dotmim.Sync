using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Event args raised when a table is reset due to Reinitialize call.
    /// </summary>
    public class TableResetAppliedArgs : ProgressArgs
    {
        /// <inheritdoc cref="TableResetAppliedArgs" />
        public TableResetAppliedArgs(SyncContext context, SyncTable schemaTable, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction) => this.Table = schemaTable;

        /// <summary>
        /// Gets or sets the table that has been reset.
        /// </summary>
        public SyncTable Table { get; set; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Information;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"[{this.Table.GetFullName()}] reset.";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 13151;
    }

    /// <summary>
    /// Intercept the provider action when a table is reset due to Reinitialize call.
    /// </summary>
    public partial class InterceptorsExtensions
    {

        /// <summary>
        /// Intercept the provider action when a table is reset due to Reinitialize call.
        /// </summary>
        public static Guid OnTableResetApplied(this BaseOrchestrator orchestrator, Action<TableChangesAppliedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a table is reset due to Reinitialize call.
        /// </summary>
        public static Guid OnTableResetApplied(this BaseOrchestrator orchestrator, Func<TableChangesAppliedArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}