using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Raised as an argument when an apply is failing. Waiting from user for the conflict resolution.
    /// </summary>
    public class ApplyChangesConflictOccuredArgs : ProgressArgs
    {
        private readonly ScopeInfo scopeInfo;
        private readonly SyncRow conflictRow;
        private BaseOrchestrator orchestrator;
        private SyncTable schemaChangesTable;
        private SyncConflict conflict;

        /// <inheritdoc cref="ApplyChangesConflictOccuredArgs"/>
        public ApplyChangesConflictOccuredArgs(ScopeInfo scopeInfo, SyncContext context, BaseOrchestrator orchestrator,
            SyncRow conflictRow, SyncTable schemaChangesTable, ConflictResolution action, Guid? senderScopeId, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            Guard.ThrowIfNull(conflictRow);

            this.scopeInfo = scopeInfo;
            this.orchestrator = orchestrator;
            this.conflictRow = conflictRow;
            this.schemaChangesTable = schemaChangesTable;
            this.Resolution = action;
            this.SenderScopeId = senderScopeId;

            var finalRowArray = new object[conflictRow.ToArray().Length];
            conflictRow.ToArray().CopyTo(finalRowArray, 0);

            this.FinalRow = new SyncRow(schemaChangesTable.Clone(), finalRowArray);
        }

        /// <summary>
        /// Gets or Sets the action to be taken when resolving the conflict.
        /// If you choose MergeRow, FinalRow will be merged in both sources.
        /// </summary>
        public ConflictResolution Resolution { get; set; }

        /// <summary>
        /// Gets the Progress level used to determine if message is output.
        /// </summary>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <summary>
        /// Gets or Sets the scope id who will be marked as winner.
        /// </summary>
        public Guid? SenderScopeId { get; set; }

        /// <summary>
        /// Gets or sets if we have a merge action, the final row represents the merged row.
        /// </summary>
        public SyncRow FinalRow { get; set; }

        /// <summary>
        /// Get the conflict that occurs by selecting the local conflict row.
        /// </summary>
        public async Task<SyncConflict> GetSyncConflictAsync()
        {
            var (_, localRow) = await this.orchestrator.InternalGetConflictRowAsync(this.scopeInfo, this.Context, this.schemaChangesTable, this.conflictRow,
                this.Connection, this.Transaction, default, default).ConfigureAwait(false);
            this.conflict = this.orchestrator.InternalGetConflict(this.Context, this.conflictRow, localRow);
            return this.conflict;
        }

        /// <inheritdoc />
        public override string Message => $"Conflict {this.conflictRow}.";

        /// <inheritdoc />
        public override int EventId => SyncEventsId.ApplyChangesFailed.Id;
    }

    /// <summary>
    /// Interceptors extensions.
    /// </summary>
    public partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when a conflict is happening.
        /// </summary>
        public static Guid OnApplyChangesConflictOccured(this BaseOrchestrator orchestrator, Action<ApplyChangesConflictOccuredArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a conflict is happening.
        /// </summary>
        public static Guid OnApplyChangesConflictOccured(this BaseOrchestrator orchestrator, Func<ApplyChangesConflictOccuredArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }

    /// <summary>
    /// Sync Events Id.
    /// </summary>
    public partial class SyncEventsId
    {
        /// <summary>
        /// Gets the unique event id.
        /// </summary>
        public static EventId ApplyChangesFailed => CreateEventId(300, nameof(ApplyChangesFailed));
    }
}