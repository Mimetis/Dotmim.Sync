using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Raised as an argument when an apply is failing. Waiting from user for the conflict resolution
    /// </summary>
    public class ApplyChangesConflictOccuredArgs : ProgressArgs
    {
        private readonly ScopeInfo scopeInfo;
        private BaseOrchestrator orchestrator;
        private DbSyncAdapter syncAdapter;
        private readonly SyncRow conflictRow;
        private SyncTable schemaChangesTable;

        // used only internally
        internal SyncConflict conflict;

        /// <summary>
        /// Gets or Sets the action to be taken when resolving the conflict. 
        /// If you choose MergeRow, FinalRow will be merged in both sources
        /// </summary>
        public ConflictResolution Resolution { get; set; }

        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <summary>
        /// Gets or Sets the scope id who will be marked as winner
        /// </summary>
        public Guid? SenderScopeId { get; set; }

        /// <summary>
        /// If we have a merge action, the final row represents the merged row
        /// </summary>
        public SyncRow FinalRow { get; set; }


        /// <summary>
        /// Get the conflict that occurs by selecting the local conflict row
        /// </summary>
        public async Task<SyncConflict> GetSyncConflictAsync()
        {
            var (_, localRow) = await orchestrator.InternalGetConflictRowAsync(scopeInfo, Context, schemaChangesTable, conflictRow, this.Connection, this.Transaction).ConfigureAwait(false);
            this.conflict = orchestrator.InternalGetConflict(conflictRow, localRow);
            return conflict;
        }

        public ApplyChangesConflictOccuredArgs(ScopeInfo scopeInfo, SyncContext context, BaseOrchestrator orchestrator, 
            SyncRow conflictRow, SyncTable schemaChangesTable, ConflictResolution action, Guid? senderScopeId, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
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
        public override string Source => Connection.Database;
        public override string Message => $"Conflict {conflictRow}.";
        public override int EventId => SyncEventsId.ApplyChangesFailed.Id;

    }


    public class ApplyChangesErrorOccuredArgs : ProgressArgs
    {
        public ApplyChangesErrorOccuredArgs(SyncContext context, SyncRow errorRow,
            SyncTable schemaChangesTable,
            DataRowState applyType, Exception exception, DbConnection connection, DbTransaction transaction) : base(context, connection, transaction)
        {
            this.ErrorRow = errorRow;
            this.SchemaTable = schemaChangesTable;
            this.ApplyType = applyType;
            this.Exception = exception;
        }

        public SyncRow ErrorRow { get; }
        public SyncTable SchemaTable { get; }
        public DataRowState ApplyType { get; }
        public Exception Exception { get; }
        public ErrorResolution Resolution { get; set; } = ErrorResolution.Throw;
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        public override string Source => Connection.Database;
        public override string Message => $"[{Connection.Database}] Error: {Exception.Message}. Row:{ErrorRow}. ApplyType:{ApplyType}";

        public override int EventId => SyncEventsId.ApplyChangesErrorOccured.Id;
    }

    public static partial class InterceptorsExtensions
    {

        /// <summary>
        /// Intercept the provider when a conflict is happening
        /// </summary>
        public static Guid OnApplyChangesConflictOccured(this BaseOrchestrator orchestrator, Action<ApplyChangesConflictOccuredArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider when a conflict is happening
        /// </summary>
        public static Guid OnApplyChangesConflictOccured(this BaseOrchestrator orchestrator, Func<ApplyChangesConflictOccuredArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an apply change is failing
        /// </summary>
        public static Guid OnApplyChangesErrorOccured(this BaseOrchestrator orchestrator, Action<ApplyChangesErrorOccuredArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider when an apply change is failing
        /// </summary>
        public static Guid OnApplyChangesErrorOccured(this BaseOrchestrator orchestrator, Func<ApplyChangesErrorOccuredArgs, Task> action)
            => orchestrator.AddInterceptor(action);

    }

    public static partial class SyncEventsId
    {
        public static EventId ApplyChangesFailed => CreateEventId(300, nameof(ApplyChangesFailed));
        public static EventId ApplyChangesErrorOccured => CreateEventId(301, nameof(ApplyChangesErrorOccured));
    }
}
