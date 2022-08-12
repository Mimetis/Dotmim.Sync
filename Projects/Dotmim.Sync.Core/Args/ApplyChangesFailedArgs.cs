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
    public class ApplyChangesFailedArgs : ProgressArgs
    {

        private BaseOrchestrator orchestrator;
        private DbSyncAdapter syncAdapter;
        private readonly SyncRow conflictRow;
        private SyncTable schemaChangesTable;
        private ConflictResolution resolution;

        // used only internally
        internal SyncConflict conflict;

        /// <summary>
        /// Gets or Sets the action to be taken when resolving the conflict. 
        /// If you choose MergeRow, you have to fill the FinalRow property
        /// </summary>
        public ConflictResolution Resolution
        {
            get => this.resolution;
            set
            {
                if (this.resolution != value)
                {
                    this.resolution = value;

                    //if (this.resolution == ConflictResolution.MergeRow)
                    //{
                    //    if (this.conflict == null)
                    //        this.conflict = this.GetSyncConflictAsync().GetAwaiter().GetResult();

                    //    var finalRowArray = this.conflict.RemoteRow.ToArray();
                    //    var finalTable = this.conflict.RemoteRow.SchemaTable.Clone();
                    //    var finalSet = this.conflict.RemoteRow.SchemaTable.Schema.Clone(false);
                    //    finalSet.Tables.Add(finalTable);
                    //    this.FinalRow = new SyncRow(this.conflict.RemoteRow.SchemaTable, finalRowArray);
                    //    finalTable.Rows.Add(this.FinalRow);
                    //}
                    //else if (this.FinalRow != null)
                    //{
                    //    var finalSet = this.FinalRow.SchemaTable.Schema;
                    //    this.FinalRow.Clear();
                    //    finalSet.Clear();
                    //    finalSet.Dispose();
                    //}
                }
            }
        }

        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <summary>
        /// Gets or Sets the scope id who will be marked as winner
        /// </summary>
        public Guid? SenderScopeId { get; set; }

        /// <summary>
        /// If we have a merge action, the final row represents the merged row
        /// </summary>
        public SyncRow FinalRow { get; set; }


        public async Task<SyncConflict> GetSyncConflictAsync()
        {
            var (_, localRow) = await orchestrator.InternalGetConflictRowAsync(Context, syncAdapter, conflictRow, schemaChangesTable, this.Connection, this.Transaction).ConfigureAwait(false);

            if (localRow != null)
                this.conflict = orchestrator.InternalGetConflict(conflictRow, localRow);

            return conflict;
        }

        public ApplyChangesFailedArgs(SyncContext context, BaseOrchestrator orchestrator, DbSyncAdapter syncAdapter, SyncRow conflictRow, SyncTable schemaChangesTable, ConflictResolution action, Guid? senderScopeId, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.orchestrator = orchestrator;
            this.syncAdapter = syncAdapter;
            this.conflictRow = conflictRow;
            this.schemaChangesTable = schemaChangesTable;
            this.resolution = action;
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
        public ApplyChangesErrorOccuredArgs(SyncContext context, SyncRow errorRow, SyncTable schemaChangesTable, DataRowState applyType, Exception exception, DbCommand command, DbConnection connection, DbTransaction transaction) : base(context, connection, transaction)
        {
            this.ErrorRow = errorRow;
            this.SchemaTable = schemaChangesTable;
            this.ApplyType = applyType;
            this.Exception = exception;
            this.Command = command;
        }

        public SyncRow ErrorRow { get; }
        public SyncTable SchemaTable { get; }
        public DataRowState ApplyType { get; }
        public Exception Exception { get; }
        public ErrorResolution Resolution { get; set; } = ErrorResolution.Throw;
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Information;

        public override string Source => Connection.Database;
        public override string Message => $"[{Connection.Database}] Error: {Exception.Message}. Row:{ErrorRow}. ApplyType:{ApplyType}";

        public override int EventId => SyncEventsId.ApplyChangesErrorOccured.Id;

        public DbCommand Command { get; }
    }

    public static partial class InterceptorsExtensions
    {

        /// <summary>
        /// Intercept the provider when an apply change is failing
        /// </summary>
        public static Guid OnApplyChangesFailed(this BaseOrchestrator orchestrator, Action<ApplyChangesFailedArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider when an apply change is failing
        /// </summary>
        public static Guid OnApplyChangesFailed(this BaseOrchestrator orchestrator, Func<ApplyChangesFailedArgs, Task> action)
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
