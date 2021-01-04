using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync
{

    /// <summary>
    /// Event args generated when a connection is opened
    /// </summary>
    public class ConnectionOpenedArgs : ProgressArgs
    {
        public ConnectionOpenedArgs(SyncContext context, DbConnection connection)
            : base(context, connection)
        {
        }

        public override string Message => $"";

        public override int EventId => SyncEventsId.ConnectionOpen.Id;
    }

    /// <summary>
    /// Event args generated when trying to reconnect
    /// </summary>
    public class ReConnectArgs : ProgressArgs
    {
        public ReConnectArgs(SyncContext context, DbConnection connection, Exception handledException, int retry, TimeSpan waitingTimeSpan)
            : base(context, connection)
        {
            this.HandledException = handledException;
            this.Retry = retry;
            this.WaitingTimeSpan = waitingTimeSpan;
        }

        public override string Message => $"";

        /// <summary>
        /// Gets the handled exception
        /// </summary>
        public Exception HandledException { get; }

        /// <summary>
        /// Gets the retry count
        /// </summary>
        public int Retry { get; }

        /// <summary>
        /// Gets the waiting timespan duration
        /// </summary>
        public TimeSpan WaitingTimeSpan { get; }
        public override int EventId => SyncEventsId.ReConnect.Id;
    }

    /// <summary>
    /// Event args generated when a connection is closed 
    /// </summary>
    public class ConnectionClosedArgs : ProgressArgs
    {
        public ConnectionClosedArgs(SyncContext context, DbConnection connection)
            : base(context, connection)
        {
        }

        public override string Message => $"";

        public override int EventId => SyncEventsId.ConnectionClose.Id;
    }

    /// <summary>
    /// Event args generated when a transaction is opened
    /// </summary>
    public class TransactionOpenedArgs : ProgressArgs
    {
        public TransactionOpenedArgs(SyncContext context, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
        }

        public override string Message => $"";

        public override int EventId => SyncEventsId.TransactionOpen.Id;
    }

    /// <summary>
    /// Event args generated when a transaction is commit
    /// </summary>
    public class TransactionCommitArgs : ProgressArgs
    {
        public TransactionCommitArgs(SyncContext context, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
        }

        public override string Message => $"";

        public override int EventId => SyncEventsId.TransactionCommit.Id;
    }

    /// <summary>
    /// Event args generated during BeginSession stage
    /// </summary>
    public class SessionBeginArgs : ProgressArgs
    {
        public SessionBeginArgs(SyncContext context, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
        }

        public override string Message => $"Session Id:{this.Context.SessionId}";

        public override int EventId => SyncEventsId.SessionBegin.Id;
    }

    /// <summary>
    /// Event args generated during EndSession stage
    /// </summary>
    public class SessionEndArgs : ProgressArgs
    {
        public SessionEndArgs(SyncContext context, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
        }

        public override string Message => $"Session Id:{this.Context.SessionId}";
        public override int EventId => SyncEventsId.SessionEnd.Id;
    }

    /// <summary>
    /// Raised as an argument when an apply is failing. Waiting from user for the conflict resolution
    /// </summary>
    public class ApplyChangesFailedArgs : ProgressArgs
    {
        ConflictResolution resolution;

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

                    if (this.resolution == ConflictResolution.MergeRow)
                    {
                        var finalRowArray = this.Conflict.RemoteRow.ToArray();
                        var finalTable = this.Conflict.RemoteRow.Table.Clone();
                        var finalSet = this.Conflict.RemoteRow.Table.Schema.Clone(false);
                        finalSet.Tables.Add(finalTable);
                        this.FinalRow = new SyncRow(finalTable.Columns.Count);
                        this.FinalRow.Table = finalTable;

                        this.FinalRow.FromArray(finalRowArray);
                        finalTable.Rows.Add(this.FinalRow);
                    }
                    else if (this.FinalRow != null)
                    {
                        var finalSet = this.FinalRow.Table.Schema;
                        this.FinalRow.Clear();
                        finalSet.Clear();
                        finalSet.Dispose();
                    }
                }
            }
        }

        /// <summary>
        /// Gets the object that contains data and metadata for the row being applied and for the existing row in the database that caused the failure.
        /// </summary>
        public SyncConflict Conflict { get; }


        /// <summary>
        /// Gets or Sets the scope id who will be marked as winner
        /// </summary>
        public Guid? SenderScopeId { get; set; }

        /// <summary>
        /// If we have a merge action, the final row represents the merged row
        /// </summary>
        public SyncRow FinalRow { get; set; }


        public ApplyChangesFailedArgs(SyncContext context, SyncConflict dbSyncConflict, ConflictResolution action, Guid? senderScopeId, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.Conflict = dbSyncConflict;
            this.resolution = action;
            this.SenderScopeId = senderScopeId;
        }

        public override string Message => $"{this.Conflict.Type}";

        public override int EventId => SyncEventsId.ApplyChangesFailed.Id;

    }


    public static partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider action whenever a connection is opened
        /// </summary>
        public static void OnConnectionOpen(this BaseOrchestrator orchestrator, Action<ConnectionOpenedArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Occurs when trying to reconnect to a database
        /// </summary>
        public static void OnReConnect(this BaseOrchestrator orchestrator, Action<ReConnectArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action whenever a transaction is opened
        /// </summary>
        public static void OnTransactionOpen(this BaseOrchestrator orchestrator, Action<TransactionOpenedArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action whenever a connection is closed
        /// </summary>
        public static void OnConnectionClose(this BaseOrchestrator orchestrator, Action<ConnectionClosedArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action whenever a transaction is commit
        /// </summary>
        public static void OnTransactionCommit(this BaseOrchestrator orchestrator, Action<TransactionCommitArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when session begin is called
        /// </summary>
        public static void OnSessionBegin(this BaseOrchestrator orchestrator, Action<SessionBeginArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when session end is called
        /// </summary>
        public static void OnSessionEnd(this BaseOrchestrator orchestrator, Action<SessionEndArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when an apply change is failing
        /// </summary>
        public static void OnApplyChangesFailed(this BaseOrchestrator orchestrator, Action<ApplyChangesFailedArgs> action)
            => orchestrator.SetInterceptor(action);

    }

    public static partial class SyncEventsId
    {
        public static EventId ConnectionOpen => CreateEventId(9000, nameof(ConnectionOpen));
        public static EventId ReConnect => CreateEventId(9100, nameof(ReConnect));
        public static EventId TransactionOpen => CreateEventId(9200, nameof(TransactionOpen));
        public static EventId ConnectionClose => CreateEventId(9300, nameof(ConnectionClose));
        public static EventId TransactionCommit => CreateEventId(9400, nameof(TransactionCommit));
        public static EventId SessionBegin => CreateEventId(9500, nameof(SessionBegin));
        public static EventId SessionEnd => CreateEventId(9600, nameof(SessionEnd));
        public static EventId ApplyChangesFailed => CreateEventId(9700, nameof(ApplyChangesFailed));
    }
}
