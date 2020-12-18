using Dotmim.Sync.Enumerations;
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

        public override int EventId => 35;
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
        public override int EventId => 36;
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

        public override int EventId => 37;
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

        public override int EventId => 38;
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

        public override int EventId => 39;
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

        public override int EventId => 40;
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
        public override int EventId => 41;
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

    }
}
