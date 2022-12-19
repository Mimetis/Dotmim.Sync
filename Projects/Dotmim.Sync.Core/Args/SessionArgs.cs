﻿using Dotmim.Sync.Enumerations;
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
    /// Event args generated when a connection is opened
    /// </summary>
    public class ConnectionOpenedArgs : ProgressArgs
    {
        public ConnectionOpenedArgs(SyncContext context, DbConnection connection)
            : base(context, connection)
        {
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        public override string Message => $"Connection Opened.";

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

        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        public override string Message => $"Trying to Reconnect...";

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
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;
        public override string Message => $"Connection Closed.";

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
        public override string Message => $"Transaction Opened.";
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;
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
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;
        public override string Message => $"Transaction Commited.";

        public override int EventId => SyncEventsId.TransactionCommit.Id;
    }

    /// <summary>
    /// Event args generated during BeginSession stage
    /// </summary>
    public class SessionBeginArgs : ProgressArgs
    {
        public SessionBeginArgs(SyncContext context, DbConnection connection)
            : base(context, connection, null)
        {
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Information;
        public override string Message => $"Session Begins. Id:{Context.SessionId}. Scope name:{Context.ScopeName}.";

        public override int EventId => SyncEventsId.SessionBegin.Id;
    }

    /// <summary>
    /// Event args generated during EndSession stage
    /// </summary>
    public class SessionEndArgs : ProgressArgs
    {
        /// <summary>
        /// Gets the sync result
        /// </summary>
        public SyncResult SyncResult { get; }

        /// <summary>
        /// Gets the exception occured if any
        /// </summary>
        public SyncException SyncException { get; }

        public SessionEndArgs(SyncContext context, SyncResult syncResult, SyncException syncException, DbConnection connection)
            : base(context, connection, null)
        {
            SyncResult = syncResult;
            this.SyncException = syncException;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Information;
        public override string Message => $"Session Ends. Id:{Context.SessionId}. Scope name:{Context.ScopeName}.";
        public override int EventId => SyncEventsId.SessionEnd.Id;
    }


    public static partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider action whenever a connection is opened
        /// </summary>
        public static Guid OnConnectionOpen(this BaseOrchestrator orchestrator, Action<ConnectionOpenedArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action whenever a connection is opened
        /// </summary>
        public static Guid OnConnectionOpen(this BaseOrchestrator orchestrator, Func<ConnectionOpenedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Occurs when trying to reconnect to a database
        /// <example>
        /// <code>
        /// localOrchestrator.OnReConnect(args => {
        ///     Console.WriteLine($"[Retry] Can't connect to database {args.Connection?.Database}. " +
        ///     $"Retry N°{args.Retry}. " +
        ///     $"Waiting {args.WaitingTimeSpan.Milliseconds}. Exception:{args.HandledException.Message}.");
        /// });  
        /// </code>
        /// </example>
        /// </summary>
        public static Guid OnReConnect(this BaseOrchestrator orchestrator, Action<ReConnectArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <inheritdoc cref="OnReConnect(BaseOrchestrator, Action{ReConnectArgs})"/>
        public static Guid OnReConnect(this BaseOrchestrator orchestrator, Func<ReConnectArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action whenever a transaction is opened
        /// </summary>
        public static Guid OnTransactionOpen(this BaseOrchestrator orchestrator, Action<TransactionOpenedArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action whenever a transaction is opened
        /// </summary>
        public static Guid OnTransactionOpen(this BaseOrchestrator orchestrator, Func<TransactionOpenedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action whenever a connection is closed
        /// </summary>
        public static Guid OnConnectionClose(this BaseOrchestrator orchestrator, Action<ConnectionClosedArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action whenever a connection is closed
        /// </summary>
        public static Guid OnConnectionClose(this BaseOrchestrator orchestrator, Func<ConnectionClosedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action whenever a transaction is commit
        /// </summary>
        public static Guid OnTransactionCommit(this BaseOrchestrator orchestrator, Action<TransactionCommitArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action whenever a transaction is commit
        /// </summary>
        public static Guid OnTransactionCommit(this BaseOrchestrator orchestrator, Func<TransactionCommitArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when session begin is called
        /// </summary>
        public static Guid OnSessionBegin(this BaseOrchestrator orchestrator, Action<SessionBeginArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action when session begin is called
        /// </summary>
        public static Guid OnSessionBegin(this BaseOrchestrator orchestrator, Func<SessionBeginArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when session end is called
        /// </summary>
        public static Guid OnSessionEnd(this BaseOrchestrator orchestrator, Action<SessionEndArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action when session end is called
        /// </summary>
        public static Guid OnSessionEnd(this BaseOrchestrator orchestrator, Func<SessionEndArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }

    public static partial class SyncEventsId
    {
        public static EventId ConnectionOpen => CreateEventId(9000, nameof(ConnectionOpen));
        public static EventId ConnectionClose => CreateEventId(9050, nameof(ConnectionClose));
        public static EventId ReConnect => CreateEventId(9010, nameof(ReConnect));
        public static EventId TransactionOpen => CreateEventId(9100, nameof(TransactionOpen));
        public static EventId TransactionCommit => CreateEventId(9150, nameof(TransactionCommit));

        public static EventId SessionBegin => CreateEventId(100, nameof(SessionBegin));
        public static EventId SessionEnd => CreateEventId(200, nameof(SessionEnd));
    }
}
