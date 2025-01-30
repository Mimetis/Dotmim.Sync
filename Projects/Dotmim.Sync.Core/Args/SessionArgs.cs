using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Event args generated when a connection is opened.
    /// </summary>
    public class ConnectionOpenedArgs : ProgressArgs
    {
        /// <inheritdoc cref="ConnectionOpenedArgs" />
        public ConnectionOpenedArgs(SyncContext context, DbConnection connection)
            : base(context, connection)
        {
        }

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"Connection Opened.";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 9000;
    }

    /// <summary>
    /// Event args generated when trying to reconnect.
    /// </summary>
    public class ReConnectArgs : ProgressArgs
    {
        /// <inheritdoc cref="ReConnectArgs" />
        public ReConnectArgs(SyncContext context, DbConnection connection, Exception handledException, int retry, TimeSpan waitingTimeSpan)
            : base(context, connection)
        {
            this.HandledException = handledException;
            this.Retry = retry;
            this.WaitingTimeSpan = waitingTimeSpan;
        }

        /// <summary>
        /// Gets the handled exception.
        /// </summary>
        public Exception HandledException { get; }

        /// <summary>
        /// Gets the retry count.
        /// </summary>
        public int Retry { get; }

        /// <summary>
        /// Gets the waiting timespan duration.
        /// </summary>
        public TimeSpan WaitingTimeSpan { get; }

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"Trying to Reconnect...";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 9010;
    }

    /// <summary>
    /// Event args generated when a transient error is happenning.
    /// </summary>
    public class TransientErrorOccuredArgs : ProgressArgs
    {
        /// <inheritdoc cref="TransientErrorOccuredArgs" />
        public TransientErrorOccuredArgs(SyncContext context, DbConnection connection, Exception handledException, int retry, TimeSpan waitingTimeSpan)
            : base(context, connection)
        {
            this.HandledException = handledException;
            this.Retry = retry;
            this.WaitingTimeSpan = waitingTimeSpan;
        }

        /// <summary>
        /// Gets the handled exception.
        /// </summary>
        public Exception HandledException { get; }

        /// <summary>
        /// Gets the retry count.
        /// </summary>
        public int Retry { get; }

        /// <summary>
        /// Gets the waiting timespan duration.
        /// </summary>
        public TimeSpan WaitingTimeSpan { get; }

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"Transient error:{this.HandledException?.Message}. Retry:{this.Retry}";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 9060;
    }

    /// <summary>
    /// Event args generated when a connection is closed.
    /// </summary>
    public class ConnectionClosedArgs : ProgressArgs
    {
        /// <inheritdoc cref="ConnectionClosedArgs" />
        public ConnectionClosedArgs(SyncContext context, DbConnection connection)
            : base(context, connection)
        {
        }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"Connection Closed.";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 9050;
    }

    /// <summary>
    /// Event args generated when a transaction is opened.
    /// </summary>
    public class TransactionOpenedArgs : ProgressArgs
    {
        /// <inheritdoc cref="TransactionOpenedArgs" />
        public TransactionOpenedArgs(SyncContext context, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
        }

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"Transaction Opened.";

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 9100;
    }

    /// <summary>
    /// Event args generated when a transaction is commit.
    /// </summary>
    public class TransactionCommitArgs : ProgressArgs
    {
        /// <inheritdoc cref="TransactionCommitArgs" />
        public TransactionCommitArgs(SyncContext context, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
        }

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"Transaction Commited.";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 9150;
    }

    /// <summary>
    /// Event args generated during BeginSession stage.
    /// </summary>
    public class SessionBeginArgs : ProgressArgs
    {
        /// <inheritdoc cref="SessionBeginArgs" />
        public SessionBeginArgs(SyncContext context, DbConnection connection)
            : base(context, connection, null)
        {
        }

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Information;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"Session Begins. Id:{this.Context.SessionId}. Scope name:{this.Context.ScopeName}.";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 100;
    }

    /// <summary>
    /// Event args generated during EndSession stage.
    /// </summary>
    public class SessionEndArgs : ProgressArgs
    {

        /// <inheritdoc cref="SessionEndArgs" />
        public SessionEndArgs(SyncContext context, SyncResult syncResult, SyncException syncException, DbConnection connection)
            : base(context, connection, null)
        {
            this.SyncResult = syncResult;
            this.SyncException = syncException;
        }

        /// <summary>
        /// Gets the sync result.
        /// </summary>
        public SyncResult SyncResult { get; }

        /// <summary>
        /// Gets the exception occured if any.
        /// </summary>
        public SyncException SyncException { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Information;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"Session Ends. Id:{this.Context.SessionId}. Scope name:{this.Context.ScopeName}.";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 200;
    }

    /// <summary>
    /// Interceptors extensions.
    /// </summary>
    public partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider action whenever a connection is opened.
        /// </summary>
        public static Guid OnConnectionOpen(this BaseOrchestrator orchestrator, Action<ConnectionOpenedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action whenever a connection is opened.
        /// </summary>
        public static Guid OnConnectionOpen(this BaseOrchestrator orchestrator, Func<ConnectionOpenedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Occurs when trying to reconnect to a database.
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
        /// Intercept the provider action whenever a transaction is opened.
        /// </summary>
        public static Guid OnTransactionOpen(this BaseOrchestrator orchestrator, Action<TransactionOpenedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action whenever a transaction is opened.
        /// </summary>
        public static Guid OnTransactionOpen(this BaseOrchestrator orchestrator, Func<TransactionOpenedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action whenever a connection is closed.
        /// </summary>
        public static Guid OnConnectionClose(this BaseOrchestrator orchestrator, Action<ConnectionClosedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action whenever a connection is closed.
        /// </summary>
        public static Guid OnConnectionClose(this BaseOrchestrator orchestrator, Func<ConnectionClosedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action whenever a transaction is commit.
        /// </summary>
        public static Guid OnTransactionCommit(this BaseOrchestrator orchestrator, Action<TransactionCommitArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action whenever a transaction is commit.
        /// </summary>
        public static Guid OnTransactionCommit(this BaseOrchestrator orchestrator, Func<TransactionCommitArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when session begin is called.
        /// </summary>
        public static Guid OnSessionBegin(this BaseOrchestrator orchestrator, Action<SessionBeginArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when session begin is called.
        /// </summary>
        public static Guid OnSessionBegin(this BaseOrchestrator orchestrator, Func<SessionBeginArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when session end is called.
        /// </summary>
        public static Guid OnSessionEnd(this BaseOrchestrator orchestrator, Action<SessionEndArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when session end is called.
        /// </summary>
        public static Guid OnSessionEnd(this BaseOrchestrator orchestrator, Func<SessionEndArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a transient error is happening.
        /// </summary>
        public static Guid OnTransientErrorOccured(this BaseOrchestrator orchestrator, Action<TransientErrorOccuredArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a transient error is happening.
        /// </summary>
        public static Guid OnTransientErrorOccured(this BaseOrchestrator orchestrator, Func<TransientErrorOccuredArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}