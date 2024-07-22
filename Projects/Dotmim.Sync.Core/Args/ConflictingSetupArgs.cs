using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Event args raised when a conflicting setup between server and one client is occuring.
    /// </summary>
    public class ConflictingSetupArgs : ProgressArgs
    {
        /// <inheritdoc cref="ConflictingSetupArgs"/>
        public ConflictingSetupArgs(SyncContext context, SyncSetup setup, ScopeInfo clientScopeInfo, ScopeInfo serverScopeInfo, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Setup = setup;
            this.ClientScopeInfo = clientScopeInfo;
            this.ServerScopeInfo = serverScopeInfo;
        }

        /// <summary>
        /// Gets or sets an action enumeration value for the action to handle the Conflicting Setups.
        /// </summary>
        public ConflictingSetupAction Action { get; set; } = ConflictingSetupAction.Rollback;

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"Client Setup is desynchronized.";

        /// <summary>
        /// Gets or sets the Setup provided from the SynchronizeAsync() method. May be null.
        /// </summary>
        public SyncSetup Setup { get; set; }

        /// <summary>
        /// Gets or Sets the client scope info used to check if the client is conflicting.
        /// </summary>
        public ScopeInfo ClientScopeInfo { get; set; }

        /// <summary>
        /// Gets or sets the server scope info to check if client is conflicting.
        /// </summary>
        public ScopeInfo ServerScopeInfo { get; set; }

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => SyncEventsId.ConflictingSetup.Id;
    }

    /// <summary>
    /// Interceptor extension methods.
    /// </summary>
    public partial class InterceptorsExtensions
    {

        /// <summary>
        /// Intercept the provider if the local setup is conflicting with the remote setup.
        /// </summary>
        public static Guid OnConflictingSetup(this BaseOrchestrator orchestrator, Action<ConflictingSetupArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider if the local setup is conflicting with the remote setup.
        /// </summary>
        public static Guid OnConflictingSetup(this BaseOrchestrator orchestrator, Func<ConflictingSetupArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }

    /// <summary>
    /// When a conflict is happening, you can choose to continue, abort or rollback the sync process.
    /// </summary>
    public enum ConflictingSetupAction
    {
        /// <summary>
        /// Abort the sync without raising any Error.
        /// </summary>
        Abort,

        /// <summary>
        /// Continue the sync process. ClientScopeInfo.Setup / Schema and ServerScopeInfo.Setup / Schema must be equals.
        /// </summary>
        Continue,

        /// <summary>
        /// Rollback the sync, raising an error.
        /// </summary>
        Rollback,
    }

    /// <summary>
    /// Sync Events Ids.
    /// </summary>
    public partial class SyncEventsId
    {
        /// <summary>
        /// Gets the event id for a conflicting setup.
        /// </summary>
        public static EventId ConflictingSetup => CreateEventId(5250, nameof(ConflictingSetup));
    }
}