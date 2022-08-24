using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public class ConflictingSetupArgs : ProgressArgs
    {
        public ConflictingSetupArgs(SyncContext context, SyncSetup setup, ScopeInfo clientScopeInfo, ScopeInfo serverScopeInfo, DbConnection connection = null, DbTransaction transaction = null) : base(context, connection, transaction)
        {
            this.Setup = setup;
            this.ClientScopeInfo = clientScopeInfo;
            this.ServerScopeInfo = serverScopeInfo;
        }

        /// <summary>
        /// Gets or sets an action enumeration value for the action to handle the Conflicting Setups.
        /// </summary>
        public ConflictingSetupAction Action { get; set; } = ConflictingSetupAction.Rollback;

        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Information;

        public override string Source => Connection.Database;
        public override string Message => $"Client Setup is desynchronized.";

        /// <summary>
        /// Gets the Setup provided from the SynchronizeAsync() method. May be null
        /// </summary>
        public SyncSetup Setup { get; set; }

        /// <summary>
        /// Gets or Sets the client scope info used to check if the client is conflicting
        /// </summary>
        public ScopeInfo ClientScopeInfo { get; set; }

        /// <summary>
        /// Gets the server scope info to check if client is conflicting
        /// </summary>
        public ScopeInfo ServerScopeInfo { get; set; }

        public override int EventId => SyncEventsId.ConflictingSetup.Id;
    }

    public static partial class InterceptorsExtensions
    {

        /// <summary>
        /// Intercept the provider if the local setup is conflicting with the remote setup
        /// </summary>
        public static Guid OnConflictingSetup(this BaseOrchestrator orchestrator, Action<ConflictingSetupArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider if the local setup is conflicting with the remote setup
        /// </summary>
        public static Guid OnConflictingSetup(this BaseOrchestrator orchestrator, Func<ConflictingSetupArgs, Task> action)
            => orchestrator.AddInterceptor(action);

    }

    public enum ConflictingSetupAction
    {
        /// <summary>
        /// Abort the sync without raising any Error
        /// </summary>
        Abort,
        /// <summary>
        /// Continue the sync process. ClientScopeInfo.Setup / Schema & ServerScopeInfo.Setup / Schema must be equals
        /// </summary>
        Continue,
        /// <summary>
        /// Rollback the sync, raising an error
        /// </summary>
        Rollback
    }

    public static partial class SyncEventsId
    {
        public static EventId ConflictingSetup => CreateEventId(5250, nameof(ConflictingSetup));
    }

}
