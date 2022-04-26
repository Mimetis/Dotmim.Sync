
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    
    public class OutdatedArgs : ProgressArgs
    {
        public OutdatedArgs(SyncContext context, ScopeInfo clientScopeInfo, ServerScopeInfo serverScopeInfo, DbConnection connection= null, DbTransaction transaction = null) : base(context, connection, transaction)
        {
            this.ClientScopeInfo = clientScopeInfo;
            this.ServerScopeInfo = serverScopeInfo;
        }

        /// <summary>
        /// Gets or sets an action enumeration value for the action to handle the outdated peer.
        /// </summary>
        public OutdatedAction Action { get; set; } = OutdatedAction.Rollback;
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Information;

        public override string Source => Connection.Database;
        public override string Message => $"Database Out Dated. Last Client Sync Endpoint < Last Server Cleanup Metadatas {ServerScopeInfo.LastCleanupTimestamp}.";

        /// <summary>
        /// Gets the client scope info used to check if the client is outdated
        /// </summary>
        public ScopeInfo ClientScopeInfo { get; }

        /// <summary>
        /// Gets the server scope info to check if client is outdated
        /// </summary>
        public ServerScopeInfo ServerScopeInfo { get; }
        public override int EventId => SyncEventsId.Outdated.Id;
    }

    public enum OutdatedAction
    {
        /// <summary>
        /// Reinitialize the whole sync database, applying all rows from the server to the client
        /// </summary>
        Reinitialize,
        /// <summary>
        /// Reinitialize the whole sync database, applying all rows from the server to the client, after trying a client upload
        /// </summary>
        ReinitializeWithUpload,
        /// <summary>
        /// Rollback the synchronization request.
        /// </summary>
        Rollback
    }

    public static partial class InterceptorsExtensions
    {

        /// <summary>
        /// Intercept the provider action when a database is out dated
        /// </summary>
        public static void OnOutdated(this BaseOrchestrator orchestrator, Action<OutdatedArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a database is out dated
        /// </summary>
        public static void OnOutdated(this BaseOrchestrator orchestrator, Func<OutdatedArgs, Task> action)
            => orchestrator.SetInterceptor(action);

    }

    public static partial class SyncEventsId
    {
        public static EventId Outdated => CreateEventId(5000, nameof(Outdated));
    }

}
