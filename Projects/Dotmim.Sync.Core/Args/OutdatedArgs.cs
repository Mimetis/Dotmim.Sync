
using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;

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

        public override string Message => $"";

        /// <summary>
        /// Gets the client scope info used to check if the client is outdated
        /// </summary>
        public ScopeInfo ClientScopeInfo { get; }

        /// <summary>
        /// Gets the server scope info to check if client is outdated
        /// </summary>
        public ServerScopeInfo ServerScopeInfo { get; }
        public override int EventId => 12;
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


    }
}
