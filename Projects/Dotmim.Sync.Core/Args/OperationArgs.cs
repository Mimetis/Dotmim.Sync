using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Raised before a sync operation is done. Can override the whole processus, depending on the <see cref="SyncOperation"/> argument.
    /// </summary>
    public class OperationArgs : ProgressArgs
    {
        /// <inheritdoc cref="OperationArgs"/>
        public OperationArgs(SyncContext context, ScopeInfo serverScopeInfo, ScopeInfo clientScopeInfo, ScopeInfoClient scopeInfoClient, DbConnection connection = null, DbTransaction transaction = null)
        : base(context, connection, transaction)
        {
            this.ScopeInfoFromServer = serverScopeInfo;
            this.ScopeInfoFromClient = clientScopeInfo;
            this.ScopeInfoClient = scopeInfoClient;
        }

        /// <summary>
        /// Gets or sets the operation to be done.
        /// </summary>
        public SyncOperation Operation { get; set; }

        /// <summary>
        /// Gets the scope info from the server.
        /// </summary>
        public ScopeInfo ScopeInfoFromServer { get; }

        /// <summary>
        /// Gets the scope info from the client.
        /// </summary>
        public ScopeInfo ScopeInfoFromClient { get; }

        /// <summary>
        /// Gets the client scope info.
        /// </summary>
        public ScopeInfoClient ScopeInfoClient { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"Overriding operation from server. Operation: {this.Operation}.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 5200;
    }

    /// <summary>
    /// Interceptor extension methods.
    /// </summary>
    public partial class InterceptorsExtensions
    {

        /// <summary>
        /// Occurs when server receives a first request for a sync. Can override the whole processus, depending
        /// on the <see cref="SyncOperation"/> argument.
        /// <example>
        /// <code>
        /// [HttpPost]
        /// public async Task Post()
        /// {
        ///     var scopeName = context.GetScopeName();
        ///     var clientScopeId = context.GetClientScopeId();
        ///     var webServerAgent = webServerAgents.First(wsa => wsa.ScopeName == scopeName);
        ///     webServerAgent.RemoteOrchestrator.OnGettingOperation(operationArgs =>
        ///     {
        ///         if (clientScopeId == A_PARTICULAR_CLIENT_ID_TO_CHECK)
        ///             operationArgs.SyncOperation = SyncOperation.ReinitializeWithUpload;
        ///     });
        ///     await webServerAgent.HandleRequestAsync(context);
        /// }
        /// </code>
        /// </example>
        /// </summary>
        public static Guid OnGettingOperation(this BaseOrchestrator orchestrator, Action<OperationArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <inheritdoc cref="OnGettingOperation(BaseOrchestrator, Action{OperationArgs})"/>
        public static Guid OnGettingOperation(this BaseOrchestrator orchestrator, Func<OperationArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}