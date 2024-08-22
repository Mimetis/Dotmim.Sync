using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Web.Client;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Represents a response from server containing the server scope info.
    /// </summary>
    public class HttpGettingScopeResponseArgs : ProgressArgs
    {
        /// <inheritdoc cref="HttpGettingScopeResponseArgs" />
        public HttpGettingScopeResponseArgs(ScopeInfo sScopeInfo, SyncContext context, string host)
            : base(context, null)
        {
            this.ScopeInfoFromServer = sScopeInfo;
            this.Host = host;
        }

        /// <inheritdoc />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc />
        public override int EventId => HttpClientSyncEventsId.HttpGettingScopeResponse.Id;

        /// <inheritdoc />
        public override string Source => this.Host;

        /// <inheritdoc />
        public override string Message => $"Received Scope. Scope Name:{this.ScopeInfoFromServer.Name}.";

        /// <summary>
        /// Gets the server scope info.
        /// </summary>
        public ScopeInfo ScopeInfoFromServer { get; }

        /// <summary>
        /// Gets the host.
        /// </summary>
        public string Host { get; }
    }

    /// <summary>
    /// Represents a request made to the server to get the server scope info.
    /// </summary>
    public class HttpGettingScopeRequestArgs : ProgressArgs
    {
        /// <inheritdoc cref="HttpGettingScopeRequestArgs" />
        public HttpGettingScopeRequestArgs(SyncContext context, string host)
            : base(context, null) => this.Host = host;

        /// <inheritdoc />
        public override int EventId => HttpClientSyncEventsId.HttpGettingScopeRequest.Id;

        /// <inheritdoc />
        public override string Source => this.Host;

        /// <inheritdoc />
        public override string Message => $"Getting Server Scope. Scope Name:{this.Context.ScopeName}.";

        /// <inheritdoc />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <summary>
        /// Gets the host.
        /// </summary>
        public string Host { get; }
    }

    /// <summary>
    /// HttpGettingScopeRequestArgs extensions.
    /// </summary>
    public partial class HttpClientSyncEventsId
    {
        /// <summary>
        /// Gets the event id for HttpGettingScopeRequest.
        /// </summary>
        public static EventId HttpGettingScopeRequest => new(20300, nameof(HttpGettingScopeRequest));

        /// <summary>
        /// Gets the event id for HttpGettingScopeResponse.
        /// </summary>
        public static EventId HttpGettingScopeResponse => new(20350, nameof(HttpGettingScopeResponse));
    }

    /// <summary>
    /// Partial Interceptors extensions.
    /// </summary>
    public partial class HttpInterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when an http is about to be done to get server scope.
        /// </summary>
        public static Guid OnHttpGettingScopeRequest(this WebRemoteOrchestrator orchestrator, Action<HttpGettingScopeRequestArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http is about to be done to get server scope.
        /// </summary>
        public static Guid OnHttpGettingScopeRequest(this WebRemoteOrchestrator orchestrator, Func<HttpGettingScopeRequestArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http call to get scope is done.
        /// </summary>
        public static Guid OnHttpGettingScopeResponse(this WebRemoteOrchestrator orchestrator, Action<HttpGettingScopeResponseArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http call to get scope is done.
        /// </summary>
        public static Guid OnHttpGettingScopeResponse(this WebRemoteOrchestrator orchestrator, Func<HttpGettingScopeResponseArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}