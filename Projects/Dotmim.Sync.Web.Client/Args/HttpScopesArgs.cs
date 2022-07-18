using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Web.Client;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Represents a response from server containing the server scope info
    /// </summary>
    public class HttpGettingScopeResponseArgs : ProgressArgs
    {
        public HttpGettingScopeResponseArgs(ServerScopeInfo scopeInfo, SyncContext context, string host) : base(context, null)
        {
            this.ServerScopeInfo = scopeInfo;
            this.Host = host;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override int EventId => HttpClientSyncEventsId.HttpGettingScopeResponse.Id;
        public override string Source => this.Host;
        public override string Message => $"Received Scope. Scope Name:{this.ServerScopeInfo.Name}.";

        public ServerScopeInfo ServerScopeInfo { get; }
        public string Host { get; }
    }

    /// <summary>
    /// Represents a request made to the server to get the server scope info
    /// </summary>
    public class HttpGettingScopeRequestArgs : ProgressArgs
    {
        public HttpGettingScopeRequestArgs(SyncContext context, string host) : base(context, null)
        {
            this.Host = host;
        }

        public override int EventId => HttpClientSyncEventsId.HttpGettingScopeRequest.Id;
        public override string Source => this.Host;
        public override string Message => $"Getting Server Scope. Scope Name:{this.Context.ScopeName}.";
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public string Host { get; }
    }

    public static partial class HttpClientSyncEventsId
    {
        public static EventId HttpGettingScopeRequest => new EventId(20300, nameof(HttpGettingScopeRequest));
        public static EventId HttpGettingScopeResponse => new EventId(20350, nameof(HttpGettingScopeResponse));
    }

    /// <summary>
    /// Partial interceptors extensions 
    /// </summary>
    public static partial class HttpInterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when an http is about to be done to get server scope 
        /// </summary>
        public static Guid OnHttpGettingScopeRequest(this WebRemoteOrchestrator orchestrator, Action<HttpGettingScopeRequestArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http is about to be done to get server scope 
        /// </summary>
        public static Guid OnHttpGettingScopeRequest(this WebRemoteOrchestrator orchestrator, Func<HttpGettingScopeRequestArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http call to get scope is done
        /// </summary>
        public static Guid OnHttpGettingScopeResponse(this WebRemoteOrchestrator orchestrator, Action<HttpGettingScopeResponseArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http call to get scope is done
        /// </summary>
        public static Guid OnHttpGettingScopeResponse(this WebRemoteOrchestrator orchestrator, Func<HttpGettingScopeResponseArgs, Task> action)
            => orchestrator.AddInterceptor(action);


    }
}
