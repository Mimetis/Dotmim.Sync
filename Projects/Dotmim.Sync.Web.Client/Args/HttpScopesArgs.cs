using Dotmim.Sync.Web.Client;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    public class HttpGettingScopeArgs : ProgressArgs
    {
        public HttpGettingScopeArgs(HttpMessageEnsureScopesResponse response, string host) : base(response.SyncContext, null)
        {
            this.Response = response;
            this.Host = host;
        }

        public override int EventId => HttpClientSyncEventsId.HttpGettingScope.Id;
        public override string Message => $"[{this.Host}] Getting Scope. Scope Name:{this.Response.ServerScopeInfo.Name}.";

        public HttpMessageEnsureScopesResponse Response { get; }
        public string Host { get; }
    }

    public static partial class HttpClientSyncEventsId
    {
        public static EventId HttpGettingScope => new EventId(20300, nameof(HttpGettingScope));
    }

    /// <summary>
    /// Partial interceptors extensions 
    /// </summary>
    public static partial class HttpInterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when an http call to get scope is done
        /// </summary>
        public static void OnHttpGettingScope(this WebClientOrchestrator orchestrator, Action<HttpGettingScopeArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http call to get scope is done
        /// </summary>
        public static void OnHttpGettingScope(this WebClientOrchestrator orchestrator, Func<HttpGettingScopeArgs, Task> action)
            => orchestrator.SetInterceptor(action);


    }
}
