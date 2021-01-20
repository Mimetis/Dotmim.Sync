using Dotmim.Sync.Web.Client;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public class HttpGettingSchemaArgs : ProgressArgs
    {
        public HttpGettingSchemaArgs(HttpMessageEnsureSchemaResponse response, string host) : base(response.SyncContext, null)
        {
            this.Response = response;
            this.Host = host;
        }
        public override int EventId => HttpClientSyncEventsId.HttpGettingSchema.Id;
        public override string Message => $"[{this.Host}] Getting Schema. Scope Name:{this.Response.ServerScopeInfo.Name}.";

        public HttpMessageEnsureSchemaResponse Response { get; }
        public string Host { get; }
    }

    public static partial class HttpClientSyncEventsId
    {
        public static EventId HttpGettingSchema => new EventId(20200, nameof(HttpGettingSchema));
    }

    /// <summary>
    /// Partial interceptors extensions 
    /// </summary>
    public static partial class HttpInterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when an http call to get schema is done
        /// </summary>
        public static void OnHttpGettingSchema(this WebClientOrchestrator orchestrator, Action<HttpGettingSchemaArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http call to get schema is done
        /// </summary>
        public static void OnHttpGettingSchema(this WebClientOrchestrator orchestrator, Func<HttpGettingSchemaArgs, Task> action)
            => orchestrator.SetInterceptor(action);

       
    }
}
