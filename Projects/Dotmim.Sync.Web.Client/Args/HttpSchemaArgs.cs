using Dotmim.Sync.Web.Client;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Represents a request made to the server to get the server scope info
    /// </summary>
    public class HttpGettingSchemaRequestArgs : ProgressArgs
    {
        public HttpGettingSchemaRequestArgs(SyncContext context, string host) : base(context, null)
        {
            this.Host = host;
        }

        public override string Source => this.Host;
        public override int EventId => HttpClientSyncEventsId.HttpGettingSchemaRequest.Id;
        public override string Message => $"Getting Server Schema. Scope Name:{this.Context.ScopeName}.";

        public string Host { get; }
    }
    public class HttpGettingSchemaResponseArgs : ProgressArgs
    {
        public HttpGettingSchemaResponseArgs(ServerScopeInfo serverScopeInfo, SyncSet schema, SyncContext context, string host) : base(context, null)
        {
            this.ServerScopeInfo = serverScopeInfo;
            this.Schema = schema;
            this.Host = host;
        }
        public override string Source => this.Host;
        public override int EventId => HttpClientSyncEventsId.HttpGettingSchemaResponse.Id;
        public override string Message => $"Received Schema From Server. Tables Count:{this.Schema.Tables.Count}.";

        public SyncSet Schema { get; set; }

        public ServerScopeInfo ServerScopeInfo { get; set; }

        public string Host { get; }
    }

    public static partial class HttpClientSyncEventsId
    {
        public static EventId HttpGettingSchemaRequest => new EventId(20200, nameof(HttpGettingSchemaRequest));
        public static EventId HttpGettingSchemaResponse => new EventId(20250, nameof(HttpGettingSchemaResponse));
    }

    /// <summary>
    /// Partial interceptors extensions 
    /// </summary>
    public static partial class HttpInterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when an http call is about to be made to get server schema
        /// </summary>
        public static void OnHttpGettingSchemaRequest(this WebClientOrchestrator orchestrator, Action<HttpGettingSchemaResponseArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http call is about to be made to get server schema
        /// </summary>
        public static void OnHttpGettingSchemaRequest(this WebClientOrchestrator orchestrator, Func<HttpGettingSchemaResponseArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http call to get schema is done
        /// </summary>
        public static void OnHttpGettingSchemaResponse(this WebClientOrchestrator orchestrator, Action<HttpGettingSchemaResponseArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http call to get schema is done
        /// </summary>
        public static void OnHttpGettingSchemaResponse(this WebClientOrchestrator orchestrator, Func<HttpGettingSchemaResponseArgs, Task> action)
            => orchestrator.SetInterceptor(action);


    }
}
