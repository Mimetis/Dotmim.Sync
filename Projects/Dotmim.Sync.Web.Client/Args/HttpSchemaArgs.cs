using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Web.Client;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Represents a request made to the server to get the server scope info.
    /// </summary>
    public class HttpGettingSchemaRequestArgs : ProgressArgs
    {
        /// <inheritdoc cref="HttpGettingSchemaRequestArgs" />
        public HttpGettingSchemaRequestArgs(SyncContext context, string host)
            : base(context, null) => this.Host = host;

        /// <inheritdoc />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc />
        public override string Source => this.Host;

        /// <inheritdoc />
        public override int EventId => HttpClientSyncEventsId.HttpGettingSchemaRequest.Id;

        /// <inheritdoc />
        public override string Message => $"Getting Server Schema. Scope Name:{this.Context.ScopeName}.";

        /// <summary>
        /// Gets the host.
        /// </summary>
        public string Host { get; }
    }

    /// <summary>
    /// Represents a response received from the server after getting the server scope info.
    /// </summary>
    public class HttpGettingSchemaResponseArgs : ProgressArgs
    {
        /// <inheritdoc cref="HttpGettingSchemaResponseArgs" />
        public HttpGettingSchemaResponseArgs(ScopeInfo sScopeInfo, SyncSet schema, SyncContext context, string host)
            : base(context, null)
        {
            this.ScopeInfoFromServer = sScopeInfo;
            this.Schema = schema;
            this.Host = host;
        }

        /// <inheritdoc />
        public override string Source => this.Host;

        /// <inheritdoc />
        public override int EventId => HttpClientSyncEventsId.HttpGettingSchemaResponse.Id;

        /// <inheritdoc />
        public override string Message => $"Received Schema From Server. Tables Count:{this.Schema.Tables.Count}.";

        /// <summary>
        /// Gets or sets the schema.
        /// </summary>
        public SyncSet Schema { get; set; }

        /// <inheritdoc />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <summary>
        /// Gets or sets the scope info from server.
        /// </summary>
        public ScopeInfo ScopeInfoFromServer { get; set; }

        /// <summary>
        /// Gets the host.
        /// </summary>
        public string Host { get; }
    }

    /// <summary>
    /// Http Client Sync Events Id.
    /// </summary>
    public partial class HttpClientSyncEventsId
    {
        /// <summary>
        /// Gets the event id for HttpGettingSchemaRequest.
        /// </summary>
        public static EventId HttpGettingSchemaRequest => new(20200, nameof(HttpGettingSchemaRequest));

        /// <summary>
        /// Gets the event id for HttpGettingSchemaResponse.
        /// </summary>
        public static EventId HttpGettingSchemaResponse => new(20250, nameof(HttpGettingSchemaResponse));
    }

    /// <summary>
    /// Partial Interceptors extensions.
    /// </summary>
    public partial class HttpInterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when an http call is about to be made to get server schema.
        /// </summary>
        public static Guid OnHttpGettingSchemaRequest(this WebRemoteOrchestrator orchestrator, Action<HttpGettingSchemaRequestArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http call is about to be made to get server schema.
        /// </summary>
        public static Guid OnHttpGettingSchemaRequest(this WebRemoteOrchestrator orchestrator, Func<HttpGettingSchemaRequestArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http call to get schema is done.
        /// </summary>
        public static Guid OnHttpGettingSchemaResponse(this WebRemoteOrchestrator orchestrator, Action<HttpGettingSchemaResponseArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http call to get schema is done.
        /// </summary>
        public static Guid OnHttpGettingSchemaResponse(this WebRemoteOrchestrator orchestrator, Func<HttpGettingSchemaResponseArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}