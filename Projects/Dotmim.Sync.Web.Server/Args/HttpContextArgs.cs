using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Http getting request event args.
    /// </summary>
    public class HttpGettingRequestArgs : ProgressArgs
    {
        /// <inheritdoc cref="HttpGettingRequestArgs"/>
        public HttpGettingRequestArgs(HttpContext httpContext, SyncContext context, SessionCache sessionCache, object data, Type requestType, HttpStep httpStep)
            : base(context, null, null)
        {
            this.HttpContext = httpContext;
            this.SessionCache = sessionCache;
            this.Data = data;
            this.RequestType = requestType;
            this.HttpStep = httpStep;
        }

        /// <inheritdoc />
        public override int EventId => HttpServerSyncEventsId.HttpGettingRequest.Id;

        /// <inheritdoc />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <summary>
        /// Gets the http context.
        /// </summary>
        public HttpContext HttpContext { get; }

        /// <summary>
        /// Gets the session cache.
        /// </summary>
        public SessionCache SessionCache { get; }

        /// <summary>
        /// Gets the data.
        /// </summary>
        public object Data { get; }

        /// <summary>
        /// Gets the request type that is being processed.
        /// </summary>
        public Type RequestType { get; }

        /// <summary>
        /// Gets the http step.
        /// </summary>
        public HttpStep HttpStep { get; }
    }

    /// <summary>
    /// Http sending response event args.
    /// </summary>
    public class HttpSendingResponseArgs : ProgressArgs
    {
        /// <inheritdoc cref="HttpSendingResponseArgs"/>
        public HttpSendingResponseArgs(HttpContext httpContext, SyncContext context, SessionCache sessionCache,
            object data, Type responseType, HttpStep httpStep)
            : base(context, null, null)
        {
            this.HttpContext = httpContext;
            this.SessionCache = sessionCache;
            this.Data = data;
            this.ResponseType = responseType;
            this.HttpStep = httpStep;
        }

        /// <inheritdoc />
        public override int EventId => HttpServerSyncEventsId.HttpSendingResponse.Id;

        /// <inheritdoc />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <summary>
        /// Gets the http context.
        /// </summary>
        public HttpContext HttpContext { get; }

        /// <summary>
        /// Gets the session cache.
        /// </summary>
        public SessionCache SessionCache { get; }

        /// <summary>
        /// Gets the data.
        /// </summary>
        public object Data { get; }

        /// <summary>
        /// Gets the response type abvout to be sent back to the client.
        /// </summary>
        public Type ResponseType { get; }

        /// <summary>
        /// Gets the http step.
        /// </summary>
        public HttpStep HttpStep { get; }
    }

    /// <summary>
    /// Http server sync events id.
    /// </summary>
    public partial class HttpServerSyncEventsId
    {
        /// <summary>
        /// Gets the event id for HttpGettingRequestArgs.
        /// </summary>
        public static EventId HttpGettingRequest => new(30100, nameof(HttpGettingRequest));

        /// <summary>
        /// Gets the event id for HttpSendingResponseArgs.
        /// </summary>
        public static EventId HttpSendingResponse => new(30150, nameof(HttpSendingResponse));
    }

    /// <summary>
    /// Partial Interceptors extensions.
    /// </summary>
    public partial class HttpServerInterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when an http response message is sent back to the client.
        /// </summary>
        public static Guid OnHttpSendingResponse(
            this WebServerAgent webServerAgent,
            Action<HttpSendingResponseArgs> action)
            => webServerAgent.RemoteOrchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http response message is sent back to the client.
        /// </summary>
        public static Guid OnHttpSendingResponse(
            this WebServerAgent webServerAgent,
            Func<HttpSendingResponseArgs, Task> action)
            => webServerAgent.RemoteOrchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http message request from the client arrived to the server.
        /// </summary>
        public static Guid OnHttpGettingRequest(
            this WebServerAgent webServerAgent,
            Action<HttpGettingRequestArgs> action)
            => webServerAgent.RemoteOrchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http message request from the client arrived to the server.
        /// </summary>
        public static Guid OnHttpGettingRequest(
            this WebServerAgent webServerAgent,
            Func<HttpGettingRequestArgs, Task> action)
            => webServerAgent.RemoteOrchestrator.AddInterceptor(action);
    }
}