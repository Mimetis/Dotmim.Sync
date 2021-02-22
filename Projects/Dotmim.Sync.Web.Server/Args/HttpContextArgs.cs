using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// When Getting response from remote orchestrator
    /// </summary>
    public class HttpGettingRequestArgs : ProgressArgs
    {
        public HttpGettingRequestArgs(HttpContext httpContext, SyncContext context, SessionCache sessionCache, HttpStep httpStep)
            : base(context, null, null)
        {
            this.HttpContext = httpContext;
            this.SessionCache = sessionCache;
            this.HttpStep = httpStep;
        }
        public override int EventId => HttpServerSyncEventsId.HttpGettingRequest.Id;

        public HttpContext HttpContext { get; }
        public SessionCache SessionCache { get; }
        public HttpStep HttpStep { get; }
    }

    /// <summary>
    /// When sending request 
    /// </summary>
    public class HttpSendingResponseArgs : ProgressArgs
    {
        public HttpSendingResponseArgs(HttpContext httpContext, SyncContext context, SessionCache sessionCache, 
            byte[] data, HttpStep httpStep)
            : base(context, null, null)
        {
            this.HttpContext = httpContext;
            this.SessionCache = sessionCache;
            this.Data = data;
            this.HttpStep = httpStep;
        }
        public override int EventId => HttpServerSyncEventsId.HttpSendingResponse.Id;

        public HttpContext HttpContext { get; }
        public SessionCache SessionCache { get; }
        public byte[] Data { get; }
        public HttpStep HttpStep { get; }
    }


    public static partial class HttpServerSyncEventsId
    {
        public static EventId HttpGettingRequest => new EventId(30100, nameof(HttpGettingRequest));
        public static EventId HttpSendingResponse => new EventId(30150, nameof(HttpSendingResponse));
    }

    /// <summary>
    /// Partial interceptors extensions 
    /// </summary>
    public static partial class HttpServerInterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when an http response message is sent back to the client
        /// </summary>
        public static void OnHttpSendingResponse(this WebServerOrchestrator orchestrator,
            Action<HttpSendingResponseArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http response message is sent back to the client
        /// </summary>
        public static void OnHttpSendingResponse(this WebServerOrchestrator orchestrator,
            Func<HttpSendingResponseArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http message request from the client arrived to the server
        /// </summary>
        public static void OnHttpGettingRequest(this WebServerOrchestrator orchestrator,
            Action<HttpGettingRequestArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http message request from the client arrived to the server
        /// </summary>
        public static void OnHttpGettingRequest(this WebServerOrchestrator orchestrator,
            Func<HttpGettingRequestArgs, Task> action)
            => orchestrator.SetInterceptor(action);

    }
}
