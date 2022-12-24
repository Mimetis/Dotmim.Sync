﻿using Dotmim.Sync.Enumerations;
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
        public HttpGettingRequestArgs(HttpContext httpContext, SyncContext context, SessionCache sessionCache, object data, Type objectType, HttpStep httpStep)
            : base(context, null, null)
        {
            this.HttpContext = httpContext;
            this.SessionCache = sessionCache;
            this.Data = data;
            this.ObjectType = objectType;
            this.HttpStep = httpStep;
        }
        public override int EventId => HttpServerSyncEventsId.HttpGettingRequest.Id;
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        public HttpContext HttpContext { get; }
        public SessionCache SessionCache { get; }
        public object Data { get; }
        public Type ObjectType { get; }
        public HttpStep HttpStep { get; }
    }

    /// <summary>
    /// When sending request 
    /// </summary>
    public class HttpSendingResponseArgs : ProgressArgs
    {
        public HttpSendingResponseArgs(HttpContext httpContext, SyncContext context, SessionCache sessionCache, 
            object data, Type objectType, HttpStep httpStep)
            : base(context, null, null)
        {
            this.HttpContext = httpContext;
            this.SessionCache = sessionCache;
            this.Data = data;
            this.ObjectType = objectType;
            this.HttpStep = httpStep;
        }
        public override int EventId => HttpServerSyncEventsId.HttpSendingResponse.Id;
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public HttpContext HttpContext { get; }
        public SessionCache SessionCache { get; }
        public object Data { get; }
        public Type ObjectType { get; }
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
        public static Guid OnHttpSendingResponse(this WebServerAgent webServerAgent,
            Action<HttpSendingResponseArgs> action)
            => webServerAgent.RemoteOrchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http response message is sent back to the client
        /// </summary>
        public static Guid OnHttpSendingResponse(this WebServerAgent webServerAgent,
            Func<HttpSendingResponseArgs, Task> action)
            => webServerAgent.RemoteOrchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http message request from the client arrived to the server
        /// </summary>
        public static Guid OnHttpGettingRequest(this WebServerAgent webServerAgent,
            Action<HttpGettingRequestArgs> action)
            => webServerAgent.RemoteOrchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http message request from the client arrived to the server
        /// </summary>
        public static Guid OnHttpGettingRequest(this WebServerAgent webServerAgent,
            Func<HttpGettingRequestArgs, Task> action)
            => webServerAgent.RemoteOrchestrator.AddInterceptor(action);

    }
}
