using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Web.Client;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// When Getting response from remote orchestrator
    /// </summary>
    public class HttpGettingResponseMessageArgs : ProgressArgs
    {
        public HttpGettingResponseMessageArgs(HttpResponseMessage response, string uri, HttpStep step, SyncContext context, object data, string host)
            : base(context, null, null)
        {
            this.Response = response;
            this.Uri = uri;
            this.Step = step;
            this.Data = data;
            this.Host = host;
        }
        public override string Source => this.Host;

        public override int EventId => HttpClientSyncEventsId.HttpGettingResponseMessage.Id;
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        public override string Message
            => $"Received a message from {this.Uri}, Step:{this.Step}, StatusCode: {(int)Response.StatusCode}, ReasonPhrase: {Response.ReasonPhrase ?? "<null>"}, Version: {Response.Version}";

        public HttpResponseMessage Response { get; }
        public string Uri { get; }
        public HttpStep Step { get; }
        public object Data { get; }
        public string Host { get; }
    }

    /// <summary>
    /// When sending request 
    /// </summary>
    public class HttpSendingRequestMessageArgs : ProgressArgs
    {
        public HttpSendingRequestMessageArgs(HttpRequestMessage request, SyncContext context, object data, string host)
            : base(context, null, null)
        {
            this.Request = request;
            this.Data = data;
            this.Host = host;
        }
        public override int EventId => HttpClientSyncEventsId.HttpSendingRequestMessage.Id;
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override string Source => this.Host;

        public HttpRequestMessage Request { get; }
        public object Data { get; }
        public string Host { get; }
    }

    /// <summary>
    /// Represents the arguments provided when an HTTP response fails.
    /// </summary>
    /// <remarks>
    /// This class encapsulates the details of a failed HTTP response, including the status code,
    /// reason phrase, error content, headers, and the URI of the original request.
    /// </remarks>
    public class HttpResponseFailureArgs : ProgressArgs
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="HttpResponseFailureArgs"/> class with the specified parameters.
        /// </summary>
        /// <param name="statusCode">The HTTP status code of the failed response.</param>
        /// <param name="reasonPhrase">The reason phrase associated with the HTTP status code.</param>
        /// <param name="errorContent">The content of the response body providing additional details about the error.</param>
        /// <param name="headers">A dictionary containing the HTTP headers associated with the failed response.</param>
        /// <param name="requestUri">The URI of the original request that resulted in the failed response.</param>
        public HttpResponseFailureArgs(int statusCode, string reasonPhrase, string errorContent, IDictionary<string, string> headers, Uri requestUri)
            : base(null, null, null)
        {
            this.StatusCode = statusCode;
            this.ReasonPhrase = reasonPhrase;
            this.ErrorContent = errorContent;
            this.Headers = headers;
            this.RequestUri = requestUri;
        }

        /// <summary>
        /// Gets the HTTP status code of the failed response.
        /// </summary>
        public int StatusCode { get; }

        /// <summary>
        /// Gets the reason phrase associated with the HTTP status code.
        /// </summary>
        public string ReasonPhrase { get; }

        /// <summary>
        /// Gets the content of the response body providing additional details about the error.
        /// </summary>
        public string ErrorContent { get; }

        /// <summary>
        /// Gets the HTTP headers associated with the failed response.
        /// </summary>
        public IDictionary<string, string> Headers { get; }

        /// <summary>
        /// Gets the URI of the original request that resulted in the failed response.
        /// </summary>
        public Uri RequestUri { get; }
    }

    public static partial class HttpClientSyncEventsId
    {
        public static EventId HttpSendingRequestMessage => new EventId(20100, nameof(HttpSendingRequestMessage));
        public static EventId HttpGettingResponseMessage => new EventId(20150, nameof(HttpGettingResponseMessage));
    }

    /// <summary>
    /// Partial Interceptors extensions 
    /// </summary>
    public static partial class HttpInterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when an http request message is sent
        /// </summary>
        public static Guid OnHttpSendingRequest(this WebRemoteOrchestrator orchestrator,
            Action<HttpSendingRequestMessageArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider when an http request message is sent
        /// </summary>
        public static Guid OnHttpSendingRequest(this WebRemoteOrchestrator orchestrator,
            Func<HttpSendingRequestMessageArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http message response is downloaded from remote side
        /// </summary>
        public static Guid OnHttpGettingResponse(this WebRemoteOrchestrator orchestrator,
            Action<HttpGettingResponseMessageArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider when an http message response is downloaded from remote side
        /// </summary>
        public static Guid OnHttpGettingResponse(this WebRemoteOrchestrator orchestrator,
            Func<HttpGettingResponseMessageArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Registers an action to be executed when a remote HTTP request made by the orchestrator fails.
        /// </summary>
        /// <param name="orchestrator">The WebRemoteOrchestrator instance.</param>
        /// <param name="action">The action to execute upon encountering a failed HTTP response.</param>
        /// <returns>The interceptor ID.</returns>
        /// <remarks>
        /// This extension method allows for the convenient registration of actions to handle failed HTTP responses
        /// during remote orchestration. Upon failure, the specified action is invoked with details about the HTTP
        /// response. This can be useful for implementing custom error handling, logging, or retry logic.
        /// </remarks>
        public static Guid OnHttpResponseFailure(this WebRemoteOrchestrator orchestrator,
            Action<HttpResponseFailureArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <inheritdoc cref="OnHttpResponseFailure(WebRemoteOrchestrator, Action{HttpResponseFailureArgs})"/>
        public static Guid OnHttpResponseFailure(this WebRemoteOrchestrator orchestrator,
           Func<HttpGettingResponseMessageArgs, Task> action)
            => orchestrator.AddInterceptor(action);

    }
}
