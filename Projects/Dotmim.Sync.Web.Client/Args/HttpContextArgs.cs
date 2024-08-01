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
    /// When Getting response from remote orchestrator.
    /// </summary>
    public class HttpGettingResponseMessageArgs : ProgressArgs
    {
        /// <inheritdoc cref="HttpGettingResponseMessageArgs" />
        public HttpGettingResponseMessageArgs(HttpResponseMessage response, Uri uri, HttpStep step, SyncContext context, object data, string host)
            : base(context, null, null)
        {
            this.Response = response;
            this.Uri = uri;
            this.Step = step;
            this.Data = data;
            this.Host = host;
        }

        /// <inheritdoc />
        public override string Source => this.Host;

        /// <inheritdoc />
        public override int EventId => HttpClientSyncEventsId.HttpGettingResponseMessage.Id;

        /// <inheritdoc />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc />
        public override string Message
            => $"Received a message from {this.Uri}, Step:{this.Step}, StatusCode: {(int)this.Response.StatusCode}, ReasonPhrase: {this.Response.ReasonPhrase ?? "<null>"}, Version: {this.Response.Version}";

        /// <summary>
        /// Gets the response message.
        /// </summary>
        public HttpResponseMessage Response { get; }

        /// <summary>
        /// Gets the uri.
        /// </summary>
        public Uri Uri { get; }

        /// <summary>
        /// Gets the step.
        /// </summary>
        public HttpStep Step { get; }

        /// <summary>
        /// Gets the data.
        /// </summary>
        public object Data { get; }

        /// <summary>
        /// Gets the host. (same as this.Source).
        /// </summary>
        public string Host { get; }
    }

    /// <summary>
    /// When sending request.
    /// </summary>
    public class HttpSendingRequestMessageArgs : ProgressArgs
    {
        /// <inheritdoc  cref="HttpSendingRequestMessageArgs"/>
        public HttpSendingRequestMessageArgs(HttpRequestMessage request, SyncContext context, object data, string host)
            : base(context, null, null)
        {
            this.Request = request;
            this.Data = data;
            this.Host = host;
        }

        /// <inheritdoc />
        public override int EventId => HttpClientSyncEventsId.HttpSendingRequestMessage.Id;

        /// <inheritdoc />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc />
        public override string Source => this.Host;

        /// <summary>
        /// Gets the request.
        /// </summary>
        public HttpRequestMessage Request { get; }

        /// <summary>
        /// Gets the data.
        /// </summary>
        public object Data { get; }

        /// <summary>
        /// Gets the host. (Similar to this.Source).
        /// </summary>
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

    /// <summary>
    /// HttpClient Sync Events Id.
    /// </summary>
    public partial class HttpClientSyncEventsId
    {
        /// <summary>
        /// Gets the event id when sending a request message.
        /// </summary>
        public static EventId HttpSendingRequestMessage => new(20100, nameof(HttpSendingRequestMessage));

        /// <summary>
        /// Gets the event id when getting a response message.
        /// </summary>
        public static EventId HttpGettingResponseMessage => new(20150, nameof(HttpGettingResponseMessage));
    }

    /// <summary>
    /// Partial Interceptors extensions.
    /// </summary>
    public partial class HttpInterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when an http request message is sent.
        /// </summary>
        public static Guid OnHttpSendingRequest(
            this WebRemoteOrchestrator orchestrator,
            Action<HttpSendingRequestMessageArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http request message is sent.
        /// </summary>
        public static Guid OnHttpSendingRequest(
            this WebRemoteOrchestrator orchestrator,
            Func<HttpSendingRequestMessageArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http message response is downloaded from remote side.
        /// </summary>
        public static Guid OnHttpGettingResponse(
            this WebRemoteOrchestrator orchestrator,
            Action<HttpGettingResponseMessageArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http message response is downloaded from remote side.
        /// </summary>
        public static Guid OnHttpGettingResponse(
            this WebRemoteOrchestrator orchestrator,
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
        public static Guid OnHttpResponseFailure(
            this WebRemoteOrchestrator orchestrator,
            Action<HttpResponseFailureArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <inheritdoc cref="OnHttpResponseFailure(WebRemoteOrchestrator, Action{HttpResponseFailureArgs})"/>
        public static Guid OnHttpResponseFailure(
            this WebRemoteOrchestrator orchestrator,
            Func<HttpGettingResponseMessageArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}