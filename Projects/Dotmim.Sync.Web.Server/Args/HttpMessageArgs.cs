using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Http getting request event args.
    /// </summary>
    public class HttpGettingClientChangesArgs : ProgressArgs
    {
        /// <inheritdoc cref="HttpGettingClientChangesArgs"/>
        public HttpGettingClientChangesArgs(HttpMessageSendChangesRequest request, string host, SessionCache sessionCache)
            : base(request.SyncContext, null, null)
        {
            this.Request = request;
            this.Host = host;
            this.SessionCache = sessionCache;
        }

        /// <inheritdoc />
        public override string Source => this.Host;

        /// <inheritdoc />
        public override string Message => this.Request.BatchCount == 0 && this.Request.BatchIndex == 0
                    ? $"Getting All Changes. Rows:{this.Request.Changes.RowsCount()}"
                    : $"Getting Batch Changes. ({this.Request.BatchIndex + 1}/{this.Request.BatchCount}). Rows:{this.Request.Changes.RowsCount()}";

        /// <summary>
        /// Gets the intercepted request.
        /// </summary>
        public HttpMessageSendChangesRequest Request { get; }

        /// <summary>
        /// Gets the host.
        /// </summary>
        public string Host { get; }

        /// <summary>
        /// Gets the session cache.
        /// </summary>
        public SessionCache SessionCache { get; }

        /// <inheritdoc />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc />
        public override int EventId => HttpServerSyncEventsId.HttpGettingChanges.Id;
    }

    /// <summary>
    /// Http sending changes event args.
    /// </summary>
    public class HttpSendingServerChangesArgs : ProgressArgs
    {
        /// <inheritdoc cref="HttpSendingServerChangesArgs"/>
        public HttpSendingServerChangesArgs(HttpMessageSendChangesResponse response, string host, SessionCache sessionCache, bool isSnapshot)
            : base(response.SyncContext, null, null)
        {
            this.Response = response;
            this.Host = host;
            this.SessionCache = sessionCache;
            this.IsSnapshot = isSnapshot;
        }

        /// <summary>
        /// Gets the intercepted response.
        /// </summary>
        public HttpMessageSendChangesResponse Response { get; }

        /// <summary>
        /// Gets the host.
        /// </summary>
        public string Host { get; }

        /// <summary>
        /// Gets the session cache.
        /// </summary>
        public SessionCache SessionCache { get; }

        /// <summary>
        /// Gets a value indicating whether the changes are a snapshot.
        /// </summary>
        public bool IsSnapshot { get; }

        /// <inheritdoc />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc />
        public override string Source => this.Host;

        /// <inheritdoc />
        public override string Message
        {
            get
            {
                var rowsCount = this.Response.Changes == null ? 0 : this.Response.Changes.RowsCount();
                var changesString = this.IsSnapshot ? "Snapshot" : string.Empty;

                return this.Response.BatchCount == 0 && this.Response.BatchIndex == 0
                    ? $"Sending All {changesString} Changes. Rows:{rowsCount}"
                    : $"Sending Batch {changesString} Changes. ({this.Response.BatchIndex + 1}/{this.Response.BatchCount}). Rows:{rowsCount}";
            }
        }

        /// <inheritdoc />
        public override int EventId => HttpServerSyncEventsId.HttpSendingChanges.Id;
    }

    /// <summary>
    /// Http sending response event args.
    /// </summary>
    public partial class HttpServerSyncEventsId
    {
        /// <summary>
        /// Gets the event id for HttpSendingResponseArgs.
        /// </summary>
        public static EventId HttpSendingChanges => new(30000, nameof(HttpSendingChanges));

        /// <summary>
        /// Gets the event id for HttpGettingRequestArgs.
        /// </summary>
        public static EventId HttpGettingChanges => new(30050, nameof(HttpGettingChanges));
    }

    /// <summary>
    /// Partial Interceptors extensions.
    /// </summary>
    public partial class HttpServerInterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when an http message is sent.
        /// </summary>
        public static Guid OnHttpSendingChanges(
            this WebServerAgent webServerAgent,
            Action<HttpSendingServerChangesArgs> action)
            => webServerAgent.RemoteOrchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http message is sent.
        /// </summary>
        public static Guid OnHttpSendingChanges(
            this WebServerAgent webServerAgent,
            Func<HttpSendingServerChangesArgs, Task> action)
            => webServerAgent.RemoteOrchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http message is downloaded from remote side.
        /// </summary>
        public static Guid OnHttpGettingChanges(
            this WebServerAgent webServerAgent,
            Action<HttpGettingClientChangesArgs> action)
            => webServerAgent.RemoteOrchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http message is downloaded from remote side.
        /// </summary>
        public static Guid OnHttpGettingChanges(
            this WebServerAgent webServerAgent,
            Func<HttpGettingClientChangesArgs, Task> action)
            => webServerAgent.RemoteOrchestrator.AddInterceptor(action);
    }
}