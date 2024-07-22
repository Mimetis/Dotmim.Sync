using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    public class HttpGettingClientChangesArgs : ProgressArgs
    {
        public HttpGettingClientChangesArgs(HttpMessageSendChangesRequest request, string host, SessionCache sessionCache)
            : base(request.SyncContext, null, null)
        {
            this.Request = request;
            this.Host = host;
            this.SessionCache = sessionCache;
        }

        public override string Source => this.Host;
        public override string Message
        {
            get
            {
                if (this.Request.BatchCount == 0 && this.Request.BatchIndex == 0)
                    return $"Getting All Changes. Rows:{this.Request.Changes.RowsCount()}";
                else
                    return $"Getting Batch Changes. ({this.Request.BatchIndex + 1}/{this.Request.BatchCount}). Rows:{this.Request.Changes.RowsCount()}";
            }
        }

        public HttpMessageSendChangesRequest Request { get; }
        public string Host { get; }
        public SessionCache SessionCache { get; }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override int EventId => HttpServerSyncEventsId.HttpGettingChanges.Id;
    }

    public class HttpSendingServerChangesArgs : ProgressArgs
    {
        public HttpSendingServerChangesArgs(HttpMessageSendChangesResponse response, string host, SessionCache sessionCache, bool isSnapshot)
            : base(response.SyncContext, null, null)
        {
            this.Response = response;
            this.Host = host;
            this.SessionCache = sessionCache;
            this.IsSnapshot = isSnapshot;
        }

        public HttpMessageSendChangesResponse Response { get; }
        public string Host { get; }
        public SessionCache SessionCache { get; }
        public bool IsSnapshot { get; }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override string Source => this.Host;
        public override string Message
        {
            get
            {
                var rowsCount = this.Response.Changes == null ? 0 : this.Response.Changes.RowsCount();
                var changesString = IsSnapshot ? "Snapshot" : ""; 

                if (this.Response.BatchCount == 0 && this.Response.BatchIndex == 0)
                    return $"Sending All {changesString} Changes. Rows:{rowsCount}";
                else
                    return $"Sending Batch {changesString} Changes. ({this.Response.BatchIndex + 1}/{this.Response.BatchCount}). Rows:{rowsCount}";
            }
        }

        public override int EventId => HttpServerSyncEventsId.HttpSendingChanges.Id;
    }


    public static partial class HttpServerSyncEventsId
    {
        public static EventId HttpSendingChanges => new EventId(30000, nameof(HttpSendingChanges));
        public static EventId HttpGettingChanges => new EventId(30050, nameof(HttpGettingChanges));
    }

    /// <summary>
    /// Partial Interceptors extensions 
    /// </summary>
    public static partial class HttpServerInterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when an http message is sent
        /// </summary>
        public static Guid OnHttpSendingChanges(this WebServerAgent webServerAgent,
            Action<HttpSendingServerChangesArgs> action)
            => webServerAgent.RemoteOrchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider when an http message is sent
        /// </summary>
        public static Guid OnHttpSendingChanges(this WebServerAgent webServerAgent,
            Func<HttpSendingServerChangesArgs, Task> action)
            => webServerAgent.RemoteOrchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http message is downloaded from remote side
        /// </summary>
        public static Guid OnHttpGettingChanges(this WebServerAgent webServerAgent,
            Action<HttpGettingClientChangesArgs> action)
            => webServerAgent.RemoteOrchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider when an http message is downloaded from remote side
        /// </summary>
        public static Guid OnHttpGettingChanges(this WebServerAgent webServerAgent,
            Func<HttpGettingClientChangesArgs, Task> action)
            => webServerAgent.RemoteOrchestrator.AddInterceptor(action);

    }
}
