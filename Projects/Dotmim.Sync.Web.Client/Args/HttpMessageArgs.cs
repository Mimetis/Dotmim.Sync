using Dotmim.Sync.Web.Client;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    public class HttpGettingServerChangesArgs : ProgressArgs
    {
        public HttpGettingServerChangesArgs(HttpMessageSendChangesResponse response, string host)
            : base(response.SyncContext, null, null)
        {
            this.Response = response;
            this.Host = host;
        }

        public override string Message
        {
            get
            {
                if (this.Response.BatchCount == 0 && this.Response.BatchIndex == 0)
                    return $"[{this.Host}] Getting All Changes. Rows:{this.Response.Changes.RowsCount()}";
                else
                    return $"[{this.Host}] Getting Batch Changes. ({this.Response.BatchIndex + 1}/{this.Response.BatchCount}). Rows:{this.Response.Changes.RowsCount()}";
            }
        }

        public HttpMessageSendChangesResponse Response { get; }
        public string Host { get; }

        public override int EventId => HttpClientSyncEventsId.HttpGettingChanges.Id;
    }

    public class HttpSendingClientChangesArgs : ProgressArgs
    {
        public HttpSendingClientChangesArgs(HttpMessageSendChangesRequest request, string host)
            : base(request.SyncContext, null, null)
        {
            this.Request = request;
            this.Host = host;
        }

        public HttpMessageSendChangesRequest Request { get; }
        public string Host { get; }
        public override string Message
        {
            get
            {
                if (this.Request.BatchCount == 0 && this.Request.BatchIndex == 0)
                    return $"[{this.Host}] Sending All Changes. Rows:{this.Request.Changes.RowsCount()}";
                else
                    return $"[{this.Host}] Sending Batch Changes. ({this.Request.BatchIndex + 1}/{this.Request.BatchCount}). Rows:{this.Request.Changes.RowsCount()}";
            }
        }

        public override int EventId => HttpClientSyncEventsId.HttpSendingChanges.Id;
    }


    public static partial class HttpClientSyncEventsId
    {
        public static EventId HttpSendingChanges => new EventId(20000, nameof(HttpSendingChanges));
        public static EventId HttpGettingChanges => new EventId(20050, nameof(HttpGettingChanges));
    }

    /// <summary>
    /// Partial interceptors extensions 
    /// </summary>
    public static partial class HttpInterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when an http message is sent
        /// </summary>
        public static void OnHttpSendingChanges(this WebClientOrchestrator orchestrator, Action<HttpSendingClientChangesArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider when an http message is sent
        /// </summary>
        public static void OnHttpSendingChanges(this WebClientOrchestrator orchestrator, Func<HttpSendingClientChangesArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http message is downloaded from remote side
        /// </summary>
        public static void OnHttpGettingChanges(this WebClientOrchestrator orchestrator, Action<HttpGettingServerChangesArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider when an http message is downloaded from remote side
        /// </summary>
        public static void OnHttpGettingChanges(this WebClientOrchestrator orchestrator, Func<HttpGettingServerChangesArgs, Task> action)
            => orchestrator.SetInterceptor(action);

    }
}
