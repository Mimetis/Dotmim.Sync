using Dotmim.Sync.Web.Client;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace Dotmim.Sync
{


    public class HttpGettingChangesArgs : ProgressArgs
    {
        public HttpGettingChangesArgs(HttpMessageSendChangesResponse response, string host)
            : base(response.SyncContext, null, null)
        {
            this.Response = response;
            this.Host = host;
        }

        public override string Message => $"[{this.Host}] Getting Changes. Index:{this.Response.BatchIndex}. Rows:{this.Response.Changes.RowsCount()}";

        public HttpMessageSendChangesResponse Response { get; }
        public string Host { get; }

        public override int EventId => HttpSyncEventsId.HttpGettingChanges.Id;
    }

    public class HttpSendingChangesArgs : ProgressArgs
    {
        public HttpSendingChangesArgs(HttpMessageSendChangesRequest request, string host)
            : base(request.SyncContext, null, null)
        {
            this.Request = request;
            this.Host = host;
        }

        public HttpMessageSendChangesRequest Request { get; }
        public string Host { get; }
        public override string Message => $"[{this.Host}] Sending Changes. Index:{this.Request.BatchIndex}. Rows:{this.Request.Changes.RowsCount()}";

        public override int EventId => HttpSyncEventsId.HttpSendingChanges.Id;
    }


    public static partial class HttpSyncEventsId
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
        public static void OnHttpSendingChanges(this BaseOrchestrator orchestrator, Action<HttpSendingChangesArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider when an http message is sent
        /// </summary>
        public static void OnHttpSendingChanges(this BaseOrchestrator orchestrator, Func<HttpSendingChangesArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when an http message is downloaded from remote side
        /// </summary>
        public static void OnHttpGettingChanges(this BaseOrchestrator orchestrator, Action<HttpGettingChangesArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider when an http message is downloaded from remote side
        /// </summary>
        public static void OnHttpGettingChanges(this BaseOrchestrator orchestrator, Func<HttpGettingChangesArgs, Task> action)
            => orchestrator.SetInterceptor(action);

    }
}
