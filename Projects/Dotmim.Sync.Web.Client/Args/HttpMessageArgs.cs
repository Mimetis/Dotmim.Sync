using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Web.Client;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Represents a request about to be send to the server to get batches .
    /// </summary>
    public class HttpGettingServerChangesRequestArgs : ProgressArgs
    {
        /// <inheritdoc cref="HttpGettingServerChangesRequestArgs" />
        public HttpGettingServerChangesRequestArgs(int batchIndexRequested, int batchCount, SyncContext context, string host)
             : base(context, null, null)
        {
            this.BatchIndexRequested = batchIndexRequested;
            this.BatchCount = batchCount;

            this.Host = host;
        }

        /// <inheritdoc />
        public override string Source => this.Host;

        /// <inheritdoc />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc />
        public override string Message
            => this.BatchCount <= 1
                    ? $"Getting Batch Changes. (1)"
                    : $"Getting Batch Changes. ({this.BatchIndexRequested + 1}/{this.BatchCount}).";

        /// <summary>
        /// Gets or sets the batch index that is asked to be retrieved from the server.
        /// </summary>
        public int BatchIndexRequested { get; set; }

        /// <summary>
        /// Gets or sets the batch count to be received from server.
        /// </summary>
        public int BatchCount { get; set; }

        /// <summary>
        /// Gets the host.
        /// </summary>
        public string Host { get; }

        /// <inheritdoc />
        public override int EventId => HttpClientSyncEventsId.HttpGettingChangesRequest.Id;
    }

    /// <summary>
    /// Represents a response received from the server after getting batches.
    /// </summary>
    public class HttpGettingServerChangesResponseArgs : ProgressArgs
    {

        /// <inheritdoc cref="HttpGettingServerChangesResponseArgs" />
        public HttpGettingServerChangesResponseArgs(BatchInfo batchInfo, int batchIndex, int batchRowsCount, SyncContext syncContext, string host)
            : base(syncContext, null, null)
        {
            this.BatchInfo = batchInfo;
            this.BatchIndex = batchIndex;
            this.BatchRowsCount = batchRowsCount;
            this.Host = host;
        }

        /// <inheritdoc />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Information;

        /// <inheritdoc />
        public override string Source => this.Host;

        /// <inheritdoc />
        public override string Message
        {
            get
            {
                var batchesCount = this.BatchInfo.BatchPartsInfo?.Count ?? 1;
                return $"Downloaded Batch Changes. ({this.BatchIndex + 1}/{batchesCount}). Rows:({this.BatchRowsCount}/{this.BatchInfo.RowsCount}).";
            }
        }

        /// <summary>
        /// Gets the batch info.
        /// </summary>
        public BatchInfo BatchInfo { get; }

        /// <summary>
        /// Gets the batch index.
        /// </summary>
        public int BatchIndex { get; }

        /// <summary>
        /// Gets the batch rows count.
        /// </summary>
        public int BatchRowsCount { get; }

        /// <summary>
        /// Gets the host.
        /// </summary>
        public string Host { get; }

        /// <inheritdoc />
        public override int EventId => HttpClientSyncEventsId.HttpGettingChangesResponse.Id;
    }

    /// <summary>
    /// Represents the arguments provided when an HTTP request is about to be sent to the server.
    /// </summary>
    public class HttpSendingClientChangesRequestArgs : ProgressArgs
    {
        /// <inheritdoc cref="HttpSendingClientChangesRequestArgs" />
        public HttpSendingClientChangesRequestArgs(HttpMessageSendChangesRequest request, int rowsCount, int totalRowsCount, string host)
            : base(request.SyncContext, null, null)
        {
            this.Request = request;
            this.RowsCount = rowsCount;
            this.TotalRowsCount = totalRowsCount;
            this.Host = host;
        }

        /// <inheritdoc />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <summary>
        /// Gets the request.
        /// </summary>
        public HttpMessageSendChangesRequest Request { get; }

        /// <summary>
        /// Gets the host.
        /// </summary>
        public string Host { get; }

        /// <inheritdoc />
        public override string Source => this.Host;

        /// <inheritdoc />
        public override string Message
            => this.Request.BatchCount == 0 && this.Request.BatchIndex == 0
                    ? $"Sending All Changes. Rows:{this.RowsCount}. Waiting Server Response..."
                    : $"Sending Batch Changes. Batches: ({this.Request.BatchIndex + 1}/{this.Request.BatchCount}). Rows: ({this.RowsCount}/{this.TotalRowsCount}). Waiting Server Response...";

        /// <summary>
        /// Gets or Sets the rows count sended.
        /// </summary>
        public int RowsCount { get; set; }

        /// <summary>
        /// Gets or Sets the total tables rows count to send.
        /// </summary>
        public int TotalRowsCount { get; set; }

        /// <inheritdoc />
        public override int EventId => HttpClientSyncEventsId.HttpSendingChangesRequest.Id;
    }

    /// <summary>
    /// HttpClient Sync Events Id.
    /// </summary>
    public static partial class HttpClientSyncEventsId
    {
        /// <summary>
        /// Gets the event id for HttpSendingChangesRequest.
        /// </summary>
        public static EventId HttpSendingChangesRequest => new(20000, nameof(HttpSendingChangesRequest));

        /// <summary>
        /// Gets the event id for HttpGettingChangesRequest.
        /// </summary>
        public static EventId HttpGettingChangesRequest => new(20100, nameof(HttpGettingChangesRequest));

        /// <summary>
        /// Gets the event id for HttpGettingChangesResponse.
        /// </summary>
        public static EventId HttpGettingChangesResponse => new(20150, nameof(HttpGettingChangesResponse));
    }

    /// <summary>
    /// Partial Interceptors extensions.
    /// </summary>
    public partial class HttpInterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when batch changes is uploading to server.
        /// </summary>
        public static Guid OnHttpSendingChangesRequest(this WebRemoteOrchestrator orchestrator, Action<HttpSendingClientChangesRequestArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when batch changes is uploading to server.
        /// </summary>
        public static Guid OnHttpSendingChangesRequest(this WebRemoteOrchestrator orchestrator, Func<HttpSendingClientChangesRequestArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when downloading a batch changes from server side.
        /// </summary>
        public static Guid OnHttpGettingChangesRequest(this WebRemoteOrchestrator orchestrator, Action<HttpGettingServerChangesRequestArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when downloading a batch changes from server side.
        /// </summary>
        public static Guid OnHttpGettingChangesRequest(this WebRemoteOrchestrator orchestrator, Func<HttpGettingServerChangesRequestArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a batch changes has been downloaded from server side.
        /// </summary>
        public static Guid OnHttpGettingChangesResponse(this WebRemoteOrchestrator orchestrator, Action<HttpGettingServerChangesResponseArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a batch changes has been downloaded from server side.
        /// </summary>
        public static Guid OnHttpGettingChangesResponse(this WebRemoteOrchestrator orchestrator, Func<HttpGettingServerChangesResponseArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}