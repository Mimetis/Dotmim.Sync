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
    public class HttpBatchesDownloadingArgs : ProgressArgs
    {
        /// <inheritdoc cref="HttpBatchesDownloadingArgs"/>
        public HttpBatchesDownloadingArgs(SyncContext context, BatchInfo serverBatchInfo, string host)
            : base(context, null)
        {
            this.ServerBatchInfo = serverBatchInfo;
            this.Host = host;
        }

        /// <inheritdoc />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc />
        public override string Source => this.Host;

        /// <inheritdoc />
        public override int EventId => HttpClientSyncEventsId.HttpGettingSchemaRequest.Id;

        /// <inheritdoc />
        public override string Message => $"Downloading Batches. Scope Name:{this.Context.ScopeName}. Batches Count:{this.ServerBatchInfo.BatchPartsInfo?.Count ?? 1}. Rows Count:{this.ServerBatchInfo.RowsCount}";

        /// <summary>
        /// Gets the server batch info.
        /// </summary>
        public BatchInfo ServerBatchInfo { get; }

        /// <summary>
        /// Gets the host.
        /// </summary>
        public string Host { get; }
    }

    /// <summary>
    /// Represents a request sent be send to the server to get batches .
    /// </summary>
    public class HttpBatchesDownloadedArgs : ProgressArgs
    {
        /// <inheritdoc cref="HttpBatchesDownloadingArgs"/>
        public HttpBatchesDownloadedArgs(HttpMessageSummaryResponse httpSummary, SyncContext context, string host)
            : base(context, null)
        {
            this.HttpSummary = httpSummary;
            this.Host = host;
        }

        /// <inheritdoc />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Information;

        /// <inheritdoc />
        public override string Source => this.Host;

        /// <inheritdoc />
        public override int EventId => HttpClientSyncEventsId.HttpGettingSchemaResponse.Id;

        /// <inheritdoc />
        public override string Message
        {
            get
            {
                var batchCount = this.HttpSummary.BatchInfo?.BatchPartsInfo?.Count ?? 1;
                var totalRows = this.HttpSummary.ServerChangesSelected?.TotalChangesSelected ?? 0;

                return $"Downloaded batches count: {batchCount}. Total Rows: {totalRows}.";
            }
        }

        /// <summary>
        /// Gets the http summary.
        /// </summary>
        public HttpMessageSummaryResponse HttpSummary { get; }

        /// <summary>
        /// Gets the host.
        /// </summary>
        public string Host { get; }
    }

    /// <summary>
    /// Partial class to add interceptor logic.
    /// </summary>
    public partial class HttpClientSyncEventsId
    {

        /// <summary>
        /// Gets the event id for HttpBatchesDownloadingArgs.
        /// </summary>
        public static EventId HttpBatchesDownloadingArgs => new(20600, nameof(HttpBatchesDownloadingArgs));

        /// <summary>
        /// Gets the event id for HttpBatchesDownloadedArgs.
        /// </summary>
        public static EventId HttpBatchesDownloadedArgs => new(20650, nameof(HttpBatchesDownloadedArgs));
    }

    /// <summary>
    /// Partial Interceptors extensions.
    /// </summary>
    public partial class HttpInterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when batches are about to be downloaded.
        /// </summary>
        public static Guid OnHttpBatchesDownloadingArgs(this WebRemoteOrchestrator orchestrator, Action<HttpBatchesDownloadingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when batches are about to be downloaded.
        /// </summary>
        public static Guid OnHttpBatchesDownloadingArgs(this WebRemoteOrchestrator orchestrator, Func<HttpBatchesDownloadingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when batches have been completely downloaded.
        /// </summary>
        public static Guid OnHttpBatchesDownloadedArgs(this WebRemoteOrchestrator orchestrator, Action<HttpBatchesDownloadedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when batches have been completely downloaded.
        /// </summary>
        public static Guid OnHttpBatchesDownloadedArgs(this WebRemoteOrchestrator orchestrator, Func<HttpBatchesDownloadedArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}