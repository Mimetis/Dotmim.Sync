using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Web.Client;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Represents a request made to the server to get the server scope info
    /// </summary>
    public class HttpBatchesDownloadingArgs : ProgressArgs
    {
        public HttpBatchesDownloadingArgs(SyncContext context, BatchInfo serverBatchInfo, string host) : base(context, null)
        {
            this.ServerBatchInfo = serverBatchInfo;
            this.Host = host;
        }

        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override string Source => this.Host;
        public override int EventId => HttpClientSyncEventsId.HttpGettingSchemaRequest.Id;
        public override string Message => $"Downloading Batches. Scope Name:{this.Context.ScopeName}. Batches Count:{this.ServerBatchInfo.BatchPartsInfo?.Count ?? 1}. Rows Count:{this.ServerBatchInfo.RowsCount}";
        public BatchInfo ServerBatchInfo { get; }
        public string Host { get; }
    }
    public class HttpBatchesDownloadedArgs : ProgressArgs
    {
        public HttpBatchesDownloadedArgs(HttpMessageSummaryResponse httpSummary, SyncContext context, string host) : base(context, null)
        {
            this.HttpSummary = httpSummary;
            this.Host = host;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Information;
        public override string Source => this.Host;
        public override int EventId => HttpClientSyncEventsId.HttpGettingSchemaResponse.Id;
        public override string Message
        {
            get
            {
                var batchCount = this.HttpSummary.BatchInfo?.BatchPartsInfo?.Count ?? 1;
                var totalRows = this.HttpSummary.ServerChangesSelected?.TotalChangesSelected ?? 0;

                return $"Snapshot Downloaded. Batches Count: {batchCount}. Total Rows: {totalRows}.";
            }
        }

        public HttpMessageSummaryResponse HttpSummary { get; }
        
        public string Host { get; }
    }

    public static partial class HttpClientSyncEventsId
    {
        public static EventId HttpBatchesDownloadingArgs => new EventId(20600, nameof(HttpBatchesDownloadingArgs));
        public static EventId HttpBatchesDownloadedArgs => new EventId(20650, nameof(HttpBatchesDownloadedArgs));
    }

    /// <summary>
    /// Partial interceptors extensions 
    /// </summary>
    public static partial class HttpInterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when batches are about to be downloaded
        /// </summary>
        public static Guid OnHttpBatchesDownloadingArgs(this WebClientOrchestrator orchestrator, Action<HttpBatchesDownloadingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when batches are about to be downloaded
        /// </summary>
        public static Guid OnHttpBatchesDownloadingArgs(this WebClientOrchestrator orchestrator, Func<HttpBatchesDownloadingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when batches have been completely downloaded
        /// </summary>
        public static Guid OnHttpBatchesDownloadedArgs(this WebClientOrchestrator orchestrator, Action<HttpBatchesDownloadedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when batches have been completely downloaded
        /// </summary>
        public static Guid OnHttpBatchesDownloadedArgs(this WebClientOrchestrator orchestrator, Func<HttpBatchesDownloadedArgs, Task> action)
            => orchestrator.AddInterceptor(action);



    }
}
