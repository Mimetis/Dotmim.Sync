using Dotmim.Sync.Batch;
using Dotmim.Sync.Web.Client;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Represents a request made to the server to get the server scope info
    /// </summary>
    public class HttpSnapshotDownloadingArgs : ProgressArgs
    {
        public HttpSnapshotDownloadingArgs(SyncContext context, DateTime startTime, BatchInfo serverBatchInfo, string host) : base(context, null)
        {
            this.StartTime = startTime;
            this.ServerBatchInfo = serverBatchInfo;
            this.Host = host;
        }

        public override string Source => this.Host;
        public override int EventId => HttpClientSyncEventsId.HttpGettingSchemaRequest.Id;
        public override string Message => $"Downloading Snapshot. Scope Name:{this.Context.ScopeName}. Batches Count:{this.ServerBatchInfo.BatchPartsInfo?.Count ?? 1}. Rows Count:{this.ServerBatchInfo.RowsCount}";

        public DateTime StartTime { get; }
        public BatchInfo ServerBatchInfo { get; }
        public string Host { get; }
    }
    public class HttpSnapshotDownloadedArgs : ProgressArgs
    {
        public HttpSnapshotDownloadedArgs(HttpMessageSummaryResponse httpSummary, SyncContext context, DateTime startTime, DateTime completeTime, string host) : base(context, null)
        {
            this.HttpSummary = httpSummary;
            this.StartTime = startTime;
            this.CompleteTime = completeTime;
            this.Host = host;
        }
        public override string Source => this.Host;
        public override int EventId => HttpClientSyncEventsId.HttpGettingSchemaResponse.Id;
        public override string Message
        {
            get
            {
                var batchCount = this.HttpSummary.BatchInfo?.BatchPartsInfo?.Count ?? 1;
                var totalRows = this.HttpSummary.ServerChangesSelected?.TotalChangesSelected ?? 0;

                return $"Snapshot Downloaded. Batches Count: {batchCount}. Total Rows: {totalRows}. Duration: {Duration:hh\\:mm\\:ss}";
            }
        }

        public HttpMessageSummaryResponse HttpSummary { get; }
        public DateTime StartTime { get; }
        public DateTime CompleteTime { get; }

        public TimeSpan Duration
        {
            get
            {
                var tsEnded = TimeSpan.FromTicks(CompleteTime.Ticks);
                var tsStarted = TimeSpan.FromTicks(StartTime.Ticks);

                var durationTs = tsEnded.Subtract(tsStarted);

                return durationTs;
            }
        }

        public string Host { get; }
    }

    public static partial class HttpClientSyncEventsId
    {
        public static EventId HttpSnapshotDownloadingArgs => new EventId(20200, nameof(HttpSnapshotDownloadingArgs));
        public static EventId HttpSnapshotDownloadedArgs => new EventId(20250, nameof(HttpSnapshotDownloadedArgs));
    }

    /// <summary>
    /// Partial interceptors extensions 
    /// </summary>
    public static partial class HttpInterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when snapshot is about to be downloaded
        /// </summary>
        public static void OnHttpSnapshotDownloadingArgs(this WebClientOrchestrator orchestrator, Action<HttpSnapshotDownloadingArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when snapshot is about to be downloaded
        /// </summary>
        public static void OnHttpSnapshotDownloadingArgs(this WebClientOrchestrator orchestrator, Func<HttpSnapshotDownloadingArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when snapshot has been completely downloaded
        /// </summary>
        public static void OnHttpSnapshotDownloadedArgs(this WebClientOrchestrator orchestrator, Action<HttpSnapshotDownloadedArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when snapshot has been completely downloaded
        /// </summary>
        public static void OnHttpSnapshotDownloadedArgs(this WebClientOrchestrator orchestrator, Func<HttpSnapshotDownloadedArgs, Task> action)
            => orchestrator.SetInterceptor(action);



    }
}
