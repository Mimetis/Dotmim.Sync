using Dotmim.Sync.Batch;
using System;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync.Web.Client
{
    /// <summary>
    /// Contains the logic to handle snapshot on the server side.
    /// </summary>
    public partial class WebRemoteOrchestrator : RemoteOrchestrator
    {

        /// <inheritdoc />
        internal override async Task<(SyncContext Context, ServerSyncChanges ServerSyncChanges)>
          InternalGetSnapshotAsync(ScopeInfo sScopeInfo, SyncContext context, DbConnection connection = default, DbTransaction transaction = default,
            IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            var serverBatchInfo = new BatchInfo();

            try
            {
                // Generate a batch directory
                var batchDirectoryRoot = this.Options.BatchDirectory;
                var batchDirectoryName = string.Concat("WEB_SNAPSHOT_GETCHANGES_", DateTime.UtcNow.ToString("yyyy_MM_dd_ss", CultureInfo.InvariantCulture), Path.GetRandomFileName().Replace(".", string.Empty));
                var batchDirectoryFullPath = Path.Combine(batchDirectoryRoot, batchDirectoryName);

                // Firstly, get the snapshot summary
                var changesToSend = new HttpMessageSendChangesRequest(context, null);

                var summaryResponseContent = await this.ProcessRequestAsync<HttpMessageSummaryResponse>(context, changesToSend, HttpStep.GetSummary, 0, progress, cancellationToken).ConfigureAwait(false);

                if (summaryResponseContent == null)
                    throw new Exception("Summary can't be null");

                serverBatchInfo.DirectoryRoot = batchDirectoryRoot;
                serverBatchInfo.DirectoryName = batchDirectoryName;
                serverBatchInfo.RowsCount = summaryResponseContent.BatchInfo?.RowsCount ?? 0;

                if (summaryResponseContent.BatchInfo?.BatchPartsInfo != null)
                {
                    foreach (var bpi in summaryResponseContent.BatchInfo.BatchPartsInfo)
                        serverBatchInfo.BatchPartsInfo.Add(bpi);
                }

                // no snapshot
                if ((serverBatchInfo.BatchPartsInfo == null || serverBatchInfo.BatchPartsInfo.Count <= 0) && serverBatchInfo.RowsCount <= 0)
                    return (context, new ServerSyncChanges(0, null, new DatabaseChangesSelected(), null));

                await this.DownladBatchInfoAsync(context, sScopeInfo.Schema, serverBatchInfo, summaryResponseContent, default, default).ConfigureAwait(false);

                return (context, new ServerSyncChanges(summaryResponseContent.RemoteClientTimestamp, serverBatchInfo, summaryResponseContent.ServerChangesSelected, null));
            }
            catch (HttpSyncWebException)
            {
                // Try to delete the local folder where we download everything from server
                await this.WebRemoteCleanFolderAsync(context, serverBatchInfo).ConfigureAwait(false);

                throw;
            } // throw server error
            catch (Exception ex)
            {
                // Try to delete the local folder where we download everything from server
                await this.WebRemoteCleanFolderAsync(context, serverBatchInfo).ConfigureAwait(false);

                throw this.GetSyncError(context, ex);
            } // throw client error
        }
    }
}