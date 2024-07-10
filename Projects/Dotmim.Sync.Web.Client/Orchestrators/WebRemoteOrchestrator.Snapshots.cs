using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;

namespace Dotmim.Sync.Web.Client
{
    public partial class WebRemoteOrchestrator : RemoteOrchestrator
    {


        internal override async Task<(SyncContext context, ServerSyncChanges ServerSyncChanges)>
          InternalGetSnapshotAsync(ScopeInfo sScopeInfo, SyncContext context, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var serverBatchInfo = new BatchInfo();

            try
            {
                // Generate a batch directory
                var batchDirectoryRoot = this.Options.BatchDirectory;
                var batchDirectoryName = string.Concat("WEB_SNAPSHOT_GETCHANGES_", DateTime.UtcNow.ToString("yyyy_MM_dd_ss"), Path.GetRandomFileName().Replace(".", ""));
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
                    foreach (var bpi in summaryResponseContent.BatchInfo.BatchPartsInfo)
                        serverBatchInfo.BatchPartsInfo.Add(bpi);

                // no snapshot
                if ((serverBatchInfo.BatchPartsInfo == null || serverBatchInfo.BatchPartsInfo.Count <= 0) && serverBatchInfo.RowsCount <= 0)
                    return (context, new ServerSyncChanges(0, null, new DatabaseChangesSelected(), null));

                await DownladBatchInfoAsync(context, sScopeInfo.Schema, serverBatchInfo, summaryResponseContent, default, default).ConfigureAwait(false);

                return (context, new ServerSyncChanges(summaryResponseContent.RemoteClientTimestamp, serverBatchInfo, summaryResponseContent.ServerChangesSelected, null));
            }
            catch (HttpSyncWebException)
            {
                // Try to delete the local folder where we download everything from server
                await WebRemoteCleanFolderAsync(context, serverBatchInfo).ConfigureAwait(false);

                throw;
            } // throw server error
            catch (Exception ex)
            {
                // Try to delete the local folder where we download everything from server
                await WebRemoteCleanFolderAsync(context, serverBatchInfo).ConfigureAwait(false);

                throw GetSyncError(context, ex);
            } // throw client error

        }

    }
}
