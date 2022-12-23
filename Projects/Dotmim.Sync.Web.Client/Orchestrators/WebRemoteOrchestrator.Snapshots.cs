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
            try
            {

                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Generate a batch directory
                var batchDirectoryRoot = this.Options.BatchDirectory;
                var batchDirectoryName = string.Concat(DateTime.UtcNow.ToString("yyyy_MM_dd_ss"), Path.GetRandomFileName().Replace(".", ""));
                var batchDirectoryFullPath = Path.Combine(batchDirectoryRoot, batchDirectoryName);

                // Create the BatchInfo serialized (forced because in a snapshot call, so we are obviously serialized on disk)
                string info = connection != null && !string.IsNullOrEmpty(connection.Database) ? $"{connection.Database}_SNAPSHOTGETCHANGES" : "SNAPSHOTGETCHANGES";
                var serverBatchInfo = new BatchInfo(batchDirectoryRoot, batchDirectoryName, info: info);

                // Firstly, get the snapshot summary
                var changesToSend = new HttpMessageSendChangesRequest(context, null);

                var summaryResponseContent = await this.ProcessRequestAsync<HttpMessageSummaryResponse>(context, changesToSend, HttpStep.GetSummary,
                  0, cancellationToken, progress).ConfigureAwait(false);

                if (summaryResponseContent == null)
                    throw new Exception("Summary can't be null");

                serverBatchInfo.RowsCount = summaryResponseContent.BatchInfo?.RowsCount ?? 0;

                if (summaryResponseContent.BatchInfo?.BatchPartsInfo != null)
                    foreach (var bpi in summaryResponseContent.BatchInfo.BatchPartsInfo)
                        serverBatchInfo.BatchPartsInfo.Add(bpi);

                // no snapshot
                if ((serverBatchInfo.BatchPartsInfo == null || serverBatchInfo.BatchPartsInfo.Count <= 0) && serverBatchInfo.RowsCount <= 0)
                    return (context, new ServerSyncChanges(0, null, new DatabaseChangesSelected(), null));

                // If we have a snapshot we are raising the batches downloading process that will occurs
                await this.InterceptAsync(new HttpBatchesDownloadingArgs(context, serverBatchInfo, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

                await serverBatchInfo.BatchPartsInfo.ForEachAsync(async bpi =>
                {
                    var changesToSend3 = new HttpMessageGetMoreChangesRequest(context, bpi.Index);

                    await this.InterceptAsync(new HttpGettingServerChangesRequestArgs(bpi.Index, serverBatchInfo.BatchPartsInfo.Count, summaryResponseContent.SyncContext, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

                    var response = await this.ProcessRequestAsync(changesToSend3, HttpStep.GetMoreChanges, 0, cancellationToken, progress).ConfigureAwait(false);

                    if (this.SerializerFactory.Key != "json" || this.interceptors.HasInterceptors<HttpGettingResponseMessageArgs>())
                    {
                        var s = this.SerializerFactory.GetSerializer();
                        using var responseStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
                        var getMoreChanges = await s.DeserializeAsync<HttpMessageSendChangesResponse>(responseStream);
                        context = getMoreChanges.SyncContext;

                        await this.InterceptAsync(new HttpGettingResponseMessageArgs(response, this.ServiceUri.ToString(),
                            HttpStep.GetMoreChanges, context, getMoreChanges, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

                        if (getMoreChanges != null && getMoreChanges.Changes != null && getMoreChanges.Changes.HasRows)
                        {
                            var localSerializer = new LocalJsonSerializer(this, context);

                            // Should have only one table
                            var table = getMoreChanges.Changes.Tables[0];
                            var schemaTable = CreateChangesTable(sScopeInfo.Schema.Tables[table.TableName, table.SchemaName]);

                            var fullPath = Path.Combine(batchDirectoryFullPath, bpi.FileName);

                            // open the file and write table header
                            if (!localSerializer.IsOpen)
                                localSerializer.OpenFile(fullPath, schemaTable);

                            foreach (var row in table.Rows)
                                await localSerializer.WriteRowToFileAsync(new SyncRow(schemaTable, row), schemaTable).ConfigureAwait(false);

                            // Close file
                            if (localSerializer.IsOpen)
                                localSerializer.CloseFile();
                        }

                    }
                    else
                    {
                        // Serialize
                        await SerializeAsync(response, bpi.FileName, batchDirectoryFullPath, this).ConfigureAwait(false);
                    }


                    // Raise response from server containing a batch changes 
                    await this.InterceptAsync(new HttpGettingServerChangesResponseArgs(serverBatchInfo, bpi.Index, bpi.RowsCount, summaryResponseContent.SyncContext, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);
                }, this.MaxDownladingDegreeOfParallelism).ConfigureAwait(false);

                await this.InterceptAsync(new HttpBatchesDownloadedArgs(summaryResponseContent, summaryResponseContent.SyncContext, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

                return (context, new ServerSyncChanges(summaryResponseContent.RemoteClientTimestamp, serverBatchInfo, summaryResponseContent.ServerChangesSelected, null));
            }
            catch (HttpSyncWebException) { throw; } // throw server error
            catch (Exception ex) { throw GetSyncError(context, ex); } // throw client error

        }

    }
}
