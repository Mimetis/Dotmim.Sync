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
        public override async Task<ServerSyncChanges> GetChangesAsync(ScopeInfoClient cScopeInfoClient, DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), cScopeInfoClient.Name, cScopeInfoClient.Parameters) { ClientId = cScopeInfoClient.Id };

            // Create the BatchInfo
            var serverBatchInfo = new BatchInfo();

            try
            {
                // Get the server scope to start a new session
                ScopeInfo sScopeInfo;
                (context, sScopeInfo, _) = await this.InternalEnsureScopeInfoAsync(context, null, false, connection, transaction, default, default).ConfigureAwait(false);

                //Direction set to Download
                context.SyncWay = SyncWay.Download;

                var changesToSend = new HttpMessageSendChangesRequest(context, cScopeInfoClient);

                context.ProgressPercentage += 0.125;

                await this.InterceptAsync(new HttpSendingClientChangesRequestArgs(changesToSend, 0, 0, this.GetServiceHost())).ConfigureAwait(false);

                // --------------------------------------------------------------
                // STEP 2 : Receive everything from the server side
                // --------------------------------------------------------------

                // Now we have sent all the datas to the server and now :
                // We have a FIRST response from the server with new datas 
                // 1) Could be the only one response (enough or InMemory is set on the server side)
                // 2) Could bt the first response and we need to download all batchs

                context.SyncStage = SyncStage.ChangesSelecting;
                var initialPctProgress = 0.55;
                context.ProgressPercentage = initialPctProgress;

                var summaryResponseContent = await this.ProcessRequestAsync<HttpMessageSummaryResponse>
                    (context, changesToSend, HttpStep.SendChangesInProgress, this.Options.BatchSize).ConfigureAwait(false);

                serverBatchInfo.RowsCount = summaryResponseContent.BatchInfo.RowsCount;
                serverBatchInfo.Timestamp = summaryResponseContent.RemoteClientTimestamp;

                if (summaryResponseContent.BatchInfo.BatchPartsInfo != null)
                    foreach (var bpi in summaryResponseContent.BatchInfo.BatchPartsInfo)
                        serverBatchInfo.BatchPartsInfo.Add(bpi);

                //-----------------------
                // In Batch Mode
                //-----------------------
                // From here, we need to serialize everything on disk

                // Generate the batch directory
                var batchDirectoryRoot = this.Options.BatchDirectory;
                var batchDirectoryName = string.Concat("WEB_REMOTE_GETCHANGES_", DateTime.UtcNow.ToString("yyyy_MM_dd_ss"), Path.GetRandomFileName().Replace(".", ""));

                serverBatchInfo.DirectoryRoot = batchDirectoryRoot;
                serverBatchInfo.DirectoryName = batchDirectoryName;

                if (!Directory.Exists(serverBatchInfo.GetDirectoryFullPath()))
                    Directory.CreateDirectory(serverBatchInfo.GetDirectoryFullPath());

                await DownladBatchInfoAsync(context, sScopeInfo.Schema, serverBatchInfo, summaryResponseContent, default, default).ConfigureAwait(false);

                // generate the new scope item
                this.CompleteTime = DateTime.UtcNow;

                // Reaffect context
                context = summaryResponseContent.SyncContext;

                return new ServerSyncChanges(summaryResponseContent.RemoteClientTimestamp, serverBatchInfo, summaryResponseContent.ServerChangesSelected, null);
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



        /// <summary>
        /// We can't get changes from server, from a web client orchestrator
        /// </summary>
        /// 
        public override async Task<ServerSyncChanges> GetEstimatedChangesCountAsync(ScopeInfoClient cScopeInfoClient, DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), cScopeInfoClient.Name, cScopeInfoClient.Parameters) { ClientId = cScopeInfoClient.Id };

            try
            {
                // Get the server scope to start a new session
                await this.InternalEnsureScopeInfoAsync(context, null, false, connection, transaction, default, default).ConfigureAwait(false);

                // generate a message to send
                var changesToSend = new HttpMessageSendChangesRequest(context, cScopeInfoClient);

                // Raise progress for sending request and waiting server response
                await this.InterceptAsync(new HttpGettingServerChangesRequestArgs(0, 0, context, this.GetServiceHost())).ConfigureAwait(false);

                // response
                var summaryResponseContent = await this.ProcessRequestAsync<HttpMessageSendChangesResponse>(context, changesToSend, HttpStep.GetEstimatedChangesCount, this.Options.BatchSize).ConfigureAwait(false);

                if (summaryResponseContent == null)
                    throw new Exception("Summary can't be null");

                // generate the new scope ite
                this.CompleteTime = DateTime.UtcNow;

                return new(summaryResponseContent.RemoteClientTimestamp, null, summaryResponseContent.ServerChangesSelected, null);

            }
            catch (HttpSyncWebException) { throw; } // throw server error
            catch (Exception ex) { throw GetSyncError(context, ex); } // throw client error
        }

        private async Task DownladBatchInfoAsync(SyncContext context, SyncSet schema, BatchInfo serverBatchInfo, HttpMessageSummaryResponse summary, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // If we have a snapshot we are raising the batches downloading process that will occurs
            await this.InterceptAsync(new HttpBatchesDownloadingArgs(context, serverBatchInfo, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

            // hook to get the last batch part info at the end
            var bpis = serverBatchInfo.BatchPartsInfo.Where(bpi => !bpi.IsLastBatch);
            var lstbpi = serverBatchInfo.BatchPartsInfo.FirstOrDefault(bpi => bpi.IsLastBatch);

            lstbpi ??= serverBatchInfo.BatchPartsInfo.OrderByDescending(bpi => bpi.Index).FirstOrDefault();

            // Parrallel download of all bpis except the last one (which will launch the delete directory on the server side)
            await bpis.ForEachAsync(bpi => DownloadBatchPartInfoAsync(context, schema, serverBatchInfo, bpi, HttpStep.GetMoreChanges, cancellationToken, progress), this.MaxDownladingDegreeOfParallelism).ConfigureAwait(false);

            // Download last batch part that will launch the server deletion of the tmp dir
            await DownloadBatchPartInfoAsync(context, schema, serverBatchInfo, lstbpi, HttpStep.GetMoreChanges, cancellationToken, progress).ConfigureAwait(false);

            // Send end of download
            await this.ProcessRequestAsync<HttpMessageSendChangesResponse>(context, new HttpMessageGetMoreChangesRequest(context, lstbpi == null ? 0 : lstbpi.Index),
                HttpStep.SendEndDownloadChanges, 0, cancellationToken, progress).ConfigureAwait(false);

            await this.InterceptAsync(new HttpBatchesDownloadedArgs(summary, context, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);
        }

        private async Task DownloadBatchPartInfoAsync(SyncContext context, SyncSet schema, BatchInfo serverBatchInfo, BatchPartInfo bpi, HttpStep step, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (cancellationToken.IsCancellationRequested)
                return;

            if (bpi == null)
                return;

            var initialPctProgress = 0.55;

            var changesToSend = new HttpMessageGetMoreChangesRequest(context, bpi.Index);

            await this.InterceptAsync(new HttpGettingServerChangesRequestArgs(bpi.Index, serverBatchInfo.BatchPartsInfo.Count, context, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

            // Raise get changes request
            context.ProgressPercentage = initialPctProgress + ((bpi.Index + 1) * 0.2d / serverBatchInfo.BatchPartsInfo.Count);

            var response = await this.ProcessRequestAsync(changesToSend, step, 0, cancellationToken, progress).ConfigureAwait(false);

            // If we are using a serializer that is not JSON, need to load in memory, then serialize to JSON
            // OR If we have an interceptor on getting response
            // OR If we have a converter
            if (this.SerializerFactory.Key != "json" || this.interceptors.HasInterceptors<HttpGettingResponseMessageArgs>() || this.Converter != null)
            {
                var webSerializer = this.SerializerFactory.GetSerializer();
                using var responseStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
                var getMoreChanges = await webSerializer.DeserializeAsync<HttpMessageSendChangesResponse>(responseStream);
                context = getMoreChanges.SyncContext;

                await this.InterceptAsync(new HttpGettingResponseMessageArgs(response, this.ServiceUri.ToString(),
                    step, context, getMoreChanges, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

                if (getMoreChanges != null && getMoreChanges.Changes != null && getMoreChanges.Changes.HasRows)
                {
                    var localSerializer = new LocalJsonSerializer(this, context);

                    // Should have only one table
                    var table = getMoreChanges.Changes.Tables[0];
                    var schemaTable = CreateChangesTable(schema.Tables[table.TableName, table.SchemaName]);

                    var fullPath = Path.Combine(serverBatchInfo.GetDirectoryFullPath(), bpi.FileName);

                    SyncRowState syncRowState = SyncRowState.None;
                    if (table.Rows!= null && table.Rows.Count > 0)
                    {
                        var sr = new SyncRow(schemaTable, table.Rows[0]);
                        syncRowState = sr.RowState;
                    }

                    // open the file and write table header
                    await localSerializer.OpenFileAsync(fullPath, schemaTable, syncRowState).ConfigureAwait(false);

                    foreach (var row in table.Rows)
                    {
                        var syncRow = new SyncRow(schemaTable, row);

                        if (this.Converter != null && syncRow.Length > 0)
                            this.Converter.AfterDeserialized(syncRow, schemaTable);

                        await localSerializer.WriteRowToFileAsync(syncRow, schemaTable).ConfigureAwait(false);
                    }

                    // Close file
                    localSerializer.CloseFile();
                }

            }
            else
            {
                await SerializeAsync(response, bpi.FileName, serverBatchInfo.GetDirectoryFullPath(), this).ConfigureAwait(false);
            }

            response.Dispose();

            // Raise response from server containing a batch changes 
            await this.InterceptAsync(new HttpGettingServerChangesResponseArgs(serverBatchInfo, bpi.Index, bpi.RowsCount, context, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);


        }
    }
}
