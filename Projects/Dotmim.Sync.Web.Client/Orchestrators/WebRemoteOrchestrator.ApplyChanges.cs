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
using Newtonsoft.Json;

namespace Dotmim.Sync.Web.Client
{
    public partial class WebRemoteOrchestrator : RemoteOrchestrator
    {


        /// <summary>
        /// Apply changes
        /// </summary>
        internal override async Task<(SyncContext context, ServerSyncChanges serverSyncChanges, ConflictResolutionPolicy serverResolutionPolicy)>
            InternalApplyThenGetChangesAsync(ScopeInfoClient cScopeInfoClient, ScopeInfo cScopeInfo, SyncContext context, ClientSyncChanges clientChanges,
            DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            SyncSet schema = cScopeInfo.Schema;
            schema.EnsureSchema();

            // if we don't have any BatchPartsInfo, just generate a new one to get, at least, something to send to the server
            // and get a response with new data from server
            clientChanges.ClientBatchInfo ??= new BatchInfo();

            // --------------------------------------------------------------
            // STEP 1 : Send everything to the server side
            // --------------------------------------------------------------

            HttpResponseMessage response = null;

            // If not in memory and BatchPartsInfo.Count == 0, nothing to send.
            // But we need to send something, so generate a little batch part
            if (clientChanges.ClientBatchInfo.BatchPartsInfo.Count == 0)
            {
                try
                {
                    HttpMessageSendChangesRequest changesToSend = null;
                    changesToSend = new HttpMessageSendChangesRequest(context, cScopeInfoClient);

                    var containerSet = new ContainerSet();

                    changesToSend.ClientLastSyncTimestamp = clientChanges.ClientTimestamp;
                    changesToSend.Changes = containerSet;
                    changesToSend.IsLastBatch = true;
                    changesToSend.BatchIndex = 0;
                    changesToSend.BatchCount = clientChanges.ClientBatchInfo.BatchPartsInfo == null ? 0 : clientChanges.ClientBatchInfo.BatchPartsInfo.Count;
                    var inMemoryRowsCount = changesToSend.Changes.RowsCount();

                    context.ProgressPercentage += 0.125;

                    await this.InterceptAsync(new HttpSendingClientChangesRequestArgs(changesToSend, inMemoryRowsCount, inMemoryRowsCount, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

                    response = await this.ProcessRequestAsync
                        (changesToSend, HttpStep.SendChangesInProgress, this.Options.BatchSize, cancellationToken, progress).ConfigureAwait(false);
                }
                catch (HttpSyncWebException) { throw; } // throw server error
                catch (Exception ex) { throw GetSyncError(context, ex); } // throw client error

            }
            else
            {
                try
                {
                    int tmpRowsSendedCount = 0;

                    // Foreach part, will have to send them to the remote
                    // once finished, return context
                    var initialPctProgress1 = context.ProgressPercentage;
                    var localSerializer = new LocalJsonSerializer(this, context);

                    foreach (var bpi in clientChanges.ClientBatchInfo.BatchPartsInfo.OrderBy(bpi => bpi.Index))
                    {
                        // Backward compatibility
                        var batchPartInfoTableName = bpi.Tables != null && bpi.Tables.Length >= 1 ? bpi.Tables[0].TableName : bpi.TableName;
                        var batchPartInfoSchemaName = bpi.Tables != null && bpi.Tables.Length >= 1 ? bpi.Tables[0].SchemaName : bpi.SchemaName;

                        // Get the updatable schema for the only table contained in the batchpartinfo
                        var schemaTable = CreateChangesTable(schema.Tables[batchPartInfoTableName, batchPartInfoSchemaName]);

                        // Generate the ContainerSet containing rows to send to the user
                        var containerSet = new ContainerSet();
                        var containerTable = new ContainerTable(schemaTable);
                        var fullPath = Path.Combine(clientChanges.ClientBatchInfo.GetDirectoryFullPath(), bpi.FileName);
                        containerSet.Tables.Add(containerTable);

                        // read rows from file
                        foreach (var row in localSerializer.GetRowsFromFile(fullPath, schemaTable))
                            containerTable.Rows.Add(row.ToArray());

                        // Call the converter if needed
                        if (this.Converter != null && containerTable.HasRows)
                            BeforeSerializeRows(containerTable, schemaTable, this.Converter);

                        // Create the send changes request
                        var changesToSend = new HttpMessageSendChangesRequest(context, cScopeInfoClient)
                        {
                            Changes = containerSet,
                            IsLastBatch = bpi.IsLastBatch,
                            BatchIndex = bpi.Index,
                            BatchCount = clientChanges.ClientBatchInfo.BatchPartsInfo.Count,
                            ClientLastSyncTimestamp = clientChanges.ClientTimestamp,
                        };

                        tmpRowsSendedCount += containerTable.Rows.Count;

                        context.ProgressPercentage = initialPctProgress1 + ((changesToSend.BatchIndex + 1) * 0.2d / changesToSend.BatchCount);
                        await this.InterceptAsync(new HttpSendingClientChangesRequestArgs(changesToSend, tmpRowsSendedCount, clientChanges.ClientBatchInfo.RowsCount, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

                        response = await this.ProcessRequestAsync
                            (changesToSend, HttpStep.SendChangesInProgress,this.Options.BatchSize, cancellationToken, progress).ConfigureAwait(false);

                        // See #721 for issue and #721 for PR from slagtejn
                        if (!bpi.IsLastBatch)
                            response.Dispose();
                    }
                }
                catch (HttpSyncWebException) { throw; } // throw server error
                catch (Exception ex) { throw GetSyncError(context, ex); } // throw client error

            }

            // --------------------------------------------------------------
            // STEP 2 : Receive everything from the server side
            // --------------------------------------------------------------

            // Now we have sent all the datas to the server and now :
            // We have a FIRST response from the server with new datas 
            // 1) Could be the only one response 
            // 2) Could be the first response and we need to download all batchs

            try
            {
                context.SyncStage = SyncStage.ChangesSelecting;
                var initialPctProgress = 0.55;
                context.ProgressPercentage = initialPctProgress;

                // Create the BatchInfo
                var serverBatchInfo = new BatchInfo();

                HttpMessageSummaryResponse summaryResponseContent = null;

                // Deserialize last response incoming from server after uploading changes
                using (var streamResponse = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
                {
                    var responseSerializer = this.SerializerFactory.GetSerializer();
                    summaryResponseContent = await responseSerializer.DeserializeAsync<HttpMessageSummaryResponse>(streamResponse);
                    context = summaryResponseContent.SyncContext;

                    await this.InterceptAsync(new HttpGettingResponseMessageArgs(response, this.ServiceUri.ToString(),
                        HttpStep.SendChangesInProgress, context, summaryResponseContent, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

                }

                serverBatchInfo.RowsCount = summaryResponseContent.BatchInfo.RowsCount;
                serverBatchInfo.Timestamp = summaryResponseContent.RemoteClientTimestamp;

                if (summaryResponseContent.BatchInfo.BatchPartsInfo != null)
                    foreach (var bpi in summaryResponseContent.BatchInfo.BatchPartsInfo)
                        serverBatchInfo.BatchPartsInfo.Add(bpi);


                // From here, we need to serialize everything on disk

                // Generate the batch directory
                var batchDirectoryRoot = this.Options.BatchDirectory;
                var batchDirectoryName = string.Concat(DateTime.UtcNow.ToString("yyyy_MM_dd_ss"), Path.GetRandomFileName().Replace(".", ""));

                serverBatchInfo.DirectoryRoot = batchDirectoryRoot;
                serverBatchInfo.DirectoryName = batchDirectoryName;

                // If we have a snapshot we are raising the batches downloading process that will occurs
                await this.InterceptAsync(new HttpBatchesDownloadingArgs(context, serverBatchInfo, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

                // function used to download one part
                var dl = new Func<BatchPartInfo, Task>(async (bpi) =>
                {
                    if (cancellationToken.IsCancellationRequested)
                        return;

                    var changesToSend3 = new HttpMessageGetMoreChangesRequest(context, bpi.Index);

                    await this.InterceptAsync(new HttpGettingServerChangesRequestArgs(bpi.Index, serverBatchInfo.BatchPartsInfo.Count, summaryResponseContent.SyncContext, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

                    // Raise get changes request
                    context.ProgressPercentage = initialPctProgress + ((bpi.Index + 1) * 0.2d / serverBatchInfo.BatchPartsInfo.Count);

                    var response = await this.ProcessRequestAsync(changesToSend3, HttpStep.GetMoreChanges,0, cancellationToken, progress).ConfigureAwait(false);

                    // If we are using a serializer that is not JSON, need to load in memory, then serialize to JSON
                    // OR If we have an interceptor on getting response
                    if (this.SerializerFactory.Key != "json" || this.interceptors.HasInterceptors<HttpGettingResponseMessageArgs>())
                    {
                        var webSerializer = this.SerializerFactory.GetSerializer();
                        using var responseStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
                        var getMoreChanges = await webSerializer.DeserializeAsync<HttpMessageSendChangesResponse>(responseStream);
                        context = getMoreChanges.SyncContext;

                        await this.InterceptAsync(new HttpGettingResponseMessageArgs(response, this.ServiceUri.ToString(),
                            HttpStep.GetMoreChanges, context, getMoreChanges, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

                        if (getMoreChanges != null && getMoreChanges.Changes != null && getMoreChanges.Changes.HasRows)
                        {
                            var localSerializer = new LocalJsonSerializer(this, context);

                            // Should have only one table
                            var table = getMoreChanges.Changes.Tables[0];
                            var schemaTable = CreateChangesTable(schema.Tables[table.TableName, table.SchemaName]);

                            var fullPath = Path.Combine(serverBatchInfo.GetDirectoryFullPath(), bpi.FileName);

                            // open the file and write table header
                            localSerializer.OpenFile(fullPath, schemaTable);

                            foreach (var row in table.Rows)
                                await localSerializer.WriteRowToFileAsync(new SyncRow(schemaTable, row), schemaTable).ConfigureAwait(false);

                            // Close file
                            localSerializer.CloseFile();
                        }

                    }
                    else
                    {
                        await SerializeAsync(response, bpi.FileName, serverBatchInfo.GetDirectoryFullPath(), this).ConfigureAwait(false);
                    }

                    // Raise response from server containing a batch changes 
                    await this.InterceptAsync(new HttpGettingServerChangesResponseArgs(serverBatchInfo, bpi.Index, bpi.RowsCount, summaryResponseContent.SyncContext, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);
                });

                // Parrallel download of all bpis (which will launch the delete directory on the server side)
                await serverBatchInfo.BatchPartsInfo.ForEachAsync(bpi => dl(bpi), this.MaxDownladingDegreeOfParallelism).ConfigureAwait(false);

                // Send order of end of download
                var lastBpi = serverBatchInfo.BatchPartsInfo.FirstOrDefault(bpi => bpi.IsLastBatch);

                if (lastBpi != null)
                {
                    var endOfDownloadChanges = new HttpMessageGetMoreChangesRequest(context, lastBpi.Index);

                    // Deserialize response incoming from server
                    // This is the last response
                    // Should contains step HttpStep.SendEndDownloadChanges
                    var endResponseContent = await this.ProcessRequestAsync<HttpMessageSendChangesResponse>(
                        context, endOfDownloadChanges, HttpStep.SendEndDownloadChanges, 0, cancellationToken, progress).ConfigureAwait(false);
                }

                // generate the new scope item
                this.CompleteTime = DateTime.UtcNow;

                await this.InterceptAsync(new HttpBatchesDownloadedArgs(summaryResponseContent, summaryResponseContent.SyncContext, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

                var serverSyncChanges = new ServerSyncChanges(
                    summaryResponseContent.RemoteClientTimestamp,
                    serverBatchInfo,
                    summaryResponseContent.ServerChangesSelected,
                    summaryResponseContent.ClientChangesApplied);


                return (context, serverSyncChanges, summaryResponseContent.ConflictResolutionPolicy);
            }
            catch (HttpSyncWebException) { throw; } // throw server error
            catch (Exception ex) { throw GetSyncError(context, ex); } // throw client error

        }
    }
}
