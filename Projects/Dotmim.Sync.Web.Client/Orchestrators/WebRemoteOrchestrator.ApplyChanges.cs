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

        
        /// <summary>
        /// Apply changes
        /// </summary>
        internal override async Task<(SyncContext context, ServerSyncChanges serverSyncChanges, DatabaseChangesApplied serverChangesApplied, ConflictResolutionPolicy serverResolutionPolicy)>
            InternalApplyThenGetChangesAsync(ScopeInfoClient cScopeInfoClient, ScopeInfo cScopeInfo, SyncContext context, 
            BatchInfo clientBatchInfo, long clientLastSyncTimestamp, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            SyncSet schema = cScopeInfo.Schema;
            schema.EnsureSchema();

            // if we don't have any BatchPartsInfo, just generate a new one to get, at least, something to send to the server
            // and get a response with new data from server
            if (clientBatchInfo == null)
                clientBatchInfo = new BatchInfo();

            // --------------------------------------------------------------
            // STEP 1 : Send everything to the server side
            // --------------------------------------------------------------

            HttpResponseMessage response = null;

            // If not in memory and BatchPartsInfo.Count == 0, nothing to send.
            // But we need to send something, so generate a little batch part
            if (clientBatchInfo.BatchPartsInfo.Count == 0)
            {
                var changesToSend = new HttpMessageSendChangesRequest(context, cScopeInfoClient);

                var containerSet = new ContainerSet();

                changesToSend.ClientLastSyncTimestamp = clientLastSyncTimestamp;
                changesToSend.Changes = containerSet;
                changesToSend.IsLastBatch = true;
                changesToSend.BatchIndex = 0;
                changesToSend.BatchCount = clientBatchInfo.BatchPartsInfo == null ? 0 : clientBatchInfo.BatchPartsInfo.Count;
                var inMemoryRowsCount = changesToSend.Changes.RowsCount();

                context.ProgressPercentage += 0.125;

                await this.InterceptAsync(new HttpSendingClientChangesRequestArgs(changesToSend, inMemoryRowsCount, inMemoryRowsCount, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

                // serialize message
                var serializer = this.SerializerFactory.GetSerializer<HttpMessageSendChangesRequest>();
                var binaryData = await serializer.SerializeAsync(changesToSend);

                response = await this.httpRequestHandler.ProcessRequestAsync
                    (this.HttpClient, context, this.ServiceUri, binaryData, HttpStep.SendChangesInProgress, 
                     this.SerializerFactory, this.Converter, this.Options.BatchSize, this.SyncPolicy, cancellationToken, progress).ConfigureAwait(false);

            }
            else
            {
                int tmpRowsSendedCount = 0;

                // Foreach part, will have to send them to the remote
                // once finished, return context
                var initialPctProgress1 = context.ProgressPercentage;
                var localSerializer = new LocalJsonSerializer();

                var interceptorsReading = this.interceptors.GetInterceptors<DeserializingRowArgs>();
                if (interceptorsReading.Count > 0)
                {
                    localSerializer.OnReadingRow(async (schemaTable, rowString) =>
                    {
                        var args = new DeserializingRowArgs(context, schemaTable, rowString);
                        await this.InterceptAsync(args);
                        return args.Result;
                    });
                }
                foreach (var bpi in clientBatchInfo.BatchPartsInfo.OrderBy(bpi => bpi.Index))
                {
                    // Get the updatable schema for the only table contained in the batchpartinfo
                    var schemaTable = CreateChangesTable(schema.Tables[bpi.Tables[0].TableName, bpi.Tables[0].SchemaName]);

                    // Generate the ContainerSet containing rows to send to the user
                    var containerSet = new ContainerSet();
                    var containerTable = new ContainerTable(schemaTable);
                    var fullPath = Path.Combine(clientBatchInfo.GetDirectoryFullPath(), bpi.FileName);
                    containerSet.Tables.Add(containerTable);

                    // read rows from file
                    foreach (var row in localSerializer.ReadRowsFromFile(fullPath, schemaTable))
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
                        BatchCount = clientBatchInfo.BatchPartsInfo.Count,
                        ClientLastSyncTimestamp = clientLastSyncTimestamp,
                    };

                    tmpRowsSendedCount += containerTable.Rows.Count;

                    context.ProgressPercentage = initialPctProgress1 + ((changesToSend.BatchIndex + 1) * 0.2d / changesToSend.BatchCount);
                    await this.InterceptAsync(new HttpSendingClientChangesRequestArgs(changesToSend, tmpRowsSendedCount, clientBatchInfo.RowsCount, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

                    // serialize message
                    var serializer = this.SerializerFactory.GetSerializer<HttpMessageSendChangesRequest>();
                    var binaryData = await serializer.SerializeAsync(changesToSend);

                    response = await this.httpRequestHandler.ProcessRequestAsync
                        (this.HttpClient, context, this.ServiceUri, binaryData, HttpStep.SendChangesInProgress, 
                         this.SerializerFactory, this.Converter, this.Options.BatchSize, this.SyncPolicy, cancellationToken, progress).ConfigureAwait(false);

                    // See #721 for issue and #721 for PR from slagtejn
                    if (!bpi.IsLastBatch)
                        response.Dispose();
                }
            }

            // --------------------------------------------------------------
            // STEP 2 : Receive everything from the server side
            // --------------------------------------------------------------

            // Now we have sent all the datas to the server and now :
            // We have a FIRST response from the server with new datas 
            // 1) Could be the only one response 
            // 2) Could be the first response and we need to download all batchs

            context.SyncStage = SyncStage.ChangesSelecting;
            var initialPctProgress = 0.55;
            context.ProgressPercentage = initialPctProgress;

            // Create the BatchInfo
            var serverBatchInfo = new BatchInfo();

            HttpMessageSummaryResponse summaryResponseContent = null;

            // Deserialize response incoming from server
            using (var streamResponse = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
            {
                var responseSerializer = this.SerializerFactory.GetSerializer<HttpMessageSummaryResponse>();
                summaryResponseContent = await responseSerializer.DeserializeAsync(streamResponse);
            }

            serverBatchInfo.RowsCount = summaryResponseContent.BatchInfo.RowsCount;
            serverBatchInfo.Timestamp = summaryResponseContent.RemoteClientTimestamp;
            context = summaryResponseContent.SyncContext;

            if (summaryResponseContent.BatchInfo.BatchPartsInfo != null)
                foreach (var bpi in summaryResponseContent.BatchInfo.BatchPartsInfo)
                    serverBatchInfo.BatchPartsInfo.Add(bpi);


            // From here, we need to serialize everything on disk

            // Generate the batch directory
            var batchDirectoryRoot = this.Options.BatchDirectory;
            var batchDirectoryName = string.Concat(DateTime.UtcNow.ToString("yyyy_MM_dd_ss"), Path.GetRandomFileName().Replace(".", ""));

            serverBatchInfo.DirectoryRoot = batchDirectoryRoot;
            serverBatchInfo.DirectoryName = batchDirectoryName;

            if (!Directory.Exists(serverBatchInfo.GetDirectoryFullPath()))
                Directory.CreateDirectory(serverBatchInfo.GetDirectoryFullPath());

            // If we have a snapshot we are raising the batches downloading process that will occurs
            await this.InterceptAsync(new HttpBatchesDownloadingArgs(context, serverBatchInfo, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

            // function used to download one part
            var dl = new Func<BatchPartInfo, Task>(async (bpi) =>
            {
                if (cancellationToken.IsCancellationRequested)
                    return;

                var changesToSend3 = new HttpMessageGetMoreChangesRequest(context, bpi.Index);

                var serializer3 = this.SerializerFactory.GetSerializer<HttpMessageGetMoreChangesRequest>();
                var binaryData3 = await serializer3.SerializeAsync(changesToSend3).ConfigureAwait(false);
                var step3 = HttpStep.GetMoreChanges;

                await this.InterceptAsync(new HttpGettingServerChangesRequestArgs(bpi.Index, serverBatchInfo.BatchPartsInfo.Count, summaryResponseContent.SyncContext, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

                // Raise get changes request
                context.ProgressPercentage = initialPctProgress + ((bpi.Index + 1) * 0.2d / serverBatchInfo.BatchPartsInfo.Count);

                var response = await this.httpRequestHandler.ProcessRequestAsync(
                this.HttpClient, context, this.ServiceUri, binaryData3, step3, 
                this.SerializerFactory, this.Converter, 0, this.SyncPolicy, cancellationToken, progress).ConfigureAwait(false);

                if (this.SerializerFactory.Key != "json")
                {
                    var webSerializer = this.SerializerFactory.GetSerializer<HttpMessageSendChangesResponse>();
                    using var responseStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
                    var getMoreChanges = await webSerializer.DeserializeAsync(responseStream);

                    context = getMoreChanges.SyncContext;

                    if (getMoreChanges != null && getMoreChanges.Changes != null && getMoreChanges.Changes.HasRows)
                    {
                        var localSerializer = new LocalJsonSerializer();

                        var interceptorsWriting = this.interceptors.GetInterceptors<SerializingRowArgs>();
                        if (interceptorsWriting.Count > 0)
                        {
                            localSerializer.OnWritingRow(async (syncTable, rowArray) =>
                            {
                                var args = new SerializingRowArgs(context, syncTable, rowArray);
                                await this.InterceptAsync(args, progress, cancellationToken).ConfigureAwait(false);
                                return args.Result;
                            });
                        }


                        // Should have only one table
                        var table = getMoreChanges.Changes.Tables[0];
                        var schemaTable = CreateChangesTable(schema.Tables[table.TableName, table.SchemaName]);

                        var fullPath = Path.Combine(serverBatchInfo.GetDirectoryFullPath(), bpi.FileName);

                        // open the file and write table header
                        await localSerializer.OpenFileAsync(fullPath, schemaTable).ConfigureAwait(false);

                        foreach (var row in table.Rows)
                            await localSerializer.WriteRowToFileAsync(new SyncRow(schemaTable, row), schemaTable).ConfigureAwait(false);

                        // Close file
                        await localSerializer.CloseFileAsync(fullPath, schemaTable).ConfigureAwait(false);
                    }

                }
                else
                {
                    // Serialize
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

                var serializerEndOfDownloadChanges = this.SerializerFactory.GetSerializer<HttpMessageGetMoreChangesRequest>();
                var binaryData3 = await serializerEndOfDownloadChanges.SerializeAsync(endOfDownloadChanges).ConfigureAwait(false);

                var endResponse =  await this.httpRequestHandler.ProcessRequestAsync(
                    this.HttpClient, context, this.ServiceUri, binaryData3, HttpStep.SendEndDownloadChanges, 
                    this.SerializerFactory, this.Converter, 0, this.SyncPolicy, cancellationToken, progress).ConfigureAwait(false);

                // Deserialize response incoming from server
                // This is the last response
                // Should contains step HttpStep.SendEndDownloadChanges
                using var streamResponse = await endResponse.Content.ReadAsStreamAsync().ConfigureAwait(false);
                var endResponseSerializer = this.SerializerFactory.GetSerializer<HttpMessageSendChangesResponse>();
                var endResponseContent = await endResponseSerializer.DeserializeAsync(streamResponse);
                context = endResponseContent.SyncContext;

            }

            // generate the new scope item
            this.CompleteTime = DateTime.UtcNow;

            await this.InterceptAsync(new HttpBatchesDownloadedArgs(summaryResponseContent, summaryResponseContent.SyncContext, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

            var serverSyncChanges = new ServerSyncChanges(
                summaryResponseContent.RemoteClientTimestamp,
                serverBatchInfo,
                summaryResponseContent.ServerChangesSelected
                );


            return (context, serverSyncChanges, summaryResponseContent.ClientChangesApplied, summaryResponseContent.ConflictResolutionPolicy);
        }




    }
}
