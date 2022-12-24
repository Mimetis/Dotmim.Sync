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
                    var changesToSend = new HttpMessageSendChangesRequest(context, cScopeInfoClient) { ClientLastSyncTimestamp = clientChanges.ClientTimestamp };

                    context.ProgressPercentage += 0.125;

                    await this.InterceptAsync(new HttpSendingClientChangesRequestArgs(changesToSend, 0, 0, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

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

                        // Create the send changes request
                        var changesToSend = new HttpMessageSendChangesRequest(context, cScopeInfoClient)
                        {
                            IsLastBatch = bpi.IsLastBatch,
                            BatchIndex = bpi.Index,
                            BatchCount = clientChanges.ClientBatchInfo.BatchPartsInfo.Count,
                            ClientLastSyncTimestamp = clientChanges.ClientTimestamp,
                        };

                        // Generate the ContainerSet containing rows to send to the user
                        var containerTable = new ContainerTable(schemaTable);
                        changesToSend.Changes.Tables.Add(containerTable);

                        // read rows from file
                        var fullPath = Path.Combine(clientChanges.ClientBatchInfo.GetDirectoryFullPath(), bpi.FileName);
                        foreach (var row in localSerializer.GetRowsFromFile(fullPath, schemaTable))
                            containerTable.Rows.Add(row.ToArray());

                        // Call the converter if needed
                        if (this.Converter != null && containerTable.HasRows)
                            BeforeSerializeRows(containerTable, schemaTable, this.Converter);

                        tmpRowsSendedCount += containerTable.Rows.Count;

                        context.ProgressPercentage = initialPctProgress1 + ((changesToSend.BatchIndex + 1) * 0.2d / changesToSend.BatchCount);
                        await this.InterceptAsync(new HttpSendingClientChangesRequestArgs(changesToSend, tmpRowsSendedCount, clientChanges.ClientBatchInfo.RowsCount, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

                        response = await this.ProcessRequestAsync(changesToSend, HttpStep.SendChangesInProgress, this.Options.BatchSize, cancellationToken, progress).ConfigureAwait(false);

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

            // Create the BatchInfo
            var serverBatchInfo = new BatchInfo();

            try
            {
                context.SyncStage = SyncStage.ChangesSelecting;
                var initialPctProgress = 0.55;
                context.ProgressPercentage = initialPctProgress;

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
                var batchDirectoryName = string.Concat("WEB_REMOTE_GETCHANGES_", DateTime.UtcNow.ToString("yyyy_MM_dd_ss"), Path.GetRandomFileName().Replace(".", ""));

                serverBatchInfo.DirectoryRoot = batchDirectoryRoot;
                serverBatchInfo.DirectoryName = batchDirectoryName;

                await DownladBatchInfoAsync(context, schema, serverBatchInfo, summaryResponseContent, cancellationToken, progress).ConfigureAwait(false);

                // generate the new scope item
                this.CompleteTime = DateTime.UtcNow;


                var serverSyncChanges = new ServerSyncChanges(
                    summaryResponseContent.RemoteClientTimestamp,
                    serverBatchInfo,
                    summaryResponseContent.ServerChangesSelected,
                    summaryResponseContent.ClientChangesApplied);


                return (context, serverSyncChanges, summaryResponseContent.ConflictResolutionPolicy);
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
