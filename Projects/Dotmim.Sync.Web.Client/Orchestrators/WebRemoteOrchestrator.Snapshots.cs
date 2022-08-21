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


        internal override async Task<(SyncContext context, long RemoteClientTimestamp, BatchInfo ServerBatchInfo, DatabaseChangesSelected DatabaseChangesSelected)>
          InternalGetSnapshotAsync(ServerScopeInfo serverScopeInfo, SyncContext context, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            // Make a remote call to get Schema from remote provider
            if (serverScopeInfo.Schema == null)
            {
                (context, serverScopeInfo) = await this.InternalGetServerScopeInfoAsync(
                    context, null, false, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                serverScopeInfo.Schema.EnsureSchema();
            }

            // Generate a batch directory
            var batchDirectoryRoot = this.Options.BatchDirectory;
            var batchDirectoryName = string.Concat(DateTime.UtcNow.ToString("yyyy_MM_dd_ss"), Path.GetRandomFileName().Replace(".", ""));
            var batchDirectoryFullPath = Path.Combine(batchDirectoryRoot, batchDirectoryName);

            if (!Directory.Exists(batchDirectoryFullPath))
                Directory.CreateDirectory(batchDirectoryFullPath);

            // Create the BatchInfo serialized (forced because in a snapshot call, so we are obviously serialized on disk)
            var serverBatchInfo = new BatchInfo(serverScopeInfo.Schema, batchDirectoryRoot, batchDirectoryName);

            // Firstly, get the snapshot summary
            var changesToSend = new HttpMessageSendChangesRequest(context, null);
            var serializer = this.SerializerFactory.GetSerializer<HttpMessageSendChangesRequest>();
            var binaryData = await serializer.SerializeAsync(changesToSend);
            
            var response0 = await this.httpRequestHandler.ProcessRequestAsync(
              this.HttpClient, context, this.ServiceUri, binaryData, HttpStep.GetSummary, 
              this.SerializerFactory, this.Converter, 0, this.SyncPolicy, cancellationToken, progress).ConfigureAwait(false);

            HttpMessageSummaryResponse summaryResponseContent = null;

            using (var streamResponse = await response0.Content.ReadAsStreamAsync().ConfigureAwait(false))
            {
                var responseSerializer = this.SerializerFactory.GetSerializer<HttpMessageSummaryResponse>();

                if (streamResponse.CanRead)
                {
                    summaryResponseContent = await responseSerializer.DeserializeAsync(streamResponse);

                    serverBatchInfo.RowsCount = summaryResponseContent.BatchInfo?.RowsCount ?? 0;

                    if (summaryResponseContent.BatchInfo?.BatchPartsInfo != null)
                        foreach (var bpi in summaryResponseContent.BatchInfo.BatchPartsInfo)
                            serverBatchInfo.BatchPartsInfo.Add(bpi);
                }

            }

            if (summaryResponseContent == null)
                throw new Exception("Summary can't be null");

            context = summaryResponseContent.SyncContext;

            // no snapshot
            if ((serverBatchInfo.BatchPartsInfo == null || serverBatchInfo.BatchPartsInfo.Count <= 0) && serverBatchInfo.RowsCount <= 0)
                return (context, 0, null, new DatabaseChangesSelected());

            // If we have a snapshot we are raising the batches downloading process that will occurs
            await this.InterceptAsync(new HttpBatchesDownloadingArgs(context, serverBatchInfo, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

            await serverBatchInfo.BatchPartsInfo.ForEachAsync(async bpi =>
            {
                var changesToSend3 = new HttpMessageGetMoreChangesRequest(context, bpi.Index);
                var serializer3 = this.SerializerFactory.GetSerializer<HttpMessageGetMoreChangesRequest>();
                var binaryData3 = await serializer3.SerializeAsync(changesToSend3);

                await this.InterceptAsync(new HttpGettingServerChangesRequestArgs(bpi.Index, serverBatchInfo.BatchPartsInfo.Count, summaryResponseContent.SyncContext, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

                var response = await this.httpRequestHandler.ProcessRequestAsync(
                  this.HttpClient, context, this.ServiceUri, binaryData3, HttpStep.GetMoreChanges,
                  this.SerializerFactory, this.Converter, 0, this.SyncPolicy, cancellationToken, progress).ConfigureAwait(false);

                if (this.SerializerFactory.Key != "json")
                {
                    var s = this.SerializerFactory.GetSerializer<HttpMessageSendChangesResponse>();
                    using var responseStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
                    var getMoreChanges = await s.DeserializeAsync(responseStream);

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
                        var schemaTable = CreateChangesTable(serverScopeInfo.Schema.Tables[table.TableName, table.SchemaName]);

                        var fullPath = Path.Combine(batchDirectoryFullPath, bpi.FileName);

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
                    await SerializeAsync(response, bpi.FileName, batchDirectoryFullPath, this).ConfigureAwait(false);
                }


                // Raise response from server containing a batch changes 
                await this.InterceptAsync(new HttpGettingServerChangesResponseArgs(serverBatchInfo, bpi.Index, bpi.RowsCount, summaryResponseContent.SyncContext, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);
            });

            await this.InterceptAsync(new HttpBatchesDownloadedArgs(summaryResponseContent, summaryResponseContent.SyncContext, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

            return (context, summaryResponseContent.RemoteClientTimestamp, serverBatchInfo, summaryResponseContent.ServerChangesSelected);
        }

    }
}
