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
        /// We can't get changes from server, from a web client orchestrator
        /// </summary>
        public override async Task<ServerSyncChanges>
                GetChangesAsync(ClientScopeInfo clientScopeInfo, SyncParameters parameters = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), clientScopeInfo.Name);

            if (parameters != null)
                context.Parameters = parameters;

            SyncSet schema;
            ServerScopeInfo serverScopeInfo;

            // Need the server scope
            (context, serverScopeInfo) = await this.InternalGetServerScopeInfoAsync(context, clientScopeInfo.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            schema = serverScopeInfo.Schema;
            schema.EnsureSchema();

            clientScopeInfo.Schema = schema;
            clientScopeInfo.Setup = serverScopeInfo.Setup;
            clientScopeInfo.Version = serverScopeInfo.Version;

            var changesToSend = new HttpMessageSendChangesRequest(context, clientScopeInfo);

            var containerSet = new ContainerSet();
            changesToSend.Changes = containerSet;
            changesToSend.IsLastBatch = true;
            changesToSend.BatchIndex = 0;
            changesToSend.BatchCount = 0;

            context.ProgressPercentage += 0.125;

            await this.InterceptAsync(new HttpSendingClientChangesRequestArgs(changesToSend, 0, 0, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

            // serialize message
            var serializer = this.SerializerFactory.GetSerializer<HttpMessageSendChangesRequest>();
            var binaryData = await serializer.SerializeAsync(changesToSend);

            var response = await this.httpRequestHandler.ProcessRequestAsync
                (this.HttpClient, context, this.ServiceUri, binaryData, HttpStep.SendChangesInProgress,
                 this.SerializerFactory, this.Converter, this.Options.BatchSize, this.SyncPolicy, cancellationToken, progress).ConfigureAwait(false);



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

            // Create the BatchInfo
            var serverBatchInfo = new BatchInfo(schema);

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

            //-----------------------
            // In Batch Mode
            //-----------------------
            // From here, we need to serialize everything on disk

            // Generate the batch directory
            var batchDirectoryRoot = this.Options.BatchDirectory;
            var batchDirectoryName = string.Concat(DateTime.UtcNow.ToString("yyyy_MM_dd_ss"), Path.GetRandomFileName().Replace(".", ""));

            serverBatchInfo.DirectoryRoot = batchDirectoryRoot;
            serverBatchInfo.DirectoryName = batchDirectoryName;

            if (!Directory.Exists(serverBatchInfo.GetDirectoryFullPath()))
                Directory.CreateDirectory(serverBatchInfo.GetDirectoryFullPath());

            // hook to get the last batch part info at the end
            var bpis = serverBatchInfo.BatchPartsInfo.Where(bpi => !bpi.IsLastBatch);
            var lstbpi = serverBatchInfo.BatchPartsInfo.First(bpi => bpi.IsLastBatch);

            // function used to download one part
            var dl = new Func<BatchPartInfo, Task>(async (bpi) =>
            {
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

                // Serialize
                await SerializeAsync(response, bpi.FileName, serverBatchInfo.GetDirectoryFullPath(), this).ConfigureAwait(false);

                // Raise response from server containing a batch changes 
                await this.InterceptAsync(new HttpGettingServerChangesResponseArgs(serverBatchInfo, bpi.Index, bpi.RowsCount, summaryResponseContent.SyncContext, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

                response.Dispose();
            });

            // Parrallel download of all bpis except the last one (which will launch the delete directory on the server side)
            await bpis.ForEachAsync(bpi => dl(bpi), this.MaxDownladingDegreeOfParallelism).ConfigureAwait(false);

            // Download last batch part that will launch the server deletion of the tmp dir
            await dl(lstbpi).ConfigureAwait(false);

            // generate the new scope item
            this.CompleteTime = DateTime.UtcNow;

            // Reaffect context
            context = summaryResponseContent.SyncContext;

            return new ServerSyncChanges(summaryResponseContent.RemoteClientTimestamp, serverBatchInfo, summaryResponseContent.ServerChangesSelected);
        }



        /// <summary>
        /// We can't get changes from server, from a web client orchestrator
        /// </summary>
        public override async Task<ServerSyncChanges>
                GetEstimatedChangesCountAsync(ClientScopeInfo clientScopeInfo, SyncParameters parameters = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            var context = new SyncContext(Guid.NewGuid(), clientScopeInfo.Name);

            if (parameters != null)
                context.Parameters = parameters;

            SyncSet schema;
            ServerScopeInfo serverScopeInfo;

            // Need the server scope
            (context, serverScopeInfo) = await this.InternalGetServerScopeInfoAsync(context, clientScopeInfo.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            schema = serverScopeInfo.Schema;
            schema.EnsureSchema();

            clientScopeInfo.Schema = schema;
            clientScopeInfo.Setup = serverScopeInfo.Setup;
            clientScopeInfo.Version = serverScopeInfo.Version;


            // generate a message to send
            var changesToSend = new HttpMessageSendChangesRequest(context, clientScopeInfo)
            {
                Changes = null,
                IsLastBatch = true,
                BatchIndex = 0,
                BatchCount = 0
            };

            var serializer = this.SerializerFactory.GetSerializer<HttpMessageSendChangesRequest>();
            var binaryData = await serializer.SerializeAsync(changesToSend);

            // Raise progress for sending request and waiting server response
            await this.InterceptAsync(new HttpGettingServerChangesRequestArgs(0, 0, context, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

            // response
            var response = await this.httpRequestHandler.ProcessRequestAsync
                    (this.HttpClient, context, this.ServiceUri, binaryData, HttpStep.GetEstimatedChangesCount,
                     this.SerializerFactory, this.Converter, this.Options.BatchSize, this.SyncPolicy, cancellationToken, progress).ConfigureAwait(false);

            HttpMessageSendChangesResponse summaryResponseContent = null;

            using (var streamResponse = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
            {
                var responseSerializer = this.SerializerFactory.GetSerializer<HttpMessageSendChangesResponse>();

                if (streamResponse.CanRead)
                    summaryResponseContent = await responseSerializer.DeserializeAsync(streamResponse);

            }

            if (summaryResponseContent == null)
                throw new Exception("Summary can't be null");

            // generate the new scope item
            this.CompleteTime = DateTime.UtcNow;

            return new (summaryResponseContent.RemoteClientTimestamp, null, summaryResponseContent.ServerChangesSelected);
        }



    }
}
