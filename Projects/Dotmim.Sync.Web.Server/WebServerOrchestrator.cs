using Dotmim.Sync.Batch;
using Dotmim.Sync.Web.Client;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync.Web.Server
{
    public class WebServerOrchestrator : RemoteOrchestrator
    {
        public WebServerOrchestrator(CoreProvider provider)
        {
            this.Provider = provider;
            this.Schema = new SyncSchema();
            this.Options = new SyncOptions();
        }

        /// <summary>
        /// Set Sync Configuration parameters
        /// </summary>
        public void SetSchema(Action<SyncSchema> onSchema)
            => onSchema?.Invoke(this.Schema);

        /// <summary>
        /// Set Sync Options parameters
        /// </summary>
        public void SetOptions(Action<SyncOptions> onOptions)
            => onOptions?.Invoke(this.Options);

        internal async Task<HttpMessage> GetResponseMessageAsync(HttpMessage httpMessage,
              CancellationToken cancellationToken)
        {
            HttpMessage httpMessageResponse = null;
            switch (httpMessage.Step)
            {
                case HttpStep.EnsureScopes:
                    httpMessageResponse = await this.EnsureScopeAsync(httpMessage, cancellationToken).ConfigureAwait(false);
                    break;
                case HttpStep.SendChanges:
                    httpMessageResponse = await this.ApplyThenGetChangesAsync(httpMessage, cancellationToken).ConfigureAwait(false);
                    break;
                case HttpStep.GetChanges:
                    httpMessageResponse = this.GetMoreChanges(httpMessage, cancellationToken);
                    break;
            }

            return httpMessageResponse;
        }

        private async Task<HttpMessage> EnsureScopeAsync(HttpMessage httpMessage, CancellationToken cancellationToken)
        {
            HttpMessageEnsureScopesRequest httpMessageEnsureScopes;
            if (httpMessage.Content is HttpMessageEnsureScopesRequest)
                httpMessageEnsureScopes = httpMessage.Content as HttpMessageEnsureScopesRequest;
            else
                httpMessageEnsureScopes = (httpMessage.Content as JObject).ToObject<HttpMessageEnsureScopesRequest>();

            if (httpMessageEnsureScopes == null)
                throw new ArgumentException("EnsureScopesAsync message could not be null");

            var clientScopeId = httpMessageEnsureScopes.ClientReferenceId;

            var (syncContext, serverScopeInfo, localScopeReferenceInfo, newSchema) = await this.EnsureScopeAsync(
                httpMessage.SyncContext, this.Schema, this.Options, clientScopeId, cancellationToken).ConfigureAwait(false);

            // Create the return value
            var returnValue = new HttpMessageEnsureScopesResponse(serverScopeInfo, localScopeReferenceInfo, newSchema);

            httpMessage.SyncContext = syncContext;
            httpMessage.Content = returnValue;

            return httpMessage;
        }


        /// <summary>
        /// This method is only used when batch mode is enabled on server and we need to send back mor BatchPartInfo 
        /// </summary>
        private HttpMessage GetMoreChanges(HttpMessage httpMessage, CancellationToken cancellationToken)
        {
            HttpMessageGetMoreChangesRequest httpMessageContent;
            if (httpMessage.Content is HttpMessageGetMoreChangesRequest)
                httpMessageContent = httpMessage.Content as HttpMessageGetMoreChangesRequest;
            else
                httpMessageContent = (httpMessage.Content as JObject).ToObject<HttpMessageGetMoreChangesRequest>();

            // We are in batch mode here
            var serverBatchInfo = this.Provider.CacheManager.GetValue<BatchInfo>("GetChangeBatch_BatchInfo");
            var stats = this.Provider.CacheManager.GetValue<DatabaseChangesSelected>("GetChangeBatch_ChangesSelected");

            if (serverBatchInfo == null)
                throw new ArgumentNullException("batchInfo stored in session can't be null if request more batch part info.");

            var changesResponse = new HttpMessageSendChangesResponse();

            // Get the first batch part info
            var batchPartInfo = serverBatchInfo.BatchPartsInfo.First(d => d.Index == httpMessageContent.BatchIndexRequested);
            batchPartInfo.LoadBatch();

            changesResponse.ChangesSelected = stats;
            changesResponse.Changes = batchPartInfo.Set;
            changesResponse.BatchIndex = httpMessageContent.BatchIndexRequested;
            changesResponse.IsLastBatch = batchPartInfo.IsLastBatch;

            var response = new HttpMessage
            {
                Content = changesResponse,
                Step = batchPartInfo.IsLastBatch ? HttpStep.SendChanges : HttpStep.InProgress,
                SyncContext = httpMessage.SyncContext
            };

            // If we have only one bpi, we can safely delete it
            if (batchPartInfo.IsLastBatch)
            {
                this.Provider.CacheManager.Remove("GetChangeBatch_BatchInfo");
                this.Provider.CacheManager.Remove("GetChangeBatch_ChangesSelected");

                // delete the folder (not the BatchPartInfo, because we have a reference on it)
                if (this.Options.CleanMetadatas)
                    serverBatchInfo.TryRemoveDirectory();
            }

            return response;

        }


        private async Task<HttpMessage> ApplyThenGetChangesAsync(HttpMessage httpMessage, CancellationToken cancellationToken)
        {

            // ------------------------------------------------------------
            // FIRST STEP : receive client changes
            // ------------------------------------------------------------

            HttpMessageSendChangesRequest httpMessageContent;
            if (httpMessage.Content is HttpMessageSendChangesRequest)
                httpMessageContent = httpMessage.Content as HttpMessageSendChangesRequest;
            else
                httpMessageContent = (httpMessage.Content as JObject).ToObject<HttpMessageSendChangesRequest>();

            if (httpMessageContent == null)
                throw new ArgumentException("ApplyChanges message could not be null");

            // Get if we need to serialize data or making everything in memory
            var workInMemory = this.Options.BatchSize == 0;

            // BatchInfo containing all BatchPartInfo objects
            // Retrieve batchinfo instance if exists
            var batchInfo = this.Provider.CacheManager.GetValue<BatchInfo>("ApplyChanges_BatchInfo");

            // Create a new batch info
            if (batchInfo == null)
                batchInfo = new BatchInfo(workInMemory, this.Options.BatchDirectory);

            // Create the BatchPartInfo object based on the message received from client and add it to the batchInfo instance
            var bpi = batchInfo.GenerateBatchInfo(httpMessageContent.BatchIndex, httpMessageContent.Changes);

            // We may have the last batch sent from client. So specify it in tbe BatchPartInfo
            bpi.IsLastBatch = httpMessageContent.IsLastBatch;

            // Save the BatchInfo
            this.Provider.CacheManager.Set("ApplyChanges_BatchInfo", batchInfo);

            // Clear the httpMessage set
            if (!workInMemory && httpMessageContent != null && httpMessageContent.Changes != null)
                httpMessageContent.Changes.Clear();

            // Until we don't have received all the batches, wait for more
            if (!httpMessageContent.IsLastBatch)
                return new HttpMessage() { SyncContext = httpMessage.SyncContext, Step = HttpStep.InProgress, Content = null };

            // ------------------------------------------------------------
            // SECOND STEP : apply then return server changes
            // ------------------------------------------------------------

            // Now all the batchs have been sent from client
            this.Provider.CacheManager.Remove("ApplyChanges_BatchInfo");

            // get changes
            var (context, serverBatchInfo, serverChangesSelected) =
               await this.ApplyThenGetChangesAsync(httpMessage.SyncContext,
                           httpMessageContent.FromScopeId, httpMessageContent.LocalScopeReferenceInfo,
                           httpMessageContent.ServerScopeInfo, batchInfo, cancellationToken).ConfigureAwait(false);


            // Create the http message response
            var response = new HttpMessage();
            response.SyncContext = context;
            response.Step = HttpStep.SendChanges;

            // Check if the serverBatchInfo is a batched
            // then send back the response (the first BPI
            var changesResponse = new HttpMessageSendChangesResponse();
            changesResponse.ChangesSelected = serverChangesSelected;

            response.Content = changesResponse;

            // If nothing to do, just send back
            if (serverBatchInfo.BatchPartsInfo.Count == 0)
            {
                changesResponse.IsLastBatch = true;
                return response;
            }

            // Get the first batch part info
            var firstServerBatchPartInfo = serverBatchInfo.BatchPartsInfo.First(d => d.Index == 0);

            // if we are not in memory, we set the BI in session, to be able to get it back on next request
            if (!workInMemory)
            {
                // Save the BatchInfo
                this.Provider.CacheManager.Set("GetChangeBatch_BatchInfo", serverBatchInfo);
                this.Provider.CacheManager.Set("GetChangeBatch_ChangesSelected", serverChangesSelected);

                // load the batchpart set directly, to be able to send it back
                firstServerBatchPartInfo.LoadBatch();
            }

            changesResponse.Changes = firstServerBatchPartInfo.Set;
            response.Step = firstServerBatchPartInfo.IsLastBatch ?HttpStep.SendChanges : HttpStep.InProgress;
            changesResponse.IsLastBatch = firstServerBatchPartInfo.IsLastBatch;

            // If we have only one bpi, we can safely delete it
            if (!workInMemory && firstServerBatchPartInfo.IsLastBatch)
            {
                this.Provider.CacheManager.Remove("GetChangeBatch_BatchInfo");
                this.Provider.CacheManager.Remove("GetChangeBatch_ChangesSelected");

                // delete the folder (not the BatchPartInfo, because we have a reference on it)
                if (this.Options.CleanMetadatas)
                    serverBatchInfo.TryRemoveDirectory();
            }

            return response;

        }
    }
}
