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
            var serverChangesSelected = this.Provider.CacheManager.GetValue<DatabaseChangesSelected>("GetChangeBatch_ChangesSelected");

            if (serverBatchInfo == null)
                throw new ArgumentNullException("batchInfo stored in session can't be null if request more batch part info.");

            // Get if we need to serialize data or making everything in memory
            var workInMemory = this.Options.BatchSize == 0;

            return GetChangesResponse(httpMessage.SyncContext, serverBatchInfo, serverChangesSelected, httpMessageContent.BatchIndexRequested);
        }


        /// <summary>
        /// Create a response message content based on a requested index in a server batch info
        /// </summary>
        private HttpMessage GetChangesResponse(SyncContext context, BatchInfo serverBatchInfo,
                                DatabaseChangesSelected serverChangesSelected, int batchIndexRequested)
        {

            // 1) Create the http message response
            var response = new HttpMessage();
            response.SyncContext = context;
            response.Step = HttpStep.GetChanges;

            // 2) Create the http message content response
            var changesResponse = new HttpMessageSendChangesResponse();

            changesResponse.ChangesSelected = serverChangesSelected;
            response.Content = changesResponse;

            // If nothing to do, just send back
            if (serverBatchInfo.InMemory || serverBatchInfo.BatchPartsInfo.Count == 0)
            {
                changesResponse.Changes = serverBatchInfo.InMemoryData;
                changesResponse.BatchIndex = 0;
                changesResponse.IsLastBatch = true;
                return response;
            }

            // Get the batch part index requested
            var batchPartInfo = serverBatchInfo.BatchPartsInfo.First(d => d.Index == batchIndexRequested);

            // if we are not in memory, we set the BI in session, to be able to get it back on next request
            batchPartInfo.LoadBatch();

            changesResponse.Changes = batchPartInfo.Data;
            changesResponse.BatchIndex = batchIndexRequested;
            changesResponse.IsLastBatch = batchPartInfo.IsLastBatch;
            response.Step = batchPartInfo.IsLastBatch ? HttpStep.GetChanges : HttpStep.GetChangesInProgress;

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

        /// <summary>
        /// Get changes from 
        /// </summary>
        private async Task<HttpMessage> ApplyThenGetChangesAsync(HttpMessage httpMessage, CancellationToken cancellationToken)
        {
            #region Load content
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

            // BatchInfo containing all the changes from the client 
            BatchInfo batchInfo = null;

            // Get if we need to serialize data or making everything in memory
            var workInMemory = this.Options.BatchSize == 0;
            #endregion

            // We are receiving changes from client
            if (httpMessage.Step == HttpStep.SendChanges)
            {
                // BatchInfo containing all BatchPartInfo objects
                // Retrieve batchinfo instance if exists
                batchInfo = this.Provider.CacheManager.GetValue<BatchInfo>("ApplyChanges_BatchInfo");

                // Create a new batch info
                if (batchInfo == null)
                    batchInfo = new BatchInfo(workInMemory, this.Options.BatchDirectory);

                // add changes to the batch info
                batchInfo.AddChanges(httpMessageContent.Changes, httpMessageContent.BatchIndex, httpMessageContent.IsLastBatch);

                // Save the BatchInfo
                this.Provider.CacheManager.Set("ApplyChanges_BatchInfo", batchInfo);

                // Clear the httpMessage set
                if (!workInMemory && httpMessageContent != null && httpMessageContent.Changes != null)
                    httpMessageContent.Changes.Clear();

                // Until we don't have received all the batches, wait for more
                if (!httpMessageContent.IsLastBatch)
                    return new HttpMessage() { SyncContext = httpMessage.SyncContext, Step = HttpStep.SendChangesInProgress, Content = null };
            }

            // Now all the batchs have been sent from client
            this.Provider.CacheManager.Remove("ApplyChanges_BatchInfo");

            // ------------------------------------------------------------
            // SECOND STEP : apply then return server changes
            // ------------------------------------------------------------

            // get changes
            var (context, serverBatchInfo, serverChangesSelected) =
               await this.ApplyThenGetChangesAsync(httpMessage.SyncContext,
                           httpMessageContent.FromScopeId, httpMessageContent.LocalScopeReferenceInfo,
                           httpMessageContent.ServerScopeInfo, batchInfo, cancellationToken).ConfigureAwait(false);


            // Save the server batch info object to cache if not working in memory
            if (!workInMemory)
            {
                // Save the BatchInfo
                this.Provider.CacheManager.Set("GetChangeBatch_BatchInfo", serverBatchInfo);
                this.Provider.CacheManager.Set("GetChangeBatch_ChangesSelected", serverChangesSelected);
            }

            // Get the firt response to send back to client
            return GetChangesResponse(context, serverBatchInfo, serverChangesSelected, 0);

        }
    }
}
