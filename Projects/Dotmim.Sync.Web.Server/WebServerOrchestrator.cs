using Dotmim.Sync.Batch;
using Dotmim.Sync.Serialization;
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

        /// <summary>
        /// Default ctor. Using default options and schema
        /// </summary>
        /// <param name="provider"></param>
        public WebServerOrchestrator(CoreProvider provider, WebServerOptions options = null, SyncSchema schema = null) 
        {
            this.Schema = schema ?? new SyncSchema();
            this.Options = options ?? new WebServerOptions();
            this.Provider = provider;

        }

        /// <summary>
        /// Gets or Sets the Schema 
        /// </summary>
        public SyncSchema Schema { get; private set; }

        /// <summary>
        /// Get or Set Web server options parameters
        /// </summary>
        public WebServerOptions Options { get; private set; }


        internal async Task<HttpMessageEnsureScopesResponse> EnsureScopeAsync(HttpMessageEnsureScopesRequest httpMessage, CancellationToken cancellationToken)
        {
            if (httpMessage == null)
                throw new ArgumentException("EnsureScopesAsync message could not be null");

            if (this.Schema == null)
                throw new ArgumentException("You need to set the tables to sync on server side");

            if (this.Options == null)
                throw new ArgumentException("You need to set the optins used on server side");

            var (syncContext, newSchema) = await this.EnsureSchemaAsync(
                httpMessage.SyncContext, this.Schema, cancellationToken).ConfigureAwait(false);

            this.Schema = newSchema;

            // Create the return value
            var httpResponse = new HttpMessageEnsureScopesResponse(syncContext, newSchema);

            return httpResponse;
        }


        /// <summary>
        /// Create a response message content based on a requested index in a server batch info
        /// </summary>
        private HttpMessageSendChangesResponse GetChangesResponse(SyncContext context, long remoteClientTimestamp, BatchInfo serverBatchInfo,
                                DatabaseChangesSelected serverChangesSelected, int batchIndexRequested)
        {

            // 1) Create the http message content response
            var changesResponse = new HttpMessageSendChangesResponse(context);
            changesResponse.ChangesSelected = serverChangesSelected;
            changesResponse.ServerStep = HttpStep.GetChanges;

            // If nothing to do, just send back
            if (serverBatchInfo.InMemory || serverBatchInfo.BatchPartsInfo.Count == 0)
            {
                changesResponse.Changes = serverBatchInfo.InMemoryData;
                changesResponse.BatchIndex = 0;
                changesResponse.IsLastBatch = true;
                changesResponse.RemoteClientTimestamp = remoteClientTimestamp;
                return changesResponse;
            }

            // Get the batch part index requested
            var batchPartInfo = serverBatchInfo.BatchPartsInfo.First(d => d.Index == batchIndexRequested);

            // if we are not in memory, we set the BI in session, to be able to get it back on next request
            batchPartInfo.LoadBatch();

            changesResponse.Changes = batchPartInfo.Data;
            changesResponse.BatchIndex = batchIndexRequested;
            changesResponse.IsLastBatch = batchPartInfo.IsLastBatch;
            changesResponse.RemoteClientTimestamp = remoteClientTimestamp;
            changesResponse.ServerStep = batchPartInfo.IsLastBatch ? HttpStep.GetChanges : HttpStep.GetChangesInProgress;

            // If we have only one bpi, we can safely delete it
            if (batchPartInfo.IsLastBatch)
            {
                this.Provider.CacheManager.Remove("GetChangeBatch_BatchInfo");
                this.Provider.CacheManager.Remove("GetChangeBatch_ChangesSelected");
                this.Provider.CacheManager.Remove("GetChangeBatch_RemoteClientTimestamp");

                // delete the folder (not the BatchPartInfo, because we have a reference on it)
                if (this.Options.CleanMetadatas)
                    serverBatchInfo.TryRemoveDirectory();
            }

            return changesResponse;
        }

        /// <summary>
        /// Get changes from 
        /// </summary>
        internal async Task<HttpMessageSendChangesResponse> ApplyThenGetChangesAsync(
            HttpMessageSendChangesRequest httpMessage, int clientBatchSize, CancellationToken cancellationToken)
        {

            // Get if we need to serialize data or making everything in memory
            var clientWorkInMemory = clientBatchSize == 0;
            // ------------------------------------------------------------
            // FIRST STEP : receive client changes
            // ------------------------------------------------------------

            // We are receiving changes from client
            // BatchInfo containing all BatchPartInfo objects
            // Retrieve batchinfo instance if exists
            var batchInfo = this.Provider.CacheManager.GetValue<BatchInfo>("ApplyChanges_BatchInfo");

            // Create a new batch info
            if (batchInfo == null)
                batchInfo = new BatchInfo(clientWorkInMemory, this.Options.BatchDirectory);

            // add changes to the batch info
            batchInfo.AddChanges(httpMessage.Changes, httpMessage.BatchIndex, httpMessage.IsLastBatch);

            // Save the BatchInfo
            this.Provider.CacheManager.Set("ApplyChanges_BatchInfo", batchInfo);

            // Clear the httpMessage set
            if (!clientWorkInMemory && httpMessage.Changes != null)
                httpMessage.Changes.Clear();

            // Until we don't have received all the batches, wait for more
            if (!httpMessage.IsLastBatch)
                return new HttpMessageSendChangesResponse(httpMessage.SyncContext) { ServerStep = HttpStep.SendChangesInProgress };

            // Now all the batchs have been sent from client
            this.Provider.CacheManager.Remove("ApplyChanges_BatchInfo");

            // ------------------------------------------------------------
            // SECOND STEP : apply then return server changes
            // ------------------------------------------------------------

            // get changes
            var (context, remoteClientTimestamp, serverBatchInfo, serverChangesSelected) =
               await this.ApplyThenGetChangesAsync(httpMessage.SyncContext,
                           httpMessage.Scope, this.Schema, batchInfo, this.Options.DisableConstraintsOnApplyChanges,
                           this.Options.UseBulkOperations, this.Options.CleanMetadatas, clientBatchSize, this.Options.BatchDirectory, cancellationToken).ConfigureAwait(false);


            // Save the server batch info object to cache if not working in memory
            if (!clientWorkInMemory)
            {
                // Save the BatchInfo
                this.Provider.CacheManager.Set("GetChangeBatch_RemoteClientTimestamp", remoteClientTimestamp);
                this.Provider.CacheManager.Set("GetChangeBatch_BatchInfo", serverBatchInfo);
                this.Provider.CacheManager.Set("GetChangeBatch_ChangesSelected", serverChangesSelected);
            }

            // Get the firt response to send back to client
            return GetChangesResponse(context, remoteClientTimestamp, serverBatchInfo, serverChangesSelected, 0);

        }


        /// <summary>
        /// This method is only used when batch mode is enabled on server and we need to send back mor BatchPartInfo 
        /// </summary>
        internal HttpMessageSendChangesResponse GetMoreChanges(HttpMessageGetMoreChangesRequest httpMessage, CancellationToken cancellationToken)
        {
            // We are in batch mode here
            var serverBatchInfo = this.Provider.CacheManager.GetValue<BatchInfo>("GetChangeBatch_BatchInfo");
            var serverChangesSelected = this.Provider.CacheManager.GetValue<DatabaseChangesSelected>("GetChangeBatch_ChangesSelected");
            var remoteClienTimestamp = this.Provider.CacheManager.GetValue<long>("GetChangeBatch_RemoteClientTimestamp");

            if (serverBatchInfo == null)
                throw new ArgumentNullException("batchInfo stored in session can't be null if request more batch part info.");

            return GetChangesResponse(httpMessage.SyncContext, remoteClienTimestamp, serverBatchInfo, serverChangesSelected, httpMessage.BatchIndexRequested);
        }


    }
}
