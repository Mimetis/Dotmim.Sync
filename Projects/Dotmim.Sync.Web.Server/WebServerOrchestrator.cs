using Dotmim.Sync.Batch;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Dotmim.Sync.Web.Client;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
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
        public WebServerOrchestrator(CoreProvider provider, WebServerOptions options = null, SyncSetup setup = null)
        {
            this.Setup = setup ?? new SyncSetup();
            this.Options = options ?? new WebServerOptions();
            this.Provider = provider;

        }

        public WebServerOrchestrator()
        {
            this.Options = new WebServerOptions();
        }

        /// <summary>
        /// Gets or Sets the Setup 
        /// </summary>
        public SyncSetup Setup { get; set; }

        /// <summary>
        /// Get or Set Web server options parameters
        /// </summary>
        public WebServerOptions Options { get; set; }

        /// <summary>
        /// Schema database
        /// </summary>
        public SyncSet Schema { get; private set; }

        /// <summary>
        /// Client converter used
        /// </summary>
        public IConverter ClientConverter { get; internal set; }

        internal async Task<HttpMessageEnsureScopesResponse> EnsureScopeAsync(HttpMessageEnsureScopesRequest httpMessage, CancellationToken cancellationToken)
        {

            if (httpMessage == null)
                throw new ArgumentException("EnsureScopesAsync message could not be null");

            if (this.Setup == null)
                throw new ArgumentException("You need to set the tables to sync on server side");

            // We can use default options on server
            this.Options = this.Options ?? new WebServerOptions();

            var (syncContext, newSchema) = await this.EnsureSchemaAsync(
                httpMessage.SyncContext, this.Setup, cancellationToken).ConfigureAwait(false);

            this.Schema = newSchema;

            var httpResponse = new HttpMessageEnsureScopesResponse(syncContext, newSchema);

            return httpResponse;


        }


        /// <summary>
        /// Get changes from 
        /// </summary>
        internal async Task<HttpMessageSendChangesResponse> ApplyThenGetChangesAsync(
            HttpMessageSendChangesRequest httpMessage, int clientBatchSize, CancellationToken cancellationToken)
        {

            // Get if we need to serialize data or making everything in memory
            var clientWorkInMemory = clientBatchSize == 0;

            // Check schema.
            // If client has stored the schema, the EnsureScope will not be called on server.
            if (this.Schema == null || !this.Schema.HasTables || !this.Schema.HasColumns)
            {
                var (_, newSchema) = await this.EnsureSchemaAsync(
                    httpMessage.SyncContext, this.Setup, cancellationToken).ConfigureAwait(false);

                newSchema.EnsureSchema();
                this.Schema = newSchema;
            }


            // ------------------------------------------------------------
            // FIRST STEP : receive client changes
            // ------------------------------------------------------------

            // We are receiving changes from client
            // BatchInfo containing all BatchPartInfo objects
            // Retrieve batchinfo instance if exists
            var batchInfo = this.Provider.CacheManager.GetValue<BatchInfo>("ApplyChanges_BatchInfo");

            // Create a new batch info
            if (batchInfo == null)
                batchInfo = new BatchInfo(clientWorkInMemory, Schema, this.Options.BatchDirectory);

            // create the in memory changes set
            var changesSet = new SyncSet(Schema.ScopeName);

            foreach (var table in httpMessage.Changes.Tables)
            {
                DbSyncAdapter.CreateChangesTable(Schema.Tables[table.TableName, table.SchemaName], changesSet);
            }

            changesSet.ImportContainerSet(httpMessage.Changes);

            // If client has made a conversion on each line, apply the reverse side of it
            if (this.ClientConverter != null && changesSet.HasRows)
                AfterDeserializedRows(changesSet, this.ClientConverter);

            // add changes to the batch info
            batchInfo.AddChanges(changesSet, httpMessage.BatchIndex, httpMessage.IsLastBatch);

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
            var (context, remoteClientTimestamp, serverBatchInfo, policy, serverChangesSelected) =
               await this.ApplyThenGetChangesAsync(httpMessage.SyncContext,
                           httpMessage.Scope, this.Schema, batchInfo, this.Options.DisableConstraintsOnApplyChanges,
                           this.Options.UseBulkOperations, this.Options.CleanMetadatas, clientBatchSize,
                           this.Options.BatchDirectory, this.Options.ConflictResolutionPolicy, cancellationToken).ConfigureAwait(false);


            // Save the server batch info object to cache if not working in memory
            if (!clientWorkInMemory)
            {
                // Save the BatchInfo
                this.Provider.CacheManager.Set("GetChangeBatch_RemoteClientTimestamp", remoteClientTimestamp);
                this.Provider.CacheManager.Set("GetChangeBatch_BatchInfo", serverBatchInfo);
                this.Provider.CacheManager.Set("GetChangeBatch_ChangesSelected", serverChangesSelected);
            }

            // Get the firt response to send back to client
            return GetChangesResponse(context, remoteClientTimestamp, serverBatchInfo, serverChangesSelected, 0, policy);

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

            return GetChangesResponse(httpMessage.SyncContext, remoteClienTimestamp, serverBatchInfo,
                serverChangesSelected, httpMessage.BatchIndexRequested, this.Options.ConflictResolutionPolicy);
        }



        /// <summary>
        /// Create a response message content based on a requested index in a server batch info
        /// </summary>
        private HttpMessageSendChangesResponse GetChangesResponse(SyncContext context, long remoteClientTimestamp, BatchInfo serverBatchInfo,
                                DatabaseChangesSelected serverChangesSelected, int batchIndexRequested, ConflictResolutionPolicy policy)
        {

            // 1) Create the http message content response
            var changesResponse = new HttpMessageSendChangesResponse(context);
            changesResponse.ChangesSelected = serverChangesSelected;
            changesResponse.ServerStep = HttpStep.GetChanges;
            changesResponse.ConflictResolutionPolicy = policy;

            // If nothing to do, just send back
            if (serverBatchInfo.InMemory || serverBatchInfo.BatchPartsInfo.Count == 0)
            {
                if (this.ClientConverter != null && serverBatchInfo.InMemoryData.HasRows)
                    BeforeSerializeRows(serverBatchInfo.InMemoryData, this.ClientConverter);

                changesResponse.Changes = serverBatchInfo.InMemoryData.GetContainerSet();
                changesResponse.BatchIndex = 0;
                changesResponse.IsLastBatch = true;
                changesResponse.RemoteClientTimestamp = remoteClientTimestamp;
                return changesResponse;
            }

            // Get the batch part index requested
            var batchPartInfo = serverBatchInfo.BatchPartsInfo.First(d => d.Index == batchIndexRequested);

            // if we are not in memory, we set the BI in session, to be able to get it back on next request

            // create the in memory changes set
            var changesSet = new SyncSet(Schema.ScopeName);

            foreach (var table in Schema.Tables)
                DbSyncAdapter.CreateChangesTable(Schema.Tables[table.TableName, table.SchemaName], changesSet);

            batchPartInfo.LoadBatch(changesSet);

            // if client request a conversion on each row, apply the conversion
            if (this.ClientConverter != null && batchPartInfo.Data.HasRows)
                BeforeSerializeRows(batchPartInfo.Data, this.ClientConverter);

            changesResponse.Changes = batchPartInfo.Data.GetContainerSet();

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
        /// Before serializing all rows, call the converter for each row
        /// </summary>
        public void BeforeSerializeRows(SyncSet data, IConverter converter)
        {
            foreach (var table in data.Tables)
            {
                if (table.Rows.Count > 0)
                {
                    foreach (var row in table.Rows)
                        converter.BeforeSerialize(row);

                }
            }
        }

        /// <summary>
        /// After deserializing all rows, call the converter for each row
        /// </summary>
        public void AfterDeserializedRows(SyncSet data, IConverter converter)
        {
            foreach (var table in data.Tables)
            {
                if (table.Rows.Count > 0)
                {
                    foreach (var row in table.Rows)
                        converter.AfterDeserialized(row);

                }
            }

        }

    }
}
