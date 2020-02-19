using Dotmim.Sync.Batch;

using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Dotmim.Sync.Web.Client;
using Microsoft.AspNetCore.Http;
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

        public WebServerOrchestrator() => this.Options = new WebServerOptions();

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
        public SyncSet Schema { get; set; }

        /// <summary>
        /// Client converter used
        /// </summary>
        public IConverter ClientConverter { get; set; }


        /// <summary>
        /// Interceptor just before sending back changes
        /// </summary>
        public void OnSendingChanges(Action<HttpMessageSendChangesResponseArgs> action) => this.On(action);

        /// <summary>
        /// Interceptor just before sending back scopes
        /// </summary>
        public void OnSendingScopes(Action<HttpMessageEnsureScopesResponseArgs> action) => this.On(action);

        internal async Task<HttpMessageEnsureScopesResponse> EnsureScopeAsync(HttpMessageEnsureScopesRequest httpMessage, SessionCache sessionCache, CancellationToken cancellationToken)
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
        internal async Task<HttpMessageSendChangesResponse> GetSnapshotAsync(
            HttpMessageSendChangesRequest httpMessage, SessionCache sessionCache, CancellationToken cancellationToken)
        {
            // Check schema.
            // If client has stored the schema, the EnsureScope will not be called on server.
            if (this.Schema == null || !this.Schema.HasTables || !this.Schema.HasColumns)
            {
                var (_, newSchema) = await this.EnsureSchemaAsync(
                    httpMessage.SyncContext, this.Setup, cancellationToken).ConfigureAwait(false);

                newSchema.EnsureSchema();
                this.Schema = newSchema;
            }

            // get changes
            var snap = await this.GetSnapshotAsync(httpMessage.SyncContext, httpMessage.Scope, this.Schema,
                                           this.Options.SnapshotsDirectory, this.Options.BatchDirectory, cancellationToken).ConfigureAwait(false);

            sessionCache.RemoteClientTimestamp = snap.remoteClientTimestamp;
            sessionCache.ServerBatchInfo = snap.serverBatchInfo;

            // if no snapshot, return empty response
            if (snap.serverBatchInfo == null)
            {
                var changesResponse = new HttpMessageSendChangesResponse(snap.context);
                changesResponse.ServerStep = HttpStep.GetSnapshot;
                changesResponse.BatchIndex = 0;
                changesResponse.IsLastBatch = true;
                changesResponse.RemoteClientTimestamp = snap.remoteClientTimestamp;
                changesResponse.Changes = new ContainerSet();
                return changesResponse;
            }


            // Get the firt response to send back to client
            return await GetChangesResponseAsync(snap.context, snap.remoteClientTimestamp, snap.serverBatchInfo, null, 0, ConflictResolutionPolicy.ServerWins);
        }

        /// <summary>
        /// Get changes from 
        /// </summary>
        internal async Task<HttpMessageSendChangesResponse> ApplyThenGetChangesAsync(
            HttpMessageSendChangesRequest httpMessage, SessionCache sessionCache, int clientBatchSize, CancellationToken cancellationToken)
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

            // Get batch info from session cache if exists, otherwise create it
            if (sessionCache.ClientBatchInfo == null)
                sessionCache.ClientBatchInfo = new BatchInfo(clientWorkInMemory, Schema, this.Options.BatchDirectory);

            // create the in memory changes set
            var changesSet = new SyncSet(Schema.ScopeName);

            foreach (var table in httpMessage.Changes.Tables)
            {
                DbSyncAdapter.CreateChangesTable(Schema.Tables[table.TableName, table.SchemaName], changesSet);
            }

            changesSet.ImportContainerSet(httpMessage.Changes, false);

            // If client has made a conversion on each line, apply the reverse side of it
            if (this.ClientConverter != null && changesSet.HasRows)
                AfterDeserializedRows(changesSet, this.ClientConverter);

            // add changes to the batch info
            await sessionCache.ClientBatchInfo.AddChangesAsync(changesSet, httpMessage.BatchIndex, httpMessage.IsLastBatch);


            // Clear the httpMessage set
            if (!clientWorkInMemory && httpMessage.Changes != null)
                httpMessage.Changes.Clear();

            // Until we don't have received all the batches, wait for more
            if (!httpMessage.IsLastBatch)
                return new HttpMessageSendChangesResponse(httpMessage.SyncContext) { ServerStep = HttpStep.SendChangesInProgress };

            // ------------------------------------------------------------
            // SECOND STEP : apply then return server changes
            // ------------------------------------------------------------

            // get changes
            var (context, remoteClientTimestamp, serverBatchInfo, policy, serverChangesSelected) =
               await this.ApplyThenGetChangesAsync(httpMessage.SyncContext,
                           httpMessage.Scope, this.Schema, sessionCache.ClientBatchInfo, this.Options.DisableConstraintsOnApplyChanges,
                           this.Options.UseBulkOperations, false, this.Options.CleanFolder, clientBatchSize,
                           this.Options.BatchDirectory, this.Options.ConflictResolutionPolicy, cancellationToken).ConfigureAwait(false);


            // Save the server batch info object to cache if not working in memory
            if (!clientWorkInMemory)
            {
                sessionCache.RemoteClientTimestamp = remoteClientTimestamp;
                sessionCache.ServerBatchInfo = serverBatchInfo;
                sessionCache.ServerChangesSelected = serverChangesSelected;
            }

            // Get the firt response to send back to client
            return await GetChangesResponseAsync(context, remoteClientTimestamp, serverBatchInfo, serverChangesSelected, 0, policy);

        }

        /// <summary>
        /// This method is only used when batch mode is enabled on server and we need to send back mor BatchPartInfo 
        /// </summary>
        internal Task<HttpMessageSendChangesResponse> GetMoreChangesAsync(HttpMessageGetMoreChangesRequest httpMessage, SessionCache sessionCache, CancellationToken cancellationToken)
        {
            if (sessionCache.ServerBatchInfo == null)
                throw new ArgumentNullException("batchInfo stored in session can't be null if request more batch part info.");

            return GetChangesResponseAsync(httpMessage.SyncContext, sessionCache.RemoteClientTimestamp, sessionCache.ServerBatchInfo,
                sessionCache.ServerChangesSelected, httpMessage.BatchIndexRequested, this.Options.ConflictResolutionPolicy);
        }


        /// <summary>
        /// Create a response message content based on a requested index in a server batch info
        /// </summary>
        private async Task<HttpMessageSendChangesResponse> GetChangesResponseAsync(SyncContext syncContext, long remoteClientTimestamp, BatchInfo serverBatchInfo,
                                DatabaseChangesSelected serverChangesSelected, int batchIndexRequested, ConflictResolutionPolicy policy)
        {

            // 1) Create the http message content response
            var changesResponse = new HttpMessageSendChangesResponse(syncContext);
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

            await batchPartInfo.LoadBatchAsync(changesSet, serverBatchInfo.GetDirectoryFullPath());

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
                // delete the folder (not the BatchPartInfo, because we have a reference on it)
                if (this.Options.CleanFolder)
                {
                    var shouldDeleteFolder = true;
                    if (!string.IsNullOrEmpty(this.Options.SnapshotsDirectory))
                    {
                        var dirInfo = new DirectoryInfo(serverBatchInfo.DirectoryRoot);
                        var snapInfo = new DirectoryInfo(this.Options.SnapshotsDirectory);
                        shouldDeleteFolder = dirInfo.FullName != snapInfo.FullName;
                    }

                    if (shouldDeleteFolder)
                        serverBatchInfo.TryRemoveDirectory();
                }
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
