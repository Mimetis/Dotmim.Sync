using System;
using System.Collections.Generic;
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
    public class WebClientOrchestrator : IRemoteOrchestrator
    {
        private readonly HttpRequestHandler httpRequestHandler = new HttpRequestHandler();

        // Collection of Interceptors
        private Interceptors interceptors = new Interceptors();

        /// <summary>
        /// Gets or Sets the provider used in this proxy Orchestrator
        /// Should be null. CoreProvider is only used on the remote side (WebProxyServerProvider)
        /// </summary>
        public CoreProvider Provider { get => null; set => throw new NotSupportedException("Proxy Web does not need any provider. Everything is made on the server side"); }

        /// <summary>
        /// Set an interceptor to get info on the current sync process
        /// </summary>
        public void On<T>(Func<T, Task> interceptorFunc) where T : ProgressArgs =>
            this.interceptors.GetInterceptor<T>().Set(interceptorFunc);

        /// <summary>
        /// Set an interceptor to get info on the current sync process
        /// </summary>
        public void On<T>(Action<T> interceptorAction) where T : ProgressArgs =>
            this.interceptors.GetInterceptor<T>().Set(interceptorAction);


        /// <summary>
        /// Set a collection of interceptors
        /// </summary>
        public void On(Interceptors interceptors) => this.interceptors = interceptors;

        /// <summary>
        /// Returns the Task associated with given type of BaseArgs 
        /// Because we are not doing anything else than just returning a task, no need to use async / await. Just return the Task itself
        /// </summary>
        public Task InterceptAsync<T>(T args) where T : ProgressArgs
        {
            if (this.interceptors == null)
                return Task.CompletedTask;

            var interceptor = this.interceptors.GetInterceptor<T>();
            return interceptor.RunAsync(args);
        }

        /// <summary>
        /// Interceptor just before sending changes
        /// </summary>
        public void OnSendingChanges(Action<HttpMessageSendChangesRequestArgs> action) => this.On(action);

        /// <summary>
        /// Interceptor just before asking for more changes
        /// </summary>
        public void OnSendingGetMoreChanges(Action<HttpMessageGetMoreChangesRequestArgs> action) => this.On(action);

        /// <summary>
        /// Interceptor just before sending scopes
        /// </summary>
        public void OnSendingScopes(Action<HttpMessageEnsureScopesRequestArgs> action) => this.On(action);


        public Dictionary<string, string> CustomHeaders => this.httpRequestHandler.CustomHeaders;
        public Dictionary<string, string> ScopeParameters => this.httpRequestHandler.ScopeParameters;


        /// <summary>
        /// Gets or Sets Serializer used by the web client orchestrator. Default is Json
        /// </summary>
        public ISerializerFactory SerializerFactory { get; set; }

        /// <summary>
        /// Gets or Sets custom converter for all rows
        /// </summary>
        public IConverter Converter { get; set; }

        /// <summary>
        /// Gets or Sets the service uri used to reach the server api.
        /// </summary>
        public string ServiceUri { get; set; }

        /// <summary>
        /// Gets or Sets the HttpClient instanced used for this web client orchestrator
        /// </summary>
        public HttpClient HttpClient { get; set; }



        /// <summary>
        /// Gets a new web proxy orchestrator
        /// </summary>
        public WebClientOrchestrator(string serviceUri = null, ISerializerFactory serializerFactory = null, IConverter customConverter = null, HttpClient client = null)
        {
            // if no HttpClient provisionned, create a new one
            if (client == null)
            {
                var handler = new HttpClientHandler();

                // Activated by default
                if (handler.SupportsAutomaticDecompression)
                    handler.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;

                this.HttpClient = new HttpClient(handler);
            }
            else
            {
                this.HttpClient = client;
            }

            this.Converter = customConverter;
            this.SerializerFactory = serializerFactory ?? SerializersCollection.JsonSerializer;
            this.ServiceUri = serviceUri;
        }

        /// <summary>
        /// Adds some scope parameters
        /// </summary>
        public void AddScopeParameter(string key, string value)
        {
            if (this.httpRequestHandler.ScopeParameters.ContainsKey(key))
                this.httpRequestHandler.ScopeParameters[key] = value;
            else
                this.httpRequestHandler.ScopeParameters.Add(key, value);

        }

        /// <summary>
        /// Adds some custom headers
        /// </summary>
        public void AddCustomHeader(string key, string value)
        {
            if (this.httpRequestHandler.CustomHeaders.ContainsKey(key))
                this.httpRequestHandler.CustomHeaders[key] = value;
            else
                this.httpRequestHandler.CustomHeaders.Add(key, value);

        }

        /// <summary>
        /// Send a request to remote web proxy for First step : Ensure scopes and schema
        /// </summary>
        public async Task<(SyncContext context, SyncSet schema)>
            EnsureSchemaAsync(SyncContext context, SyncSetup setup, CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            // Create the message to be sent
            var httpMessage = new HttpMessageEnsureScopesRequest(context, setup.ScopeName);

            // serialize message
            var serializer = this.SerializerFactory.GetSerializer<HttpMessageEnsureScopesRequest>();
            var binaryData = await serializer.SerializeAsync(httpMessage);

            await InterceptAsync(new HttpMessageEnsureScopesRequestArgs(binaryData)).ConfigureAwait(false);

            // No batch size submitted here, because the schema will be generated in memory and send back to the user.
            var ensureScopesResponse = await this.httpRequestHandler.ProcessRequestAsync<HttpMessageEnsureScopesResponse>
                (this.HttpClient, this.ServiceUri, binaryData, HttpStep.EnsureScopes, context.SessionId,
                 this.SerializerFactory, this.Converter, 0, cancellationToken).ConfigureAwait(false);


            if (ensureScopesResponse == null)
                throw new ArgumentException("Http Message content for Ensure scope can't be null");

            if (ensureScopesResponse.Schema == null || ensureScopesResponse.Schema.Tables.Count <= 0)
                throw new ArgumentException("Schema from EnsureScope can't be null and may contains at least one table");

            ensureScopesResponse.Schema.EnsureSchema();
            // Return scopes and new shema
            return (ensureScopesResponse.SyncContext, ensureScopesResponse.Schema);
        }



        public async Task<(SyncContext, long, BatchInfo, ConflictResolutionPolicy, DatabaseChangesSelected)>
            ApplyThenGetChangesAsync(SyncContext context, ScopeInfo scope, SyncSet schema, BatchInfo clientBatchInfo,
                                     bool disableConstraintsOnApplyChanges, bool useBulkOperations, bool cleanMetadatas, bool cleanFolder,
                                     int clientBatchSize, string batchDirectory, ConflictResolutionPolicy policy,
                                     CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            // disableConstraintsOnApplyChanges, useBulkOperations, cleanMetadatas, client policy
            // are not used, since it's handled by server side
            // clientBatchSize is sent to server to specify if the client wants a batch in return

            // create the in memory changes set
            var changesSet = new SyncSet(schema.ScopeName);

            foreach (var table in schema.Tables)
                DbSyncAdapter.CreateChangesTable(schema.Tables[table.TableName, table.SchemaName], changesSet);

            // if we don't have any BatchPartsInfo, just generate a new one to get, at least, something to send to the server
            // and get a response with new data from server
            if (clientBatchInfo == null)
                clientBatchInfo = new BatchInfo(true, changesSet);

            // --------------------------------------------------------------
            // STEP 1 : Send everything to the server side
            // --------------------------------------------------------------

            // response
            HttpMessageSendChangesResponse httpMessageContent = null;


            // If not in memory and BatchPartsInfo.Count == 0, nothing to send.
            // But we need to send something, so generate a little batch part
            if (clientBatchInfo.InMemory || (!clientBatchInfo.InMemory && clientBatchInfo.BatchPartsInfo.Count == 0))
            {
                var changesToSend = new HttpMessageSendChangesRequest(context, scope);

                if (this.Converter != null && clientBatchInfo.InMemoryData.HasRows)
                    this.BeforeSerializeRows(clientBatchInfo.InMemoryData);

                var containerSet = clientBatchInfo.InMemoryData.GetContainerSet();
                changesToSend.Changes = containerSet;
                changesToSend.IsLastBatch = true;
                changesToSend.BatchIndex = 0;

                // serialize message
                var serializer = this.SerializerFactory.GetSerializer<HttpMessageSendChangesRequest>();
                var binaryData = await serializer.SerializeAsync(changesToSend);

                await InterceptAsync(new HttpMessageSendChangesRequestArgs(binaryData)).ConfigureAwait(false);

                httpMessageContent = await this.httpRequestHandler.ProcessRequestAsync<HttpMessageSendChangesResponse>
                    (this.HttpClient, this.ServiceUri, binaryData, HttpStep.SendChanges, context.SessionId,
                     this.SerializerFactory, this.Converter, clientBatchSize, cancellationToken).ConfigureAwait(false);

            }
            else
            {
                // Foreach part, will have to send them to the remote
                // once finished, return context
                foreach (var bpi in clientBatchInfo.BatchPartsInfo.OrderBy(bpi => bpi.Index))
                {
                    // If BPI is InMempory, no need to deserialize from disk
                    // othewise load it
                    await bpi.LoadBatchAsync(changesSet, clientBatchInfo.GetDirectoryFullPath());

                    var changesToSend = new HttpMessageSendChangesRequest(context, scope);

                    if (this.Converter != null && bpi.Data.HasRows)
                        BeforeSerializeRows(bpi.Data);

                    // Set the change request properties
                    changesToSend.Changes = bpi.Data.GetContainerSet();
                    changesToSend.IsLastBatch = bpi.IsLastBatch;
                    changesToSend.BatchIndex = bpi.Index;

                    // serialize message
                    var serializer = this.SerializerFactory.GetSerializer<HttpMessageSendChangesRequest>();
                    var binaryData = await serializer.SerializeAsync(changesToSend);

                    await InterceptAsync(new HttpMessageSendChangesRequestArgs(binaryData)).ConfigureAwait(false);

                    httpMessageContent = await this.httpRequestHandler.ProcessRequestAsync<HttpMessageSendChangesResponse>
                        (this.HttpClient, this.ServiceUri, binaryData, HttpStep.SendChanges, context.SessionId,
                         this.SerializerFactory, this.Converter, clientBatchSize, cancellationToken).ConfigureAwait(false);


                    // for some reasons, if server don't want to wait for more, just break
                    // That should never happened, actually
                    if (httpMessageContent.ServerStep != HttpStep.SendChangesInProgress)
                        break;

                }

            }

            // --------------------------------------------------------------
            // STEP 2 : Receive everything from the server side
            // --------------------------------------------------------------

            // Now we have sent all the datas to the server and now :
            // We have a FIRST response from the server with new datas 
            // 1) Could be the only one response (enough or InMemory is set on the server side)
            // 2) Could bt the first response and we need to download all batchs

            // While we have an other batch to process
            var isLastBatch = false;

            // Get if we need to work in memory or serialize things
            var workInMemoryLocally = clientBatchSize == 0;

            // Create the BatchInfo and SyncContext to return at the end
            // Set InMemory by default to "true", but the real value is coming from server side
            var serverBatchInfo = new BatchInfo(workInMemoryLocally, changesSet, batchDirectory);

            // stats
            DatabaseChangesSelected serverChangesSelected = null;

            //timestamp generated by the server, hold in the client db
            long remoteClientTimestamp = 0;

            // While we are not reaching the last batch from server
            do
            {
                // Check if we are at the last batch.
                // If so, we won't make another loop
                isLastBatch = httpMessageContent.IsLastBatch;
                serverChangesSelected = httpMessageContent.ChangesSelected;
                context = httpMessageContent.SyncContext;
                remoteClientTimestamp = httpMessageContent.RemoteClientTimestamp;

                changesSet = changesSet.Clone();

                changesSet.ImportContainerSet(httpMessageContent.Changes, false);

                if (this.Converter != null && changesSet.HasRows)
                    AfterDeserializedRows(changesSet);

                // Create a BatchPartInfo instance
                await serverBatchInfo.AddChangesAsync(changesSet, httpMessageContent.BatchIndex, isLastBatch);

                // free some memory
                if (!workInMemoryLocally && httpMessageContent.Changes != null)
                    httpMessageContent.Changes.Clear();

                if (!isLastBatch)
                {
                    // Ask for the next batch index
                    var requestBatchIndex = httpMessageContent.BatchIndex + 1;

                    // Create the message enveloppe
                    var httpMessage = new HttpMessageGetMoreChangesRequest(context, requestBatchIndex);

                    // serialize message
                    var serializer = this.SerializerFactory.GetSerializer<HttpMessageGetMoreChangesRequest>();
                    var binaryData = await serializer.SerializeAsync(httpMessage);

                    await InterceptAsync(new HttpMessageGetMoreChangesRequestArgs(binaryData)).ConfigureAwait(false);

                    httpMessageContent = await this.httpRequestHandler.ProcessRequestAsync<HttpMessageSendChangesResponse>(
                               this.HttpClient, this.ServiceUri, binaryData, HttpStep.GetChanges, context.SessionId,
                               this.SerializerFactory, this.Converter, clientBatchSize, cancellationToken).ConfigureAwait(false);
                }

            } while (!isLastBatch);


            return (context, remoteClientTimestamp, serverBatchInfo, httpMessageContent.ConflictResolutionPolicy, serverChangesSelected);
        }


        public async Task<(SyncContext context, long remoteClientTimestamp, BatchInfo serverBatchInfo)>
            GetSnapshotAsync(SyncContext context, ScopeInfo scope, SyncSet schema, string snapshotDirectory, string batchDirectory, CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            // create the in memory changes set
            var changesSet = new SyncSet(schema.ScopeName);

            foreach (var table in schema.Tables)
                DbSyncAdapter.CreateChangesTable(schema.Tables[table.TableName, table.SchemaName], changesSet);

            // Create the BatchInfo and SyncContext to return at the end
            // Set InMemory by default to "true", but the real value is coming from server side
            var serverBatchInfo = new BatchInfo(false, changesSet, batchDirectory);

            bool isLastBatch;
            //timestamp generated by the server, hold in the client db
            long remoteClientTimestamp;

            // generate a message to send
            var changesToSend = new HttpMessageSendChangesRequest(context, scope)
            {
                Changes = null,
                IsLastBatch = true,
                BatchIndex = 0
            };

            var serializer = this.SerializerFactory.GetSerializer<HttpMessageSendChangesRequest>();
            var binaryData = await serializer.SerializeAsync(changesToSend);

            var httpMessageContent = await this.httpRequestHandler.ProcessRequestAsync<HttpMessageSendChangesResponse>(
                      this.HttpClient, this.ServiceUri, binaryData, HttpStep.GetSnapshot, context.SessionId,
                      this.SerializerFactory, this.Converter, 0, cancellationToken).ConfigureAwait(false);

            // if no snapshot available, return empty response
            if (httpMessageContent.Changes == null)
                return (httpMessageContent.SyncContext, httpMessageContent.RemoteClientTimestamp, null);

            // While we are not reaching the last batch from server
            do
            {
                // Check if we are at the last batch.
                // If so, we won't make another loop
                isLastBatch = httpMessageContent.IsLastBatch;
                context = httpMessageContent.SyncContext;
                remoteClientTimestamp = httpMessageContent.RemoteClientTimestamp;

                changesSet = changesSet.Clone();

                changesSet.ImportContainerSet(httpMessageContent.Changes, false);

                if (this.Converter != null && changesSet.HasRows)
                    AfterDeserializedRows(changesSet);

                // Create a BatchPartInfo instance
                await serverBatchInfo.AddChangesAsync(changesSet, httpMessageContent.BatchIndex, isLastBatch);

                // free some memory
                if (httpMessageContent.Changes != null)
                    httpMessageContent.Changes.Clear();

                if (!isLastBatch)
                {
                    // Ask for the next batch index
                    var requestBatchIndex = httpMessageContent.BatchIndex + 1;

                    // Create the message enveloppe
                    var httpMessage = new HttpMessageGetMoreChangesRequest(context, requestBatchIndex);

                    // serialize message
                    var serializer2 = this.SerializerFactory.GetSerializer<HttpMessageGetMoreChangesRequest>();
                    var binaryData2 = await serializer2.SerializeAsync(httpMessage);

                    await InterceptAsync(new HttpMessageGetMoreChangesRequestArgs(binaryData)).ConfigureAwait(false);

                    httpMessageContent = await this.httpRequestHandler.ProcessRequestAsync<HttpMessageSendChangesResponse>(
                               this.HttpClient, this.ServiceUri, binaryData2, HttpStep.GetChanges, context.SessionId,
                               this.SerializerFactory, this.Converter, 0, cancellationToken).ConfigureAwait(false);
                }

            } while (!isLastBatch);


            return (context, remoteClientTimestamp, serverBatchInfo);
        }

        /// <summary>
        /// We can't delete metadats on request from client
        /// </summary>
        public Task DeleteMetadatasAsync(SyncContext context, SyncSetup setup, long timeStampStart, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => throw new NotImplementedException();

        public void BeforeSerializeRows(SyncSet data)
        {
            foreach (var table in data.Tables)
            {
                if (table.Rows.Count > 0)
                {
                    foreach (var row in table.Rows)
                        this.Converter.BeforeSerialize(row);

                }
            }
        }

        public void AfterDeserializedRows(SyncSet data)
        {
            foreach (var table in data.Tables)
            {
                if (table.Rows.Count > 0)
                {
                    foreach (var row in table.Rows)
                        this.Converter.AfterDeserialized(row);

                }
            }

        }


    }
}
