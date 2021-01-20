using System;
using System.Collections.Generic;
using System.Data.Common;
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
    public class WebClientOrchestrator : RemoteOrchestrator
    {

        /// <summary>
        /// Even if web client is acting as a proxy remote orchestrator, we are using it on the client side
        /// </summary>
        public override SyncSide Side => SyncSide.ClientSide;

        private readonly HttpRequestHandler httpRequestHandler;

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


        public string GetServiceHost()
        {
            var uri = new Uri(this.ServiceUri);

            if (uri == null)
                return "Undefined";

            return uri.Host;
        }

        /// <summary>
        /// Sets the current context
        /// </summary>
        internal override void SetContext(SyncContext context)
        {
            // we get a different reference from the web server,
            // so we copy the properties to the correct reference object
            var ctx = this.GetContext();

            context.CopyTo(ctx);
        }

        /// <summary>
        /// Gets a new web proxy orchestrator
        /// </summary>
        public WebClientOrchestrator(string serviceUri, ISerializerFactory serializerFactory = null, IConverter customConverter = null, HttpClient client = null)
            : base(new FancyCoreProvider(), new SyncOptions(), new SyncSetup())
        {

            this.httpRequestHandler = new HttpRequestHandler(this);

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
        /// Get the schema from server, by sending an http request to the server
        /// </summary>
        public override async Task<SyncSet> GetSchemaAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            var serverScopeInfo = await this.EnsureSchemaAsync(cancellationToken, progress);

            return serverScopeInfo.Schema;

        }

        public override Task<bool> IsOutDated(ScopeInfo clientScopeInfo, ServerScopeInfo serverScopeInfo, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null) 
            => base.IsOutDated(clientScopeInfo, serverScopeInfo, cancellationToken, progress);

        /// <summary>
        /// Get server scope from server, by sending an http request to the server 
        /// </summary>
        public override async Task<ServerScopeInfo> GetServerScopeAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            // Get context or create a new one
            var ctx = this.GetContext();

            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Create the message to be sent
            var httpMessage = new HttpMessageEnsureScopesRequest(ctx);

            // serialize message
            var serializer = this.SerializerFactory.GetSerializer<HttpMessageEnsureScopesRequest>();
            var binaryData = await serializer.SerializeAsync(httpMessage);

            // No batch size submitted here, because the schema will be generated in memory and send back to the user.
            var ensureScopesResponse = await this.httpRequestHandler.ProcessRequestAsync<HttpMessageEnsureScopesResponse>
                (this.HttpClient, this.ServiceUri, binaryData, HttpStep.EnsureScopes, ctx.SessionId, this.ScopeName,
                 this.SerializerFactory, this.Converter, 0, cancellationToken).ConfigureAwait(false);

            if (ensureScopesResponse == null)
                throw new ArgumentException("Http Message content for Ensure scope can't be null");

            if (ensureScopesResponse.ServerScopeInfo == null)
                throw new ArgumentException("Server scope from EnsureScopesAsync can't be null and may contains a server scope");

            var args = new HttpGettingScopeArgs(ensureScopesResponse, this.GetServiceHost());
            await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);
            this.ReportProgress(ctx, progress, args);

            // Affect local setup
            this.Setup = ensureScopesResponse.ServerScopeInfo.Setup;

            // Reaffect context
            this.SetContext(ensureScopesResponse.SyncContext);

            // Return scopes and new shema
            return ensureScopesResponse.ServerScopeInfo;
        }

        /// <summary>
        /// Send a request to remote web proxy for First step : Ensure scopes and schema
        /// </summary>
        internal override async Task<ServerScopeInfo> EnsureSchemaAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            // Get context or create a new one
            var ctx = this.GetContext();

            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Create the message to be sent
            var httpMessage = new HttpMessageEnsureScopesRequest(ctx);

            // serialize message
            var serializer = this.SerializerFactory.GetSerializer<HttpMessageEnsureScopesRequest>();
            var binaryData = await serializer.SerializeAsync(httpMessage);

            // No batch size submitted here, because the schema will be generated in memory and send back to the user.
            var ensureScopesResponse = await this.httpRequestHandler.ProcessRequestAsync<HttpMessageEnsureSchemaResponse>
                (this.HttpClient, this.ServiceUri, binaryData, HttpStep.EnsureSchema, ctx.SessionId, this.ScopeName,
                 this.SerializerFactory, this.Converter, 0, cancellationToken).ConfigureAwait(false);

            if (ensureScopesResponse == null)
                throw new ArgumentException("Http Message content for Ensure Schema can't be null");

            if (ensureScopesResponse.ServerScopeInfo == null || ensureScopesResponse.Schema == null || ensureScopesResponse.Schema.Tables.Count <= 0)
                throw new ArgumentException("Schema from EnsureScope can't be null and may contains at least one table");

            ensureScopesResponse.Schema.EnsureSchema();
            ensureScopesResponse.ServerScopeInfo.Schema = ensureScopesResponse.Schema;

            var args = new HttpGettingSchemaArgs(ensureScopesResponse, this.GetServiceHost());
            await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);
            this.ReportProgress(ctx, progress, args);

            // Affect local setup
            this.Setup = ensureScopesResponse.ServerScopeInfo.Setup;

            // Reaffect context
            this.SetContext(ensureScopesResponse.SyncContext);

            // Return scopes and new shema
            return ensureScopesResponse.ServerScopeInfo;
        }

        /// <summary>
        /// Apply changes
        /// </summary>
        internal override async Task<(long RemoteClientTimestamp, BatchInfo ServerBatchInfo, ConflictResolutionPolicy ServerPolicy,
                                      DatabaseChangesApplied ClientChangesApplied, DatabaseChangesSelected ServerChangesSelected)>
            ApplyThenGetChangesAsync(ScopeInfo scope, BatchInfo clientBatchInfo, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            SyncSet schema;
            // Get context or create a new one
            var ctx = this.GetContext();

            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // is it something that could happens ?
            if (scope.Schema == null)
            {
                // Make a remote call to get Schema from remote provider
                var serverScopeInfo = await this.EnsureSchemaAsync(cancellationToken, progress).ConfigureAwait(false);
                schema = serverScopeInfo.Schema;
            }
            else
            {
                schema = scope.Schema;
            }

            schema.EnsureSchema();

            // if we don't have any BatchPartsInfo, just generate a new one to get, at least, something to send to the server
            // and get a response with new data from server
            if (clientBatchInfo == null)
                clientBatchInfo = new BatchInfo(true, schema);

            // Get sanitized schema, without readonly columns
            var sanitizedSchema = clientBatchInfo.SanitizedSchema;

            // --------------------------------------------------------------
            // STEP 1 : Send everything to the server side
            // --------------------------------------------------------------

            // response
            HttpMessageSendChangesResponse httpMessageContent = null;


            // If not in memory and BatchPartsInfo.Count == 0, nothing to send.
            // But we need to send something, so generate a little batch part
            if (clientBatchInfo.InMemory || (!clientBatchInfo.InMemory && clientBatchInfo.BatchPartsInfo.Count == 0))
            {
                var changesToSend = new HttpMessageSendChangesRequest(ctx, scope);

                if (this.Converter != null && clientBatchInfo.InMemoryData != null && clientBatchInfo.InMemoryData.HasRows)
                    this.BeforeSerializeRows(clientBatchInfo.InMemoryData);

                var containerSet = clientBatchInfo.InMemoryData == null ? new ContainerSet() : clientBatchInfo.InMemoryData.GetContainerSet();
                changesToSend.Changes = containerSet;
                changesToSend.IsLastBatch = true;
                changesToSend.BatchIndex = 0;
                changesToSend.BatchCount = clientBatchInfo.InMemoryData == null ? 0 : clientBatchInfo.BatchPartsInfo == null ? 0 : clientBatchInfo.BatchPartsInfo.Count;

                var args2 = new HttpSendingClientChangesArgs(changesToSend, this.GetServiceHost());
                await this.InterceptAsync(args2, cancellationToken).ConfigureAwait(false);
                this.ReportProgress(ctx, progress, args2);

                // serialize message
                var serializer = this.SerializerFactory.GetSerializer<HttpMessageSendChangesRequest>();
                var binaryData = await serializer.SerializeAsync(changesToSend);

                httpMessageContent = await this.httpRequestHandler.ProcessRequestAsync<HttpMessageSendChangesResponse>
                    (this.HttpClient, this.ServiceUri, binaryData, HttpStep.SendChanges, ctx.SessionId, scope.Name,
                     this.SerializerFactory, this.Converter, this.Options.BatchSize, cancellationToken).ConfigureAwait(false);

            }
            else
            {
                // Foreach part, will have to send them to the remote
                // once finished, return context
                foreach (var bpi in clientBatchInfo.BatchPartsInfo.OrderBy(bpi => bpi.Index))
                {
                    // If BPI is InMempory, no need to deserialize from disk
                    // othewise load it
                    await bpi.LoadBatchAsync(sanitizedSchema, clientBatchInfo.GetDirectoryFullPath(), this);

                    var changesToSend = new HttpMessageSendChangesRequest(ctx, scope);

                    if (this.Converter != null && bpi.Data.HasRows)
                        BeforeSerializeRows(bpi.Data);

                    // Set the change request properties
                    changesToSend.Changes = bpi.Data.GetContainerSet();
                    changesToSend.IsLastBatch = bpi.IsLastBatch;
                    changesToSend.BatchIndex = bpi.Index;
                    changesToSend.BatchCount = clientBatchInfo.BatchPartsInfo.Count;

                    var args2 = new HttpSendingClientChangesArgs(changesToSend, this.GetServiceHost());
                    await this.InterceptAsync(args2, cancellationToken).ConfigureAwait(false);
                    this.ReportProgress(ctx, progress, args2);

                    // serialize message
                    var serializer = this.SerializerFactory.GetSerializer<HttpMessageSendChangesRequest>();
                    var binaryData = await serializer.SerializeAsync(changesToSend);


                    httpMessageContent = await this.httpRequestHandler.ProcessRequestAsync<HttpMessageSendChangesResponse>
                        (this.HttpClient, this.ServiceUri, binaryData, HttpStep.SendChanges, ctx.SessionId, scope.Name,
                         this.SerializerFactory, this.Converter, this.Options.BatchSize, cancellationToken).ConfigureAwait(false);


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
            var workInMemoryLocally = this.Options.BatchSize == 0;

            // Create the BatchInfo and SyncContext to return at the end
            // Set InMemory by default to "true", but the real value is coming from server side
            var serverBatchInfo = new BatchInfo(workInMemoryLocally, schema, this.Options.BatchDirectory);

            // stats
            DatabaseChangesSelected serverChangesSelected = null;
            DatabaseChangesApplied clientChangesApplied = null;

            //timestamp generated by the server, hold in the client db
            long remoteClientTimestamp = 0;

            var args = new HttpGettingServerChangesArgs(httpMessageContent, this.GetServiceHost());
            await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);
            this.ReportProgress(ctx, progress, args);

            // While we are not reaching the last batch from server
            do
            {
                // Check if we are at the last batch.
                // If so, we won't make another loop
                isLastBatch = httpMessageContent.IsLastBatch;
                serverChangesSelected = httpMessageContent.ServerChangesSelected;
                clientChangesApplied = httpMessageContent.ClientChangesApplied;
                ctx = httpMessageContent.SyncContext;
                remoteClientTimestamp = httpMessageContent.RemoteClientTimestamp;

                var changesSet = sanitizedSchema.Clone();

                changesSet.ImportContainerSet(httpMessageContent.Changes, false);

                if (this.Converter != null && changesSet.HasRows)
                    AfterDeserializedRows(changesSet);

                // Create a BatchPartInfo instance
                await serverBatchInfo.AddChangesAsync(changesSet, httpMessageContent.BatchIndex, isLastBatch, this);


                // free some memory
                if (!workInMemoryLocally && httpMessageContent.Changes != null)
                    httpMessageContent.Changes.Clear();

                if (!isLastBatch)
                {
                    // Ask for the next batch index
                    var requestBatchIndex = httpMessageContent.BatchIndex + 1;

                    // Create the message enveloppe
                    var httpMessage = new HttpMessageGetMoreChangesRequest(ctx, requestBatchIndex);

                    // serialize message
                    var serializer = this.SerializerFactory.GetSerializer<HttpMessageGetMoreChangesRequest>();
                    var binaryData = await serializer.SerializeAsync(httpMessage);

                    httpMessageContent = await this.httpRequestHandler.ProcessRequestAsync<HttpMessageSendChangesResponse>(
                               this.HttpClient, this.ServiceUri, binaryData, HttpStep.GetMoreChanges, ctx.SessionId, scope.Name,
                               this.SerializerFactory, this.Converter, this.Options.BatchSize, cancellationToken).ConfigureAwait(false);

                    var args2 = new HttpGettingServerChangesArgs(httpMessageContent, this.GetServiceHost());
                    await this.InterceptAsync(args2, cancellationToken).ConfigureAwait(false);
                    this.ReportProgress(ctx, progress, args2);

                }

            } while (!isLastBatch);

            // generate the new scope item
            this.CompleteTime = DateTime.UtcNow;

            // Reaffect context
            this.SetContext(httpMessageContent.SyncContext);

            return (remoteClientTimestamp, serverBatchInfo,
                    httpMessageContent.ConflictResolutionPolicy, clientChangesApplied, serverChangesSelected);
        }


        public override async Task<(long RemoteClientTimestamp, BatchInfo ServerBatchInfo)>
            GetSnapshotAsync(SyncSet schema = null, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            // Get context or create a new one
            var ctx = this.GetContext();

            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Make a remote call to get Schema from remote provider
            if (schema == null)
            {
                var serverScopeInfo = await this.EnsureSchemaAsync(cancellationToken, progress).ConfigureAwait(false);
                schema = serverScopeInfo.Schema;
                schema.EnsureSchema();
            }


            // Create the BatchInfo and SyncContext to return at the end
            // Set InMemory by default to "true", but the real value is coming from server side
            var serverBatchInfo = new BatchInfo(false, schema, this.Options.BatchDirectory);

            bool isLastBatch;
            //timestamp generated by the server, hold in the client db
            long remoteClientTimestamp;

            // generate a message to send
            var changesToSend = new HttpMessageSendChangesRequest(ctx, null)
            {
                Changes = null,
                IsLastBatch = true,
                BatchIndex = 0,
                BatchCount = 0
            };

            var serializer = this.SerializerFactory.GetSerializer<HttpMessageSendChangesRequest>();
            var binaryData = await serializer.SerializeAsync(changesToSend);

            var httpMessageContent = await this.httpRequestHandler.ProcessRequestAsync<HttpMessageSendChangesResponse>(
                      this.HttpClient, this.ServiceUri, binaryData, HttpStep.GetSnapshot, ctx.SessionId, this.ScopeName,
                      this.SerializerFactory, this.Converter, 0, cancellationToken).ConfigureAwait(false);

            // if no snapshot available, return empty response
            if (httpMessageContent.Changes == null)
                return (httpMessageContent.RemoteClientTimestamp, null);

            var args = new HttpGettingServerChangesArgs(httpMessageContent, this.GetServiceHost());
            await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);
            this.ReportProgress(ctx, progress, args);

            // While we are not reaching the last batch from server
            do
            {
                // Check if we are at the last batch.
                // If so, we won't make another loop
                isLastBatch = httpMessageContent.IsLastBatch;
                ctx = httpMessageContent.SyncContext;
                remoteClientTimestamp = httpMessageContent.RemoteClientTimestamp;

                var changesSet = serverBatchInfo.SanitizedSchema.Clone();

                changesSet.ImportContainerSet(httpMessageContent.Changes, false);

                if (this.Converter != null && changesSet.HasRows)
                    AfterDeserializedRows(changesSet);

                // Create a BatchPartInfo instance
                await serverBatchInfo.AddChangesAsync(changesSet, httpMessageContent.BatchIndex, isLastBatch, this);

                // free some memory
                if (httpMessageContent.Changes != null)
                    httpMessageContent.Changes.Clear();

                if (!isLastBatch)
                {
                    // Ask for the next batch index
                    var requestBatchIndex = httpMessageContent.BatchIndex + 1;

                    // Create the message enveloppe
                    var httpMessage = new HttpMessageGetMoreChangesRequest(ctx, requestBatchIndex);

                    // serialize message
                    var serializer2 = this.SerializerFactory.GetSerializer<HttpMessageGetMoreChangesRequest>();
                    var binaryData2 = await serializer2.SerializeAsync(httpMessage);

                    httpMessageContent = await this.httpRequestHandler.ProcessRequestAsync<HttpMessageSendChangesResponse>(
                               this.HttpClient, this.ServiceUri, binaryData2, HttpStep.GetMoreChanges, ctx.SessionId, this.ScopeName,
                               this.SerializerFactory, this.Converter, 0, cancellationToken).ConfigureAwait(false);

                    var args2 = new HttpGettingServerChangesArgs(httpMessageContent, this.GetServiceHost());
                    await this.InterceptAsync(args2, cancellationToken).ConfigureAwait(false);
                    this.ReportProgress(ctx, progress, args2);

                }

            } while (!isLastBatch);

            // Reaffect context
            this.SetContext(httpMessageContent.SyncContext);

            return (remoteClientTimestamp, serverBatchInfo);
        }

        /// <summary>
        /// We can't delete metadats on request from client
        /// </summary>
        public override Task<DatabaseMetadatasCleaned> DeleteMetadatasAsync(long timeStampStart, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => throw new NotImplementedException();

        /// <summary>
        /// We can't get changes from server, from a web client orchestrator
        /// </summary>
        public override async Task<(long RemoteClientTimestamp, BatchInfo ServerBatchInfo, DatabaseChangesSelected ServerChangesSelected)>
                                GetChangesAsync(ScopeInfo clientScope, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            SyncSet schema;
            // Get context or create a new one
            var ctx = this.GetContext();

            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            ServerScopeInfo serverScopeInfo;

            // Need the server scope
            serverScopeInfo = await this.EnsureSchemaAsync(cancellationToken, progress).ConfigureAwait(false);
            schema = serverScopeInfo.Schema;
            schema.EnsureSchema();

            clientScope.Schema = schema;
            clientScope.Setup = serverScopeInfo.Setup;
            clientScope.Version = serverScopeInfo.Version;


            // generate a message to send
            var changesToSend = new HttpMessageSendChangesRequest(ctx, clientScope)
            {
                Changes = null,
                IsLastBatch = true,
                BatchIndex = 0,
                BatchCount = 0
            };

            var serializer = this.SerializerFactory.GetSerializer<HttpMessageSendChangesRequest>();
            var binaryData = await serializer.SerializeAsync(changesToSend);
            
            // response
            var httpMessageContent = await this.httpRequestHandler.ProcessRequestAsync<HttpMessageSendChangesResponse>
                    (this.HttpClient, this.ServiceUri, binaryData, HttpStep.GetChanges, ctx.SessionId, clientScope.Name,
                     this.SerializerFactory, this.Converter, this.Options.BatchSize, cancellationToken).ConfigureAwait(false);

            // if nothing available, return empty response
            if (httpMessageContent.Changes == null)
                return (httpMessageContent.RemoteClientTimestamp, null, new DatabaseChangesSelected());

            var args = new HttpGettingServerChangesArgs(httpMessageContent, this.GetServiceHost());
            await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);
            this.ReportProgress(ctx, progress, args);

            // Get if we need to work in memory or serialize things
            var workInMemoryLocally = this.Options.BatchSize == 0;

            bool isLastBatch;
            //timestamp generated by the server, hold in the client db
            long remoteClientTimestamp;

            // Create the BatchInfo and SyncContext to return at the end
            var serverBatchInfo = new BatchInfo(workInMemoryLocally, schema, this.Options.BatchDirectory);

            // stats
            DatabaseChangesSelected serverChangesSelected;
            // While we are not reaching the last batch from server
            do
            {
                // Check if we are at the last batch.
                // If so, we won't make another loop
                isLastBatch = httpMessageContent.IsLastBatch;
                ctx = httpMessageContent.SyncContext;
                remoteClientTimestamp = httpMessageContent.RemoteClientTimestamp;
                serverChangesSelected = httpMessageContent.ServerChangesSelected;

                var changesSet = serverBatchInfo.SanitizedSchema.Clone();

                changesSet.ImportContainerSet(httpMessageContent.Changes, false);

                if (this.Converter != null && changesSet.HasRows)
                    AfterDeserializedRows(changesSet);

                // Create a BatchPartInfo instance
                await serverBatchInfo.AddChangesAsync(changesSet, httpMessageContent.BatchIndex, isLastBatch, this);

                // free some memory
                if (httpMessageContent.Changes != null)
                    httpMessageContent.Changes.Clear();

                if (!isLastBatch)
                {
                    // Ask for the next batch index
                    var requestBatchIndex = httpMessageContent.BatchIndex + 1;

                    // Create the message enveloppe
                    var httpMessage = new HttpMessageGetMoreChangesRequest(ctx, requestBatchIndex);

                    // serialize message
                    var serializer2 = this.SerializerFactory.GetSerializer<HttpMessageGetMoreChangesRequest>();
                    var binaryData2 = await serializer2.SerializeAsync(httpMessage);

                    httpMessageContent = await this.httpRequestHandler.ProcessRequestAsync<HttpMessageSendChangesResponse>(
                               this.HttpClient, this.ServiceUri, binaryData2, HttpStep.GetMoreChanges, ctx.SessionId, this.ScopeName,
                               this.SerializerFactory, this.Converter, 0, cancellationToken).ConfigureAwait(false);

                    var args2 = new HttpGettingServerChangesArgs(httpMessageContent, this.GetServiceHost());
                    await this.InterceptAsync(args2, cancellationToken).ConfigureAwait(false);
                    this.ReportProgress(ctx, progress, args2);

                }

            } while (!isLastBatch);

            // generate the new scope item
            this.CompleteTime = DateTime.UtcNow;

            // Reaffect context
            this.SetContext(httpMessageContent.SyncContext);

            return (remoteClientTimestamp, serverBatchInfo, serverChangesSelected);
        }



        /// <summary>
        /// We can't get changes from server, from a web client orchestrator
        /// </summary>
        public override async Task<(long RemoteClientTimestamp, DatabaseChangesSelected ServerChangesSelected)>
                                GetEstimatedChangesCountAsync(ScopeInfo clientScope, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            SyncSet schema;
            // Get context or create a new one
            var ctx = this.GetContext();

            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            ServerScopeInfo serverScopeInfo;

            // Need the server scope
            serverScopeInfo = await this.EnsureSchemaAsync(cancellationToken, progress).ConfigureAwait(false);
            schema = serverScopeInfo.Schema;
            schema.EnsureSchema();

            clientScope.Schema = schema;
            clientScope.Setup = serverScopeInfo.Setup;
            clientScope.Version = serverScopeInfo.Version;


            // generate a message to send
            var changesToSend = new HttpMessageSendChangesRequest(ctx, clientScope)
            {
                Changes = null,
                IsLastBatch = true,
                BatchIndex = 0,
                BatchCount = 0
            };

            var serializer = this.SerializerFactory.GetSerializer<HttpMessageSendChangesRequest>();
            var binaryData = await serializer.SerializeAsync(changesToSend);

            // response
            var httpMessageContent = await this.httpRequestHandler.ProcessRequestAsync<HttpMessageSendChangesResponse>
                    (this.HttpClient, this.ServiceUri, binaryData, HttpStep.GetEstimatedChangesCount, ctx.SessionId, clientScope.Name,
                     this.SerializerFactory, this.Converter, this.Options.BatchSize, cancellationToken).ConfigureAwait(false);

            var args = new HttpGettingServerChangesArgs(httpMessageContent, this.GetServiceHost());
            await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);
            this.ReportProgress(ctx, progress, args);

            // if nothing available, return empty response
            if (httpMessageContent.Changes == null)
                return (httpMessageContent.RemoteClientTimestamp, new DatabaseChangesSelected());

            var serverChangesSelected = httpMessageContent.ServerChangesSelected ?? new DatabaseChangesSelected();

            // generate the new scope item
            this.CompleteTime = DateTime.UtcNow;

            // Reaffect context
            this.SetContext(httpMessageContent.SyncContext);

            return (httpMessageContent.RemoteClientTimestamp, serverChangesSelected);
        }



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
