using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dotmim.Sync.Batch;
using Microsoft.Net.Http.Headers;
using Newtonsoft.Json.Linq;

namespace Dotmim.Sync.Web.Client
{
    public class WebClientOrchestrator : IRemoteOrchestrator
    {
        private readonly HttpRequestHandler httpRequestHandler = new HttpRequestHandler();

        public Dictionary<string, string> CustomHeaders => this.httpRequestHandler.CustomHeaders;
        public Dictionary<string, string> ScopeParameters => this.httpRequestHandler.ScopeParameters;


        public SyncOptions Options { get; private set; }
        public SyncSchema Schema { get; private set; }

        /// <summary>
        /// Gets or Sets the provider used in this proxy Orchestrator
        /// Should be null. CoreProvider is only used on the remote side (WebProxyServerProvider)
        /// </summary>
        public CoreProvider Provider { get => null; set => throw new NotSupportedException("Proxy Web does not need any provider. Everything is made on the server side"); }

        /// <summary>
        /// Gets or Sets the service uri to the server side
        /// </summary>
        public Uri ServiceUri
        {
            get => this.httpRequestHandler.BaseUri;
            set => this.httpRequestHandler.BaseUri = value;
        }

        public HttpClientHandler Handler
        {
            get => this.httpRequestHandler.Handler;
            set => this.httpRequestHandler.Handler = value;
        }
        public CookieHeaderValue Cookie
        {
            get => this.httpRequestHandler.Cookie;
            set => this.httpRequestHandler.Cookie = value;
        }

        public WebClientOrchestrator()
        {

        }
        public WebClientOrchestrator(Uri serviceUri)
        {
            this.httpRequestHandler = new HttpRequestHandler(serviceUri, CancellationToken.None);
        }

        /// <summaryWebProxyClientOrchestrator
        /// Use this constructor when you are on the Remote Side, only
        /// </summary>
        public WebClientOrchestrator(Uri serviceUri,
                                      Dictionary<string, string> scopeParameters = null,
                                      Dictionary<string, string> customHeaders = null)
        {
            this.httpRequestHandler = new HttpRequestHandler(serviceUri, CancellationToken.None);

            foreach (var sp in scopeParameters)
                this.AddScopeParameter(sp.Key, sp.Value);

            foreach (var ch in customHeaders)
                this.AddCustomHeader(ch.Key, ch.Value);
        }

        public void AddScopeParameter(string key, string value)
        {
            if (this.httpRequestHandler.ScopeParameters.ContainsKey(key))
                this.httpRequestHandler.ScopeParameters[key] = value;
            else
                this.httpRequestHandler.ScopeParameters.Add(key, value);

        }

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
        public async Task<(SyncContext context, ScopeInfo serverScopeInfo, ScopeInfo localScopeReferenceInfo, SyncSchema schema)>
            EnsureScopeAsync(SyncContext context, SyncSchema schema, SyncOptions options, Guid clientScopeId,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {


            // Create the message to be sent
            var httpMessage = new HttpMessage
            {
                SyncContext = context,
                Step = HttpStep.EnsureScopes,
                Content = new HttpMessageEnsureScopesRequest(schema.ScopeName, clientScopeId)
            };

            // Post the request and get the response from server
            var httpMessageResponse = await this.httpRequestHandler.ProcessRequestAsync(httpMessage, context.SessionId,
                    schema.SerializationFormat, cancellationToken).ConfigureAwait(false);

            if (httpMessageResponse == null)
                throw new Exception("Can't have an empty body");

            HttpMessageEnsureScopesResponse ensureScopesResponse;
            if (httpMessageResponse.Content is HttpMessageEnsureScopesResponse)
                ensureScopesResponse = httpMessageResponse.Content as HttpMessageEnsureScopesResponse;
            else
                ensureScopesResponse = (httpMessageResponse.Content as JObject).ToObject<HttpMessageEnsureScopesResponse>();


            if (ensureScopesResponse == null)
                throw new ArgumentException("Http Message content for Ensure scope can't be null");

            if (ensureScopesResponse.Schema == null || ensureScopesResponse.Schema.Set == null || ensureScopesResponse.Schema.Set.Tables.Count <= 0)
                throw new ArgumentException("Schema from EnsureScope can't be null and may contains at least one table");

            this.Options = options;
            this.Schema = ensureScopesResponse.Schema;

            // Return scopes and new shema
            return (httpMessageResponse.SyncContext,
                    ensureScopesResponse.ServerScopeInfo,
                    ensureScopesResponse.LocalScopeReferenceInfo,
                    ensureScopesResponse.Schema);
        }

        public async Task<(SyncContext context, BatchInfo serverBatchInfo, DatabaseChangesSelected serverChangesSelected)>
            ApplyThenGetChangesAsync(SyncContext context, Guid clientScopeId, ScopeInfo localScopeReferenceInfo, ScopeInfo serverScopeInfo,
                                     BatchInfo clientBatchInfo, CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            SyncContext syncContext = null;
            DatabaseChangesApplied changesApplied = null;

            // if we don't have any BatchPartsInfo, just generate a new one to get, at least, something to send to the server
            // and get a response with new data from server
            if (clientBatchInfo == null)
                clientBatchInfo = new BatchInfo(true, this.Options.BatchDirectory);

            if (clientBatchInfo.BatchPartsInfo == null)
                clientBatchInfo.BatchPartsInfo = new List<BatchPartInfo>();

            if (clientBatchInfo.BatchPartsInfo.Count <= 0)
            {
                clientBatchInfo.InMemory = true;
                clientBatchInfo.GenerateBatchInfo(0, null);
            }

            // --------------------------------------------------------------
            // STEP 1 : Send everything to the server side
            // --------------------------------------------------------------

            // Response from server
            HttpMessageSendChangesResponse httpMessageContent = null;
            HttpMessage httpMessageResponse = null;

            // Foreach part, will have to send them to the remote
            // once finished, return context
            foreach (var bpi in clientBatchInfo.BatchPartsInfo.OrderBy(bpi => bpi.Index))
            {
                // If BPI is InMempory, no need to deserialize from disk
                // othewise load it
                if (!clientBatchInfo.InMemory)
                    bpi.LoadBatch();

                //if (bpi.Set == null || bpi.Set.Tables == null)
                //    throw new ArgumentException("No changes to upload found.");

                var changesToSend = new HttpMessageSendChangesRequest(clientScopeId, localScopeReferenceInfo, serverScopeInfo);

                // Set the change request properties
                changesToSend.Changes = bpi.Set;
                changesToSend.IsLastBatch = bpi.IsLastBatch;
                changesToSend.BatchIndex = bpi.Index;

                // Create the message enveloppe
                var httpMessage = new HttpMessage
                {
                    Step = HttpStep.SendChanges,
                    SyncContext = context,
                    Content = changesToSend
                };

                //Post request and get response
                httpMessageResponse = await this.httpRequestHandler.ProcessRequestAsync(
                    httpMessage, context.SessionId, this.Schema.SerializationFormat, cancellationToken).ConfigureAwait(false);

                if (httpMessageResponse == null)
                    throw new Exception("Can't have an empty body");

                if (httpMessageResponse.Content is HttpMessageSendChangesResponse)
                    httpMessageContent = httpMessageResponse.Content as HttpMessageSendChangesResponse;
                else
                    httpMessageContent = (httpMessageResponse.Content as JObject).ToObject<HttpMessageSendChangesResponse>();

                syncContext = httpMessageResponse.SyncContext;

                // for some reasons, if server don't want to wait for more, just break
                // That should never happened, actually
                if (httpMessageResponse.Step != HttpStep.InProgress)
                    break;

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
            var serverBatchInfo = new BatchInfo(workInMemoryLocally, this.Options.BatchDirectory);

            // stats
            DatabaseChangesSelected serverChangesSelected = null;

            // While we are not reaching the last batch from server
            do
            {
                // Check if we are at the last batch.
                // If so, we won't make another loop
                isLastBatch = httpMessageContent.IsLastBatch;
                serverChangesSelected = httpMessageContent.ChangesSelected;
                syncContext = httpMessageResponse.SyncContext;

                // Create a BatchPartInfo instance
                var bpi = serverBatchInfo.GenerateBatchInfo(httpMessageContent.BatchIndex, httpMessageContent.Changes);

                // free some memory
                if (!workInMemoryLocally && httpMessageContent.Changes != null)
                    httpMessageContent.Changes.Clear();

                if (!isLastBatch)
                {
                    // Ask for the next batch index
                    var requestBatchIndex = httpMessageContent.BatchIndex + 1;

                    // Create the message enveloppe
                    var httpMessage = new HttpMessage
                    {
                        Step = HttpStep.GetChanges,
                        SyncContext = context,
                    };

                    // Maybe miss some info here
                    httpMessage.Content = new HttpMessageGetMoreChangesRequest
                    {
                        BatchIndexRequested = requestBatchIndex,
                    };
                    
                    httpMessageResponse = await this.httpRequestHandler.ProcessRequestAsync(
                                httpMessage, context.SessionId, this.Schema.SerializationFormat, cancellationToken).ConfigureAwait(false);

                    if (httpMessageResponse == null)
                        throw new Exception("Can't have an empty body");

                    if (httpMessageResponse.Content is HttpMessageSendChangesResponse)
                        httpMessageContent = httpMessageResponse.Content as HttpMessageSendChangesResponse;
                    else
                        httpMessageContent = (httpMessageResponse.Content as JObject).ToObject<HttpMessageSendChangesResponse>();
                }


            } while (!isLastBatch);


            return (context, serverBatchInfo, serverChangesSelected);

            //SyncContext context, long serverTimestamp, BatchInfo serverBatchInfo, DatabaseChangesSelected serverChangesSelected


        }
    }
}
