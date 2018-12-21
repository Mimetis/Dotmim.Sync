using Dotmim.Sync.Batch;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Messages;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
#if NETSTANDARD
using Microsoft.Net.Http.Headers;
#else
using System.Net.Http.Headers;
#endif

namespace Dotmim.Sync.Web.Client
{

    /// <summary>
    /// Class used when you have to deal with a Web Server
    /// </summary>
#pragma warning disable CS0067
    public class WebProxyClientProvider : IProvider, IDisposable
    {
        private readonly HttpRequestHandler httpRequestHandler;
        private CancellationToken cancellationToken;

        public event EventHandler<ProgressEventArgs> SyncProgress;
        public event EventHandler<ApplyChangeFailedEventArgs> ApplyChangedFailed;
        public event EventHandler<BeginSessionEventArgs> BeginSession;
        public event EventHandler<EndSessionEventArgs> EndSession;
        public event EventHandler<ScopeEventArgs> ScopeLoading;
        public event EventHandler<ScopeEventArgs> ScopeSaved;
        public event EventHandler<DatabaseApplyingEventArgs> DatabaseApplying;
        public event EventHandler<DatabaseAppliedEventArgs> DatabaseApplied;
        public event EventHandler<DatabaseTableApplyingEventArgs> DatabaseTableApplying;
        public event EventHandler<DatabaseTableAppliedEventArgs> DatabaseTableApplied;
        public event EventHandler<SchemaApplyingEventArgs> SchemaApplying;
        public event EventHandler<SchemaAppliedEventArgs> SchemaApplied;
        public event EventHandler<TableChangesSelectingEventArgs> TableChangesSelecting;
        public event EventHandler<TableChangesSelectedEventArgs> TableChangesSelected;
        public event EventHandler<TableChangesApplyingEventArgs> TableChangesApplying;
        public event EventHandler<TableChangesAppliedEventArgs> TableChangesApplied;

        public Dictionary<string, string> CustomHeaders => this.httpRequestHandler.CustomHeaders;
        public Dictionary<string, string> ScopeParameters => this.httpRequestHandler.ScopeParameters;

        public Uri ServiceUri
        {
            get => this.httpRequestHandler.BaseUri;
            set => this.httpRequestHandler.BaseUri = value;
        }
        public CancellationToken CancellationToken
        {
            get => this.httpRequestHandler.CancellationToken;
            set => this.httpRequestHandler.CancellationToken = value;
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

        public WebProxyClientProvider() => this.httpRequestHandler = new HttpRequestHandler();
        /// <summary>
        /// Use this Constructor if you are on the Client Side, only
        /// </summary>
        public WebProxyClientProvider(Uri serviceUri) => this.httpRequestHandler = new HttpRequestHandler(serviceUri, CancellationToken.None);


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
        /// Use this constructor when you are on the Remote Side, only
        /// </summary>
        public WebProxyClientProvider(Uri serviceUri,
                                      Dictionary<string, string> scopeParameters = null,
                                      Dictionary<string, string> customHeaders = null)
        {
            this.httpRequestHandler = new HttpRequestHandler(serviceUri, CancellationToken.None);

            foreach (var sp in scopeParameters)
                this.AddScopeParameter(sp.Key, sp.Value);

            foreach (var ch in customHeaders)
                this.AddCustomHeader(ch.Key, ch.Value);
        }

        public async Task<(SyncContext, SyncConfiguration)> BeginSessionAsync(SyncContext context, MessageBeginSession message)
        {
            var httpMessage = new HttpMessage
            {
                Step = HttpStep.BeginSession,
                SyncContext = context,
                Content = new HttpMessageBeginSession
                {
                    SyncConfiguration = message.SyncConfiguration
                }
            };

            //Post request and get response
            // This first request is always JSON based, to be able to get the format from the server side
            var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(httpMessage, SerializationFormat.Json, this.cancellationToken);

            if (httpMessageResponse == null || httpMessageResponse.Content == null)
                throw new Exception("Can't have an empty body");

            HttpMessageBeginSession httpMessageContent;
            if (httpMessageResponse.Content is HttpMessageBeginSession)
                httpMessageContent = httpMessageResponse.Content as HttpMessageBeginSession;
            else
                httpMessageContent = (httpMessageResponse.Content as JObject).ToObject<HttpMessageBeginSession>();

            return (httpMessageResponse.SyncContext, httpMessageContent.SyncConfiguration);
        }


        public async Task<SyncContext> EndSessionAsync(SyncContext context)
        {
            var message = new HttpMessage
            {
                Step = HttpStep.EndSession,
                SyncContext = context
            };

            //Post request and get response
            var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(message, SerializationFormat.Json, this.cancellationToken);

            if (httpMessageResponse == null)
                throw new Exception("Can't have an empty body");

            return httpMessageResponse.SyncContext;
        }

        public async Task<(SyncContext, List<ScopeInfo>)> EnsureScopesAsync(SyncContext context, MessageEnsureScopes message)
        {
            var httpMessage = new HttpMessage
            {
                SyncContext = context,
                Step = HttpStep.EnsureScopes,
                Content = new HttpMessageEnsureScopes
                {
                    ClientReferenceId = message.ClientReferenceId,
                    ScopeInfoTableName = message.ScopeInfoTableName,
                    ScopeName = message.ScopeName,
                    SerializationFormat = message.SerializationFormat
                }
            };

            //Post request and get response
            var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(httpMessage, message.SerializationFormat, this.cancellationToken);

            if (httpMessageResponse == null)
                throw new Exception("Can't have an empty body");

            HttpMessageEnsureScopes httpMessageContent;
            if (httpMessageResponse.Content is HttpMessageEnsureScopes)
                httpMessageContent = httpMessageResponse.Content as HttpMessageEnsureScopes;
            else
                httpMessageContent = (httpMessageResponse.Content as JObject).ToObject<HttpMessageEnsureScopes>();

            return (httpMessageResponse.SyncContext, httpMessageContent.Scopes);
        }

        public async Task<(SyncContext, DmSet)> EnsureSchemaAsync(SyncContext context, MessageEnsureSchema message)
        {
            var httpMessage = new HttpMessage
            {
                SyncContext = context,
                Step = HttpStep.EnsureConfiguration,
                Content = new HttpMessageEnsureSchema
                {
                    Schema = message.Schema == null ? null : new DmSetSurrogate(message.Schema),
                    SerializationFormat = message.SerializationFormat
                }
            };

            //Post request and get response
            var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(httpMessage, message.SerializationFormat, this.cancellationToken);

            if (httpMessageResponse == null)
                throw new Exception("Can't have an empty body");

            HttpMessageEnsureSchema httpMessageContent;
            if (httpMessageResponse.Content is HttpMessageEnsureSchema)
                httpMessageContent = httpMessageResponse.Content as HttpMessageEnsureSchema;
            else
                httpMessageContent = (httpMessageResponse.Content as JObject).ToObject<HttpMessageEnsureSchema>();

            if (httpMessageContent == null || httpMessageContent.Schema == null || httpMessageContent.Schema.Tables.Count <= 0)
                throw new ArgumentException("Schema can't be null");

            // get schema & deserialize the surrogate
            message.Schema = httpMessageContent.Schema.ConvertToDmSet();
            httpMessageContent.Schema.Clear();
            httpMessageContent.Schema.Dispose();
            httpMessageContent.Schema = null;

            // get context
            var syncContext = httpMessageResponse.SyncContext;

            return (syncContext, message.Schema);
        }

        public async Task<SyncContext> EnsureDatabaseAsync(SyncContext context, MessageEnsureDatabase message)
        {
            var httpMessage = new HttpMessage
            {
                SyncContext = context,
                Step = HttpStep.EnsureDatabase,
                Content = new HttpMessageEnsureDatabase
                {
                    ScopeInfo = message.ScopeInfo,
                    Schema = new DmSetSurrogate(message.Schema),
                    Filters = message.Filters,
                    SerializationFormat = message.SerializationFormat
                }
            };

            //Post request and get response
            var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(httpMessage, message.SerializationFormat, this.cancellationToken);

            if (httpMessageResponse == null)
                throw new Exception("Can't have an empty body");

            return httpMessageResponse.SyncContext;
        }

        public async Task<(SyncContext, BatchInfo, ChangesSelected)> GetChangeBatchAsync(
            SyncContext context, MessageGetChangesBatch message)
        {
            // While we have an other batch to process
            var isLastBatch = false;

            // Create the BatchInfo and SyncContext to return at the end
            // Set InMemory by default to "true", but the real value comes from server side
            var changes = new BatchInfo(true, message.BatchDirectory);

            // Generate new directory name
            changes.GenerateNewDirectoryName();

            SyncContext syncContext = null;
            ChangesSelected changesSelected = null;

            while (!isLastBatch)
            {
                var httpMessage = new HttpMessage
                {
                    SyncContext = context,
                    Step = HttpStep.GetChangeBatch,

                    Content = new HttpMessageGetChangesBatch
                    {
                        ScopeInfo = message.ScopeInfo,
                        BatchIndexRequested = changes.BatchIndex,
                        DownloadBatchSizeInKB = message.DownloadBatchSizeInKB,
                        BatchDirectory = message.BatchDirectory,
                        Schema = new DmSetSurrogate(message.Schema),
                        Filters = message.Filters,
                        Policy = message.Policy,
                        SerializationFormat = message.SerializationFormat
                    }
                };

                var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(httpMessage, message.SerializationFormat, this.cancellationToken);

                if (httpMessageResponse == null)
                    throw new Exception("Can't have an empty body");

                HttpMessageGetChangesBatch httpMessageContent;
                if (httpMessageResponse.Content is HttpMessageGetChangesBatch)
                    httpMessageContent = httpMessageResponse.Content as HttpMessageGetChangesBatch;
                else
                    httpMessageContent = (httpMessageResponse.Content as JObject).ToObject<HttpMessageGetChangesBatch>();

                if (httpMessageContent == null)
                    throw new Exception("Can't have an empty GetChangeBatch");

                changesSelected = httpMessageContent.ChangesSelected;
                changes.InMemory = httpMessageContent.InMemory;
                syncContext = httpMessageResponse.SyncContext;

                // get the bpi and add it to the BatchInfo
                var bpi = httpMessageContent.BatchPartInfo;
                if (bpi != null)
                {
                    changes.BatchIndex = bpi.Index;
                    changes.BatchPartsInfo.Add(bpi);
                    isLastBatch = bpi.IsLastBatch;
                }
                else
                {
                    changes.BatchIndex = 0;
                    isLastBatch = true;

                    // break the while { } story
                    break;
                }

                if (changes.InMemory)
                {
                    // load the DmSet in memory
                    bpi.Set = httpMessageContent.Set.ConvertToDmSet();
                }
                else
                {
                    // Serialize the file !
                    var bpId = changes.GenerateNewFileName(changes.BatchIndex.ToString());
                    var fileName = Path.Combine(message.BatchDirectory, changes.DirectoryName, bpId);
                    BatchPart.Serialize(httpMessageContent.Set, fileName);
                    bpi.FileName = fileName;
                    bpi.Clear();

                }

                // Clear the DmSetSurrogate from response, we don't need it anymore
                if (httpMessageContent.Set != null)
                {
                    httpMessageContent.Set.Dispose();
                    httpMessageContent.Set = null;
                }

                // if not last, increment batchIndex for next request
                if (!isLastBatch)
                    changes.BatchIndex++;

            }

            return (syncContext, changes, changesSelected);
        }

        /// <summary>
        /// Send changes to server
        /// </summary>
        public async Task<(SyncContext, ChangesApplied)> ApplyChangesAsync(SyncContext context, MessageApplyChanges message)
        {
            if (message.Changes == null || message.Changes.BatchPartsInfo.Count == 0)
            {
                message.Changes?.Clear(message.CleanMetadatas);
                return (context, new ChangesApplied());
            }

            SyncContext syncContext = null;
            ChangesApplied changesApplied = null;

            // Foreach part, will have to send them to the remote
            // once finished, return context
            foreach (var bpi in message.Changes.BatchPartsInfo.OrderBy(bpi => bpi.Index))
            {
                var applyChanges = new HttpMessageApplyChanges
                {
                    FromScope = message.FromScope,
                    Schema = new DmSetSurrogate(message.Schema),
                    Policy = message.Policy,
                    UseBulkOperations = message.UseBulkOperations,
                    CleanMetadatas = message.CleanMetadatas,
                    ScopeInfoTableName = message.ScopeInfoTableName,
                    SerializationFormat = message.SerializationFormat

                };

                var httpMessage = new HttpMessage
                {
                    Step = HttpStep.ApplyChanges,
                    SyncContext = context,
                    Content = applyChanges
                };

                // If BPI is InMempory, no need to deserialize from disk
                // Set already contained in part.Set
                if (!message.Changes.InMemory)
                {
                    // get the batch
                    var partBatch = bpi.GetBatch();

                    // get the surrogate dmSet
                    if (partBatch != null)
                        applyChanges.Set = partBatch.DmSetSurrogate;
                }
                else if (bpi.Set != null)
                {
                    applyChanges.Set = new DmSetSurrogate(bpi.Set);
                }

                if (applyChanges.Set == null || applyChanges.Set.Tables == null)
                    throw new ArgumentException("No changes to upload found.");

                // no need to send filename
                applyChanges.BatchPartInfo = new BatchPartInfo
                {
                    FileName = null,
                    Index = bpi.Index,
                    IsLastBatch = bpi.IsLastBatch,
                    Tables = bpi.Tables
                };
                applyChanges.InMemory = message.Changes.InMemory;
                applyChanges.BatchIndex = bpi.Index;

                //Post request and get response
                var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(httpMessage, message.SerializationFormat, this.cancellationToken);

                // Clear surrogate
                applyChanges.Set.Dispose();
                applyChanges.Set = null;

                if (httpMessageResponse == null)
                    throw new Exception("Can't have an empty body");

                HttpMessageApplyChanges httpMessageContent;
                if (httpMessageResponse.Content is HttpMessageApplyChanges)
                    httpMessageContent = httpMessageResponse.Content as HttpMessageApplyChanges;
                else
                    httpMessageContent = (httpMessageResponse.Content as JObject).ToObject<HttpMessageApplyChanges>();

                syncContext = httpMessageResponse.SyncContext;
                changesApplied = httpMessageContent.ChangesApplied;
            }

            // Once all bpi sent, we can safely delete the local tmp folder
            message.Changes?.Clear(message.CleanMetadatas);

            return (syncContext, changesApplied);

        }


        public async Task<(SyncContext, long)> GetLocalTimestampAsync(SyncContext context, MessageTimestamp message)
        {
            var httpMessage = new HttpMessage
            {
                Step = HttpStep.GetLocalTimestamp,
                SyncContext = context,
                Content = new HttpMessageTimestamp
                {
                    ScopeInfoTableName = message.ScopeInfoTableName,
                    SerializationFormat = message.SerializationFormat
                }
            };

            //Post request and get response
            var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(httpMessage, message.SerializationFormat, this.cancellationToken);

            if (httpMessageResponse == null)
                throw new Exception("Can't have an empty body");

            HttpMessageTimestamp httpMessageContent;
            if (httpMessageResponse.Content is HttpMessageTimestamp)
                httpMessageContent = httpMessageResponse.Content as HttpMessageTimestamp;
            else
                httpMessageContent = (httpMessageResponse.Content as JObject).ToObject<HttpMessageTimestamp>();

            if (httpMessageContent == null)
                throw new ArgumentException("Timestamp required from server");

            return (httpMessageResponse.SyncContext, httpMessageContent.LocalTimestamp);
        }

        public async Task<SyncContext> WriteScopesAsync(SyncContext context, MessageWriteScopes message)
        {
            var httpMessage = new HttpMessage
            {
                Step = HttpStep.WriteScopes,
                SyncContext = context,
                Content = new HttpMessageWriteScopes
                {
                    ScopeInfoTableName = message.ScopeInfoTableName,
                    Scopes = message.Scopes,
                    SerializationFormat = message.SerializationFormat
                }
            };

            //Post request and get response
            var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(httpMessage, message.SerializationFormat, this.cancellationToken);

            if (httpMessageResponse == null)
                throw new Exception("Can't have an empty body");

            return httpMessageResponse.SyncContext;
        }


        public void SetCancellationToken(CancellationToken token) => this.cancellationToken = token;

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!this.disposedValue)
            {
                if (disposing)
                {
                    if (this.httpRequestHandler != null)
                        this.httpRequestHandler.Dispose();
                }
                this.disposedValue = true;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose() =>
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            this.Dispose(true);
        #endregion
    }
}
