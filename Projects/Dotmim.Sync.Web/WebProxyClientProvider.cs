using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Filter;
using Dotmim.Sync.Messages;
using Microsoft.Net.Http.Headers;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync.Web
{

    /// <summary>
    /// Class used when you have to deal with a Web Server
    /// </summary>
    public class WebProxyClientProvider : IProvider, IDisposable
    {
        private HttpRequestHandler httpRequestHandler;
        private CancellationToken cancellationToken;
        private SyncConfiguration syncConfiguration;

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
        public event EventHandler<ConfigurationApplyingEventArgs> ConfigurationApplying;
        public event EventHandler<ConfigurationAppliedEventArgs> ConfigurationApplied;
        public event EventHandler<TableChangesSelectingEventArgs> TableChangesSelecting;
        public event EventHandler<TableChangesSelectedEventArgs> TableChangesSelected;
        public event EventHandler<TableChangesApplyingEventArgs> TableChangesApplying;
        public event EventHandler<TableChangesAppliedEventArgs> TableChangesApplied;

        public Dictionary<string, string> CustomHeaders
        {
            get
            {
                return this.httpRequestHandler.CustomHeaders;
            }
        }
        public Dictionary<string, string> ScopeParameters
        {
            get
            {
                return this.httpRequestHandler.ScopeParameters;
            }

        }
        public SerializationFormat SerializationFormat
        {
            get
            {
                return this.httpRequestHandler.SerializationFormat;
            }
            set
            {
                this.httpRequestHandler.SerializationFormat = value;
            }
        }
        public Uri ServiceUri
        {
            get
            {
                return this.httpRequestHandler.BaseUri;
            }
            set
            {
                this.httpRequestHandler.BaseUri = value;
            }
        }
        public CancellationToken CancellationToken
        {
            get
            {
                return this.httpRequestHandler.CancellationToken;
            }
            set
            {
                this.httpRequestHandler.CancellationToken = value;
            }
        }
        public HttpClientHandler Handler
        {
            get
            {
                return this.httpRequestHandler.Handler;
            }
            set
            {
                this.httpRequestHandler.Handler = value;
            }
        }
        public CookieHeaderValue Cookie
        {
            get
            {
                return this.httpRequestHandler.Cookie;
            }
            set
            {
                this.httpRequestHandler.Cookie = value;
            }
        }

        public WebProxyClientProvider()
        {
            this.httpRequestHandler = new HttpRequestHandler();

        }
        /// <summary>
        /// Use this Constructor if you are on the Client Side, only
        /// </summary>
        public WebProxyClientProvider(Uri serviceUri) : this(serviceUri, SerializationFormat.Json)
        {
        }

        public WebProxyClientProvider(Uri serviceUri, SerializationFormat serializationFormat)
        {
            this.httpRequestHandler = new HttpRequestHandler(serviceUri, serializationFormat, CancellationToken.None);
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
        /// Use this constructor when you are on the Remote Side, only
        /// </summary>
        public WebProxyClientProvider(Uri serviceUri,
                                      Dictionary<string, string> scopeParameters = null,
                                      Dictionary<string, string> customHeaders = null,
                                      SerializationFormat serializationFormat = SerializationFormat.Json)
        {
            this.httpRequestHandler = new HttpRequestHandler(serviceUri, serializationFormat, CancellationToken.None);

            foreach (var sp in scopeParameters)
                this.AddScopeParameter(sp.Key, sp.Value);

            foreach (var ch in customHeaders)
                this.AddCustomHeader(ch.Key, ch.Value);
        }

        public async Task<(SyncContext, SyncConfiguration)> BeginSessionAsync(SyncContext context, MessageBeginSession message)
        {
            HttpMessage httpMessage = new HttpMessage
            {
                Step = HttpStep.BeginSession,
                SyncContext = context,
                Content = new HttpMessageBeginSession
                {
                    SyncConfiguration = message.SyncConfiguration
                }
            };

            //Post request and get response
            var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(httpMessage, cancellationToken);

            if (httpMessageResponse == null || httpMessageResponse.Content == null)
                throw new Exception("Can't have an empty body");

            var httpMessageContent = (httpMessageResponse.Content as JObject).ToObject<HttpMessageBeginSession>();

            return (httpMessageResponse.SyncContext, httpMessageContent.SyncConfiguration);
        }


        public async Task<SyncContext> EndSessionAsync(SyncContext context)
        {
            HttpMessage message = new HttpMessage
            {
                Step = HttpStep.EndSession,
                SyncContext = context
            };

            //Post request and get response
            var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(message, cancellationToken);

            if (httpMessageResponse == null)
                throw new Exception("Can't have an empty body");

            return httpMessageResponse.SyncContext;
        }

        public async Task<(SyncContext, List<ScopeInfo>)> EnsureScopesAsync(SyncContext context,
            String scopeInfoTableName, String scopeName, Guid? clientReferenceId = null)
        {
            HttpMessage httpMessage = new HttpMessage
            {
                SyncContext = context,
                Step = HttpStep.EnsureScopes,
                Content = new HttpMessageEnsureScopes
                {
                    ClientReferenceId = clientReferenceId,
                    ScopeInfoTableName = scopeInfoTableName,
                    ScopeName = scopeName
                }
            };
      
            //Post request and get response
            var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(httpMessage, cancellationToken);

            if (httpMessageResponse == null)
                throw new Exception("Can't have an empty body");

            var httpMessageContent = (httpMessageResponse.Content as JObject).ToObject<HttpMessageEnsureScopes>();

            return (httpMessageResponse.SyncContext, httpMessageContent.Scopes);
        }

        public async Task<(SyncContext, DmSet)> EnsureSchemaAsync(SyncContext context, DmSet schema = null)
        {
            HttpMessage httpMessage = new HttpMessage
            {
                SyncContext = context,
                Step = HttpStep.EnsureConfiguration,
                Content = new HttpMessageEnsureSchema
                {
                    Schema = new DmSetSurrogate(schema)
                }
            };

            //Post request and get response
            var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(httpMessage, cancellationToken);

            if (httpMessageResponse == null)
                throw new Exception("Can't have an empty body");

            var httpMessageContent = (httpMessageResponse.Content as JObject).ToObject<HttpMessageEnsureSchema>();

            if (httpMessageContent == null || httpMessageContent.Schema == null || httpMessageContent.Schema.Tables.Count <= 0)
                throw new ArgumentException("Schema can't be null");

            // get schema & deserialize the surrogate
            schema = httpMessageContent.Schema.ConvertToDmSet();
            httpMessageContent.Schema.Clear();
            httpMessageContent.Schema.Dispose();
            httpMessageContent.Schema = null;

            // get context
            var syncContext = httpMessageResponse.SyncContext;

            return (syncContext, schema);
        }

        public async Task<SyncContext> EnsureDatabaseAsync(SyncContext context, ScopeInfo scopeInfo, DmSet schema, ICollection<FilterClause> filters)
        {
            HttpMessage httpMessage = new HttpMessage
            {
                SyncContext = context,
                Step = HttpStep.EnsureDatabase,
                Content = new HttpMessageEnsureDatabase
                {
                    ScopeInfo = scopeInfo,
                    Schema = new DmSetSurrogate(schema),
                    Filters = filters
                }
            };

            //Post request and get response
            var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(httpMessage, cancellationToken);

            if (httpMessageResponse == null)
                throw new Exception("Can't have an empty body");

            return httpMessageResponse.SyncContext;
        }

        public async Task<(SyncContext, BatchInfo, ChangesSelected)> GetChangeBatchAsync(
            SyncContext context, ScopeInfo scopeInfo,
            DmSet schema, int downloadBatchSizeInKB, string batchDirectory, ConflictResolutionPolicy policy, ICollection<FilterClause> filters)
        {
            // While we have an other batch to process
            var isLastBatch = false;

            // Create the BatchInfo and SyncContext to return at the end
            BatchInfo changes = new BatchInfo();
            changes.Directory = BatchInfo.GenerateNewDirectoryName();
            SyncContext syncContext = null;
            ChangesSelected changesSelected = null;

            while (!isLastBatch)
            {
                HttpMessage httpMessage = new HttpMessage
                {
                    SyncContext = context,
                    Step = HttpStep.GetChangeBatch,

                    Content = new HttpMessageGetChangesBatch
                    {
                        ScopeInfo = scopeInfo,
                        BatchIndexRequested = changes.BatchIndex,
                        DownloadBatchSizeInKB = downloadBatchSizeInKB,
                        BatchDirectory = batchDirectory,
                        Schema = new DmSetSurrogate(schema),
                        Filters = filters,
                        Policy = policy
                    }
                };

                var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(httpMessage, cancellationToken);

                if (httpMessageResponse == null)
                    throw new Exception("Can't have an empty body");

                var httpMessageContent = (httpMessageResponse.Content as JObject).ToObject<HttpMessageGetChangesBatch>();

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
                    var bpId = BatchInfo.GenerateNewFileName(changes.BatchIndex.ToString());
                    var fileName = Path.Combine(this.syncConfiguration.BatchDirectory, changes.Directory, bpId);
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
        public async Task<(SyncContext, ChangesApplied)> ApplyChangesAsync(SyncContext context, ScopeInfo fromScope,
            DmSet schema, ConflictResolutionPolicy policy, Boolean useBulkOperations, String scopeInfoTableName, BatchInfo changes)
        {
            if (changes == null || changes.BatchPartsInfo.Count == 0)
                return (context, new ChangesApplied());

            SyncContext syncContext = null;
            ChangesApplied changesApplied = null;

            // Foreach part, will have to send them to the remote
            // once finished, return context
            foreach (var bpi in changes.BatchPartsInfo.OrderBy(bpi => bpi.Index))
            {
                var applyChanges = new HttpMessageApplyChanges
                {
                    ScopeInfo = fromScope,
                    Schema = new DmSetSurrogate(schema),
                    Policy = policy,
                    UseBulkOperations = useBulkOperations,
                    ScopeInfoTableName = scopeInfoTableName
                };

                HttpMessage httpMessage = new HttpMessage
                {
                    Step = HttpStep.ApplyChanges,
                    SyncContext = context,
                    Content = applyChanges
                };

                // If BPI is InMempory, no need to deserialize from disk
                // Set already contained in part.Set
                if (!changes.InMemory)
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
                applyChanges.InMemory = changes.InMemory;
                applyChanges.BatchIndex = bpi.Index;

                //Post request and get response
                var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(httpMessage, cancellationToken);

                // Clear surrogate
                applyChanges.Set.Dispose();
                applyChanges.Set = null;

                if (httpMessageResponse == null)
                    throw new Exception("Can't have an empty body");

                var httpMessageContent = (httpMessageResponse.Content as JObject).ToObject<HttpMessageApplyChanges>();

                syncContext = httpMessageResponse.SyncContext;
                changesApplied = httpMessageContent.ChangesApplied;
            }

            return (syncContext, changesApplied);

        }


        public async Task<(SyncContext, long)> GetLocalTimestampAsync(SyncContext context, string scopeInfoTableName)
        {
            HttpMessage message = new HttpMessage
            {
                Step = HttpStep.GetLocalTimestamp,
                SyncContext = context,
                Content = new HttpMessageTimestamp
                {
                    ScopeInfoTableName = scopeInfoTableName,
                }
            };

            //Post request and get response
            var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(message, cancellationToken);

            if (httpMessageResponse == null)
                throw new Exception("Can't have an empty body");

            var httpMessageContent = (httpMessageResponse.Content as JObject).ToObject<HttpMessageTimestamp>();

            if (httpMessageContent == null)
                throw new ArgumentException("Timestamp required from server");

            return (httpMessageResponse.SyncContext, httpMessageContent.LocalTimestamp);
        }

        public async Task<SyncContext> WriteScopesAsync(SyncContext context, String scopeInfoTableName, List<ScopeInfo> scopes)
        {
            HttpMessage message = new HttpMessage
            {
                Step = HttpStep.WriteScopes,
                SyncContext = context,
                Content = new HttpMessageWriteScopes
                {
                    ScopeInfoTableName = scopeInfoTableName,
                    Scopes = scopes
                }
            };

            //Post request and get response
            var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(message, cancellationToken);

            if (httpMessageResponse == null)
                throw new Exception("Can't have an empty body");

            return httpMessageResponse.SyncContext;
        }


        public void SetCancellationToken(CancellationToken token)
        {
            this.cancellationToken = token;
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    if (this.httpRequestHandler != null)
                        this.httpRequestHandler.Dispose();
                }
                disposedValue = true;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
        }
        #endregion
    }
}
