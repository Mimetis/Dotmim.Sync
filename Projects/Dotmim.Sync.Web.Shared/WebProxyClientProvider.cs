﻿using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Enumerations;
#if CORE
using Microsoft.Net.Http.Headers;
#else
using System.Net.Http.Headers;
#endif
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

        public async Task<SyncContext> BeginSessionAsync(SyncContext context)
        {
            HttpMessage message = new HttpMessage();
            message.Step = HttpStep.BeginSession;
            message.SyncContext = context;

            //Post request and get response
            var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(message, cancellationToken);

            if (httpMessageResponse == null)
                throw new Exception("Can't have an empty body");

            return httpMessageResponse.SyncContext;
        }


        public async Task<SyncContext> EndSessionAsync(SyncContext context)
        {
            HttpMessage message = new HttpMessage();
            message.Step = HttpStep.EndSession;
            message.SyncContext = context;

            //Post request and get response
            var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(message, cancellationToken);

            if (httpMessageResponse == null)
                throw new Exception("Can't have an empty body");

            return httpMessageResponse.SyncContext;
        }

        public async Task<(SyncContext, List<ScopeInfo>)> EnsureScopesAsync(SyncContext context, string scopeName, Guid? clientReferenceId = null)
        {
            HttpMessage httpMessage = new HttpMessage();
            httpMessage.Step = HttpStep.EnsureScopes;

            HttpEnsureScopesMessage ensureScopeMessage = new HttpEnsureScopesMessage
            {
                ClientReferenceId = clientReferenceId,
                ScopeName = scopeName
            };

            httpMessage.EnsureScopes = ensureScopeMessage;
            httpMessage.SyncContext = context;

            //Post request and get response
            var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(httpMessage, cancellationToken);

            if (httpMessageResponse == null)
                throw new Exception("Can't have an empty body");

            return (httpMessageResponse.SyncContext, httpMessageResponse.EnsureScopes.Scopes);
        }

        public async Task<(SyncContext, SyncConfiguration)> EnsureConfigurationAsync(SyncContext context, SyncConfiguration configuration = null)
        {
            HttpMessage httpMessage = new HttpMessage();
            httpMessage.SyncContext = context;
            httpMessage.Step = HttpStep.EnsureConfiguration;

            //Post request and get response
            var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(httpMessage, cancellationToken);

            if (httpMessageResponse == null)
                throw new Exception("Can't have an empty body");

            if (httpMessageResponse.EnsureConfiguration == null || httpMessageResponse.EnsureConfiguration.Configuration == null || httpMessageResponse.EnsureConfiguration.ConfigurationSet == null || httpMessageResponse.EnsureConfiguration.ConfigurationSet.Tables.Count <= 0)
                throw new ArgumentException("Configuration can't be null");

            // get config & deserialize set
            var conf = httpMessageResponse.EnsureConfiguration.Configuration;
            var set = httpMessageResponse.EnsureConfiguration.ConfigurationSet.ConvertToDmSet();
            httpMessageResponse.EnsureConfiguration.ConfigurationSet.Clear();
            httpMessageResponse.EnsureConfiguration.ConfigurationSet.Dispose();
            httpMessageResponse.EnsureConfiguration.ConfigurationSet = null;
            conf.ScopeSet = set;

            // get context
            var syncContext = httpMessageResponse.SyncContext;

            // because we need it after
            this.syncConfiguration = conf;

            return (syncContext, conf);
        }

        public async Task<SyncContext> EnsureDatabaseAsync(SyncContext context, ScopeInfo scopeInfo)
        {
            HttpMessage httpMessage = new HttpMessage { SyncContext = context };
            httpMessage.Step = HttpStep.EnsureDatabase;

            HttpEnsureDatabaseMessage ensureDatabaseMessage = new HttpEnsureDatabaseMessage
            {
                ScopeInfo = scopeInfo
            };
            httpMessage.EnsureDatabase = ensureDatabaseMessage;

            //Post request and get response
            var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(httpMessage, cancellationToken);

            if (httpMessageResponse == null)
                throw new Exception("Can't have an empty body");

            return httpMessageResponse.SyncContext;
        }

        public async Task<(SyncContext, BatchInfo, ChangesSelected)> GetChangeBatchAsync(SyncContext context, ScopeInfo scopeInfo)
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
                HttpMessage httpMessage = new HttpMessage();
                httpMessage.SyncContext = context;
                httpMessage.Step = HttpStep.GetChangeBatch;

                httpMessage.GetChangeBatch = new HttpGetChangeBatchMessage
                {
                    ScopeInfo = scopeInfo,
                    BatchIndexRequested = changes.BatchIndex
                };

                var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(httpMessage, cancellationToken);

                if (httpMessageResponse == null)
                    throw new Exception("Can't have an empty body");

                if (httpMessageResponse.GetChangeBatch == null)
                    throw new Exception("Can't have an empty GetChangeBatch");


                changesSelected = httpMessageResponse.GetChangeBatch.ChangesSelected;
                changes.InMemory = httpMessageResponse.GetChangeBatch.InMemory;
                syncContext = httpMessageResponse.SyncContext;

                // get the bpi and add it to the BatchInfo
                var bpi = httpMessageResponse.GetChangeBatch.BatchPartInfo;
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
                    bpi.Set = httpMessageResponse.GetChangeBatch.Set.ConvertToDmSet();
                }
                else
                {
                    // Serialize the file !
                    var bpId = BatchInfo.GenerateNewFileName(changes.BatchIndex.ToString());
                    var fileName = Path.Combine(this.syncConfiguration.BatchDirectory, changes.Directory, bpId);
                    BatchPart.Serialize(httpMessageResponse.GetChangeBatch.Set, fileName);
                    bpi.FileName = fileName;
                    bpi.Clear();

                }

                // Clear the DmSetSurrogate from response, we don't need it anymore
                if (httpMessageResponse.GetChangeBatch.Set != null)
                {
                    httpMessageResponse.GetChangeBatch.Set.Dispose();
                    httpMessageResponse.GetChangeBatch.Set = null;
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
        public async Task<(SyncContext, ChangesApplied)> ApplyChangesAsync(SyncContext context, ScopeInfo fromScope, BatchInfo changes)
        {
            if (changes == null || changes.BatchPartsInfo.Count == 0)
                return (context, new ChangesApplied());

            SyncContext syncContext = null;
            ChangesApplied changesApplied = null;

            // Foreach part, will have to send them to the remote
            // once finished, return context
            foreach (var bpi in changes.BatchPartsInfo.OrderBy(bpi => bpi.Index))
            {
                HttpMessage httpMessage = new HttpMessage();
                httpMessage.Step = HttpStep.ApplyChanges;
                httpMessage.SyncContext = context;

                httpMessage.ApplyChanges = new HttpApplyChangesMessage();
                httpMessage.ApplyChanges.ScopeInfo = fromScope;

                // If BPI is InMempory, no need to deserialize from disk
                // Set already contained in part.Set
                if (!changes.InMemory)
                {
                    // get the batch
                    var partBatch = bpi.GetBatch();

                    // get the surrogate dmSet
                    if (partBatch != null)
                        httpMessage.ApplyChanges.Set = partBatch.DmSetSurrogate;
                }
                else if (bpi.Set != null)
                {
                    httpMessage.ApplyChanges.Set = new DmSetSurrogate(bpi.Set);
                }

                if (httpMessage.ApplyChanges.Set == null || httpMessage.ApplyChanges.Set.Tables == null)
                    throw new ArgumentException("No changes to upload found.");

                // no need to send filename
                httpMessage.ApplyChanges.BatchPartInfo = new BatchPartInfo
                {
                    FileName = null,
                    Index = bpi.Index,
                    IsLastBatch = bpi.IsLastBatch,
                    Tables = bpi.Tables
                };
                httpMessage.ApplyChanges.InMemory = changes.InMemory;
                httpMessage.ApplyChanges.BatchIndex = bpi.Index;

                //Post request and get response
                var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(httpMessage, cancellationToken);

                // Clear surrogate
                httpMessage.ApplyChanges.Set.Dispose();
                httpMessage.ApplyChanges.Set = null;

                if (httpMessageResponse == null)
                    throw new Exception("Can't have an empty body");

                syncContext = httpMessageResponse.SyncContext;
                changesApplied = httpMessageResponse.ApplyChanges.ChangesApplied;
            }

            return (syncContext, changesApplied);

        }


        public async Task<(SyncContext, long)> GetLocalTimestampAsync(SyncContext context)
        {
            HttpMessage message = new HttpMessage();
            message.Step = HttpStep.GetLocalTimestamp;
            message.SyncContext = context;

            //Post request and get response
            var httpMessageResponse = await this.httpRequestHandler.ProcessRequest(message, cancellationToken);

            if (httpMessageResponse == null)
                throw new Exception("Can't have an empty body");

            if (httpMessageResponse.GetLocalTimestamp == null)
                throw new ArgumentException("Timestamp required from server");

            return (httpMessageResponse.SyncContext, httpMessageResponse.GetLocalTimestamp.LocalTimestamp);
        }

        public async Task<SyncContext> WriteScopesAsync(SyncContext context, List<ScopeInfo> scopes)
        {
            HttpMessage message = new HttpMessage();
            message.Step = HttpStep.WriteScopes;
            message.SyncContext = context;
            message.WriteScopes = new HttpWriteScopesMessage();
            message.WriteScopes.Scopes = scopes;

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
