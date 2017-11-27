using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Enumerations;
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
        public event EventHandler<SyncProgressEventArgs> SyncProgress;
        public event EventHandler<ApplyChangeFailedEventArgs> ApplyChangedFailed;
        private Uri serviceUri;
        private SerializationFormat serializationFormat;

        public Uri ServiceUri
        {
            get
            {
                return this.serviceUri;
            }
            set
            {
                if (this.serviceUri != value)
                {
                    this.serviceUri = value;
                    this.httpRequestHandler = new HttpRequestHandler(this.serviceUri, this.SerializationFormat);
                }
            }
        }

        public SerializationFormat SerializationFormat
        {
            get
            {
                return this.serializationFormat;
            }
            set
            {
                if (this.serializationFormat != value)
                {
                    this.serializationFormat = value;
                    this.httpRequestHandler = new HttpRequestHandler(this.ServiceUri, this.serializationFormat);
                }
            }
        }

        public WebProxyClientProvider()
        {

        }
        /// <summary>
        /// Use this Constructor if you are on the Client Side, only
        /// </summary>
        public WebProxyClientProvider(Uri serviceUri) : this(serviceUri, SerializationFormat.Json)
        {
        }

        public WebProxyClientProvider(Uri serviceUri, SerializationFormat serializationFormat)
        {
            this.ServiceUri = serviceUri;
            this.httpRequestHandler = new HttpRequestHandler(this.ServiceUri, serializationFormat);
            this.SerializationFormat = serializationFormat;
        }

        /// <summary>
        /// Use this constructor when you are on the Remote Side, only
        /// </summary>
        public WebProxyClientProvider(Uri serviceUri, HttpClientHandler handler, Dictionary<string, string> scopeParameters = null, SerializationFormat serializationFormat = SerializationFormat.Json)
        {
            this.ServiceUri = serviceUri;
            this.httpRequestHandler = new HttpRequestHandler(this.ServiceUri, serializationFormat, handler, scopeParameters);
            this.SerializationFormat = serializationFormat;
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



        public async Task<(SyncContext, BatchInfo, ChangesStatistics)> GetChangeBatchAsync(SyncContext context, ScopeInfo scopeInfo)
        {
            // While we have an other batch to process
            var isLastBatch = false;

            // Create the BatchInfo and SyncContext to return at the end
            BatchInfo changes = new BatchInfo();
            changes.Directory = BatchInfo.GenerateNewDirectoryName();
            SyncContext syncContext = null;
            ChangesStatistics changesStatistics = null;

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


                changesStatistics = httpMessageResponse.GetChangeBatch.ChangesStatistics;
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

            return (syncContext, changes, changesStatistics);
        }

        /// <summary>
        /// Send changes to server
        /// </summary>
        public async Task<(SyncContext, ChangesStatistics)> ApplyChangesAsync(SyncContext context, ScopeInfo fromScope, BatchInfo changes)
        {
            if (changes == null || changes.BatchPartsInfo.Count == 0)
                return (context, new ChangesStatistics());

            SyncContext syncContext = null;
            ChangesStatistics changesStatistics = null;

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

                if (httpMessage.ApplyChanges.Set == null || httpMessage.ApplyChanges.Set.Tables == null || httpMessage.ApplyChanges.Set.Tables.Count == 0)
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
                changesStatistics = httpMessageResponse.ApplyChanges.ChangesStatistics;
            }

            return (syncContext, changesStatistics);

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
