using System;
using System.Collections.Generic;
using Dotmim.Sync.Batch;
using System.Threading.Tasks;
using System.IO;
using System.Linq;
using System.Net.Http.Headers;
using System.Threading;
using Newtonsoft.Json;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Data;
using Dotmim.Sync.Messages;
using Dotmim.Sync.Web.Client;
using Newtonsoft.Json.Linq;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Session;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Dotmim.Sync.Web.Server
{

    public class SyncMemoryProvider : IProvider, IDisposable
    {

        /// <summary>
        /// Use this constructor when you are on the Remote Side, only
        /// </summary>
        public SyncMemoryProvider(CoreProvider localProvider) => this.LocalProvider = localProvider;

        public CoreProvider LocalProvider { get; private set; }

        /// <summary>
        /// Set Options parameters
        /// </summary>
        public void SetOptions(Action<SyncOptions> options) 
            => this.LocalProvider.SetOptions(options);

        /// <summary>
        /// Set Sync Configuration parameters
        /// </summary>
        public void SetConfiguration(Action<SyncConfiguration> configuration)
            => this.LocalProvider.SetConfiguration(configuration);

        /// <summary>
        /// set the progress action used to get progression on the provider
        /// </summary>
        public void SetProgress(IProgress<ProgressArgs> progress) 
            => this.LocalProvider.SetProgress(progress);


        public void SetInterceptor(InterceptorBase interceptorBase)
            => this.LocalProvider.SetInterceptor(interceptorBase);


        internal async Task<HttpMessage> GetResponseMessageAsync(HttpMessage httpMessage,
                 CancellationToken cancellationToken)
        {
            HttpMessage httpMessageResponse = null;
            switch (httpMessage.Step)
            {
                case HttpStep.BeginSession:
                    // on first message, replace the Configuration with the server one !
                    httpMessageResponse = await this.BeginSessionAsync(httpMessage);
                    break;
                case HttpStep.EnsureScopes:
                    httpMessageResponse = await this.EnsureScopesAsync(httpMessage);
                    break;
                case HttpStep.EnsureConfiguration:
                    httpMessageResponse = await this.EnsureSchemaAsync(httpMessage);
                    break;
                case HttpStep.EnsureDatabase:
                    httpMessageResponse = await this.EnsureDatabaseAsync(httpMessage);
                    break;
                case HttpStep.GetChangeBatch:
                    httpMessageResponse = await this.GetChangeBatchAsync(httpMessage);
                    break;
                case HttpStep.ApplyChanges:
                    httpMessageResponse = await this.ApplyChangesAsync(httpMessage);
                    break;
                case HttpStep.GetLocalTimestamp:
                    httpMessageResponse = await this.GetLocalTimestampAsync(httpMessage);
                    break;
                case HttpStep.WriteScopes:
                    httpMessageResponse = await this.WriteScopesAsync(httpMessage);
                    break;
                case HttpStep.EndSession:
                    httpMessageResponse = await this.EndSessionAsync(httpMessage);
                    break;
            }

            return httpMessageResponse;
        }



        /// <summary>
        /// Handle begin session
        /// Here we compare Options to see if client supplied it or not
        /// If not returns Options defined on server side
        /// </summary>
        private async Task<HttpMessage> BeginSessionAsync(HttpMessage httpMessage)
        {
            HttpMessageBeginSession httpMessageBeginSession;
            if (httpMessage.Content is HttpMessageBeginSession)
                httpMessageBeginSession = httpMessage.Content as HttpMessageBeginSession;
            else
                httpMessageBeginSession = (httpMessage.Content as JObject).ToObject<HttpMessageBeginSession>();

            // the Conf is hosted by the server ? if not, get the client configuration
            httpMessageBeginSession.Configuration =
                this.LocalProvider.Configuration ?? httpMessageBeginSession.Configuration;

            // Begin the session, requesting the server for the correct configuration
            (var ctx, var conf) =
                await this.BeginSessionAsync(httpMessage.SyncContext, httpMessageBeginSession as MessageBeginSession);

            httpMessageBeginSession.Configuration = conf;

            httpMessage.SyncContext = ctx;

            // Dont forget to re-assign since it's a JObject, until now
            httpMessage.Content = httpMessageBeginSession;

            return httpMessage;
        }

        private async Task<HttpMessage> EndSessionAsync(HttpMessage httpMessage)
        {
            // call inner provider
            httpMessage.SyncContext = await this.EndSessionAsync(httpMessage.SyncContext);

            return httpMessage;

        }

        private async Task<HttpMessage> EnsureScopesAsync(HttpMessage httpMessage)
        {
            HttpMessageEnsureScopes httpMessageEnsureScopes;
            if (httpMessage.Content is HttpMessageEnsureScopes)
                httpMessageEnsureScopes = httpMessage.Content as HttpMessageEnsureScopes;
            else
                httpMessageEnsureScopes = (httpMessage.Content as JObject).ToObject<HttpMessageEnsureScopes>();

            if (httpMessageEnsureScopes == null)
                throw new ArgumentException("EnsureScopeMessage could not be null");

            var (syncContext, lstScopes) = await this.EnsureScopesAsync(
                httpMessage.SyncContext, httpMessageEnsureScopes as MessageEnsureScopes);

            // Local scope is the server scope here
            httpMessageEnsureScopes.Scopes = lstScopes;

            httpMessage.SyncContext = syncContext;

            // Dont forget to re-assign since it's a JObject, until now
            httpMessage.Content = httpMessageEnsureScopes;

            return httpMessage;
        }

        private async Task<HttpMessage> EnsureSchemaAsync(HttpMessage httpMessage)
        {
            HttpMessageEnsureSchema httpMessageContent;
            if (httpMessage.Content is HttpMessageEnsureSchema)
                httpMessageContent = httpMessage.Content as HttpMessageEnsureSchema;
            else
                httpMessageContent = (httpMessage.Content as JObject).ToObject<HttpMessageEnsureSchema>();

            if (httpMessageContent == null)
                throw new ArgumentException("EnsureSchema message could not be null");

            if (this.LocalProvider.Configuration == null)
                throw new InvalidOperationException("No sync configuration was provided. Make sure you create a SyncConfiguration object and pass it to the WebProxyServerProvider!");

            // If the Conf is hosted by the server, we try to get the tables from it, overriding the client schema, if passed
            DmSet schema = null;
            if (this.LocalProvider.Configuration.Schema != null) {
                schema = this.LocalProvider.Configuration.Schema;
            }
            else if (httpMessageContent.Schema != null)
            {
                schema = httpMessageContent.Schema.ConvertToDmSet();
                this.LocalProvider.Configuration.Schema = schema;
            }

            if (httpMessageContent.Schema != null)
            {
                httpMessageContent.Schema.Dispose();
                httpMessageContent.Schema = null;
            }

            (httpMessage.SyncContext, schema) = await this.EnsureSchemaAsync(httpMessage.SyncContext,
                new MessageEnsureSchema { Schema = schema });

            httpMessageContent.Schema = new DmSetSurrogate(schema);

            // Dont forget to re-assign since it's a JObject, until now
            httpMessage.Content = httpMessageContent;

            return httpMessage;
        }

        private async Task<HttpMessage> EnsureDatabaseAsync(HttpMessage httpMessage)
        {
            HttpMessageEnsureDatabase httpMessageContent;
            if (httpMessage.Content is HttpMessageEnsureDatabase)
                httpMessageContent = httpMessage.Content as HttpMessageEnsureDatabase;
            else
                httpMessageContent = (httpMessage.Content as JObject).ToObject<HttpMessageEnsureDatabase>();

            if (httpMessageContent == null)
                throw new ArgumentException("EnsureDatabase message could not be null");

            httpMessage.SyncContext = await this.EnsureDatabaseAsync(
                httpMessage.SyncContext,
                new MessageEnsureDatabase
                {
                    ScopeInfo = httpMessageContent.ScopeInfo,
                    Schema = httpMessageContent.Schema.ConvertToDmSet(),
                    Filters = httpMessageContent.Filters
                });

            return httpMessage;
        }

        /// <summary>
        /// From request, I get a BI
        /// I need to send back a BPI
        /// </summary>
        /// <param name="httpMessage"></param>
        /// <returns></returns>
        private async Task<HttpMessage> GetChangeBatchAsync(HttpMessage httpMessage)
        {
            HttpMessageGetChangesBatch httpMessageContent;
            if (httpMessage.Content is HttpMessageGetChangesBatch)
                httpMessageContent = httpMessage.Content as HttpMessageGetChangesBatch;
            else
                httpMessageContent = (httpMessage.Content as JObject).ToObject<HttpMessageGetChangesBatch>();

            if (httpMessageContent == null)
                throw new ArgumentException("GetChangeBatch message could not be null");

            var scopeInfo = httpMessageContent.ScopeInfo;

            if (scopeInfo == null)
                throw new ArgumentException("GetChangeBatch ScopeInfo could not be null");

            // if we get the first batch info request, made it.
            // Server is able to define if it's in memory or not
            if (httpMessageContent.BatchIndexRequested == 0)
            {
                var (syncContext, bi, changesSelected) = await this.GetChangeBatchAsync(
                    httpMessage.SyncContext,
                    new MessageGetChangesBatch
                    {
                        ScopeInfo = scopeInfo,
                        Schema = httpMessageContent.Schema.ConvertToDmSet(),
                        //BatchSize = httpMessageContent.BatchSize,
                        //BatchDirectory = httpMessageContent.BatchDirectory,
                        Policy = httpMessageContent.Policy,
                        Filters = httpMessageContent.Filters
                    });

                // Select the first bpi needed (index == 0)
                if (bi.BatchPartsInfo.Count > 0)
                    httpMessageContent.BatchPartInfo = bi.BatchPartsInfo.First(bpi => bpi.Index == 0);

                httpMessageContent.InMemory = bi.InMemory;
                httpMessageContent.ChangesSelected = changesSelected;

                // if no changes, return
                if (httpMessageContent.BatchPartInfo == null)
                    return httpMessage;

                // if we are not in memory, we set the BI in session, to be able to get it back on next request
                if (!bi.InMemory)
                {
                    // Save the BatchInfo
                    this.LocalProvider.CacheManager.Set("GetChangeBatch_BatchInfo", bi);
                    this.LocalProvider.CacheManager.Set("GetChangeBatch_ChangesSelected", changesSelected);

                    // load the batchpart set directly, to be able to send it back
                    var batchPart = httpMessageContent.BatchPartInfo.GetBatch();
                    httpMessageContent.Set = batchPart.DmSetSurrogate;
                }
                else
                {
                    // We are in memory, so generate the DmSetSurrogate to be able to send it back
                    httpMessageContent.Set = new DmSetSurrogate(httpMessageContent.BatchPartInfo.Set);
                    httpMessageContent.BatchPartInfo.Set.Clear();
                    httpMessageContent.BatchPartInfo.Set = null;
                }

                // no need fileName info
                httpMessageContent.BatchPartInfo.FileName = null;

                httpMessage.SyncContext = syncContext;
                httpMessage.Content = httpMessageContent;

                // If we have only one bpi, we can safely delete it
                if (httpMessageContent.BatchPartInfo.IsLastBatch)
                {
                    this.LocalProvider.CacheManager.Remove("GetChangeBatch_BatchInfo");
                    this.LocalProvider.CacheManager.Remove("GetChangeBatch_ChangesSelected");
                    // delete the folder (not the BatchPartInfo, because we have a reference on it)
                    if (this.LocalProvider.Options.CleanMetadatas)
                        bi.TryRemoveDirectory();
                }

                return httpMessage;
            }

            // We are in batch mode here
            var batchInfo = this.LocalProvider.CacheManager.GetValue<BatchInfo>("GetChangeBatch_BatchInfo");
            var stats = this.LocalProvider.CacheManager.GetValue<DatabaseChangesSelected>("GetChangeBatch_ChangesSelected");

            if (batchInfo == null)
                throw new ArgumentNullException("batchInfo stored in session can't be null if request more batch part info.");

            httpMessageContent.ChangesSelected = stats;
            httpMessageContent.InMemory = batchInfo.InMemory;

            var batchPartInfo = batchInfo.BatchPartsInfo.FirstOrDefault(bpi => bpi.Index == httpMessageContent.BatchIndexRequested);

            httpMessageContent.BatchPartInfo = batchPartInfo;

            // load the batchpart set directly, to be able to send it back
            httpMessageContent.Set = httpMessageContent.BatchPartInfo.GetBatch().DmSetSurrogate;

            if (httpMessageContent.BatchPartInfo.IsLastBatch)
            {
                this.LocalProvider.CacheManager.Remove("GetChangeBatch_BatchInfo");
                this.LocalProvider.CacheManager.Remove("GetChangeBatch_ChangesSelected");
                // delete the folder (not the BatchPartInfo, because we have a reference on it)
                if (this.LocalProvider.Options.CleanMetadatas)
                    batchInfo.TryRemoveDirectory();
            }

            httpMessage.Content = httpMessageContent;
            return httpMessage;
        }

        private async Task<HttpMessage> ApplyChangesAsync(HttpMessage httpMessage)
        {
            HttpMessageApplyChanges httpMessageContent;
            if (httpMessage.Content is HttpMessageApplyChanges)
                httpMessageContent = httpMessage.Content as HttpMessageApplyChanges;
            else
                httpMessageContent = (httpMessage.Content as JObject).ToObject<HttpMessageApplyChanges>();

            if (httpMessageContent == null)
                throw new ArgumentException("ApplyChanges message could not be null");

            var scopeInfo = httpMessageContent.FromScope;

            if (scopeInfo == null)
                throw new ArgumentException("ApplyChanges ScopeInfo could not be null");

            var schema = httpMessageContent.Schema.ConvertToDmSet();

            BatchInfo batchInfo;
            var bpi = httpMessageContent.BatchPartInfo;

            if (httpMessageContent.InMemory)
            {
                batchInfo = new BatchInfo(true, this.LocalProvider.Options.BatchDirectory)
                {
                    BatchIndex = 0,
                    BatchPartsInfo = new List<BatchPartInfo>(new[] { bpi }),
                };

                bpi.Set = httpMessageContent.Set.ConvertToDmSet();

                httpMessageContent.Set.Dispose();
                httpMessageContent.Set = null;

                var (c, s) = await this.ApplyChangesAsync(httpMessage.SyncContext,
                    new MessageApplyChanges
                    {
                        FromScope = scopeInfo,
                        Schema = schema,
                        Policy = httpMessageContent.Policy,
                        //UseBulkOperations = httpMessageContent.UseBulkOperations,
                        //CleanMetadatas = httpMessageContent.CleanMetadatas,
                        ScopeInfoTableName = httpMessageContent.ScopeInfoTableName,
                        Changes = batchInfo
                    });

                httpMessageContent.ChangesApplied = s;
                httpMessageContent.BatchPartInfo.Clear();
                httpMessageContent.BatchPartInfo.FileName = null;

                httpMessage.SyncContext = c;
                httpMessage.Content = httpMessageContent;

                return httpMessage;
            }

            // not in memory
            batchInfo = this.LocalProvider.CacheManager.GetValue<BatchInfo>("ApplyChanges_BatchInfo");

            if (batchInfo == null)
            {
                batchInfo = new BatchInfo(false, this.LocalProvider.Options.BatchDirectory)
                {
                    BatchIndex = 0,
                    BatchPartsInfo = new List<BatchPartInfo>(new[] { bpi }),
                    InMemory = false,
                };

                batchInfo.GenerateNewDirectoryName();
            }
            else
            {
                batchInfo.BatchPartsInfo.Add(bpi);
            }

            var bpId = batchInfo.GenerateNewFileName(httpMessageContent.BatchIndex.ToString());

            // to save the file, we should use the local configuration batch directory
            var fileName = Path.Combine(batchInfo.GetDirectoryFullPath(), bpId);

            BatchPart.Serialize(httpMessageContent.Set, fileName);
            bpi.FileName = fileName;

            this.LocalProvider.CacheManager.Set("ApplyChanges_BatchInfo", batchInfo);

            // Clear the httpMessage set
            if (httpMessageContent != null)
            {
                httpMessageContent.Set.Dispose();
                httpMessageContent.Set = null;
            }


            // if it's last batch sent
            if (bpi.IsLastBatch)
            {
                var (c, s) = await this.ApplyChangesAsync(httpMessage.SyncContext,
                    new MessageApplyChanges
                    {
                        FromScope = scopeInfo,
                        Schema = schema,
                        Policy = httpMessageContent.Policy,
                        //UseBulkOperations = httpMessageContent.UseBulkOperations,
                        //CleanMetadatas = httpMessageContent.CleanMetadatas,
                        ScopeInfoTableName = httpMessageContent.ScopeInfoTableName,
                        Changes = batchInfo
                    });

                this.LocalProvider.CacheManager.Remove("ApplyChanges_BatchInfo");

                httpMessage.SyncContext = c;
                httpMessageContent.ChangesApplied = s;
            }

            httpMessageContent.BatchPartInfo.Clear();
            httpMessageContent.BatchPartInfo.FileName = null;

            httpMessage.Content = httpMessageContent;

            return httpMessage;
        }

        private async Task<HttpMessage> GetLocalTimestampAsync(HttpMessage httpMessage)
        {
            HttpMessageTimestamp httpMessageContent;
            if (httpMessage.Content is HttpMessageTimestamp)
                httpMessageContent = httpMessage.Content as HttpMessageTimestamp;
            else
                httpMessageContent = (httpMessage.Content as JObject).ToObject<HttpMessageTimestamp>();

            if (httpMessageContent == null)
                throw new ArgumentException("Timestamp message could not be null");


            var (ctx, ts) = await this.GetLocalTimestampAsync(httpMessage.SyncContext,
                new MessageTimestamp { ScopeInfoTableName = httpMessageContent.ScopeInfoTableName });

            httpMessageContent.LocalTimestamp = ts;
            httpMessage.SyncContext = ctx;
            httpMessage.Content = httpMessageContent;
            return httpMessage;
        }

        private async Task<HttpMessage> WriteScopesAsync(HttpMessage httpMessage)
        {
            HttpMessageWriteScopes httpMessageContent;
            if (httpMessage.Content is HttpMessageWriteScopes)
                httpMessageContent = httpMessage.Content as HttpMessageWriteScopes;
            else
                httpMessageContent = (httpMessage.Content as JObject).ToObject<HttpMessageWriteScopes>();

            if (httpMessageContent == null)
                throw new ArgumentException("WriteScopes message could not be null");

            var scopes = httpMessageContent.Scopes;

            if (scopes == null || scopes.Count <= 0)
                throw new ArgumentException("WriteScopes scopes could not be null and should have at least 1 scope info");

            var ctx = await this.WriteScopesAsync(httpMessage.SyncContext,
                new MessageWriteScopes
                {
                    ScopeInfoTableName = httpMessageContent.ScopeInfoTableName,
                    Scopes = scopes
                });

            httpMessage.SyncContext = ctx;

            return httpMessage;
        }


        public async Task<(SyncContext, SyncConfiguration)> BeginSessionAsync(SyncContext ctx, MessageBeginSession message)
            => await this.LocalProvider.BeginSessionAsync(ctx, message);
        public async Task<(SyncContext, List<ScopeInfo>)> EnsureScopesAsync(SyncContext ctx, MessageEnsureScopes message)
            => await this.LocalProvider.EnsureScopesAsync(ctx, message);
        public async Task<(SyncContext, DmSet)> EnsureSchemaAsync(SyncContext ctx, MessageEnsureSchema message)
            => await this.LocalProvider.EnsureSchemaAsync(ctx, message);
        public async Task<SyncContext> EnsureDatabaseAsync(SyncContext ctx, MessageEnsureDatabase message)
            => await this.LocalProvider.EnsureDatabaseAsync(ctx, message);
        public async Task<(SyncContext, BatchInfo, DatabaseChangesSelected)> GetChangeBatchAsync(SyncContext ctx, MessageGetChangesBatch message)
            => await this.LocalProvider.GetChangeBatchAsync(ctx, message);
        public async Task<(SyncContext, DatabaseChangesApplied)> ApplyChangesAsync(SyncContext ctx, MessageApplyChanges message)
            => await this.LocalProvider.ApplyChangesAsync(ctx, message);
        public async Task<(SyncContext, long)> GetLocalTimestampAsync(SyncContext ctx, MessageTimestamp message)
            => await this.LocalProvider.GetLocalTimestampAsync(ctx, message);
        public async Task<SyncContext> WriteScopesAsync(SyncContext ctx, MessageWriteScopes message)
            => await this.LocalProvider.WriteScopesAsync(ctx, message);
        public async Task<SyncContext> EndSessionAsync(SyncContext ctx)
            => await this.LocalProvider.EndSessionAsync(ctx);

        public void SetCancellationToken(CancellationToken token)
        {
            // no need in proxy mode, since it's different token than SyncAgent
            return;
        }



        // --------------------------------------------------------------------
        // Dispose
        // --------------------------------------------------------------------

        /// <summary>
        /// Releases all resources
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Releases the unmanaged resources used 
        /// </summary>
        protected virtual void Dispose(bool cleanup)
        {
        }
    }
}
