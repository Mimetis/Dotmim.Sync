using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Core.Builders;
using Dotmim.Sync.Core.Scope;
using Dotmim.Sync.Core.Batch;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using System.IO;
using System.Linq;
using DmBinaryFormatter;
using System.Net;
using System.Threading;
using Newtonsoft.Json;
using Dotmim.Sync.Core.Enumerations;
using Dotmim.Sync.Core.Serialization;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Primitives;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Data;
using Dotmim.Sync.Core.Cache;

namespace Dotmim.Sync.Core.Proxy
{

    /// <summary>
    /// Class used when you have to deal with a Web Server
    /// </summary>
    public class WebProxyServerProvider : IResponseHandler
    {
        private CoreProvider localProvider;
        private SerializationFormat serializationFormat;
        private BaseConverter<HttpMessage> serializer;

        public event EventHandler<SyncProgressEventArgs> SyncProgress;
        public event EventHandler<ApplyChangeFailedEventArgs> ApplyChangedFailed;

        /// <summary>
        /// Use this constructor when you are on the Remote Side, only
        /// </summary>
        public WebProxyServerProvider(CoreProvider localProvider) : this(localProvider, SerializationFormat.Json)
        {
        }
        public WebProxyServerProvider(CoreProvider localProvider, SerializationFormat serializationFormat)
        {
            this.localProvider = localProvider;
            this.serializationFormat = serializationFormat;
            this.serializer = BaseConverter<HttpMessage>.GetConverter(this.serializationFormat);
        }

        /// <summary>
        /// Call this method to handle requests on the server, sent by the client
        /// </summary>
        public async Task HandleRequestAsync(HttpContext context)
        {
            await HandleRequestAsync(context, CancellationToken.None);
        }


        /// <summary>
        /// Call this method to handle requests on the server, sent by the client
        /// </summary>
        public async Task HandleRequestAsync(HttpContext context, CancellationToken cancellationToken)
        {
            var httpRequest = context.Request;
            var httpResponse = context.Response;
            var streamArray = httpRequest.Body;
            this.localProvider.CacheManager = new SessionCache(context);

            try
            {
                var httpMessage = serializer.Deserialize(streamArray);

                HttpMessage httpMessageResponse = null;
                switch (httpMessage.Step)
                {
                    case HttpStep.BeginSession:
                        httpMessageResponse = await BeginSessionAsync(httpMessage);
                        break;
                    case HttpStep.EnsureScopes:
                        httpMessageResponse = await EnsureScopesAsync(httpMessage);
                        break;
                    case HttpStep.EnsureConfiguration:
                        httpMessageResponse = await EnsureConfigurationAsync(httpMessage);
                        break;
                    case HttpStep.EnsureDatabase:
                        httpMessageResponse = await EnsureDatabaseAsync(httpMessage);
                        break;
                    case HttpStep.GetChangeBatch:
                        httpMessageResponse = await GetChangeBatchAsync(httpMessage);
                        break;
                    case HttpStep.ApplyChanges:
                        httpMessageResponse = await ApplyChangesAsync(httpMessage);

                        break;
                    case HttpStep.GetLocalTimestamp:
                        httpMessageResponse = await GetLocalTimestampAsync(httpMessage);
                        break;
                    case HttpStep.WriteScopes:
                        httpMessageResponse = await WriteScopesAsync(httpMessage);
                        break;
                    case HttpStep.EndSession:
                        httpMessageResponse = await EndSessionAsync(httpMessage);
                        break;
                }

                var binaryData = serializer.Serialize(httpMessageResponse);
                await httpResponse.Body.WriteAsync(binaryData, 0, binaryData.Length);

            }
            catch (Exception ex)
            {
                await this.WriteExceptionAsync(httpResponse, ex);
            }
        }


        private async Task<HttpMessage> BeginSessionAsync(HttpMessage httpMessage)
        {
            // call inner provider
            httpMessage.SyncContext = await this.BeginSessionAsync(httpMessage.SyncContext);

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
            if (httpMessage.EnsureScopes == null)
                throw new ArgumentException("EnsureScopeMessage could not be null");

            var (syncContext, lstScopes) = await this.EnsureScopesAsync(httpMessage.SyncContext, httpMessage.EnsureScopes.ScopeName, httpMessage.EnsureScopes.ClientReferenceId);

            // Local scope is the server scope here
            httpMessage.EnsureScopes.Scopes = lstScopes;
            httpMessage.SyncContext = syncContext;

            return httpMessage;
        }

        private async Task<HttpMessage> EnsureConfigurationAsync(HttpMessage httpMessage)
        {
            // we are calling the server side, so no need to pass a config object, since the config is stored on the server
            var (syncContext, conf) = await this.EnsureConfigurationAsync(httpMessage.SyncContext);

            httpMessage.SyncContext = syncContext;
            var scopeSurrogate = new DmSetSurrogate(conf.ScopeSet);
            conf.ScopeSet = null;
            httpMessage.EnsureConfiguration = new HttpEnsureConfigurationMessage
            {
                Configuration = conf,
                ConfigurationSet = scopeSurrogate
            };

            return httpMessage;
        }

        private async Task<HttpMessage> EnsureDatabaseAsync(HttpMessage httpMessage)
        {
            if (httpMessage.EnsureDatabase == null)
                throw new ArgumentException("EnsureDatabase message could not be null");

            httpMessage.SyncContext = await this.EnsureDatabaseAsync(httpMessage.SyncContext, httpMessage.EnsureDatabase.ScopeInfo, httpMessage.EnsureDatabase.DbBuilderOption);

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
            if (httpMessage.GetChangeBatch == null)
                throw new ArgumentException("GetChangeBatch message could not be null");

            var scopeInfo = httpMessage.GetChangeBatch.ScopeInfo;

            if (scopeInfo == null)
                throw new ArgumentException("GetChangeBatch ScopeInfo could not be null");

            // if we get the first batch info request, made it.
            // Server is able to define if it's in memory or not
            if (httpMessage.GetChangeBatch.BatchIndexRequested == 0)
            {
                var (syncContext, bi, s) = await this.GetChangeBatchAsync(httpMessage.SyncContext, scopeInfo);

                // Select the first bpi needed (index == 0)
                if (bi.BatchPartsInfo.Count > 0)
                    httpMessage.GetChangeBatch.BatchPartInfo = bi.BatchPartsInfo.First(bpi => bpi.Index == 0);

                httpMessage.SyncContext = syncContext;
                httpMessage.GetChangeBatch.InMemory = bi.InMemory;
                httpMessage.GetChangeBatch.ChangesStatistics = s;

                // if no changes, return
                if (httpMessage.GetChangeBatch.BatchPartInfo == null)
                    return httpMessage;

                // if we are not in memory, we set the BI in session, to be able to get it back on next request
                if (!bi.InMemory)
                {
                    // Save the BatchInfo
                    this.localProvider.CacheManager.Set("GetChangeBatch_BatchInfo", bi);
                    this.localProvider.CacheManager.Set("GetChangeBatch_ChangesStatistics", s);

                    // load the batchpart set directly, to be able to send it back
                    var batchPart = httpMessage.GetChangeBatch.BatchPartInfo.GetBatch();
                    httpMessage.GetChangeBatch.Set = batchPart.DmSetSurrogate;
                }
                else
                {
                    // We are in memory, so generate the DmSetSurrogate to be able to send it back
                    httpMessage.GetChangeBatch.Set = new DmSetSurrogate(httpMessage.GetChangeBatch.BatchPartInfo.Set);
                    httpMessage.GetChangeBatch.BatchPartInfo.Set.Clear();
                    httpMessage.GetChangeBatch.BatchPartInfo.Set = null;
                }

                // no need fileName info
                httpMessage.GetChangeBatch.BatchPartInfo.FileName = null;

                return httpMessage;
            }

            // We are in batch mode here
            var batchInfo = this.localProvider.CacheManager.GetValue<BatchInfo>("GetChangeBatch_BatchInfo");
            var stats = this.localProvider.CacheManager.GetValue<ChangesStatistics>("GetChangeBatch_ChangesStatistics");

            if (batchInfo == null)
                throw new ArgumentNullException("batchInfo stored in session can't be null if request more batch part info.");

            httpMessage.GetChangeBatch.ChangesStatistics = stats;
            httpMessage.GetChangeBatch.InMemory = batchInfo.InMemory;

            var batchPartInfo = batchInfo.BatchPartsInfo.FirstOrDefault(bpi => bpi.Index == httpMessage.GetChangeBatch.BatchIndexRequested);

            httpMessage.GetChangeBatch.BatchPartInfo = batchPartInfo;

            // load the batchpart set directly, to be able to send it back
            httpMessage.GetChangeBatch.Set = httpMessage.GetChangeBatch.BatchPartInfo.GetBatch().DmSetSurrogate;

            if (httpMessage.GetChangeBatch.BatchPartInfo.IsLastBatch)
            {
                this.localProvider.CacheManager.Remove("GetChangeBatch_BatchInfo");
                this.localProvider.CacheManager.Remove("GetChangeBatch_ChangesStatistics");
            }

            return httpMessage;
        }

        private async Task<HttpMessage> ApplyChangesAsync(HttpMessage httpMessage)
        {
            if (httpMessage.ApplyChanges == null)
                throw new ArgumentException("ApplyChanges message could not be null");

            var scopeInfo = httpMessage.ApplyChanges.ScopeInfo;

            if (scopeInfo == null)
                throw new ArgumentException("ApplyChanges ScopeInfo could not be null");

            BatchInfo batchInfo;
            var bpi = httpMessage.ApplyChanges.BatchPartInfo;

            if (httpMessage.ApplyChanges.InMemory)
            {
                batchInfo = new BatchInfo
                {
                    BatchIndex = 0,
                    BatchPartsInfo = new List<BatchPartInfo>(new[] { bpi }),
                    InMemory = true
                };

                bpi.Set = httpMessage.ApplyChanges.Set.ConvertToDmSet();

                httpMessage.ApplyChanges.Set.Dispose();
                httpMessage.ApplyChanges.Set = null;

                var (c, s) = await this.ApplyChangesAsync(httpMessage.SyncContext, scopeInfo, batchInfo);

                httpMessage.SyncContext = c;
                httpMessage.ApplyChanges.ChangesStatistics = s;

                return httpMessage;
            }

            // not in memory
            batchInfo = this.localProvider.CacheManager.GetValue<BatchInfo>("ApplyChanges_BatchInfo");

            if (batchInfo == null)
            {
                batchInfo = new BatchInfo
                {
                    BatchIndex = 0,
                    BatchPartsInfo = new List<BatchPartInfo>(new[] { bpi }),
                    InMemory = false,
                    Directory = BatchInfo.GenerateNewDirectoryName()
                };
            }
            else
            {
                batchInfo.BatchPartsInfo.Add(bpi);
            }

            var bpId = BatchInfo.GenerateNewFileName(httpMessage.ApplyChanges.BatchIndex.ToString());
            var fileName = Path.Combine(this.localProvider.GetCacheConfiguration().BatchDirectory,
                batchInfo.Directory, bpId);
            BatchPart.Serialize(httpMessage.ApplyChanges.Set, fileName);
            bpi.FileName = fileName;
            this.localProvider.CacheManager.Set("ApplyChanges_BatchInfo", batchInfo);

            // Clear the httpMessage set
            httpMessage.ApplyChanges.Set.Dispose();
            httpMessage.ApplyChanges.Set = null;

            // if it's last batch sent
            if (bpi.IsLastBatch)
            {
                var (c, s) = await this.ApplyChangesAsync(httpMessage.SyncContext, scopeInfo, batchInfo);
                this.localProvider.CacheManager.Remove("ApplyChanges_BatchInfo");
                httpMessage.SyncContext = c;
                httpMessage.ApplyChanges.ChangesStatistics = s;
            }

            return httpMessage;
        }


        private async Task<HttpMessage> GetLocalTimestampAsync(HttpMessage httpMessage)
        {
            var (ctx, ts) = await this.GetLocalTimestampAsync(httpMessage.SyncContext);

            httpMessage.GetLocalTimestamp = new HttpGetLocalTimestampMessage
            {
                LocalTimestamp = ts
            };

            httpMessage.SyncContext = ctx;

            return httpMessage;
        }

        private async Task<HttpMessage> WriteScopesAsync(HttpMessage httpMessage)
        {
            if (httpMessage.WriteScopes == null)
                throw new ArgumentException("WriteScopes message could not be null");

            var scopes = httpMessage.WriteScopes.Scopes;

            if (scopes == null || scopes.Count <= 0)
                throw new ArgumentException("WriteScopes scopes could not be null and should have at least 1 scope info");

            var ctx = await this.WriteScopesAsync(httpMessage.SyncContext, scopes);

            httpMessage.SyncContext = ctx;

            return httpMessage;
        }


        /// <summary>
        /// Extract session id and sync stage from header
        /// </summary>
        //private static SyncContext GetSyncContextFromHeader(HttpRequest httpRequest)
        //{
        //    if (!httpRequest.Headers.ContainsKey("Dotmim-Sync-Step"))
        //        throw new Exception("No header containing SyncStage");

        //    if (!httpRequest.Headers.ContainsKey("Dotmim-Sync-SessionId"))
        //        throw new Exception("No header containing Session Id");

        //    var stepValue = httpRequest.Headers.First(kvp => kvp.Key == "Dotmim-Sync-Step");
        //    SyncStage httpStep = (SyncStage)Enum.Parse(typeof(SyncStage), stepValue.Value);

        //    var sessionIdValue = httpRequest.Headers.First(kvp => kvp.Key == "Dotmim-Sync-SessionId");
        //    Guid sessionId = Guid.Parse(sessionIdValue.Value);

        //    SyncContext syncContext = new SyncContext(sessionId);
        //    syncContext.SyncStage = httpStep;

        //    return syncContext;
        //}

        /// <summary>
        /// Write exception to output message
        /// </summary>
        public async Task WriteExceptionAsync(HttpResponse httpResponse, Exception ex)
        {
            var webx = WebSyncException.GetWebSyncException(ex);
            var webXMessage = JsonConvert.SerializeObject(webx);
            httpResponse.StatusCode = StatusCodes.Status400BadRequest;
            httpResponse.ContentLength = webXMessage.Length;
            await httpResponse.WriteAsync(webXMessage);

        }

        public async Task<(SyncContext, ChangesStatistics)> ApplyChangesAsync(SyncContext ctx, ScopeInfo fromScope, BatchInfo changes)
            => await this.localProvider.ApplyChangesAsync(ctx, fromScope, changes);
        public async Task<SyncContext> BeginSessionAsync(SyncContext ctx)
            => await this.localProvider.BeginSessionAsync(ctx);
        public async Task<SyncContext> EndSessionAsync(SyncContext ctx)
            => await this.localProvider.EndSessionAsync(ctx);
        public async Task<SyncContext> EnsureDatabaseAsync(SyncContext ctx, ScopeInfo scopeInfo, DbBuilderOption options)
            => await this.localProvider.EnsureDatabaseAsync(ctx, scopeInfo, options);
        public async Task<(SyncContext, List<ScopeInfo>)> EnsureScopesAsync(SyncContext ctx, string scopeName, Guid? clientReferenceId = null)
            => await this.localProvider.EnsureScopesAsync(ctx, scopeName, clientReferenceId);
        public async Task<(SyncContext, BatchInfo, ChangesStatistics)> GetChangeBatchAsync(SyncContext ctx, ScopeInfo scopeInfo)
            => await this.localProvider.GetChangeBatchAsync(ctx, scopeInfo);
        public async Task<(SyncContext, Int64)> GetLocalTimestampAsync(SyncContext ctx)
            => await this.localProvider.GetLocalTimestampAsync(ctx);
        public async Task<SyncContext> WriteScopesAsync(SyncContext ctx, List<ScopeInfo> scopes)
            => await this.localProvider.WriteScopesAsync(ctx, scopes);
        public async Task<(SyncContext, ServiceConfiguration)> EnsureConfigurationAsync(SyncContext ctx, ServiceConfiguration configuration = null)
            => await this.localProvider.EnsureConfigurationAsync(ctx, configuration);

        public void SetCancellationToken(CancellationToken token)
        {
            // no need in proxy mode, since it's different token than from syncagent
            return;
        }
    }
}
