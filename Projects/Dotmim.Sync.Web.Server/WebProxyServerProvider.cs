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
#if NETSTANDARD
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Session;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
#else
using System.Collections.Specialized;
using System.Net;
using System.Web;
using HttpContext = System.Web.HttpContextBase;
using HttpRequest = System.Web.HttpRequestBase;
using HttpResponse = System.Web.HttpResponseBase;
using System.Net.Http;
#endif

namespace Dotmim.Sync.Web.Server
{

    /// <summary>
    /// Class used when you have to deal with a Web Server
    /// </summary>
    public class WebProxyServerProvider : IProvider, IDisposable
    {
        public CoreProvider LocalProvider { get; private set; }

        /// <summary>
        /// Gets or Sets differents options that could be different from server and client
        /// </summary>
        public SyncOptions Options
        {
            get => this.LocalProvider?.Options;
            set => this.LocalProvider.Options = value;
        }


        /// <summary>
        /// set the progress action used to get progression on the provider
        /// </summary>
        public void SetProgress(IProgress<ProgressArgs> progress) => this.LocalProvider.SetProgress(progress);


        /// <summary>
        /// Subscribe an apply changes failed action
        /// </summary>
        public void InterceptApplyChangesFailed(Func<ApplyChangesFailedArgs, Task> action)
            => this.LocalProvider.GetInterceptor<ApplyChangesFailedArgs>().Set(action);


        /// <summary>
        /// Gets or sets a boolean value to indicate if this service is register as Singleton on web server.
        /// if true, we don't need to use Session, if false, we will try to use session
        /// </summary>
        public bool IsRegisterAsSingleton { get; set; }

        /// <summary>
        /// Use this constructor when you are on the Remote Side, only
        /// </summary>
        public WebProxyServerProvider(CoreProvider localProvider) => this.LocalProvider = localProvider;

        /// <summary>
        /// Gets or sets the Sync configuration handled by the server
        /// </summary>
        public SyncConfiguration Configuration { get; set; }

#if NETSTANDARD
        /// <summary>
        /// Call this method to handle requests on the server, sent by the client
        /// </summary>
        public static async Task HandleRequestAsync(HttpContext context) =>
            await HandleRequestAsync(context, null, CancellationToken.None);
        
        /// <summary>
        /// Call this method to handle requests on the server, sent by the client
        /// </summary>
        public static async Task HandleRequestAsync(HttpContext context, Action<WebProxyServerProvider> action) =>
            await HandleRequestAsync(context, action, CancellationToken.None);

        /// <summary>
        /// Call this method to handle requests on the server, sent by the client
        /// </summary>
        public static async Task HandleRequestAsync(HttpContext context, CancellationToken token) =>
            await HandleRequestAsync(context, null, token);
        
        /// <summary>
        /// Call this method to handle requests on the server, sent by the client
        /// </summary>
        public static async Task HandleRequestAsync(HttpContext context, Action<WebProxyServerProvider> action, CancellationToken cancellationToken)
        {
            var httpRequest = context.Request;
            var httpResponse = context.Response;
            var streamArray = httpRequest.GetBody();

            var serializationFormat = SerializationFormat.Json;
            // Get the serialization format
            if (context.Request.Headers.TryGetHeaderValue("dotmim-sync-serialization-format", out var vs))
                serializationFormat = vs.ToLowerInvariant() == "json" ? SerializationFormat.Json : SerializationFormat.Binary;
            
            WebProxyServerProvider webProxyServerProvider = null;
            var syncSessionId = "";
            HttpMessage httpMessage = null;
            try
            {
                var serializer = BaseConverter<HttpMessage>.GetConverter(serializationFormat);
                httpMessage = serializer.Deserialize(streamArray);
                syncSessionId = httpMessage.SyncContext.SessionId.ToString();

                webProxyServerProvider = GetInstance(context, syncSessionId);
                if (webProxyServerProvider == null) throw new SyncException("WebProxyServerProvider is null!");
                action?.Invoke(webProxyServerProvider);

                // Check if we should handle a session store to handle configuration
                if (!webProxyServerProvider.IsRegisterAsSingleton)
                {
                    if (IsSessionEnabled(context))
                        webProxyServerProvider.LocalProvider.CacheManager = new SessionCache(context);
                }

                var httpMessageResponse =
                    await webProxyServerProvider.GetResponseMessageAsync(httpMessage, cancellationToken);

                var binaryData = serializer.Serialize(httpMessageResponse);
                await httpResponse.GetBody().WriteAsync(binaryData, 0, binaryData.Length);

            }
            catch (Exception ex)
            {
                await WriteExceptionAsync(httpResponse, ex, webProxyServerProvider?.LocalProvider?.ProviderTypeName ?? "ServerLocalProvider");
            }
            finally
            {
                if (!DependencyInjection.RegisterAsSingleton)
                {
                    if (httpMessage != null && httpMessage.Step == HttpStep.EndSession)
                        Cleanup(context.RequestServices.GetService(typeof(DependencyInjection.SyncMemoryCache)), syncSessionId);
                }
            }
        }

        private static void Cleanup(object memoryCache, string syncSessionId)
        {
            if (memoryCache == null || string.IsNullOrWhiteSpace(syncSessionId)) return;
            Task.Run(() =>
            {
                try
                {
                    (memoryCache as IMemoryCache)?.Remove(syncSessionId);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
            });
        }

        private static WebProxyServerProvider GetInstance(HttpContext context, string syncSessionId)
        {
            WebProxyServerProvider webProxyServerProvider;
            if (DependencyInjection.RegisterAsSingleton)
            {
                webProxyServerProvider = (WebProxyServerProvider) context.RequestServices.GetService(typeof(WebProxyServerProvider));
                if (webProxyServerProvider == null) throw new SyncException("WebProxyServerProvider service not found");
                return webProxyServerProvider;
            }

            if (!(context.RequestServices.GetService(typeof(DependencyInjection.SyncMemoryCache)) is IMemoryCache cache))
                throw new SyncException("Cache is not configured!");
                
            if (string.IsNullOrWhiteSpace(syncSessionId))
                throw new ArgumentNullException(nameof(syncSessionId));
            
            webProxyServerProvider = (WebProxyServerProvider) cache.Get(syncSessionId);
            if (webProxyServerProvider != null)
                return webProxyServerProvider;
            
            webProxyServerProvider = DependencyInjection.GetNewWebProxyServerProvider();
            cache.Set(syncSessionId, webProxyServerProvider, TimeSpan.FromHours(1));
            return webProxyServerProvider;
        }

        private async Task<HttpMessage> GetResponseMessageAsync(HttpMessage httpMessage,
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
        /// Write exception to output message
        /// </summary>
        public static async Task WriteExceptionAsync(HttpResponse httpResponse, Exception ex, string providerTypeName)
        {
            // Check if it's an unknown error, not managed (yet)
            if (!(ex is SyncException syncException))
                syncException = new SyncException(ex.Message, SyncStage.None, providerTypeName, SyncExceptionType.Unknown);

            var webXMessage = JsonConvert.SerializeObject(syncException);

            httpResponse.StatusCode = StatusCodes.Status400BadRequest;
            httpResponse.ContentLength = webXMessage.Length;
            await httpResponse.WriteAsync(webXMessage);
            Console.WriteLine(syncException);
        }
#else
        /// <summary>
        /// Call this method to handle requests on the server, sent by the client
        /// </summary>
        public async Task<HttpResponseMessage> HandleRequestAsync(HttpRequestMessage httpRequest, HttpContext context)
        {
            return await HandleRequestAsync(httpRequest, context, CancellationToken.None);
        }

        /// <summary>
        /// Call this method to handle requests on the server, sent by the client
        /// </summary>
        public async Task<HttpResponseMessage> HandleRequestAsync(HttpRequestMessage httpRequest, HttpContext context, CancellationToken cancellationToken)
        {
            var streamArray = await httpRequest.Content.ReadAsStreamAsync();

            SerializationFormat serializationFormat = SerializationFormat.Json;
            // Get the serialization format
            if (httpRequest.Headers.TryGetHeaderValue("dotmim-sync-serialization-format", out string vs))
                serializationFormat = vs.ToLowerInvariant() == "json" ? SerializationFormat.Json : SerializationFormat.Binary;

            // Check if we should handle a session store to handle configuration
            if (!this.IsRegisterAsSingleton)
            {
                if (IsSessionEnabled(context))
                    this.LocalProvider.CacheManager = new SessionCache(context);
            }

            try
            {
                var serializer = BaseConverter<HttpMessage>.GetConverter(serializationFormat);

                var httpMessage = serializer.Deserialize(streamArray);

                HttpMessage httpMessageResponse = null;
                switch (httpMessage.Step)
                {
                    case HttpStep.BeginSession:
                        // on first message, replace the Configuration with the server one !
                        httpMessageResponse = await BeginSessionAsync(httpMessage);
                        break;
                    case HttpStep.EnsureScopes:
                        httpMessageResponse = await EnsureScopesAsync(httpMessage);
                        break;
                    case HttpStep.EnsureConfiguration:
                        httpMessageResponse = await EnsureSchemaAsync(httpMessage);
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
                //await httpResponse.GetBody().WriteAsync(binaryData, 0, binaryData.Length);
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new ByteArrayContent(binaryData)
                };

            }
            catch (Exception ex)
            {
                return await this.WriteExceptionAsync(ex);
            }
        }

        /// <summary>
        /// Write exception to output message
        /// </summary>
        public Task<HttpResponseMessage> WriteExceptionAsync(Exception ex)
        {

            // Check if it's an unknwon error, not managed (yet)
            if (!(ex is SyncException syncException))
                syncException = new SyncException(ex.Message, SyncStage.None, SyncExceptionType.Unknown);

            var webXMessage = JsonConvert.SerializeObject(syncException);

            var response = new HttpResponseMessage(HttpStatusCode.BadRequest)
            {
                Content = new StringContent(webXMessage)
            };

            return Task.FromResult(response);
        }

#endif


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
                this.Configuration ?? httpMessageBeginSession.Configuration;

            // Begin the session, requesting the server for the correct configuration
            (var ctx, var conf) =
                await this.BeginSessionAsync(httpMessage.SyncContext,
                        httpMessageBeginSession as MessageBeginSession);

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

            if (this.Configuration == null)
                throw new InvalidOperationException("No sync configuration was provided. Make sure you create a SyncConfiguration object and pass it to the WebProxyServerProvider!");

            // If the Conf is hosted by the server, we try to get the tables from it, overriding the client schema, if passed
            DmSet schema = null;
            if (this.Configuration.Schema != null)
                schema = this.Configuration.Schema;
            else if (httpMessageContent.Schema != null)
                schema = httpMessageContent.Schema.ConvertToDmSet();

            if (httpMessageContent.Schema != null)
            {
                httpMessageContent.Schema.Dispose();
                httpMessageContent.Schema = null;
            }

            (httpMessage.SyncContext, schema) = await this.EnsureSchemaAsync(httpMessage.SyncContext,
                new MessageEnsureSchema { Schema = schema });

            httpMessageContent.Schema = new DmSetSurrogate(schema);

            schema.Clear();
            schema = null;

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
                    if (this.Options.CleanMetadatas)
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
                if (this.Options.CleanMetadatas)
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
                batchInfo = new BatchInfo(true, this.Options.BatchDirectory)
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
                batchInfo = new BatchInfo(false, this.Options.BatchDirectory)
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


#if NETSTANDARD
        public static bool IsSessionEnabled(HttpContext context)
        {
            // try to get the session store service from DI
            var sessionStore = context.RequestServices.GetService(typeof(ISessionStore));
            return sessionStore != null;
        }
#else
        public bool IsSessionEnabled(HttpContext context)
        {
            try
            {
                return context.Session != null;
            }
            // if httpcontextbase is called without any overload, it will throw a NotImplementedException
            catch (NotImplementedException)
            {
                return false;
            }
        }
#endif

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

    internal static class Extensions
    {
#if NETSTANDARD
        public static bool TryGetHeaderValue(this IHeaderDictionary n, string key, out string header)
        {
            if (n.TryGetValue(key, out var vs))
            {
                header = vs[0];
                return true;
            }

            header = null;
            return false;
        }

        public static Stream GetBody(this HttpRequest r) => r.Body;
        public static Stream GetBody(this HttpResponse r) => r.Body;
#else
        public static bool TryGetHeaderValue(this HttpRequestHeaders n, string key, out string header)
        {
            if (n.TryGetValues(key, out var vs))
            {
                header = vs.First();
                return true;
            }

            header = null;
            return false;
        }
#endif
    }
}
