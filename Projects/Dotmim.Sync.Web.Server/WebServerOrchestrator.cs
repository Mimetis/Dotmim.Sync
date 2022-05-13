//using Dotmim.Sync.Batch;
//using Dotmim.Sync.Builders;
//using Dotmim.Sync.Enumerations;
//using Dotmim.Sync.Serialization;
//using Dotmim.Sync.Web.Client;
//using Microsoft.AspNetCore.Http;
//using Newtonsoft.Json;
//using Newtonsoft.Json.Linq;
//using System;
//using System.Collections.Generic;
//using System.IO;
//using System.IO.Compression;
//using System.Linq;
//using System.Text;
//using System.Threading;
//using System.Threading.Tasks;

//namespace Dotmim.Sync.Web.Server
//{
//    public class WebServerOrchestrator : RemoteOrchestrator
//    {
//        /// <summary>
//        /// Default ctor. Using default options and schema
//        /// </summary>
//        public WebServerOrchestrator(CoreProvider provider, SyncOptions options, SyncSetup setup, WebServerOptions webServerOptions = null, string scopeName = SyncOptions.DefaultScopeName)
//            : base(provider, options)
//        {
//            this.Setup = setup;
//            this.WebServerOptions = webServerOptions ?? new WebServerOptions();
//            this.ScopeName = scopeName;
//        }

//        public WebServerOrchestrator(CoreProvider provider, string[] tables, string scopeName = SyncOptions.DefaultScopeName)
//            : this(provider, new SyncOptions(), new SyncSetup(tables), new WebServerOptions(), scopeName)
//        {
//        }

//        /// Client Converter
//        private IConverter clientConverter;

//        public SyncSetup Setup { get; set; }

//        /// <summary>
//        /// Gets or Sets Web server options parameters
//        /// </summary>
//        public WebServerOptions WebServerOptions { get; set; }
//        public string ScopeName { get; set; }


//        /// <summary>
//        /// Call this method to handle requests on the server, sent by the client
//        /// </summary>
//        public virtual Task HandleRequestAsync(HttpContext context, CancellationToken token = default, IProgress<ProgressArgs> progress = null) =>
//            HandleRequestAsync(context, null, token, progress);

//        /// <summary>
//        /// Call this method to handle requests on the server, sent by the client
//        /// </summary>
//        public virtual async Task HandleRequestAsync(HttpContext httpContext, Action<RemoteOrchestrator> action, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
//        {
//            var httpRequest = httpContext.Request;
//            var httpResponse = httpContext.Response;
//            var serAndsizeString = string.Empty;
//            var cliConverterKey = string.Empty;

//            if (TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-serialization-format", out var vs))
//                serAndsizeString = vs.ToLowerInvariant();

//            if (TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-converter", out var cs))
//                cliConverterKey = cs.ToLowerInvariant();

//            if (!TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-session-id", out var sessionId))
//                throw new HttpHeaderMissingException("dotmim-sync-session-id");

//            if (!TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-scope-name", out var scopeName))
//                throw new HttpHeaderMissingException("dotmim-sync-scope-name");

//            if (!TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-step", out string iStep))
//                throw new HttpHeaderMissingException("dotmim-sync-step");

//            var step = (HttpStep)Convert.ToInt32(iStep);
//            var readableStream = new MemoryStream();

//            try
//            {
//                // Copty stream to a readable and seekable stream
//                // HttpRequest.Body is a HttpRequestStream that is readable but can't be Seek
//                await httpRequest.Body.CopyToAsync(readableStream);
//                httpRequest.Body.Close();
//                httpRequest.Body.Dispose();

//                // if Hash is present in header, check hash
//                if (TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-hash", out string hashStringRequest))
//                    HashAlgorithm.SHA256.EnsureHash(readableStream, hashStringRequest);
//                else
//                    readableStream.Seek(0, SeekOrigin.Begin);

//                // load session
//                await httpContext.Session.LoadAsync(cancellationToken);

//                // Get schema and clients batch infos / summaries, from session
//                var schema = httpContext.Session.Get<SyncSet>(scopeName);
//                var sessionCache = httpContext.Session.Get<SessionCache>(sessionId);

//                // HttpStep.EnsureSchema is the first call from client when client is new
//                // HttpStep.EnsureScopes is the first call from client when client is not new
//                // This is the only moment where we are initializing the sessionCache and store it in session
//                if (sessionCache == null && (step == HttpStep.EnsureSchema || step == HttpStep.EnsureScopes || step == HttpStep.GetRemoteClientTimestamp))
//                {
//                    sessionCache = new SessionCache();
//                    httpContext.Session.Set(sessionId, sessionCache);
//                    httpContext.Session.SetString("session_id", sessionId);
//                }

//                // if sessionCache is still null, then we are in a step where it should not be null.
//                // Probably because of a weird server restart or something...
//                if (sessionCache == null)
//                    throw new HttpSessionLostException();

//                // check session id
//                var tempSessionId = httpContext.Session.GetString("session_id");

//                // scope context
//                var ctx = this.GetContext(scopeName);

//                if (string.IsNullOrEmpty(tempSessionId) || tempSessionId != sessionId)
//                    throw new HttpSessionLostException();

//                // Check if sanitized schema is still there
//                if (sessionCache.ClientBatchInfo != null
//                    && sessionCache.ClientBatchInfo.SanitizedSchema != null && sessionCache.ClientBatchInfo.SanitizedSchema.Tables.Count == 0
//                    && schema != null && schema.Tables.Count > 0)
//                    foreach (var table in schema.Tables)
//                        DbSyncAdapter.CreateChangesTable(schema.Tables[table.TableName, table.SchemaName], sessionCache.ClientBatchInfo.SanitizedSchema);

//                // action from user if available
//                action?.Invoke(this);

//                // Get the serializer and batchsize
//                (var clientBatchSize, var clientSerializerFactory) = this.GetClientSerializer(serAndsizeString);

//                // Get converter used by client
//                // Can be null
//                var clientConverter = this.GetClientConverter(cliConverterKey);
//                this.clientConverter = clientConverter;

//                byte[] binaryData = null;
//                switch (step)
//                {
//                    case HttpStep.EnsureScopes:
//                        var m1 = await clientSerializerFactory.GetSerializer<HttpMessageEnsureScopesRequest>().DeserializeAsync(readableStream);
//                        await this.InterceptAsync(new HttpGettingRequestArgs(httpContext, m1.SyncContext, sessionCache, step), progress, cancellationToken).ConfigureAwait(false);
//                        var s1 = await this.EnsureScopesAsync(httpContext, m1, sessionCache, cancellationToken, progress).ConfigureAwait(false);
//                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageEnsureScopesResponse>().SerializeAsync(s1);
//                        break;
//                    //case HttpStep.EnsureSchema:
//                    //    var m11 = await clientSerializerFactory.GetSerializer<HttpMessageEnsureScopesRequest>().DeserializeAsync(readableStream);
//                    //    await this.InterceptAsync(new HttpGettingRequestArgs(httpContext, m11.SyncContext, sessionCache, step), progress, cancellationToken).ConfigureAwait(false);
//                    //    var s11 = await this.EnsureSchemaAsync(httpContext, m11, sessionCache, cancellationToken, progress).ConfigureAwait(false);
//                    //    binaryData = await clientSerializerFactory.GetSerializer<HttpMessageEnsureSchemaResponse>().SerializeAsync(s11);
//                    //    break;

//                    // version >= 0.8    
//                    case HttpStep.SendChangesInProgress:
//                        var m22 = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesRequest>().DeserializeAsync(readableStream);
//                        await this.InterceptAsync(new HttpGettingRequestArgs(httpContext, m22.SyncContext, sessionCache, step), progress, cancellationToken).ConfigureAwait(false);
//                        await this.InterceptAsync(new HttpGettingClientChangesArgs(m22, httpContext.Request.Host.Host, sessionCache), progress, cancellationToken).ConfigureAwait(false);
//                        var s22 = await this.ApplyThenGetChangesAsync2(httpContext, m22, sessionCache, clientBatchSize, cancellationToken, progress).ConfigureAwait(false);
//                        //await this.InterceptAsync(new HttpSendingServerChangesArgs(s22.HttpMessageSendChangesResponse, context.Request.Host.Host, sessionCache, false), cancellationToken).ConfigureAwait(false);
//                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageSummaryResponse>().SerializeAsync(s22);
//                        break;

//                    case HttpStep.GetChanges:
//                        var m3 = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesRequest>().DeserializeAsync(readableStream);
//                        await this.InterceptAsync(new HttpGettingRequestArgs(httpContext, m3.SyncContext, sessionCache, step), progress, cancellationToken).ConfigureAwait(false);
//                        await this.InterceptAsync(new HttpGettingClientChangesArgs(m3, httpContext.Request.Host.Host, sessionCache), progress, cancellationToken).ConfigureAwait(false);
//                        var s3 = await this.GetChangesAsync(httpContext, m3, sessionCache, clientBatchSize, cancellationToken, progress);
//                        await this.InterceptAsync(new HttpSendingServerChangesArgs(s3, httpContext.Request.Host.Host, sessionCache, false), progress, cancellationToken).ConfigureAwait(false);
//                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesResponse>().SerializeAsync(s3);
//                        break;

//                    case HttpStep.GetMoreChanges:
//                        var m4 = await clientSerializerFactory.GetSerializer<HttpMessageGetMoreChangesRequest>().DeserializeAsync(readableStream);
//                        await this.InterceptAsync(new HttpGettingRequestArgs(httpContext, m4.SyncContext, sessionCache, step), progress, cancellationToken).ConfigureAwait(false);
//                        var s4 = await this.GetMoreChangesAsync(httpContext, m4, sessionCache, cancellationToken, progress);
//                        await this.InterceptAsync(new HttpSendingServerChangesArgs(s4, httpContext.Request.Host.Host, sessionCache, false), progress, cancellationToken).ConfigureAwait(false);
//                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesResponse>().SerializeAsync(s4);
//                        break;

//                    case HttpStep.GetSnapshot:
//                        var m5 = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesRequest>().DeserializeAsync(readableStream);
//                        await this.InterceptAsync(new HttpGettingRequestArgs(httpContext, m5.SyncContext, sessionCache, step), progress, cancellationToken).ConfigureAwait(false);
//                        var s5 = await this.GetSnapshotAsync(httpContext, m5, sessionCache, cancellationToken, progress);
//                        await this.InterceptAsync(new HttpSendingServerChangesArgs(s5, httpContext.Request.Host.Host, sessionCache, true), progress, cancellationToken).ConfigureAwait(false);
//                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesResponse>().SerializeAsync(s5);
//                        break;

//                    // version >= 0.8    
//                    case HttpStep.GetSummary:
//                        var m55 = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesRequest>().DeserializeAsync(readableStream);
//                        await this.InterceptAsync(new HttpGettingRequestArgs(httpContext, m55.SyncContext, sessionCache, step), progress, cancellationToken).ConfigureAwait(false);
//                        var s55 = await this.GetSnapshotSummaryAsync(httpContext, m55, sessionCache, cancellationToken, progress);
//                        //await this.InterceptAsync(new HttpSendingServerChangesArgs(s5.HttpMessageSendChangesResponse, context.Request.Host.Host, sessionCache, true), cancellationToken).ConfigureAwait(false);
//                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageSummaryResponse>().SerializeAsync(s55);

//                        break;
//                    case HttpStep.SendEndDownloadChanges:
//                        var m56 = await clientSerializerFactory.GetSerializer<HttpMessageGetMoreChangesRequest>().DeserializeAsync(readableStream);
//                        await this.InterceptAsync(new HttpGettingRequestArgs(httpContext, m56.SyncContext, sessionCache, step), progress, cancellationToken).ConfigureAwait(false);
//                        var s56 = await this.SendEndDownloadChangesAsync(httpContext, m56, sessionCache, cancellationToken, progress);
//                        await this.InterceptAsync(new HttpSendingServerChangesArgs(s56, httpContext.Request.Host.Host, sessionCache, false), progress, cancellationToken).ConfigureAwait(false);
//                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesResponse>().SerializeAsync(s56);
//                        break;

//                    case HttpStep.GetEstimatedChangesCount:
//                        var m6 = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesRequest>().DeserializeAsync(readableStream);
//                        await this.InterceptAsync(new HttpGettingRequestArgs(httpContext, m6.SyncContext, sessionCache, step), progress, cancellationToken).ConfigureAwait(false);
//                        var s6 = await this.GetEstimatedChangesCountAsync(httpContext, m6, cancellationToken, progress);
//                        await this.InterceptAsync(new HttpSendingServerChangesArgs(s6, httpContext.Request.Host.Host, sessionCache, false), progress, cancellationToken).ConfigureAwait(false);
//                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesResponse>().SerializeAsync(s6);
//                        break;
//                    case HttpStep.GetRemoteClientTimestamp:
//                        var m7 = await clientSerializerFactory.GetSerializer<HttpMessageRemoteTimestampRequest>().DeserializeAsync(readableStream);
//                        await this.InterceptAsync(new HttpGettingRequestArgs(httpContext, m7.SyncContext, sessionCache, step), progress, cancellationToken).ConfigureAwait(false);
//                        var s7 = await this.GetRemoteClientTimestampAsync(httpContext, m7, cancellationToken, progress);
//                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageRemoteTimestampResponse>().SerializeAsync(s7);
//                        break;
//                }

//                httpContext.Session.Set(scopeName, schema);
//                httpContext.Session.Set(sessionId, sessionCache);
//                await httpContext.Session.CommitAsync(cancellationToken);

//                // Adding the serialization format used and session id
//                httpResponse.Headers.Add("dotmim-sync-session-id", sessionId.ToString());
//                httpResponse.Headers.Add("dotmim-sync-serialization-format", clientSerializerFactory.Key);

//                // calculate hash
//                var hash = HashAlgorithm.SHA256.Create(binaryData);
//                var hashString = Convert.ToBase64String(hash);
//                // Add hash to header
//                httpResponse.Headers.Add("dotmim-sync-hash", hashString);

//                // data to send back, as the response
//                byte[] data = this.EnsureCompression(httpRequest, httpResponse, binaryData);

//                // save session
//                await httpContext.Session.CommitAsync(cancellationToken);


//                await this.InterceptAsync(new HttpSendingResponseArgs(httpContext, ctx, sessionCache, data, step), progress, cancellationToken).ConfigureAwait(false);

//                await httpResponse.Body.WriteAsync(data, 0, data.Length, cancellationToken).ConfigureAwait(false);
//            }
//            catch (Exception ex)
//            {
//                await WriteExceptionAsync(httpRequest, httpResponse, ex);
//            }
//            finally
//            {
//                readableStream.Flush();
//                readableStream.Close();
//                readableStream.Dispose();
//            }
//        }

//        /// <summary>
//        /// Ensure we have a Compression setting or not
//        /// </summary>
//        public virtual byte[] EnsureCompression(HttpRequest httpRequest, HttpResponse httpResponse, byte[] binaryData)
//        {
//            string encoding = httpRequest.Headers["Accept-Encoding"];

//            // Compress data if client accept Gzip / Deflate
//            if (!string.IsNullOrEmpty(encoding) && (encoding.Contains("gzip") || encoding.Contains("deflate")))
//            {
//                if (!httpResponse.Headers.ContainsKey("Content-Encoding"))
//                    httpResponse.Headers.Add("Content-Encoding", "gzip");

//                using var writeSteam = new MemoryStream();

//                using (var compress = new GZipStream(writeSteam, CompressionMode.Compress))
//                {
//                    compress.Write(binaryData, 0, binaryData.Length);
//                    compress.Flush();
//                }

//                var b = writeSteam.ToArray();
//                writeSteam.Flush();
//                return b;
//            }

//            return binaryData;
//        }

//        /// <summary>
//        /// Returns the serializer used by the client, that should be used on the server
//        /// </summary>
//        public virtual (int clientBatchSize, ISerializerFactory clientSerializer) GetClientSerializer(string serAndsizeString)
//        {
//            try
//            {
//                if (string.IsNullOrEmpty(serAndsizeString))
//                    throw new Exception("Serializer header is null, coming from http header");

//                var serAndsize = JsonConvert.DeserializeAnonymousType(serAndsizeString, new { f = "", s = 0 });

//                var clientBatchSize = serAndsize.s;

//                var clientSerializerFactory = this.WebServerOptions.SerializerFactories.FirstOrDefault(sf => sf.Key == serAndsize.f);
//                if (clientSerializerFactory == null) clientSerializerFactory = SerializersCollection.JsonSerializerFactory;

//                return (clientBatchSize, clientSerializerFactory);
//            }
//            catch
//            {
//                throw new Exception("Serializer header is incorrect, coming from http header");
//                //throw new HttpSerializerNotConfiguredException(this.WebServerOptions.Serializers.Select(sf => sf.Key));
//            }
//        }

//        /// <summary>
//        /// Returns the converter used by the client, that should be used on the server
//        /// </summary>
//        public virtual IConverter GetClientConverter(string cliConverterKey)
//        {
//            try
//            {
//                if (string.IsNullOrEmpty(cliConverterKey))
//                    return null;

//                var clientConverter = this.WebServerOptions.Converters.First(c => c.Key.ToLowerInvariant() == cliConverterKey);

//                return clientConverter;
//            }
//            catch
//            {
//                throw new HttpConverterNotConfiguredException(this.WebServerOptions.Converters.Select(sf => sf.Key));
//            }
//        }

//        /// <summary>
//        /// Get Scope Name sent by the client
//        /// </summary>
//        public static string GetScopeName(HttpContext httpContext) => TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-scope-name", out var val) ? val : null;

//        /// <summary>
//        /// Get the current client session id
//        /// </summary>
//        public static string GetClientSessionId(HttpContext httpContext) => TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-session-id", out var val) ? val : null;

//        /// <summary>
//        /// Get the current Step
//        /// </summary>
//        public static HttpStep GetCurrentStep(HttpContext httpContext) => TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-step", out var val) ? (HttpStep)Convert.ToInt32(val) : HttpStep.None;

//        /// <summary>
//        /// Get an header value
//        /// </summary>
//        public static bool TryGetHeaderValue(IHeaderDictionary n, string key, out string header)
//        {
//            if (n.TryGetValue(key, out var vs))
//            {
//                header = vs[0];
//                return true;
//            }

//            header = null;
//            return false;
//        }

//        internal protected virtual async Task<HttpMessageEnsureScopesResponse> EnsureScopesAsync(HttpContext httpContext, HttpMessageEnsureScopesRequest httpMessage, SessionCache sessionCache,
//                                            CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
//        {
//            if (httpMessage == null)
//                throw new ArgumentException("EnsureScopesAsync message could not be null");

//            if (this.Setup == null)
//                throw new ArgumentException("You need to set the tables to sync on server side");

//            // Get context from request message
//            var ctx = httpMessage.SyncContext;

//            // Set the context coming from the client
//            this.SetContext(ctx);

//            // Get schema
//            var serverScopeInfo = await this.GetServerScopeInfoAsync(httpMessage.SyncContext.ScopeName, default, default, default, cancellationToken, progress).ConfigureAwait(false);

//            // Provision if needed
//            if (serverScopeInfo != null && serverScopeInfo.IsNewScope)
//            {
//                // 2) Provision
//                var provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;
//                await this.ProvisionAsync(serverScopeInfo, provision, false, default, default, cancellationToken, progress).ConfigureAwait(false);
//            }

//            // Create http response
//            var httpResponse = new HttpMessageEnsureScopesResponse(ctx, serverScopeInfo);

//            return httpResponse;
//        }

//        //internal protected virtual async Task<HttpMessageEnsureSchemaResponse> EnsureSchemaAsync(HttpContext httpContext, HttpMessageEnsureScopesRequest httpMessage, SessionCache sessionCache,
//        //    CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
//        //{

//        //    if (httpMessage == null)
//        //        throw new ArgumentException("EnsureScopesAsync message could not be null");

//        //    if (this.Setup == null)
//        //        throw new ArgumentException("You need to set the tables to sync on server side");

//        //    // Get context from request message
//        //    var ctx = httpMessage.SyncContext;

//        //    // Set the context coming from the client
//        //    this.SetContext(ctx);

//        //    // Get schema
//        //    var serverScopeInfo = await base.GetServerScopeAsync(default, default, cancellationToken, progress).ConfigureAwait(false);

//        //    var schema = serverScopeInfo.Schema;
//        //    schema.EnsureSchema();
//        //    httpContext.Session.Set(httpMessage.SyncContext.ScopeName, schema);

//        //    var httpResponse = new HttpMessageEnsureSchemaResponse(ctx, serverScopeInfo);

//        //    return httpResponse;


//        //}

//        internal protected virtual async Task<HttpMessageSendChangesResponse> GetChangesAsync(HttpContext httpContext, HttpMessageSendChangesRequest httpMessage, SessionCache sessionCache,
//                        int clientBatchSize, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
//        {

//            // Overriding batch size options value, coming from client
//            // having changes from server in batch size or not is decided by the client.
//            // Basically this options is not used on the server, since it's always overriden by the client
//            this.Options.BatchSize = clientBatchSize;

//            // Get context from request message
//            var ctx = httpMessage.SyncContext;

//            // Set the context coming from the client
//            this.SetContext(ctx);

//            var changes = await base.GetChangesAsync(httpMessage.Scope, default, default, cancellationToken, progress);

//            // no changes applied to server
//            var clientChangesApplied = new DatabaseChangesApplied();

//            // Save the server batch info object to cache if not working in memory
//            sessionCache.RemoteClientTimestamp = changes.RemoteClientTimestamp;
//            sessionCache.ServerBatchInfo = changes.ServerBatchInfo;
//            sessionCache.ServerChangesSelected = changes.ServerChangesSelected;
//            sessionCache.ClientChangesApplied = clientChangesApplied;

//            // Get the firt response to send back to client
//            return await GetChangesResponseAsync(httpContext, ctx, changes.RemoteClientTimestamp, changes.ServerBatchInfo, clientChangesApplied, changes.ServerChangesSelected, 0);
//        }

//        internal protected virtual async Task<HttpMessageSendChangesResponse> GetEstimatedChangesCountAsync(HttpContext httpContext, HttpMessageSendChangesRequest httpMessage,
//                        CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
//        {
//            // Get context from request message
//            var ctx = httpMessage.SyncContext;

//            // Set the context coming from the client
//            this.SetContext(ctx);

//            var changes = await base.GetEstimatedChangesCountAsync(httpMessage.Scope, default, default, cancellationToken, progress);

//            var changesResponse = new HttpMessageSendChangesResponse(ctx)
//            {
//                ServerChangesSelected = changes.ServerChangesSelected,
//                ClientChangesApplied = new DatabaseChangesApplied(),
//                ServerStep = HttpStep.GetMoreChanges,
//                ConflictResolutionPolicy = this.Options.ConflictResolutionPolicy,
//                IsLastBatch = true,
//                RemoteClientTimestamp = changes.RemoteClientTimestamp
//            };

//            return changesResponse;
//        }

//        internal protected virtual async Task<HttpMessageRemoteTimestampResponse> GetRemoteClientTimestampAsync(HttpContext httpContext, HttpMessageRemoteTimestampRequest httpMessage,
//                CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
//        {
//            // Get context from request message
//            var ctx = httpMessage.SyncContext;

//            // Set the context coming from the client
//            this.SetContext(ctx);

//            var ts = await base.GetLocalTimestampAsync(ctx.ScopeName, default, default, cancellationToken, progress);

//            return new HttpMessageRemoteTimestampResponse(ctx, ts);
//        }




//        internal protected virtual async Task<HttpMessageSummaryResponse> GetSnapshotSummaryAsync(HttpContext httpContext, HttpMessageSendChangesRequest httpMessage, SessionCache sessionCache,
//                        CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
//        {
//            // Check schema.
//            // If client has stored the schema, the EnsureScope will not be called on server.
//            var schema = await EnsureSchemaFromSessionAsync(httpContext, httpMessage.SyncContext.ScopeName, progress, cancellationToken).ConfigureAwait(false);

//            // Get context from request message
//            var ctx = httpMessage.SyncContext;

//            // Set the context coming from the client
//            this.SetContext(ctx);

//            var serverScopeInfo = await this.GetServerScopeInfoAsync(ctx.ScopeName);

//            // get changes
//            var snap = await this.GetSnapshotAsync(serverScopeInfo, default, default, cancellationToken, progress).ConfigureAwait(false);

//            var summaryResponse = new HttpMessageSummaryResponse(ctx)
//            {
//                BatchInfo = snap.ServerBatchInfo,
//                RemoteClientTimestamp = snap.RemoteClientTimestamp,
//                ClientChangesApplied = new DatabaseChangesApplied(),
//                ServerChangesSelected = snap.DatabaseChangesSelected,
//                ConflictResolutionPolicy = this.Options.ConflictResolutionPolicy,
//                Step = HttpStep.GetSummary,
//            };

//            // Save the server batch info object to cache
//            sessionCache.RemoteClientTimestamp = snap.RemoteClientTimestamp;
//            sessionCache.ServerBatchInfo = snap.ServerBatchInfo;
//            sessionCache.ServerChangesSelected = snap.DatabaseChangesSelected;

//            return summaryResponse;
//        }

//        internal protected virtual async Task<SyncSet> EnsureSchemaFromSessionAsync(HttpContext httpContext, string scopeName, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
//        {
//            var schema = httpContext.Session.Get<SyncSet>(scopeName);

//            if (schema == null || !schema.HasTables || !schema.HasColumns)
//            {
//                var serverScopeInfo = await base.GetServerScopeInfoAsync(scopeName, default, default, default, cancellationToken, progress).ConfigureAwait(false);
//                // TODO : if serverScope.Schema is null, should we Provision here ?

//                schema = serverScopeInfo.Schema;
//                schema.EnsureSchema();
//                httpContext.Session.Set(scopeName, schema);
//            }

//            return schema;
//        }

//        internal protected virtual async Task<HttpMessageSendChangesResponse> GetSnapshotAsync(HttpContext httpContext, HttpMessageSendChangesRequest httpMessage, SessionCache sessionCache,
//                            CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
//        {
//            // Check schema.
//            var schema = await EnsureSchemaFromSessionAsync(httpContext, httpMessage.SyncContext.ScopeName, progress, cancellationToken).ConfigureAwait(false);

//            // Get context from request message
//            var ctx = httpMessage.SyncContext;

//            // Set the context coming from the client
//            this.SetContext(ctx);

//            var serverScopeInfo = await this.GetServerScopeInfoAsync(ctx.ScopeName);

//            // get changes
//            var snap = await this.GetSnapshotAsync(serverScopeInfo, default, default, cancellationToken, progress).ConfigureAwait(false);

//            // Save the server batch info object to cache
//            sessionCache.RemoteClientTimestamp = snap.RemoteClientTimestamp;
//            sessionCache.ServerBatchInfo = snap.ServerBatchInfo;
//            sessionCache.ServerChangesSelected = snap.DatabaseChangesSelected;
//            //httpContext.Session.Set(sessionId, sessionCache);

//            // if no snapshot, return empty response
//            if (snap.ServerBatchInfo == null)
//            {
//                var changesResponse = new HttpMessageSendChangesResponse(ctx)
//                {
//                    ServerStep = HttpStep.GetSnapshot,
//                    BatchIndex = 0,
//                    BatchCount = 0,
//                    IsLastBatch = true,
//                    RemoteClientTimestamp = 0,
//                    Changes = null
//                };
//                return changesResponse;
//            }

//            sessionCache.RemoteClientTimestamp = snap.RemoteClientTimestamp;
//            sessionCache.ServerBatchInfo = snap.ServerBatchInfo;

//            // Get the firt response to send back to client
//            return await GetChangesResponseAsync(httpContext, ctx, snap.RemoteClientTimestamp, snap.ServerBatchInfo, null, snap.DatabaseChangesSelected, 0);
//        }

//        internal protected virtual async Task<HttpMessageSummaryResponse> ApplyThenGetChangesAsync2(HttpContext httpContext, HttpMessageSendChangesRequest httpMessage, SessionCache sessionCache,
//                        int clientBatchSize, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
//        {
//            // Overriding batch size options value, coming from client
//            // having changes from server in batch size or not is decided by the client.
//            // Basically this options is not used on the server, since it's always overriden by the client
//            this.Options.BatchSize = clientBatchSize;

//            // Get context from request message
//            var ctx = httpMessage.SyncContext;

//            // Set the context coming from the client
//            this.SetContext(ctx);

//            // Check schema.
//            var schema = await EnsureSchemaFromSessionAsync(httpContext, httpMessage.SyncContext.ScopeName, progress, cancellationToken).ConfigureAwait(false);

//            // ------------------------------------------------------------
//            // FIRST STEP : receive client changes
//            // ------------------------------------------------------------

//            // We are receiving changes from client
//            // BatchInfo containing all BatchPartInfo objects
//            // Retrieve batchinfo instance if exists
//            // Get batch info from session cache if exists, otherwise create it
//            if (sessionCache.ClientBatchInfo == null)
//            {
//                sessionCache.ClientBatchInfo = new BatchInfo(schema, this.Options.BatchDirectory);
//                sessionCache.ClientBatchInfo.TryRemoveDirectory();
//            }

//            // we may have an error on first instance.
//            // so far we have a clientbatch in session, but the directory does not exists
//            // then we are checking the directory creation everytime
//            sessionCache.ClientBatchInfo.CreateDirectory();

//            if (httpMessage.Changes != null && httpMessage.Changes.HasRows)
//            {
//                // we have only one table here
//                var localSerializer = new LocalJsonSerializer();
//                var containerTable = httpMessage.Changes.Tables[0];
//                var schemaTable = DbSyncAdapter.CreateChangesTable(schema.Tables[containerTable.TableName, containerTable.SchemaName]);
//                var tableName = ParserName.Parse(new SyncTable(containerTable.TableName, containerTable.SchemaName)).Unquoted().Schema().Normalized().ToString();
//                var fileName = BatchInfo.GenerateNewFileName(httpMessage.BatchIndex.ToString(), tableName, localSerializer.Extension);
//                var fullPath = Path.Combine(sessionCache.ClientBatchInfo.GetDirectoryFullPath(), fileName);

//                // If client has made a conversion on each line, apply the reverse side of it
//                if (this.clientConverter != null)
//                    AfterDeserializedRows(containerTable, schemaTable, this.clientConverter);

//                var interceptorWriting = this.interceptors.GetInterceptor<SerializingRowArgs>();
//                if (!interceptorWriting.IsEmpty)
//                {
//                    localSerializer.OnWritingRow(async (syncTable, rowArray) =>
//                    {
//                        var args = new SerializingRowArgs(ctx, syncTable, rowArray);
//                        await this.InterceptAsync(args, progress, cancellationToken).ConfigureAwait(false);
//                        return args.Result;
//                    });
//                }
//                // open the file and write table header
//                await localSerializer.OpenFileAsync(fullPath, schemaTable).ConfigureAwait(false);

//                foreach (var row in containerTable.Rows)
//                    await localSerializer.WriteRowToFileAsync(new SyncRow(schemaTable, row), schemaTable).ConfigureAwait(false);

//                // Close file
//                await localSerializer.CloseFileAsync(fullPath, schemaTable).ConfigureAwait(false);

//                // Create the info on the batch part
//                BatchPartTableInfo tableInfo = new BatchPartTableInfo
//                {
//                    TableName = containerTable.TableName,
//                    SchemaName = containerTable.SchemaName,
//                    RowsCount = containerTable.Rows.Count

//                };
//                var bpi = new BatchPartInfo { FileName = fileName };
//                bpi.Tables = new BatchPartTableInfo[] { tableInfo };
//                bpi.RowsCount = tableInfo.RowsCount;
//                bpi.IsLastBatch = httpMessage.IsLastBatch;
//                bpi.Index = httpMessage.BatchIndex;
//                sessionCache.ClientBatchInfo.RowsCount += bpi.RowsCount;
//                sessionCache.ClientBatchInfo.BatchPartsInfo.Add(bpi);


//            }

//            // Clear the httpMessage set
//            if (httpMessage.Changes != null)
//                httpMessage.Changes.Clear();

//            // Until we don't have received all the batches, wait for more
//            if (!httpMessage.IsLastBatch)
//                return new HttpMessageSummaryResponse(ctx) { Step = HttpStep.SendChangesInProgress };

//            // ------------------------------------------------------------
//            // SECOND STEP : apply then return server changes
//            // ------------------------------------------------------------

//            // get changes
//            var (remoteClientTimestamp, serverBatchInfo, _, clientChangesApplied, serverChangesSelected) =
//                   await base.ApplyThenGetChangesAsync(httpMessage.Scope, sessionCache.ClientBatchInfo, default, default, cancellationToken, progress).ConfigureAwait(false);

//            // Set session cache infos
//            sessionCache.RemoteClientTimestamp = remoteClientTimestamp;
//            sessionCache.ServerBatchInfo = serverBatchInfo;
//            sessionCache.ServerChangesSelected = serverChangesSelected;
//            sessionCache.ClientChangesApplied = clientChangesApplied;

//            // delete the folder (not the BatchPartInfo, because we have a reference on it)
//            var cleanFolder = this.Options.CleanFolder;

//            if (cleanFolder)
//                cleanFolder = await this.InternalCanCleanFolderAsync(ScopeName, ctx.Parameters, sessionCache.ClientBatchInfo, default).ConfigureAwait(false);

//            sessionCache.ClientBatchInfo.Clear(cleanFolder);

//            // we do not need client batch info now
//            sessionCache.ClientBatchInfo = null;

//            // Retro compatiblité to version < 0.9.3
//            if (serverBatchInfo.BatchPartsInfo == null)
//                serverBatchInfo.BatchPartsInfo = new List<BatchPartInfo>();


//            var summaryResponse = new HttpMessageSummaryResponse(ctx)
//            {
//                BatchInfo = serverBatchInfo,
//                Step = HttpStep.GetSummary,
//                RemoteClientTimestamp = remoteClientTimestamp,
//                ClientChangesApplied = clientChangesApplied,
//                ServerChangesSelected = serverChangesSelected,
//                ConflictResolutionPolicy = this.Options.ConflictResolutionPolicy,
//            };


//            // Compatibility with last versions where InMemory is set
//            if (clientBatchSize <= 0)
//            {
//                var containerSet = new ContainerSet();
//                foreach (var table in serverBatchInfo.SanitizedSchema.Tables)
//                {
//                    var containerTable = new ContainerTable(table);
//                    foreach (var part in serverBatchInfo.GetBatchPartsInfo(table))
//                    {
//                        var paths = serverBatchInfo.GetBatchPartInfoPath(part);
//                        var localSerializer = new LocalJsonSerializer();
//                        foreach (var syncRow in localSerializer.ReadRowsFromFile(paths.FullPath, table))
//                        {
//                            containerTable.Rows.Add(syncRow.ToArray());
//                        }
//                    }
//                    if (containerTable.Rows.Count > 0)
//                        containerSet.Tables.Add(containerTable);
//                }

//                summaryResponse.Changes = containerSet;
//                summaryResponse.BatchInfo.BatchPartsInfo.Clear();
//                summaryResponse.BatchInfo.BatchPartsInfo = null;

//            }


//            // Get the firt response to send back to client
//            return summaryResponse;

//        }

//        internal protected virtual Task<HttpMessageSendChangesResponse> GetMoreChangesAsync(HttpContext httpContext, HttpMessageGetMoreChangesRequest httpMessage,
//            SessionCache sessionCache, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
//        {
//            return GetChangesResponseAsync(httpContext, httpMessage.SyncContext, sessionCache.RemoteClientTimestamp,
//                sessionCache.ServerBatchInfo, sessionCache.ClientChangesApplied,
//                sessionCache.ServerChangesSelected, httpMessage.BatchIndexRequested);
//        }

//        internal protected virtual async Task<HttpMessageSendChangesResponse> GetChangesResponseAsync(HttpContext httpContext, SyncContext syncContext, long remoteClientTimestamp, BatchInfo serverBatchInfo,
//                              DatabaseChangesApplied clientChangesApplied, DatabaseChangesSelected serverChangesSelected, int batchIndexRequested)
//        {
//            var schema = await EnsureSchemaFromSessionAsync(httpContext, syncContext.ScopeName, default, default).ConfigureAwait(false);

//            // 1) Create the http message content response
//            var changesResponse = new HttpMessageSendChangesResponse(syncContext)
//            {
//                ServerChangesSelected = serverChangesSelected,
//                ClientChangesApplied = clientChangesApplied,
//                ServerStep = HttpStep.GetMoreChanges,
//                ConflictResolutionPolicy = this.Options.ConflictResolutionPolicy
//            };

//            if (serverBatchInfo == null)
//                throw new Exception("serverBatchInfo is Null and should not be ....");

//            // If nothing to do, just send back
//            if (serverBatchInfo.BatchPartsInfo == null || serverBatchInfo.BatchPartsInfo.Count <= 0)
//            {
//                changesResponse.Changes = new ContainerSet();
//                changesResponse.BatchIndex = 0;
//                changesResponse.BatchCount = serverBatchInfo.BatchPartsInfo == null ? 0 : serverBatchInfo.BatchPartsInfo.Count;
//                changesResponse.IsLastBatch = true;
//                changesResponse.RemoteClientTimestamp = remoteClientTimestamp;
//                return changesResponse;
//            }

//            // Get the batch part index requested
//            var batchPartInfo = serverBatchInfo.BatchPartsInfo.First(d => d.Index == batchIndexRequested);

//            // Get the updatable schema for the only table contained in the batchpartinfo
//            var schemaTable = DbSyncAdapter.CreateChangesTable(schema.Tables[batchPartInfo.Tables[0].TableName, batchPartInfo.Tables[0].SchemaName]);

//            // Generate the ContainerSet containing rows to send to the user
//            var containerSet = new ContainerSet();
//            var containerTable = new ContainerTable(schemaTable);
//            var fullPath = Path.Combine(serverBatchInfo.GetDirectoryFullPath(), batchPartInfo.FileName);
//            containerSet.Tables.Add(containerTable);

//            // read rows from file
//            var localSerializer = new LocalJsonSerializer();
//            foreach (var row in localSerializer.ReadRowsFromFile(fullPath, schemaTable))
//                containerTable.Rows.Add(row.ToArray());

//            // if client request a conversion on each row, apply the conversion
//            if (this.clientConverter != null && containerTable.HasRows)
//                BeforeSerializeRows(containerTable, schemaTable, this.clientConverter);

//            // generate the response
//            changesResponse.Changes = containerSet;
//            changesResponse.BatchIndex = batchIndexRequested;
//            changesResponse.BatchCount = serverBatchInfo.BatchPartsInfo.Count;
//            changesResponse.IsLastBatch = batchPartInfo.IsLastBatch;
//            changesResponse.RemoteClientTimestamp = remoteClientTimestamp;
//            changesResponse.ServerStep = batchPartInfo.IsLastBatch ? HttpStep.GetMoreChanges : HttpStep.GetChangesInProgress;

//            return changesResponse;
//        }

//        internal protected virtual async Task<HttpMessageSendChangesResponse> SendEndDownloadChangesAsync(HttpContext httpContext, HttpMessageGetMoreChangesRequest httpMessage,
//            SessionCache sessionCache, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
//        {
//            var batchPartInfo = sessionCache.ServerBatchInfo.BatchPartsInfo.First(d => d.Index == httpMessage.BatchIndexRequested);

//            // If we have only one bpi, we can safely delete it
//            if (batchPartInfo.IsLastBatch)
//            {
//                // delete the folder (not the BatchPartInfo, because we have a reference on it)
//                var cleanFolder = this.Options.CleanFolder;

//                if (cleanFolder)
//                    cleanFolder = await this.InternalCanCleanFolderAsync(ScopeName, httpMessage.SyncContext.Parameters, sessionCache.ServerBatchInfo, default).ConfigureAwait(false);

//                if (cleanFolder)
//                    sessionCache.ServerBatchInfo.TryRemoveDirectory();
//            }
//            return new HttpMessageSendChangesResponse(httpMessage.SyncContext) { ServerStep = HttpStep.SendEndDownloadChanges };
//        }


//        /// <summary>
//        /// Before serializing all rows, call the converter for each row
//        /// </summary>
//        public virtual void BeforeSerializeRows(ContainerTable table, SyncTable schemaTable, IConverter converter)
//        {
//            if (table.Rows.Count > 0)
//            {
//                foreach (var row in table.Rows)
//                    converter.BeforeSerialize(row, schemaTable);
//            }
//        }

//        /// <summary>
//        /// After deserializing all rows, call the converter for each row
//        /// </summary>
//        public virtual void AfterDeserializedRows(ContainerTable table, SyncTable schemaTable, IConverter converter)
//        {
//            if (table.Rows.Count > 0)
//            {
//                foreach (var row in table.Rows)
//                    converter.AfterDeserialized(row, schemaTable);
//            }
//        }


//        /// <summary>
//        /// Write exception to output message
//        /// </summary>
//        public virtual async Task WriteExceptionAsync(HttpRequest httpRequest, HttpResponse httpResponse, Exception ex, string additionalInfo = null)
//        {
//            // Check if it's an unknown error, not managed (yet)
//            if (!(ex is SyncException syncException))
//                syncException = new SyncException(ex);

//            var message = new StringBuilder();
//            message.AppendLine(syncException.Message);
//            message.AppendLine("-----------------------");
//            message.AppendLine(syncException.StackTrace);
//            message.AppendLine("-----------------------");
//            if (syncException.InnerException != null)
//            {
//                message.AppendLine("-----------------------");
//                message.AppendLine("INNER EXCEPTION");
//                message.AppendLine("-----------------------");
//                message.AppendLine(syncException.InnerException.Message);
//                message.AppendLine("-----------------------");
//                message.AppendLine(syncException.InnerException.StackTrace);
//                message.AppendLine("-----------------------");

//            }
//            if (!string.IsNullOrEmpty(additionalInfo))
//            {
//                message.AppendLine("-----------------------");
//                message.AppendLine("ADDITIONAL INFO");
//                message.AppendLine("-----------------------");
//                message.AppendLine(additionalInfo);
//                message.AppendLine("-----------------------");

//            }

//            var webException = new WebSyncException
//            {
//                Message = message.ToString(),
//                SyncStage = syncException.SyncStage,
//                TypeName = syncException.TypeName,
//                DataSource = syncException.DataSource,
//                InitialCatalog = syncException.InitialCatalog,
//                Number = syncException.Number,
//                Side = syncException.Side
//            };

//            var jobject = JObject.FromObject(webException);

//            using var ms = new MemoryStream();
//            using var sw = new StreamWriter(ms);
//            using var jtw = new JsonTextWriter(sw);

//#if DEBUG
//            jtw.Formatting = Formatting.Indented;
//#endif
//            await jobject.WriteToAsync(jtw);

//            await jtw.FlushAsync();
//            await sw.FlushAsync();

//            var data = ms.ToArray();

//            // data to send back, as the response
//            byte[] compressedData = this.EnsureCompression(httpRequest, httpResponse, data);

//            httpResponse.Headers.Add("dotmim-sync-error", syncException.TypeName);
//            httpResponse.StatusCode = StatusCodes.Status400BadRequest;
//            httpResponse.ContentLength = compressedData.Length;
//            await httpResponse.Body.WriteAsync(compressedData, 0, compressedData.Length, default).ConfigureAwait(false);

//        }

//        public static Task WriteHelloAsync(HttpContext context, WebServerOrchestrator orchestrator, CancellationToken cancellationToken = default)
//                    => WriteHelloAsync(context, new[] { orchestrator }, cancellationToken);

//        public static async Task WriteHelloAsync(HttpContext context, IEnumerable<WebServerOrchestrator> orchestrators, CancellationToken cancellationToken = default)
//        {
//            var httpResponse = context.Response;
//            var stringBuilder = new StringBuilder();


//            stringBuilder.AppendLine("<!doctype html>");
//            stringBuilder.AppendLine("<html>");
//            stringBuilder.AppendLine("<head>");
//            stringBuilder.AppendLine("<meta charset='utf-8'>");
//            stringBuilder.AppendLine("<meta name='viewport' content='width=device-width, initial-scale=1, shrink-to-fit=no'>");
//            stringBuilder.AppendLine("<script src='https://cdn.jsdelivr.net/gh/google/code-prettify@master/loader/run_prettify.js'></script>");
//            stringBuilder.AppendLine("<link rel='stylesheet' href='https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/css/bootstrap.min.css' integrity='sha384-Vkoo8x4CGsO3+Hhxv8T/Q5PaXtkKtu6ug5TOeNV6gBiFeWPGFN9MuhOf23Q9Ifjh' crossorigin='anonymous'>");
//            stringBuilder.AppendLine("</head>");
//            stringBuilder.AppendLine("<title>Web Server properties</title>");
//            stringBuilder.AppendLine("<body>");


//            stringBuilder.AppendLine("<div class='container'>");
//            stringBuilder.AppendLine("<h2>Web Server properties</h2>");

//            foreach (var webOrchestrator in orchestrators)
//            {

//                string dbName = null;
//                string version = null;
//                string exceptionMessage = null;
//                bool hasException = false;
//                try
//                {
//                    (dbName, version) = await webOrchestrator.GetHelloAsync();
//                }
//                catch (Exception ex)
//                {
//                    exceptionMessage = ex.Message;
//                    hasException = true;

//                }

//                stringBuilder.AppendLine("<ul class='list-group mb-2'>");
//                stringBuilder.AppendLine($"<li class='list-group-item active'>Trying to reach database</li>");
//                stringBuilder.AppendLine("</ul>");
//                if (hasException)
//                {
//                    stringBuilder.AppendLine("<ul class='list-group mb-2'>");
//                    stringBuilder.AppendLine($"<li class='list-group-item list-group-item-primary'>Exception occured</li>");
//                    stringBuilder.AppendLine($"<li class='list-group-item list-group-item-danger'>");
//                    stringBuilder.AppendLine($"{exceptionMessage}");
//                    stringBuilder.AppendLine("</li>");
//                    stringBuilder.AppendLine("</ul>");
//                }
//                else
//                {
//                    stringBuilder.AppendLine("<ul class='list-group mb-2'>");
//                    stringBuilder.AppendLine($"<li class='list-group-item list-group-item-primary'>Database</li>");
//                    stringBuilder.AppendLine($"<li class='list-group-item list-group-item-light'>");
//                    stringBuilder.AppendLine($"Check database {dbName}: Done.");
//                    stringBuilder.AppendLine("</li>");
//                    stringBuilder.AppendLine("</ul>");

//                    stringBuilder.AppendLine("<ul class='list-group mb-2'>");
//                    stringBuilder.AppendLine($"<li class='list-group-item list-group-item-primary'>Engine version</li>");
//                    stringBuilder.AppendLine($"<li class='list-group-item list-group-item-light'>");
//                    stringBuilder.AppendLine($"{version}");
//                    stringBuilder.AppendLine("</li>");
//                    stringBuilder.AppendLine("</ul>");
//                }

//                stringBuilder.AppendLine("<ul class='list-group mb-2'>");
//                stringBuilder.AppendLine($"<li class='list-group-item active'>ScopeName: {webOrchestrator.ScopeName}</li>");
//                stringBuilder.AppendLine("</ul>");

//                var s = JsonConvert.SerializeObject(webOrchestrator.Setup, Formatting.Indented);
//                stringBuilder.AppendLine("<ul class='list-group mb-2'>");
//                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-primary'>Setup</li>");
//                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-light'>");
//                stringBuilder.AppendLine("<pre class='prettyprint' style='border:0px;font-size:75%'>");
//                stringBuilder.AppendLine(s);
//                stringBuilder.AppendLine("</pre>");
//                stringBuilder.AppendLine("</li>");
//                stringBuilder.AppendLine("</ul>");

//                s = JsonConvert.SerializeObject(webOrchestrator.Provider, Formatting.Indented);
//                stringBuilder.AppendLine("<ul class='list-group mb-2'>");
//                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-primary'>Provider</li>");
//                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-light'>");
//                stringBuilder.AppendLine("<pre class='prettyprint' style='border:0px;font-size:75%'>");
//                stringBuilder.AppendLine(s);
//                stringBuilder.AppendLine("</pre>");
//                stringBuilder.AppendLine("</li>");
//                stringBuilder.AppendLine("</ul>");

//                s = JsonConvert.SerializeObject(webOrchestrator.Options, Formatting.Indented);
//                stringBuilder.AppendLine("<ul class='list-group mb-2'>");
//                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-primary'>Options</li>");
//                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-light'>");
//                stringBuilder.AppendLine("<pre class='prettyprint' style='border:0px;font-size:75%'>");
//                stringBuilder.AppendLine(s);
//                stringBuilder.AppendLine("</pre>");
//                stringBuilder.AppendLine("</li>");
//                stringBuilder.AppendLine("</ul>");

//                s = JsonConvert.SerializeObject(webOrchestrator.WebServerOptions, Formatting.Indented);
//                stringBuilder.AppendLine("<ul class='list-group mb-2'>");
//                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-primary'>Web Server Options</li>");
//                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-light'>");
//                stringBuilder.AppendLine("<pre class='prettyprint' style='border:0px;font-size:75%'>");
//                stringBuilder.AppendLine(s);
//                stringBuilder.AppendLine("</pre>");
//                stringBuilder.AppendLine("</li>");
//                stringBuilder.AppendLine("</ul>");



//            }
//            stringBuilder.AppendLine("</div>");
//            stringBuilder.AppendLine("</body>");
//            stringBuilder.AppendLine("</html>");

//            await httpResponse.WriteAsync(stringBuilder.ToString(), cancellationToken);


//        }

//    }
//}
