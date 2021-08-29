using Dotmim.Sync.Batch;
using Dotmim.Sync.Serialization;
using Dotmim.Sync.Web.Client;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Caching.Memory;
using Newtonsoft.Json;
using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync.Web.Server
{
    public class WebServerOrchestrator : RemoteOrchestrator
    {
        /// <summary>
        /// Default ctor. Using default options and schema
        /// </summary>
        /// <param name="provider"></param>
        public WebServerOrchestrator(CoreProvider provider, SyncOptions options, SyncSetup setup, WebServerOptions webServerOptions = null, string scopeName = SyncOptions.DefaultScopeName)
            : base(provider, options, setup, scopeName)
        {
            this.WebServerOptions = webServerOptions ?? new WebServerOptions();
        }
        public WebServerOrchestrator(CoreProvider provider, string[] tables, string scopeName = SyncOptions.DefaultScopeName)
            : this(provider, new SyncOptions(), new SyncSetup(tables), new WebServerOptions(), scopeName)
        {
        }

        /// <summary>
        /// Gets or Sets Web server options parameters
        /// </summary>
        public WebServerOptions WebServerOptions { get; set; }

        /// <summary>
        /// Gets or Sets the Client Converter
        /// </summary>
        public IConverter ClientConverter { get; private set; }

        /// <summary>
        /// Gets the current Http Context
        /// </summary>
        public HttpContext HttpContext { get; private set; }

        /// <summary>
        /// Call this method to handle requests on the server, sent by the client
        /// </summary>
        public Task HandleRequestAsync(HttpContext context, CancellationToken token = default, IProgress<ProgressArgs> progress = null) =>
            HandleRequestAsync(context, null, token, progress);

        /// <summary>
        /// Call this method to handle requests on the server, sent by the client
        /// </summary>
        public async Task HandleRequestAsync(HttpContext context, Action<RemoteOrchestrator> action, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            this.HttpContext = context;
            var httpRequest = context.Request;
            var httpResponse = context.Response;
            var serAndsizeString = string.Empty;
            var cliConverterKey = string.Empty;

            // Get the serialization and batch size format
            if (TryGetHeaderValue(context.Request.Headers, "dotmim-sync-serialization-format", out var vs))
                serAndsizeString = vs.ToLowerInvariant();

            // Get the serialization and batch size format
            if (TryGetHeaderValue(context.Request.Headers, "dotmim-sync-converter", out var cs))
                cliConverterKey = cs.ToLowerInvariant();

            if (!TryGetHeaderValue(context.Request.Headers, "dotmim-sync-session-id", out var sessionId))
                throw new HttpHeaderMissingException("dotmim-sync-session-id");

            if (!TryGetHeaderValue(context.Request.Headers, "dotmim-sync-scope-name", out var scopeName))
                throw new HttpHeaderMissingException("dotmim-sync-scope-name");

            if (!TryGetHeaderValue(context.Request.Headers, "dotmim-sync-step", out string iStep))
                throw new HttpHeaderMissingException("dotmim-sync-step");

            var step = (HttpStep)Convert.ToInt32(iStep);
            var readableStream = new MemoryStream();

            try
            {
                // Copty stream to a readable and seekable stream
                // HttpRequest.Body is a HttpRequestStream that is readable but can't be Seek
                await httpRequest.Body.CopyToAsync(readableStream);
                httpRequest.Body.Close();
                httpRequest.Body.Dispose();

                // if Hash is present in header, check hash
                if (TryGetHeaderValue(context.Request.Headers, "dotmim-sync-hash", out string hashStringRequest))
                    HashAlgorithm.SHA256.EnsureHash(readableStream, hashStringRequest);
                else
                    readableStream.Seek(0, SeekOrigin.Begin);


                // Get schema and clients batch infos / summaries, from session
                var schema = context.Session.Get<SyncSet>(scopeName);

                var sessionCache = context.Session.Get<SessionCache>(sessionId);
                if (sessionCache == null)
                    sessionCache = new SessionCache();

                if (sessionCache.ClientBatchInfo != null
                    && sessionCache.ClientBatchInfo.SanitizedSchema != null && sessionCache.ClientBatchInfo.SanitizedSchema.Tables.Count == 0
                    && schema != null && schema.Tables.Count > 0)
                    foreach (var table in schema.Tables)
                        DbSyncAdapter.CreateChangesTable(schema.Tables[table.TableName, table.SchemaName], sessionCache.ClientBatchInfo.SanitizedSchema);



                // action from user if available
                action?.Invoke(this);

                // Get the serializer and batchsize
                (var clientBatchSize, var clientSerializerFactory) = this.GetClientSerializer(serAndsizeString);

                // Get converter used by client
                // Can be null
                var clientConverter = this.GetClientConverter(cliConverterKey);
                this.ClientConverter = clientConverter;

                //HttpHeaderInfo httpHeaderInfo = null;

                byte[] binaryData = null;
                switch (step)
                {
                    case HttpStep.EnsureScopes:
                        var m1 = await clientSerializerFactory.GetSerializer<HttpMessageEnsureScopesRequest>().DeserializeAsync(readableStream);
                        await this.InterceptAsync(new HttpGettingRequestArgs(context, m1.SyncContext, sessionCache, step), cancellationToken).ConfigureAwait(false);
                        var s1 = await this.EnsureScopesAsync(m1, sessionCache, cancellationToken, progress).ConfigureAwait(false);
                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageEnsureScopesResponse>().SerializeAsync(s1);
                        break;
                    case HttpStep.EnsureSchema:
                        var m11 = await clientSerializerFactory.GetSerializer<HttpMessageEnsureScopesRequest>().DeserializeAsync(readableStream);
                        await this.InterceptAsync(new HttpGettingRequestArgs(context, m11.SyncContext, sessionCache, step), cancellationToken).ConfigureAwait(false);
                        var s11 = await this.EnsureSchemaAsync(m11, sessionCache, cancellationToken, progress).ConfigureAwait(false);
                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageEnsureSchemaResponse>().SerializeAsync(s11);
                        break;

                    // version < 0.8    
                    case HttpStep.SendChanges:
                        var m2 = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesRequest>().DeserializeAsync(readableStream);
                        await this.InterceptAsync(new HttpGettingRequestArgs(context, m2.SyncContext, sessionCache, step), cancellationToken).ConfigureAwait(false);
                        await this.InterceptAsync(new HttpGettingClientChangesArgs(m2, context.Request.Host.Host, sessionCache), cancellationToken).ConfigureAwait(false);
                        var s2 = await this.ApplyThenGetChangesAsync(m2, sessionCache, clientBatchSize, cancellationToken, progress).ConfigureAwait(false);
                        await this.InterceptAsync(new HttpSendingServerChangesArgs(s2, context.Request.Host.Host, sessionCache, false), cancellationToken).ConfigureAwait(false);
                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesResponse>().SerializeAsync(s2);
                        break;

                    // version >= 0.8    
                    case HttpStep.SendChangesInProgress:
                        var m22 = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesRequest>().DeserializeAsync(readableStream);
                        await this.InterceptAsync(new HttpGettingRequestArgs(context, m22.SyncContext, sessionCache, step), cancellationToken).ConfigureAwait(false);
                        await this.InterceptAsync(new HttpGettingClientChangesArgs(m22, context.Request.Host.Host, sessionCache), cancellationToken).ConfigureAwait(false);
                        var s22 = await this.ApplyThenGetChangesAsync2(m22, sessionCache, clientBatchSize, cancellationToken, progress).ConfigureAwait(false);
                        //await this.InterceptAsync(new HttpSendingServerChangesArgs(s22.HttpMessageSendChangesResponse, context.Request.Host.Host, sessionCache, false), cancellationToken).ConfigureAwait(false);
                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageSummaryResponse>().SerializeAsync(s22);
                        break;

                    case HttpStep.GetChanges:
                        var m3 = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesRequest>().DeserializeAsync(readableStream);
                        await this.InterceptAsync(new HttpGettingRequestArgs(context, m3.SyncContext, sessionCache, step), cancellationToken).ConfigureAwait(false);
                        await this.InterceptAsync(new HttpGettingClientChangesArgs(m3, context.Request.Host.Host, sessionCache), cancellationToken).ConfigureAwait(false);
                        var s3 = await this.GetChangesAsync(m3, sessionCache, clientBatchSize, cancellationToken, progress);
                        await this.InterceptAsync(new HttpSendingServerChangesArgs(s3, context.Request.Host.Host, sessionCache, false), cancellationToken).ConfigureAwait(false);
                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesResponse>().SerializeAsync(s3);
                        break;

                    case HttpStep.GetMoreChanges:
                        var m4 = await clientSerializerFactory.GetSerializer<HttpMessageGetMoreChangesRequest>().DeserializeAsync(readableStream);
                        await this.InterceptAsync(new HttpGettingRequestArgs(context, m4.SyncContext, sessionCache, step), cancellationToken).ConfigureAwait(false);
                        var s4 = await this.GetMoreChangesAsync(m4, sessionCache, cancellationToken, progress);
                        await this.InterceptAsync(new HttpSendingServerChangesArgs(s4, context.Request.Host.Host, sessionCache, false), cancellationToken).ConfigureAwait(false);
                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesResponse>().SerializeAsync(s4);
                        break;

                    case HttpStep.GetSnapshot:
                        var m5 = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesRequest>().DeserializeAsync(readableStream);
                        await this.InterceptAsync(new HttpGettingRequestArgs(context, m5.SyncContext, sessionCache, step), cancellationToken).ConfigureAwait(false);
                        var s5 = await this.GetSnapshotAsync(m5, sessionCache, cancellationToken, progress);
                        await this.InterceptAsync(new HttpSendingServerChangesArgs(s5, context.Request.Host.Host, sessionCache, true), cancellationToken).ConfigureAwait(false);
                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesResponse>().SerializeAsync(s5);
                        break;

                    // version >= 0.8    
                    case HttpStep.GetSummary:
                        var m55 = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesRequest>().DeserializeAsync(readableStream);
                        await this.InterceptAsync(new HttpGettingRequestArgs(context, m55.SyncContext, sessionCache, step), cancellationToken).ConfigureAwait(false);
                        var s55 = await this.GetSnapshotSummaryAsync(m55, sessionCache, cancellationToken, progress);
                        //await this.InterceptAsync(new HttpSendingServerChangesArgs(s5.HttpMessageSendChangesResponse, context.Request.Host.Host, sessionCache, true), cancellationToken).ConfigureAwait(false);
                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageSummaryResponse>().SerializeAsync(s55);

                        break;
                    case HttpStep.SendEndDownloadChanges:
                        var m56 = await clientSerializerFactory.GetSerializer<HttpMessageGetMoreChangesRequest>().DeserializeAsync(readableStream);
                        await this.InterceptAsync(new HttpGettingRequestArgs(context, m56.SyncContext, sessionCache, step), cancellationToken).ConfigureAwait(false);
                        var s56 = await this.SendEndDownloadChangesAsync(m56, sessionCache, cancellationToken, progress);
                        await this.InterceptAsync(new HttpSendingServerChangesArgs(s56, context.Request.Host.Host, sessionCache, false), cancellationToken).ConfigureAwait(false);
                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesResponse>().SerializeAsync(s56);
                        break;

                    case HttpStep.GetEstimatedChangesCount:
                        var m6 = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesRequest>().DeserializeAsync(readableStream);
                        await this.InterceptAsync(new HttpGettingRequestArgs(context, m6.SyncContext, sessionCache, step), cancellationToken).ConfigureAwait(false);
                        var s6 = await this.GetEstimatedChangesCountAsync(m6, cancellationToken, progress);
                        await this.InterceptAsync(new HttpSendingServerChangesArgs(s6, context.Request.Host.Host, sessionCache, false), cancellationToken).ConfigureAwait(false);
                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesResponse>().SerializeAsync(s6);
                        break;
                }

                context.Session.Set(scopeName, schema);
                context.Session.Set(sessionId, sessionCache);

                // Adding the serialization format used and session id
                httpResponse.Headers.Add("dotmim-sync-session-id", sessionId.ToString());
                httpResponse.Headers.Add("dotmim-sync-serialization-format", clientSerializerFactory.Key);

                // calculate hash
                var hash = HashAlgorithm.SHA256.Create(binaryData);
                var hashString = Convert.ToBase64String(hash);
                // Add hash to header
                httpResponse.Headers.Add("dotmim-sync-hash", hashString);

                // data to send back, as the response
                byte[] data = this.EnsureCompression(httpRequest, httpResponse, binaryData);

                await this.InterceptAsync(new HttpSendingResponseArgs(context, this.GetContext(), sessionCache, data, step), cancellationToken).ConfigureAwait(false);

                await httpResponse.Body.WriteAsync(data, 0, data.Length, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await WebServerManager.WriteExceptionAsync(httpResponse, ex);
            }
            finally
            {
                readableStream.Flush();
                readableStream.Close();
                readableStream.Dispose();
            }
        }


        /// <summary>
        /// Ensure we have a Compression setting or not
        /// </summary>
        private byte[] EnsureCompression(HttpRequest httpRequest, HttpResponse httpResponse, byte[] binaryData)
        {
            string encoding = httpRequest.Headers["Accept-Encoding"];

            // Compress data if client accept Gzip / Deflate
            if (!string.IsNullOrEmpty(encoding) && (encoding.Contains("gzip") || encoding.Contains("deflate")))
            {
                httpResponse.Headers.Add("Content-Encoding", "gzip");

                using var writeSteam = new MemoryStream();

                using (var compress = new GZipStream(writeSteam, CompressionMode.Compress))
                {
                    compress.Write(binaryData, 0, binaryData.Length);
                    compress.Flush();
                }

                var b = writeSteam.ToArray();
                writeSteam.Flush();
                return b;
            }

            return binaryData;
        }

        private (int clientBatchSize, ISerializerFactory clientSerializer) GetClientSerializer(string serAndsizeString)
        {
            try
            {
                if (string.IsNullOrEmpty(serAndsizeString))
                    throw new Exception("Serializer header is null, coming from http header");

                var serAndsize = JsonConvert.DeserializeAnonymousType(serAndsizeString, new { f = "", s = 0 });

                var clientBatchSize = serAndsize.s;

                // V0.8 Serializer is now fixed from options
                // var clientSerializerFactory = this.WebServerOptions.Serializers[serAndsize.f];
                // return (clientBatchSize, clientSerializerFactory);

                return (clientBatchSize, this.Options.SerializerFactory);
            }
            catch
            {
                throw new Exception("Serializer header is incorrect, coming from http header");
                //throw new HttpSerializerNotConfiguredException(this.WebServerOptions.Serializers.Select(sf => sf.Key));
            }
        }

        private IConverter GetClientConverter(string cliConverterKey)
        {
            try
            {
                if (string.IsNullOrEmpty(cliConverterKey))
                    return null;

                var clientConverter = this.WebServerOptions.Converters.First(c => c.Key.ToLowerInvariant() == cliConverterKey);

                return clientConverter;
            }
            catch
            {
                throw new HttpConverterNotConfiguredException(this.WebServerOptions.Converters.Select(sf => sf.Key));
            }
        }


        public static bool TryGetHeaderValue(IHeaderDictionary n, string key, out string header)
        {
            if (n.TryGetValue(key, out var vs))
            {
                header = vs[0];
                return true;
            }

            header = null;
            return false;
        }



        internal async Task<HttpMessageEnsureScopesResponse> EnsureScopesAsync(HttpMessageEnsureScopesRequest httpMessage, SessionCache sessionCache,
                                            CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            if (httpMessage == null)
                throw new ArgumentException("EnsureScopesAsync message could not be null");

            if (this.Setup == null)
                throw new ArgumentException("You need to set the tables to sync on server side");

            // Get context from request message
            var ctx = httpMessage.SyncContext;

            // Set the context coming from the client
            this.SetContext(ctx);

            // Get schema
            var serverScopeInfo = await this.GetServerScopeAsync(default, default, cancellationToken, progress).ConfigureAwait(false);

            // Create http response
            var httpResponse = new HttpMessageEnsureScopesResponse(ctx, serverScopeInfo);

            return httpResponse;
        }


        internal async Task<HttpMessageEnsureSchemaResponse> EnsureSchemaAsync(HttpMessageEnsureScopesRequest httpMessage, SessionCache sessionCache,
            CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            if (httpMessage == null)
                throw new ArgumentException("EnsureScopesAsync message could not be null");

            if (this.Setup == null)
                throw new ArgumentException("You need to set the tables to sync on server side");

            // Get context from request message
            var ctx = httpMessage.SyncContext;

            // Set the context coming from the client
            this.SetContext(ctx);

            // Get schema
            var serverScopeInfo = await base.EnsureSchemaAsync(connection: default, default, cancellationToken, progress).ConfigureAwait(false);

            var schema = serverScopeInfo.Schema;
            schema.EnsureSchema();
            this.HttpContext.Session.Set(httpMessage.SyncContext.ScopeName, schema);

            var httpResponse = new HttpMessageEnsureSchemaResponse(ctx, serverScopeInfo);

            return httpResponse;


        }


        internal async Task<HttpMessageSendChangesResponse> GetChangesAsync(HttpMessageSendChangesRequest httpMessage, SessionCache sessionCache,
                        int clientBatchSize, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            // Overriding batch size options value, coming from client
            // having changes from server in batch size or not is decided by the client.
            // Basically this options is not used on the server, since it's always overriden by the client
            this.Options.BatchSize = clientBatchSize;

            // Get if we need to serialize data or making everything in memory
            var clientWorkInMemory = clientBatchSize == 0;

            // Get context from request message
            var ctx = httpMessage.SyncContext;

            // Set the context coming from the client
            this.SetContext(ctx);

            var changes = await base.GetChangesAsync(httpMessage.Scope, default, default, cancellationToken, progress);

            // no changes applied to server
            var clientChangesApplied = new DatabaseChangesApplied();

            // Save the server batch info object to cache if not working in memory
            if (!clientWorkInMemory)
            {
                sessionCache.RemoteClientTimestamp = changes.RemoteClientTimestamp;
                sessionCache.ServerBatchInfo = changes.ServerBatchInfo;
                sessionCache.ServerChangesSelected = changes.ServerChangesSelected;
                sessionCache.ClientChangesApplied = clientChangesApplied;
            }

            // Get the firt response to send back to client
            return await GetChangesResponseAsync(ctx, changes.RemoteClientTimestamp, changes.ServerBatchInfo, clientChangesApplied, changes.ServerChangesSelected, 0);


        }

        internal async Task<HttpMessageSendChangesResponse> GetEstimatedChangesCountAsync(HttpMessageSendChangesRequest httpMessage,
                        CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            // Get context from request message
            var ctx = httpMessage.SyncContext;

            // Set the context coming from the client
            this.SetContext(ctx);

            var changes = await base.GetEstimatedChangesCountAsync(httpMessage.Scope, default, default, cancellationToken, progress);


            var changesResponse = new HttpMessageSendChangesResponse(syncContext)
            {
                ServerChangesSelected = changes.ServerChangesSelected,
                ClientChangesApplied = new DatabaseChangesApplied(),
                ServerStep = HttpStep.GetMoreChanges,
                ConflictResolutionPolicy = this.Options.ConflictResolutionPolicy,
                IsLastBatch = true,
                RemoteClientTimestamp = changes.RemoteClientTimestamp
            };

            return changesResponse;
        }


        internal async Task<HttpMessageSummaryResponse> GetSnapshotSummaryAsync(HttpMessageSendChangesRequest httpMessage, SessionCache sessionCache,
                        CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            // TODO : check Snapshot with version and scopename

            // Check schema.
            // If client has stored the schema, the EnsureScope will not be called on server.
            var schema = await EnsureSchemaAsync(httpMessage.SyncContext.ScopeName, progress, cancellationToken).ConfigureAwait(false);

            // Get context from request message
            var ctx = httpMessage.SyncContext;

            // Set the context coming from the client
            this.SetContext(ctx);

            // get changes
            var snap = await this.GetSnapshotAsync(schema, cancellationToken, progress).ConfigureAwait(false);

            var summaryResponse = new HttpMessageSummaryResponse(ctx)
            {
                BatchInfo = snap.ServerBatchInfo,
                RemoteClientTimestamp = snap.RemoteClientTimestamp,
                ClientChangesApplied = new DatabaseChangesApplied(),
                ServerChangesSelected = snap.DatabaseChangesSelected,
                ConflictResolutionPolicy = this.Options.ConflictResolutionPolicy,
                Step = HttpStep.GetSummary,
            };

            // Save the server batch info object to cache
            sessionCache.RemoteClientTimestamp = snap.RemoteClientTimestamp;
            sessionCache.ServerBatchInfo = snap.ServerBatchInfo;
            sessionCache.ServerChangesSelected = snap.DatabaseChangesSelected;

            return summaryResponse;
        }

        private async Task<SyncSet> EnsureSchemaAsync(string scopeName, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            var schema = this.HttpContext.Session.Get<SyncSet>(scopeName);

            if (schema == null || !schema.HasTables || !schema.HasColumns)
            {
                var serverScopeInfo = await base.EnsureSchemaAsync(default, default, cancellationToken, progress).ConfigureAwait(false);

                schema = serverScopeInfo.Schema;
                schema.EnsureSchema();
                this.HttpContext.Session.Set(scopeName, schema);
            }

            return schema;
        }

        internal async Task<HttpMessageSendChangesResponse> GetSnapshotAsync(HttpMessageSendChangesRequest httpMessage, SessionCache sessionCache,
                            CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            // TODO : check Snapshot with version and scopename

            // Check schema.
            var schema = await EnsureSchemaAsync(httpMessage.SyncContext.ScopeName, progress, cancellationToken).ConfigureAwait(false);


            // Get context from request message
            var ctx = httpMessage.SyncContext;

            // Set the context coming from the client
            this.SetContext(ctx);

            // get changes
            var snap = await this.GetSnapshotAsync(schema, cancellationToken, progress).ConfigureAwait(false);

            // Save the server batch info object to cache
            sessionCache.RemoteClientTimestamp = snap.RemoteClientTimestamp;
            sessionCache.ServerBatchInfo = snap.ServerBatchInfo;
            sessionCache.ServerChangesSelected = snap.DatabaseChangesSelected;

            // if no snapshot, return empty response
            if (snap.ServerBatchInfo == null)
            {
                var changesResponse = new HttpMessageSendChangesResponse(ctx)
                {
                    ServerStep = HttpStep.GetSnapshot,
                    BatchIndex = 0,
                    BatchCount = 0,
                    IsLastBatch = true,
                    RemoteClientTimestamp = 0,
                    Changes = null
                };
                return changesResponse;
            }

            sessionCache.RemoteClientTimestamp = snap.RemoteClientTimestamp;
            sessionCache.ServerBatchInfo = snap.ServerBatchInfo;

            // Get the firt response to send back to client
            return await GetChangesResponseAsync(ctx, snap.RemoteClientTimestamp, snap.ServerBatchInfo, null, snap.DatabaseChangesSelected, 0);
        }

        /// <summary>
        /// Get changes from 
        /// </summary>
        internal async Task<HttpMessageSendChangesResponse> ApplyThenGetChangesAsync(HttpMessageSendChangesRequest httpMessage, SessionCache sessionCache,
                        int clientBatchSize, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            // Overriding batch size options value, coming from client
            // having changes from server in batch size or not is decided by the client.
            // Basically this options is not used on the server, since it's always overriden by the client
            this.Options.BatchSize = clientBatchSize;

            // Get if we need to serialize data or making everything in memory
            var clientWorkInMemory = clientBatchSize == 0;

            // Get context from request message
            var ctx = httpMessage.SyncContext;

            // Set the context coming from the client
            this.SetContext(ctx);

            // Check schema.
            var schema = await EnsureSchemaAsync(httpMessage.SyncContext.ScopeName, progress, cancellationToken).ConfigureAwait(false);

            // ------------------------------------------------------------
            // FIRST STEP : receive client changes
            // ------------------------------------------------------------

            // ensure that the blientBatchInfo is still available in the session - it **must** only be null when the batchIndex is 0!!
            if (sessionCache.ClientBatchInfo == null && httpMessage.BatchIndex != 0)
                throw new HttpSessionLostException();

            // We are receiving changes from client
            // BatchInfo containing all BatchPartInfo objects
            // Retrieve batchinfo instance if exists
            // Get batch info from session cache if exists, otherwise create it
            if (sessionCache.ClientBatchInfo == null)
                sessionCache.ClientBatchInfo = new BatchInfo(clientWorkInMemory, schema, this.Options.BatchDirectory);

            // create the in memory changes set
            var changesSet = new SyncSet();

            foreach (var table in httpMessage.Changes.Tables)
                DbSyncAdapter.CreateChangesTable(schema.Tables[table.TableName, table.SchemaName], changesSet);

            changesSet.ImportContainerSet(httpMessage.Changes, false);

            // If client has made a conversion on each line, apply the reverse side of it
            if (this.ClientConverter != null && changesSet.HasRows)
                AfterDeserializedRows(changesSet, this.ClientConverter);

            // add changes to the batch info
            await sessionCache.ClientBatchInfo.AddChangesAsync(changesSet, httpMessage.BatchIndex, httpMessage.IsLastBatch, this.Options.SerializerFactory, this);

            // Clear the httpMessage set
            if (!clientWorkInMemory && httpMessage.Changes != null)
                httpMessage.Changes.Clear();

            // Until we don't have received all the batches, wait for more
            if (!httpMessage.IsLastBatch)
                return new HttpMessageSendChangesResponse(httpMessage.SyncContext) { ServerStep = HttpStep.SendChangesInProgress };

            // ------------------------------------------------------------
            // SECOND STEP : apply then return server changes
            // ------------------------------------------------------------

            // get changes
            var (remoteClientTimestamp, serverBatchInfo, _, clientChangesApplied, serverChangesSelected) =
                   await base.ApplyThenGetChangesAsync(httpMessage.Scope, sessionCache.ClientBatchInfo, cancellationToken, progress).ConfigureAwait(false);


            // Save the server batch info object to cache if not working in memory
            if (!clientWorkInMemory)
            {
                sessionCache.RemoteClientTimestamp = remoteClientTimestamp;
                sessionCache.ServerBatchInfo = serverBatchInfo;
                sessionCache.ServerChangesSelected = serverChangesSelected;
                sessionCache.ClientChangesApplied = clientChangesApplied;
            }

            // delete the folder (not the BatchPartInfo, because we have a reference on it)
            var cleanFolder = this.Options.CleanFolder;

            if (cleanFolder)
                cleanFolder = await this.InternalCanCleanFolderAsync(ctx, sessionCache.ClientBatchInfo, default).ConfigureAwait(false);

            if (cleanFolder)
                sessionCache.ClientBatchInfo.TryRemoveDirectory();


            // Get the firt response to send back to client
            return await GetChangesResponseAsync(ctx, remoteClientTimestamp, serverBatchInfo, clientChangesApplied, serverChangesSelected, 0);

        }

        /// <summary>
        /// Get changes from 
        /// </summary>
        internal async Task<HttpMessageSummaryResponse> ApplyThenGetChangesAsync2(HttpMessageSendChangesRequest httpMessage, SessionCache sessionCache,
                        int clientBatchSize, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            // Overriding batch size options value, coming from client
            // having changes from server in batch size or not is decided by the client.
            // Basically this options is not used on the server, since it's always overriden by the client
            this.Options.BatchSize = clientBatchSize;

            // Get if we need to serialize data or making everything in memory
            var clientWorkInMemory = clientBatchSize == 0;

            // Get context from request message
            var ctx = httpMessage.SyncContext;

            // Set the context coming from the client
            this.SetContext(ctx);

            // Check schema.
            var schema = await EnsureSchemaAsync(httpMessage.SyncContext.ScopeName, progress, cancellationToken).ConfigureAwait(false);

            // ------------------------------------------------------------
            // FIRST STEP : receive client changes
            // ------------------------------------------------------------

            // ensure that the blientBatchInfo is still available in the session - it **must** only be null when the batchIndex is 0!!
            if (sessionCache.ClientBatchInfo == null && httpMessage.BatchIndex != 0)
                throw new HttpSessionLostException();

            // We are receiving changes from client
            // BatchInfo containing all BatchPartInfo objects
            // Retrieve batchinfo instance if exists
            // Get batch info from session cache if exists, otherwise create it
            if (sessionCache.ClientBatchInfo == null)
                sessionCache.ClientBatchInfo = new BatchInfo(clientWorkInMemory, schema, this.Options.BatchDirectory);

            // create the in memory changes set
            var changesSet = new SyncSet();

            foreach (var table in httpMessage.Changes.Tables)
                DbSyncAdapter.CreateChangesTable(schema.Tables[table.TableName, table.SchemaName], changesSet);

            changesSet.ImportContainerSet(httpMessage.Changes, false);

            // If client has made a conversion on each line, apply the reverse side of it
            if (this.ClientConverter != null && changesSet.HasRows)
                AfterDeserializedRows(changesSet, this.ClientConverter);

            // add changes to the batch info
            await sessionCache.ClientBatchInfo.AddChangesAsync(changesSet, httpMessage.BatchIndex, httpMessage.IsLastBatch, this.Options.SerializerFactory, this);

            // Clear the httpMessage set
            if (!clientWorkInMemory && httpMessage.Changes != null)
                httpMessage.Changes.Clear();

            // Until we don't have received all the batches, wait for more
            if (!httpMessage.IsLastBatch)
                return new HttpMessageSummaryResponse(ctx) { Step = HttpStep.SendChangesInProgress };

            // ------------------------------------------------------------
            // SECOND STEP : apply then return server changes
            // ------------------------------------------------------------

            // get changes
            var (remoteClientTimestamp, serverBatchInfo, _, clientChangesApplied, serverChangesSelected) =
                   await base.ApplyThenGetChangesAsync(httpMessage.Scope, sessionCache.ClientBatchInfo, cancellationToken, progress).ConfigureAwait(false);

            // Save the server batch info object to cache if not working in memory
            if (!clientWorkInMemory)
            {
                sessionCache.RemoteClientTimestamp = remoteClientTimestamp;
                sessionCache.ServerBatchInfo = serverBatchInfo;
                sessionCache.ServerChangesSelected = serverChangesSelected;
                sessionCache.ClientChangesApplied = clientChangesApplied;
            }

            // delete the folder (not the BatchPartInfo, because we have a reference on it)
            var cleanFolder = this.Options.CleanFolder;

            if (cleanFolder)
                cleanFolder = await this.InternalCanCleanFolderAsync(ctx, sessionCache.ClientBatchInfo, default).ConfigureAwait(false);

            if (cleanFolder)
                sessionCache.ClientBatchInfo.TryRemoveDirectory();

            var summaryResponse = new HttpMessageSummaryResponse(ctx)
            {
                BatchInfo = serverBatchInfo,
                Step = HttpStep.GetSummary,
                RemoteClientTimestamp = remoteClientTimestamp,
                ClientChangesApplied = clientChangesApplied,
                ServerChangesSelected = serverChangesSelected,
                ConflictResolutionPolicy = this.Options.ConflictResolutionPolicy,
            };

            if (clientWorkInMemory)
            {
                if (this.ClientConverter != null && serverBatchInfo.InMemoryData != null && serverBatchInfo.InMemoryData.HasRows)
                    BeforeSerializeRows(serverBatchInfo.InMemoryData, this.ClientConverter);

                summaryResponse.Changes = serverBatchInfo.InMemoryData == null ? new ContainerSet() : serverBatchInfo.InMemoryData.GetContainerSet();

            }
            // Get the firt response to send back to client
            return summaryResponse;

        }

        /// <summary>
        /// This method is only used when batch mode is enabled on server and we need to send back mor BatchPartInfo 
        /// </summary>
        internal Task<HttpMessageSendChangesResponse> GetMoreChangesAsync(HttpMessageGetMoreChangesRequest httpMessage,
            SessionCache sessionCache, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (sessionCache.ServerBatchInfo == null)
                throw new HttpSessionLostException();

            return GetChangesResponseAsync(httpMessage.SyncContext, sessionCache.RemoteClientTimestamp,
                sessionCache.ServerBatchInfo, sessionCache.ClientChangesApplied,
                sessionCache.ServerChangesSelected, httpMessage.BatchIndexRequested);
        }

        /// <summary>
        /// This method is only used when batch mode is enabled on server and we need send to the server the order to delete tmp folder 
        /// </summary>
        internal async Task<HttpMessageSendChangesResponse> SendEndDownloadChangesAsync(HttpMessageGetMoreChangesRequest httpMessage,
            SessionCache sessionCache, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (sessionCache.ServerBatchInfo == null)
                throw new HttpSessionLostException();

            var batchPartInfo = sessionCache.ServerBatchInfo.BatchPartsInfo.First(d => d.Index == httpMessage.BatchIndexRequested);

            // If we have only one bpi, we can safely delete it
            if (batchPartInfo.IsLastBatch)
            {
                // delete the folder (not the BatchPartInfo, because we have a reference on it)
                var cleanFolder = this.Options.CleanFolder;

                if (cleanFolder)
                    cleanFolder = await this.InternalCanCleanFolderAsync(httpMessage.SyncContext, sessionCache.ServerBatchInfo, default).ConfigureAwait(false);

                if (cleanFolder)
                    sessionCache.ServerBatchInfo.TryRemoveDirectory();
            }

            return new HttpMessageSendChangesResponse(httpMessage.SyncContext) { ServerStep = HttpStep.SendEndDownloadChanges };
        }


        /// <summary>
        /// Create a response message content based on a requested index in a server batch info
        /// </summary>
        private async Task<HttpMessageSendChangesResponse> GetChangesResponseAsync(SyncContext context, long remoteClientTimestamp, BatchInfo serverBatchInfo,
                              DatabaseChangesApplied clientChangesApplied, DatabaseChangesSelected serverChangesSelected, int batchIndexRequested)
        {

            var schema = await EnsureSchemaAsync(context.ScopeName, default, default).ConfigureAwait(false);

            // 1) Create the http message content response
            var changesResponse = new HttpMessageSendChangesResponse(context)
            {
                ServerChangesSelected = serverChangesSelected,
                ClientChangesApplied = clientChangesApplied,
                ServerStep = HttpStep.GetMoreChanges,
                ConflictResolutionPolicy = this.Options.ConflictResolutionPolicy
            };

            // If nothing to do, just send back
            if (serverBatchInfo.InMemory || serverBatchInfo.BatchPartsInfo.Count == 0)
            {
                if (this.ClientConverter != null && serverBatchInfo.InMemoryData != null && serverBatchInfo.InMemoryData.HasRows)
                    BeforeSerializeRows(serverBatchInfo.InMemoryData, this.ClientConverter);

                changesResponse.Changes = serverBatchInfo.InMemoryData == null ? new ContainerSet() : serverBatchInfo.InMemoryData.GetContainerSet();
                changesResponse.BatchIndex = 0;
                changesResponse.BatchCount = serverBatchInfo.InMemoryData == null ? 0 : serverBatchInfo.BatchPartsInfo == null ? 0 : serverBatchInfo.BatchPartsInfo.Count;
                changesResponse.IsLastBatch = true;
                changesResponse.RemoteClientTimestamp = remoteClientTimestamp;

                return changesResponse;
            }

            // Get the batch part index requested
            var batchPartInfo = serverBatchInfo.BatchPartsInfo.First(d => d.Index == batchIndexRequested);

            // if we are not in memory, we set the BI in session, to be able to get it back on next request

            // create the in memory changes set
            var changesSet = new SyncSet();

            foreach (var table in schema.Tables)
                DbSyncAdapter.CreateChangesTable(schema.Tables[table.TableName, table.SchemaName], changesSet);

            // Backward compatibility for client < v0.8.0
            var serializerFactory = this.Options.SerializerFactory;
            if (this.Options.SerializerFactory.Key != SerializersCollection.JsonSerializer.Key && (string.IsNullOrEmpty(serverBatchInfo.SerializerFactoryKey) || serverBatchInfo.SerializerFactoryKey == SerializersCollection.JsonSerializer.Key))
                serializerFactory = SerializersCollection.JsonSerializer;

            await batchPartInfo.LoadBatchAsync(changesSet, serverBatchInfo.GetDirectoryFullPath(), serializerFactory, this);

            // if client request a conversion on each row, apply the conversion
            if (this.ClientConverter != null && batchPartInfo.Data.HasRows)
                BeforeSerializeRows(batchPartInfo.Data, this.ClientConverter);

            changesResponse.Changes = batchPartInfo.Data.GetContainerSet();
            changesResponse.BatchIndex = batchIndexRequested;
            changesResponse.BatchCount = serverBatchInfo.BatchPartsInfo.Count;
            changesResponse.IsLastBatch = batchPartInfo.IsLastBatch;
            changesResponse.RemoteClientTimestamp = remoteClientTimestamp;
            changesResponse.ServerStep = batchPartInfo.IsLastBatch ? HttpStep.GetMoreChanges : HttpStep.GetChangesInProgress;

            batchPartInfo.Clear();


            return changesResponse;
        }


        /// <summary>
        /// Before serializing all rows, call the converter for each row
        /// </summary>
        public void BeforeSerializeRows(SyncSet data, IConverter converter)
        {
            foreach (var table in data.Tables)
            {
                if (table.Rows.Count > 0)
                {
                    foreach (var row in table.Rows)
                        converter.BeforeSerialize(row);

                }
            }
        }

        /// <summary>
        /// After deserializing all rows, call the converter for each row
        /// </summary>
        public void AfterDeserializedRows(SyncSet data, IConverter converter)
        {
            foreach (var table in data.Tables)
            {
                if (table.Rows.Count > 0)
                {
                    foreach (var row in table.Rows)
                        converter.AfterDeserialized(row);

                }
            }

        }

    }
}
