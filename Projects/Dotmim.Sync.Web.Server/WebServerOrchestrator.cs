using Dotmim.Sync.Batch;

using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Dotmim.Sync.Web.Client;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Caching.Memory;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;
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
        public WebServerOrchestrator(CoreProvider provider, SyncOptions options, WebServerOptions webServerOptions, SyncSetup setup, IMemoryCache cache = null, string scopeName = SyncOptions.DefaultScopeName)
            : base(provider, options, setup, scopeName)
        {
            this.WebServerOptions = webServerOptions ?? throw new ArgumentNullException(nameof(webServerOptions));
            
            this.Cache = cache ?? new MemoryCache(new MemoryCacheOptions());
        }

        /// <summary>
        /// Gets the cache system used to cache schema and options
        /// </summary>
        public IMemoryCache Cache { get; }

        /// <summary>
        /// Gets or Sets Web server options parameters
        /// </summary>
        public WebServerOptions WebServerOptions { get; set; }

        /// <summary>
        /// Schema database
        /// </summary>
        public SyncSet Schema { get; set; }

        /// <summary>
        /// Client converter used
        /// </summary>
        public IConverter ClientConverter { get; set; }

        /// <summary>
        /// Interceptor just before sending back changes
        /// </summary>
        public void OnSendingChanges(Action<HttpMessageSendChangesResponseArgs> action) => this.On(action);

        /// <summary>
        /// Interceptor just before sending back scopes
        /// </summary>
        public void OnSendingScopes(Action<HttpMessageEnsureScopesResponseArgs> action) => this.On(action);

        /// <summary>
        /// Call this method to handle requests on the server, sent by the client
        /// </summary>
        public Task HandleRequestAsync(HttpContext context, CancellationToken token = default, IProgress<ProgressArgs> progress = null) =>
            HandleRequestAsync(context, null, token, progress);

        /// <summary>
        /// Call this method to handle requests on the server, sent by the client
        /// </summary>
        public async Task HandleRequestAsync(HttpContext context, Action<RemoteOrchestrator> action = null, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var httpRequest = context.Request;
            var httpResponse = context.Response;
            var serAndsizeString = string.Empty;
            var converter = string.Empty;


            // Get the serialization and batch size format
            if (TryGetHeaderValue(context.Request.Headers, "dotmim-sync-serialization-format", out var vs))
                serAndsizeString = vs.ToLowerInvariant();

            // Get the serialization and batch size format
            if (TryGetHeaderValue(context.Request.Headers, "dotmim-sync-converter", out var cs))
                converter = cs.ToLowerInvariant();

            if (!TryGetHeaderValue(context.Request.Headers, "dotmim-sync-session-id", out var sessionId))
                throw new HttpHeaderMissingExceptiopn("dotmim-sync-session-id");

            if (!TryGetHeaderValue(context.Request.Headers, "dotmim-sync-scope-name", out var scopeName))
                throw new HttpHeaderMissingExceptiopn("dotmim-sync-scope-name");

            if (!TryGetHeaderValue(context.Request.Headers, "dotmim-sync-step", out string iStep))
                throw new HttpHeaderMissingExceptiopn("dotmim-sync-step");

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

                // try get schema from cache
                if (this.Cache.TryGetValue<SyncSet>(scopeName, out var cachedSchema))
                    this.Schema = cachedSchema;

                // try get session cache from current sessionId
                if (!this.Cache.TryGetValue<SessionCache>(sessionId, out var sessionCache))
                    sessionCache = new SessionCache();

                // action from user if available
                action?.Invoke(this);

                // Get the serializer and batchsize
                (var clientBatchSize, var clientSerializerFactory) = GetClientSerializer(serAndsizeString, this);

                // Get converter used by client
                // Can be null
                var clientConverter = GetClientConverter(converter, this);
                this.ClientConverter = clientConverter;

                byte[] binaryData = null;
                switch (step)
                {
                    case HttpStep.EnsureScopes:
                        var m1 = await clientSerializerFactory.GetSerializer<HttpMessageEnsureScopesRequest>().DeserializeAsync(readableStream);
                        var s1 = await this.EnsureScopesAsync(m1, sessionCache, cancellationToken, progress).ConfigureAwait(false);
                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageEnsureScopesResponse>().SerializeAsync(s1);
                        await this.InterceptAsync(new HttpMessageEnsureScopesResponseArgs(binaryData), cancellationToken).ConfigureAwait(false);
                        break;
                    case HttpStep.EnsureSchema:
                        var m11 = await clientSerializerFactory.GetSerializer<HttpMessageEnsureScopesRequest>().DeserializeAsync(readableStream);
                        var s11 = await this.EnsureSchemaAsync(m11, sessionCache, cancellationToken, progress).ConfigureAwait(false);
                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageEnsureSchemaResponse>().SerializeAsync(s11);
                        await this.InterceptAsync(new HttpMessageEnsureSchemaResponseArgs(binaryData), cancellationToken).ConfigureAwait(false);
                        break;
                    case HttpStep.SendChanges:
                        var m2 = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesRequest>().DeserializeAsync(readableStream);
                        var s2 = await this.ApplyThenGetChangesAsync(m2, sessionCache, clientBatchSize, cancellationToken, progress).ConfigureAwait(false);
                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesResponse>().SerializeAsync(s2);
                        if (s2.Changes != null && s2.Changes.HasRows)
                            await this.InterceptAsync(new HttpMessageSendChangesResponseArgs(binaryData), cancellationToken).ConfigureAwait(false);
                        break;
                    case HttpStep.GetChanges:
                        var m3 = await clientSerializerFactory.GetSerializer<HttpMessageGetMoreChangesRequest>().DeserializeAsync(readableStream);
                        var s3 = await this.GetMoreChangesAsync(m3, sessionCache, cancellationToken, progress);
                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesResponse>().SerializeAsync(s3);
                        if (s3.Changes != null && s3.Changes.HasRows)
                            await this.InterceptAsync(new HttpMessageSendChangesResponseArgs(binaryData), cancellationToken).ConfigureAwait(false);
                        break;
                    case HttpStep.GetSnapshot:
                        var m4 = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesRequest>().DeserializeAsync(readableStream);
                        var s4 = await this.GetSnapshotAsync(m4, sessionCache, cancellationToken, progress);
                        binaryData = await clientSerializerFactory.GetSerializer<HttpMessageSendChangesResponse>().SerializeAsync(s4);
                        if (s4.Changes != null && s4.Changes.HasRows)
                            await this.InterceptAsync(new HttpMessageSendChangesResponseArgs(binaryData), cancellationToken).ConfigureAwait(false);
                        break;
                }

                // Save schema to cache with a sliding expiration
                this.Cache.Set(scopeName, this.Schema, this.WebServerOptions.GetServerCacheOptions());

                // Save session client to cache with a sliding expiration
                this.Cache.Set(sessionId, sessionCache, this.WebServerOptions.GetClientCacheOptions());

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

                await httpResponse.Body.WriteAsync(data, 0, data.Length).ConfigureAwait(false);

            }
            catch (Exception ex)
            {
                await WebServerManager.WriteExceptionAsync(httpResponse, ex);
            }
            finally
            {
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

                using (var writeSteam = new MemoryStream())
                {
                    using (var compress = new GZipStream(writeSteam, CompressionMode.Compress))
                    {
                        compress.Write(binaryData, 0, binaryData.Length);
                    }

                    return writeSteam.ToArray();
                }

            }

            return binaryData;
        }

        private (int clientBatchSize, ISerializerFactory clientSerializer) GetClientSerializer(string serAndsizeString, WebServerOrchestrator serverOrchestrator)
        {
            try
            {
                if (string.IsNullOrEmpty(serAndsizeString))
                    throw new Exception();

                var serAndsize = JsonConvert.DeserializeAnonymousType(serAndsizeString, new { f = "", s = 0 });

                var clientBatchSize = serAndsize.s;
                var clientSerializerFactory = serverOrchestrator.WebServerOptions.Serializers[serAndsize.f];

                return (clientBatchSize, clientSerializerFactory);
            }
            catch
            {
                throw new HttpSerializerNotConfiguredException(serverOrchestrator.WebServerOptions.Serializers.Select(sf => sf.Key));
            }
        }

        private IConverter GetClientConverter(string cs, WebServerOrchestrator serverOrchestrator)
        {
            try
            {
                if (string.IsNullOrEmpty(cs))
                    return null;

                var clientConverter = serverOrchestrator.WebServerOptions.Converters.First(c => c.Key == cs);

                return clientConverter;
            }
            catch
            {
                throw new HttpConverterNotConfiguredException(serverOrchestrator.WebServerOptions.Converters.Select(sf => sf.Key));
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
            var serverScopeInfo = await this.GetServerScopeAsync(cancellationToken, progress).ConfigureAwait(false);

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
            (var schema, var version) = await this.EnsureSchemaAsync(cancellationToken, progress).ConfigureAwait(false);

            this.Schema = schema;

            var httpResponse = new HttpMessageEnsureSchemaResponse(ctx, schema, version);

            return httpResponse;


        }

        /// <summary>
        /// Get changes from 
        /// </summary>
        internal async Task<HttpMessageSendChangesResponse> GetSnapshotAsync(HttpMessageSendChangesRequest httpMessage, SessionCache sessionCache, 
                            CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            // TODO : check Snapshot with version and scopename

            // Check schema.
            // If client has stored the schema, the EnsureScope will not be called on server.
            if (this.Schema == null || !this.Schema.HasTables || !this.Schema.HasColumns)
            {
                (var schema, var version) = await this.EnsureSchemaAsync(cancellationToken, progress).ConfigureAwait(false);

                schema.EnsureSchema();
                this.Schema = schema;

            }

            // Get context from request message
            var ctx = httpMessage.SyncContext;

            // Set the context coming from the client
            this.SetContext(ctx);

            // get changes
            var snap = await this.GetSnapshotAsync(cancellationToken, progress).ConfigureAwait(false);

            sessionCache.RemoteClientTimestamp = snap.RemoteClientTimestamp;
            sessionCache.ServerBatchInfo = snap.ServerBatchInfo;

            // if no snapshot, return empty response
            if (snap.ServerBatchInfo == null)
            {
                var changesResponse = new HttpMessageSendChangesResponse(ctx);
                changesResponse.ServerStep = HttpStep.GetSnapshot;
                changesResponse.BatchIndex = 0;
                changesResponse.IsLastBatch = true;
                changesResponse.RemoteClientTimestamp = snap.RemoteClientTimestamp;
                changesResponse.Changes = new ContainerSet();
                return changesResponse;
            }


            // Get the firt response to send back to client
            return await GetChangesResponseAsync(ctx, snap.RemoteClientTimestamp, snap.ServerBatchInfo, null, null, 0);
        }

        /// <summary>
        /// Get changes from 
        /// </summary>
        internal async Task<HttpMessageSendChangesResponse> ApplyThenGetChangesAsync( HttpMessageSendChangesRequest httpMessage, SessionCache sessionCache, 
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
            // If client has stored the schema, the EnsureScope will not be called on server.
            if (this.Schema == null || !this.Schema.HasTables || !this.Schema.HasColumns)
            {
                var (schema, version) = await this.EnsureSchemaAsync(cancellationToken, progress).ConfigureAwait(false);

                schema.EnsureSchema();
                this.Schema = schema;
            }

            // ------------------------------------------------------------
            // FIRST STEP : receive client changes
            // ------------------------------------------------------------

            // We are receiving changes from client
            // BatchInfo containing all BatchPartInfo objects
            // Retrieve batchinfo instance if exists

            // Get batch info from session cache if exists, otherwise create it
            if (sessionCache.ClientBatchInfo == null)
                sessionCache.ClientBatchInfo = new BatchInfo(clientWorkInMemory, Schema, this.Options.BatchDirectory);

            // create the in memory changes set
            var changesSet = new SyncSet();

            foreach (var table in httpMessage.Changes.Tables)
                DbSyncAdapter.CreateChangesTable(Schema.Tables[table.TableName, table.SchemaName], changesSet);

            changesSet.ImportContainerSet(httpMessage.Changes, false);

            // If client has made a conversion on each line, apply the reverse side of it
            if (this.ClientConverter != null && changesSet.HasRows)
                AfterDeserializedRows(changesSet, this.ClientConverter);

            // add changes to the batch info
            await sessionCache.ClientBatchInfo.AddChangesAsync(changesSet, httpMessage.BatchIndex, httpMessage.IsLastBatch);


            // Clear the httpMessage set
            if (!clientWorkInMemory && httpMessage.Changes != null)
                httpMessage.Changes.Clear();

            // Until we don't have received all the batches, wait for more
            if (!httpMessage.IsLastBatch)
            {

                return new HttpMessageSendChangesResponse(httpMessage.SyncContext)
                {
                    ServerStep = HttpStep.SendChangesInProgress
                };
            }

            // ------------------------------------------------------------
            // SECOND STEP : apply then return server changes
            // ------------------------------------------------------------


            // get changes
            var (remoteClientTimestamp, serverBatchInfo, _, clientChangesApplied, serverChangesSelected) =
               await this.ApplyThenGetChangesAsync(httpMessage.Scope, sessionCache.ClientBatchInfo, cancellationToken, progress).ConfigureAwait(false);


            // Save the server batch info object to cache if not working in memory
            if (!clientWorkInMemory)
            {
                sessionCache.RemoteClientTimestamp = remoteClientTimestamp;
                sessionCache.ServerBatchInfo = serverBatchInfo;
                sessionCache.ServerChangesSelected = serverChangesSelected;
                sessionCache.ClientChangesApplied = clientChangesApplied;
            }

            // Get the firt response to send back to client
            return await GetChangesResponseAsync(ctx, remoteClientTimestamp, serverBatchInfo, clientChangesApplied, serverChangesSelected, 0);

        }

        /// <summary>
        /// This method is only used when batch mode is enabled on server and we need to send back mor BatchPartInfo 
        /// </summary>
        internal Task<HttpMessageSendChangesResponse> GetMoreChangesAsync(HttpMessageGetMoreChangesRequest httpMessage, 
            SessionCache sessionCache, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (sessionCache.ServerBatchInfo == null)
                throw new ArgumentNullException("batchInfo stored in session can't be null if request more batch part info.");

            return GetChangesResponseAsync(httpMessage.SyncContext, sessionCache.RemoteClientTimestamp, 
                sessionCache.ServerBatchInfo, sessionCache.ClientChangesApplied,
                sessionCache.ServerChangesSelected, httpMessage.BatchIndexRequested);
        }


        /// <summary>
        /// Create a response message content based on a requested index in a server batch info
        /// </summary>
        private async Task<HttpMessageSendChangesResponse> GetChangesResponseAsync(SyncContext syncContext, long remoteClientTimestamp, BatchInfo serverBatchInfo,
                              DatabaseChangesApplied clientChangesApplied,  DatabaseChangesSelected serverChangesSelected, int batchIndexRequested)
        {

            // 1) Create the http message content response
            var changesResponse = new HttpMessageSendChangesResponse(syncContext);
            changesResponse.ServerChangesSelected = serverChangesSelected;
            changesResponse.ClientChangesApplied = clientChangesApplied;
            changesResponse.ServerStep = HttpStep.GetChanges;
            changesResponse.ConflictResolutionPolicy = this.Options.ConflictResolutionPolicy;

            // If nothing to do, just send back
            if (serverBatchInfo.InMemory || serverBatchInfo.BatchPartsInfo.Count == 0)
            {
                if (this.ClientConverter != null && serverBatchInfo.InMemoryData.HasRows)
                    BeforeSerializeRows(serverBatchInfo.InMemoryData, this.ClientConverter);

                changesResponse.Changes = serverBatchInfo.InMemoryData.GetContainerSet();
                changesResponse.BatchIndex = 0;
                changesResponse.IsLastBatch = true;
                changesResponse.RemoteClientTimestamp = remoteClientTimestamp;
                return changesResponse;
            }

            // Get the batch part index requested
            var batchPartInfo = serverBatchInfo.BatchPartsInfo.First(d => d.Index == batchIndexRequested);

            // if we are not in memory, we set the BI in session, to be able to get it back on next request

            // create the in memory changes set
            var changesSet = new SyncSet();

            foreach (var table in Schema.Tables)
                DbSyncAdapter.CreateChangesTable(Schema.Tables[table.TableName, table.SchemaName], changesSet);

            await batchPartInfo.LoadBatchAsync(changesSet, serverBatchInfo.GetDirectoryFullPath());

            // if client request a conversion on each row, apply the conversion
            if (this.ClientConverter != null && batchPartInfo.Data.HasRows)
                BeforeSerializeRows(batchPartInfo.Data, this.ClientConverter);

            changesResponse.Changes = batchPartInfo.Data.GetContainerSet();

            changesResponse.BatchIndex = batchIndexRequested;
            changesResponse.IsLastBatch = batchPartInfo.IsLastBatch;
            changesResponse.RemoteClientTimestamp = remoteClientTimestamp;
            changesResponse.ServerStep = batchPartInfo.IsLastBatch ? HttpStep.GetChanges : HttpStep.GetChangesInProgress;

            // If we have only one bpi, we can safely delete it
            if (batchPartInfo.IsLastBatch)
            {
                // delete the folder (not the BatchPartInfo, because we have a reference on it)
                if (this.Options.CleanFolder)
                {
                    var shouldDeleteFolder = true;
                    if (!string.IsNullOrEmpty(this.Options.SnapshotsDirectory))
                    {
                        var dirInfo = new DirectoryInfo(serverBatchInfo.DirectoryRoot);
                        var snapInfo = new DirectoryInfo(this.Options.SnapshotsDirectory);
                        shouldDeleteFolder = dirInfo.FullName != snapInfo.FullName;
                    }

                    if (shouldDeleteFolder)
                        serverBatchInfo.TryRemoveDirectory();
                }
            }

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
