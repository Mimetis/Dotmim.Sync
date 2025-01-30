using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Extensions;
using Dotmim.Sync.Serialization;
using Dotmim.Sync.Web.Client;
using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync.Web.Server
{
    /// <summary>
    /// Web server agent.
    /// </summary>
    public class WebServerAgent
    {
        private static readonly ISerializer JsonSerializer = SerializersFactory.JsonSerializerFactory.GetSerializer();

        private static bool checkUpgradeDone;

        /// <inheritdoc cref="WebServerAgent"/>
        public WebServerAgent(CoreProvider provider, SyncSetup setup, SyncOptions options = null, WebServerOptions webServerOptions = null,
            string scopeName = null, string identifier = null)
        {
            this.Setup = setup;
            this.WebServerOptions = webServerOptions ?? new WebServerOptions();
            this.Provider = provider;
            this.ScopeName = string.IsNullOrEmpty(scopeName) ? SyncOptions.DefaultScopeName : scopeName;
            this.RemoteOrchestrator = new RemoteOrchestrator(this.Provider, options ?? new SyncOptions());
            this.Identifier = identifier;
        }

        /// <inheritdoc cref="WebServerAgent"/>
        public WebServerAgent(CoreProvider provider, string[] tables, SyncOptions options = null, WebServerOptions webServerOptions = null,
            string scopeName = null,
            string identifier = null)
        {
            this.Setup = new SyncSetup(tables);
            this.WebServerOptions = webServerOptions ?? new WebServerOptions();
            this.Provider = provider;
            this.RemoteOrchestrator = new RemoteOrchestrator(this.Provider, options ?? new SyncOptions());
            this.ScopeName = string.IsNullOrEmpty(scopeName) ? SyncOptions.DefaultScopeName : scopeName;
            this.Identifier = identifier;
        }

        /// <summary>
        /// Client Converter.
        /// </summary>
        private IConverter clientConverter;

        /// <summary>
        /// Gets or Sets the setup used in this webServerAgent.
        /// </summary>
        public SyncSetup Setup { get; set; }

        /// <summary>
        /// Gets the options used in this webServerAgent.
        /// </summary>
        public SyncOptions Options => this.RemoteOrchestrator?.Options;

        /// <summary>
        /// Gets ts the options used in this webServerAgent.
        /// </summary>
        public CoreProvider Provider { get; private set; }

        /// <summary>
        /// Gets ts Web server options parameters.
        /// </summary>
        public WebServerOptions WebServerOptions { get; private set; }

        /// <summary>
        /// Gets ts an identifier used to identify your webServerAgent.
        /// Can be really usefull in multi sync scenarios.
        /// </summary>
        public string Identifier { get; private set; }

        /// <summary>
        /// Gets ts a scope name used when multiple SyncSetup in one server.
        /// Can be really usefull in multi sync scenarios.
        /// </summary>
        public string ScopeName { get; private set; }

        /// <summary>
        /// Gets the RemoteOrchestrator used in this webServerAgent.
        /// </summary>
        public RemoteOrchestrator RemoteOrchestrator { get; private set; }

        /// <summary>
        /// Get Scope Name sent by the client.
        /// </summary>
        public static Guid? GetClientScopeId(HttpContext httpContext) => TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-scope-id", out var val) ? new Guid(val) : null;

        /// <summary>
        /// Get Scope Name sent by the client.
        /// </summary>
        public static string GetScopeName(HttpContext httpContext) => TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-scope-name", out var val) ? val : null;

        /// <summary>
        /// Get the DMS Version used by the client.
        /// </summary>
        public static string GetVersion(HttpContext httpContext) => TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-version", out var v) ? v : null;

        /// <summary>
        /// Get the current client session id.
        /// </summary>
        public static string GetClientSessionId(HttpContext httpContext) => TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-session-id", out var val) ? val : null;

        /// <summary>
        /// Get the current Step.
        /// </summary>
        public static HttpStep GetCurrentStep(HttpContext httpContext) => TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-step", out var val) ? (HttpStep)SyncTypeConverter.TryConvertTo<int>(val) : HttpStep.None;

        /// <summary>
        /// Get an header value.
        /// </summary>
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

        /// <summary>
        /// Write server debug information.
        /// </summary>
        public static async Task WriteHelloAsync(HttpContext httpContext, IEnumerable<WebServerAgent> webServerAgents, CancellationToken cancellationToken = default)
        {
            var httpResponse = httpContext.Response;
            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine("<!doctype html>");
            stringBuilder.AppendLine("<html>");
            stringBuilder.AppendLine("<head>");
            stringBuilder.AppendLine("<meta charset='utf-8'>");
            stringBuilder.AppendLine("<meta name='viewport' content='width=device-width, initial-scale=1, shrink-to-fit=no'>");
            stringBuilder.AppendLine("<script src='https://cdn.jsdelivr.net/gh/google/code-prettify@master/loader/run_prettify.js'></script>");
            stringBuilder.AppendLine("<link rel='stylesheet' href='https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/css/bootstrap.min.css' integrity='sha384-Vkoo8x4CGsO3+Hhxv8T/Q5PaXtkKtu6ug5TOeNV6gBiFeWPGFN9MuhOf23Q9Ifjh' crossorigin='anonymous'>");
            stringBuilder.AppendLine("</head>");
            stringBuilder.AppendLine("<title>Web Server properties</title>");
            stringBuilder.AppendLine("<body>");

            stringBuilder.AppendLine("<div class='container'>");
            stringBuilder.AppendLine("<h2>Web Server properties</h2>");

            foreach (var webServerAgent in webServerAgents)
            {
                string dbName = null;
                string version = null;
                string exceptionMessage = null;
                SyncContext context;
                bool hasException = false;

                try
                {
                    (context, dbName, version) = await webServerAgent.RemoteOrchestrator.GetHelloAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    exceptionMessage = ex.Message;
                    hasException = true;
                }

                stringBuilder.AppendLine("<ul class='list-group mb-2'>");
                stringBuilder.AppendLine($"<li class='list-group-item active'>Trying to reach database</li>");
                stringBuilder.AppendLine("</ul>");
                if (hasException)
                {
                    stringBuilder.AppendLine("<ul class='list-group mb-2'>");
                    stringBuilder.AppendLine($"<li class='list-group-item list-group-item-primary'>Exception occured</li>");
                    stringBuilder.AppendLine($"<li class='list-group-item list-group-item-danger'>");
                    stringBuilder.AppendLine(exceptionMessage);
                    stringBuilder.AppendLine("</li>");
                    stringBuilder.AppendLine("</ul>");
                }
                else
                {
                    stringBuilder.AppendLine("<ul class='list-group mb-2'>");
                    stringBuilder.AppendLine($"<li class='list-group-item list-group-item-primary'>Database</li>");
                    stringBuilder.AppendLine($"<li class='list-group-item list-group-item-light'>");
                    stringBuilder.AppendLine(CultureInfo.InvariantCulture, $"Check database {dbName}: Done.");
                    stringBuilder.AppendLine("</li>");
                    stringBuilder.AppendLine("</ul>");

                    stringBuilder.AppendLine("<ul class='list-group mb-2'>");
                    stringBuilder.AppendLine($"<li class='list-group-item list-group-item-primary'>Engine version</li>");
                    stringBuilder.AppendLine($"<li class='list-group-item list-group-item-light'>");
                    stringBuilder.AppendLine(version);
                    stringBuilder.AppendLine("</li>");
                    stringBuilder.AppendLine("</ul>");
                }

                var setup = await JsonSerializer.SerializeAsync(webServerAgent.Setup).ConfigureAwait(false);
                stringBuilder.AppendLine("<ul class='list-group mb-2'>");
                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-primary'>Setup</li>");
                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-light'>");
                stringBuilder.AppendLine("<pre class='prettyprint' style='border:0px;font-size:75%'>");
                stringBuilder.AppendLine(setup.ToUtf8String());
                stringBuilder.AppendLine("</pre>");
                stringBuilder.AppendLine("</li>");
                stringBuilder.AppendLine("</ul>");

                var provider = await JsonSerializer.SerializeAsync(webServerAgent.Provider).ConfigureAwait(false);
                stringBuilder.AppendLine("<ul class='list-group mb-2'>");
                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-primary'>Provider</li>");
                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-light'>");
                stringBuilder.AppendLine("<pre class='prettyprint' style='border:0px;font-size:75%'>");
                stringBuilder.AppendLine(provider.ToUtf8String());
                stringBuilder.AppendLine("</pre>");
                stringBuilder.AppendLine("</li>");
                stringBuilder.AppendLine("</ul>");

                var options = await JsonSerializer.SerializeAsync(webServerAgent.Options).ConfigureAwait(false);
                stringBuilder.AppendLine("<ul class='list-group mb-2'>");
                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-primary'>Options</li>");
                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-light'>");
                stringBuilder.AppendLine("<pre class='prettyprint' style='border:0px;font-size:75%'>");
                stringBuilder.AppendLine(options.ToUtf8String());
                stringBuilder.AppendLine("</pre>");
                stringBuilder.AppendLine("</li>");
                stringBuilder.AppendLine("</ul>");

                var webServerOptions = await JsonSerializer.SerializeAsync(webServerAgent.WebServerOptions).ConfigureAwait(false);
                stringBuilder.AppendLine("<ul class='list-group mb-2'>");
                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-primary'>Web Server Options</li>");
                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-light'>");
                stringBuilder.AppendLine("<pre class='prettyprint' style='border:0px;font-size:75%'>");
                stringBuilder.AppendLine(webServerOptions.ToUtf8String());
                stringBuilder.AppendLine("</pre>");
                stringBuilder.AppendLine("</li>");
                stringBuilder.AppendLine("</ul>");
            }

            stringBuilder.AppendLine("</div>");
            stringBuilder.AppendLine("</body>");
            stringBuilder.AppendLine("</html>");

            await httpResponse.WriteAsync(stringBuilder.ToString(), cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Call this method to handle requests on the server, sent by the client.
        /// </summary>
        public virtual Task HandleRequestAsync(HttpContext context, IProgress<ProgressArgs> progress = null, CancellationToken token = default) =>
            this.HandleRequestAsync(context, null, progress, token);

        /// <summary>
        /// Call this method to handle requests on the server, sent by the client.
        /// </summary>
        public virtual async Task HandleRequestAsync(HttpContext httpContext, Action<RemoteOrchestrator> action,
            IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            var httpRequest = httpContext.Request;
            var httpResponse = httpContext.Response;

            TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-serialization-format", out var serializerInfoString);
            TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-converter", out var cliConverterKey);
            TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-version", out string version);

            if (!TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-session-id", out var sessionId))
                throw new HttpHeaderMissingException("dotmim-sync-session-id");

            if (!TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-scope-name", out var scopeName))
                throw new HttpHeaderMissingException("dotmim-sync-scope-name");

            if (!TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-step", out string iStep))
                throw new HttpHeaderMissingException("dotmim-sync-step");

            var step = (HttpStep)SyncTypeConverter.TryConvertTo<int>(iStep);
            var readableStream = new MemoryStream();

            try
            {
                // check if we need to upgrade
                await UpgradeAsync(this.RemoteOrchestrator).ConfigureAwait(false);

                // Copty stream to a readable and seekable stream
                // HttpRequest.Body is a HttpRequestStream that is readable but can't be Seek
#if NET6_0_OR_GREATER
                await httpRequest.Body.CopyToAsync(readableStream, cancellationToken).ConfigureAwait(false);
                httpRequest.Body.Close();
                await httpRequest.Body.DisposeAsync().ConfigureAwait(false);
#else
                await httpRequest.Body.CopyToAsync(readableStream).ConfigureAwait(false);
                httpRequest.Body.Close();
                httpRequest.Body.Dispose();
#endif

                // if Hash is present in header, check hash
                if (TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-hash", out string hashStringRequest))
                    HashAlgorithm.SHA256.EnsureHash(readableStream, hashStringRequest);
                else
                    readableStream.Seek(0, SeekOrigin.Begin);

                if (!string.Equals(scopeName, this.ScopeName, SyncGlobalization.DataSourceStringComparison))
                    throw new HttpScopeNameFromClientIsInvalidException(scopeName, this.ScopeName);

                // load session
                await httpContext.Session.LoadAsync(cancellationToken).ConfigureAwait(false);

                // Get schema and clients batch infos / summaries, from session
                // var schema = httpContext.Session.Get<SyncSet>(scopeName);
                var sessionCache = httpContext.Session.Get<SessionCache>(sessionId);

                // HttpStep.EnsureSchema is the first call from client when client is new
                // HttpStep.EnsureScopes is the first call from client when client is not new
                // This is the only moment where we are initializing the sessionCache and store it in session
                if (sessionCache == null &&
                    (step == HttpStep.EnsureSchema || step == HttpStep.EnsureScopes || step == HttpStep.GetRemoteClientTimestamp))
                {
                    sessionCache = new SessionCache();
                    httpContext.Session.Set(sessionId, sessionCache);
                    httpContext.Session.SetString("session_id", sessionId);
                }

                // if sessionCache is still null, then we are in a step where it should not be null.
                // Probably because of a weird server restart or something...
                if (sessionCache == null)
                    throw new HttpSessionLostException(sessionId);

                // check session id
                var tempSessionId = httpContext.Session.GetString("session_id");

                if (string.IsNullOrEmpty(tempSessionId) || tempSessionId != sessionId)
                    throw new HttpSessionLostException(sessionId);

                // Get the serializer and batchsize
                (var clientBatchSize, var clientSerializerFactory) = this.GetClientSerializer(serializerInfoString);

                // Get converter used by client
                // Can be null
                this.clientConverter = this.GetClientConverter(cliConverterKey);

                byte[] binaryData = null;
                Type responseSerializerType = null;
                Type requestSerializerType = null;
                IScopeMessage messageResponse = null;

                switch (step)
                {
                    case HttpStep.None:
                        break;
                    case HttpStep.EnsureSchema:
                        requestSerializerType = typeof(HttpMessageEnsureScopesRequest);
                        responseSerializerType = typeof(HttpMessageEnsureSchemaResponse);
                        break;
                    case HttpStep.EnsureScopes:
                        requestSerializerType = typeof(HttpMessageEnsureScopesRequest);
                        responseSerializerType = typeof(HttpMessageEnsureScopesResponse);
                        break;
                    case HttpStep.SendChanges:
                        break;
                    case HttpStep.SendChangesInProgress:
                        requestSerializerType = typeof(HttpMessageSendChangesRequest);
                        responseSerializerType = typeof(HttpMessageSummaryResponse);
                        break;
                    case HttpStep.GetChanges:
                        break;
                    case HttpStep.GetEstimatedChangesCount:
                        requestSerializerType = typeof(HttpMessageSendChangesRequest);
                        responseSerializerType = typeof(HttpMessageSendChangesResponse);
                        break;
                    case HttpStep.GetMoreChanges:
                        requestSerializerType = typeof(HttpMessageGetMoreChangesRequest);
                        responseSerializerType = typeof(HttpMessageSendChangesResponse);
                        break;
                    case HttpStep.GetChangesInProgress:
                        break;
                    case HttpStep.GetSnapshot:
                        requestSerializerType = typeof(HttpMessageSendChangesRequest);
                        responseSerializerType = typeof(HttpMessageSendChangesResponse);
                        break;
                    case HttpStep.GetSummary:
                        requestSerializerType = typeof(HttpMessageSendChangesRequest);
                        responseSerializerType = typeof(HttpMessageSummaryResponse);
                        break;
                    case HttpStep.SendEndDownloadChanges:
                        requestSerializerType = typeof(HttpMessageGetMoreChangesRequest);
                        responseSerializerType = typeof(HttpMessageSendChangesResponse);
                        break;
                    case HttpStep.GetRemoteClientTimestamp:
                        requestSerializerType = typeof(HttpMessageRemoteTimestampRequest);
                        responseSerializerType = typeof(HttpMessageRemoteTimestampResponse);
                        break;
                    case HttpStep.GetOperation:
                        requestSerializerType = typeof(HttpMessageOperationRequest);
                        responseSerializerType = typeof(HttpMessageOperationResponse);
                        break;
                    case HttpStep.EndSession:
                        requestSerializerType = typeof(HttpMessageEndSessionRequest);
                        responseSerializerType = typeof(HttpMessageEndSessionResponse);
                        break;
                }

                IScopeMessage messsageRequest = await clientSerializerFactory.GetSerializer().DeserializeAsync(readableStream, requestSerializerType).ConfigureAwait(false) as IScopeMessage;
                await this.RemoteOrchestrator.InterceptAsync(new HttpGettingRequestArgs(httpContext, messsageRequest.SyncContext, sessionCache, messsageRequest, requestSerializerType, step), progress, cancellationToken).ConfigureAwait(false);

                switch (step)
                {
                    case HttpStep.EnsureScopes:
                        messageResponse = await this.EnsureScopesAsync(httpContext, (HttpMessageEnsureScopesRequest)messsageRequest, sessionCache, progress, cancellationToken).ConfigureAwait(false);
                        break;
                    case HttpStep.EnsureSchema: // pre v 0.9.6
                        var s11 = await this.EnsureScopesAsync(httpContext, (HttpMessageEnsureScopesRequest)messsageRequest, sessionCache, progress, cancellationToken).ConfigureAwait(false);
                        messageResponse = new HttpMessageEnsureSchemaResponse(s11.SyncContext, s11.ServerScopeInfo);
                        break;
                    case HttpStep.SendChangesInProgress:
                        var sendChangesRequest = (HttpMessageSendChangesRequest)messsageRequest;
                        await this.RemoteOrchestrator.InterceptAsync(new HttpGettingClientChangesArgs(sendChangesRequest, httpContext.Request.Host.Host, sessionCache), progress, cancellationToken).ConfigureAwait(false);
                        messageResponse = await this.ApplyThenGetChangesAsync2(httpContext, sendChangesRequest, sessionCache, clientBatchSize, progress, cancellationToken).ConfigureAwait(false);
                        break;
                    case HttpStep.GetMoreChanges:
                        messageResponse = await this.GetMoreChangesAsync(httpContext, (HttpMessageGetMoreChangesRequest)messsageRequest, sessionCache, progress, cancellationToken).ConfigureAwait(false);
                        break;
                    case HttpStep.GetSnapshot:
                        messageResponse = await this.GetSnapshotAsync(httpContext, (HttpMessageSendChangesRequest)messsageRequest, sessionCache, progress, cancellationToken).ConfigureAwait(false);
                        break;

                    // version >= 0.8
                    case HttpStep.GetSummary:
                        messageResponse = await this.GetSnapshotSummaryAsync(httpContext, (HttpMessageSendChangesRequest)messsageRequest, sessionCache, progress, cancellationToken).ConfigureAwait(false);
                        break;
                    case HttpStep.SendEndDownloadChanges:
                        messageResponse = await this.SendEndDownloadChangesAsync(httpContext, (HttpMessageGetMoreChangesRequest)messsageRequest, sessionCache, progress, cancellationToken).ConfigureAwait(false);
                        break;
                    case HttpStep.GetEstimatedChangesCount:
                        messageResponse = await this.GetEstimatedChangesCountAsync(httpContext, (HttpMessageSendChangesRequest)messsageRequest, progress, cancellationToken).ConfigureAwait(false);
                        break;
                    case HttpStep.GetRemoteClientTimestamp:
                        messageResponse = await this.GetRemoteClientTimestampAsync(httpContext, (HttpMessageRemoteTimestampRequest)messsageRequest, progress, cancellationToken).ConfigureAwait(false);
                        break;
                    case HttpStep.GetOperation:
                        messageResponse = await this.GetOperationAsync(httpContext, (HttpMessageOperationRequest)messsageRequest, progress, cancellationToken).ConfigureAwait(false);
                        break;
                    case HttpStep.EndSession:
                        messageResponse = await this.EndSessionAsync(httpContext, (HttpMessageEndSessionRequest)messsageRequest, progress, cancellationToken).ConfigureAwait(false);
                        break;
                }

                httpContext.Session.Set(sessionId, sessionCache);
                await httpContext.Session.CommitAsync(cancellationToken).ConfigureAwait(false);

                if (messageResponse is HttpMessageSendChangesResponse httpMessageSendChangesResponse)
                    await this.RemoteOrchestrator.InterceptAsync(new HttpSendingServerChangesArgs(httpMessageSendChangesResponse, httpContext.Request.Host.Host, sessionCache, false), progress, cancellationToken).ConfigureAwait(false);

                await this.RemoteOrchestrator.InterceptAsync(new HttpSendingResponseArgs(httpContext, messageResponse.SyncContext, sessionCache, messageResponse, responseSerializerType, step), progress, cancellationToken).ConfigureAwait(false);

                binaryData = await clientSerializerFactory.GetSerializer().SerializeAsync(messageResponse, responseSerializerType).ConfigureAwait(false);

                // Adding the serialization format used and session id
                httpResponse.Headers.Append("dotmim-sync-session-id", sessionId.ToString());
                httpResponse.Headers.Append("dotmim-sync-serialization-format", clientSerializerFactory.Key);

                // calculate hash
                var hash = HashAlgorithm.SHA256.Create(binaryData);
                var hashString = Convert.ToBase64String(hash);

                // Add hash to header
                httpResponse.Headers.Append("dotmim-sync-hash", hashString);

                // data to send back, as the response
                byte[] data = this.EnsureCompression(httpRequest, httpResponse, binaryData);

#if NET6_0_OR_GREATER
                await httpResponse.Body.WriteAsync(data.AsMemory(0, data.Length), cancellationToken).ConfigureAwait(false);
#else
                await httpResponse.Body.WriteAsync(data, 0, data.Length, cancellationToken).ConfigureAwait(false);
#endif

            }
            catch (Exception ex)
            {
                await this.WriteExceptionAsync(httpRequest, httpResponse, ex).ConfigureAwait(false);
            }
            finally
            {
                await readableStream.FlushAsync(cancellationToken).ConfigureAwait(false);
                readableStream.Close();
#if NET6_0_OR_GREATER
                await readableStream.DisposeAsync().ConfigureAwait(false);
#else
                readableStream.Dispose();

#endif
            }
        }

        /// <summary>
        /// Write exception to output message.
        /// </summary>
        public virtual async Task WriteExceptionAsync(HttpRequest httpRequest, HttpResponse httpResponse, Exception exception)
        {

            string message;

            if (this.Options.UseVerboseErrors)
            {
                message = exception is SyncException se && se.BaseMessage != null ? se.BaseMessage : exception.Message;
                var innerException = exception.InnerException;
                int cpt = 1;
                if (innerException != null)
                {
                    message += Environment.NewLine;
                    message += "  -----------------------" + Environment.NewLine;
                }

                while (innerException != null)
                {
                    message += Environment.NewLine;
                    var sign = innerException.InnerException != null ? "  ├" : "  └";
                    message += sign;

                    for (int i = 0; i < cpt; i++)
                        message += "  ─";

                    message += $" {innerException.Message}";

                    innerException = innerException.InnerException;
                    cpt++;
                }
            }
            else
            {
                message = "Synchronization failed on the server side. Please contact your admin.";
            }

            var syncException = new SyncException(exception, message);

            var webException = new WebSyncException
            {
                Message = message,
                SyncStage = syncException.SyncStage,
                TypeName = syncException.TypeName,
                DataSource = syncException.DataSource,
                InitialCatalog = syncException.InitialCatalog,
                Number = syncException.Number,
            };

            var data = await JsonSerializer.SerializeAsync(webException).ConfigureAwait(false);

            // data to send back, as the response
            byte[] compressedData = this.EnsureCompression(httpRequest, httpResponse, data);

            httpResponse.Headers.Append("dotmim-sync-error", syncException.TypeName);
            httpResponse.StatusCode = StatusCodes.Status400BadRequest;
            httpResponse.ContentLength = compressedData.Length;
#if NET6_0_OR_GREATER
            await httpResponse.Body.WriteAsync(compressedData).ConfigureAwait(false);
#else
            await httpResponse.Body.WriteAsync(compressedData, 0, compressedData.Length).ConfigureAwait(false);
#endif
        }

        /// <summary>
        /// Write server debug information.
        /// </summary>
        public Task WriteHelloAsync(HttpContext context, CancellationToken cancellationToken = default)
            => WriteHelloAsync(context, [this], cancellationToken);

        /// <summary>
        /// Ensure we have a Compression setting or not.
        /// </summary>
        public virtual byte[] EnsureCompression(HttpRequest httpRequest, HttpResponse httpResponse, byte[] binaryData)
        {
            // Compress data if client accept Gzip / Deflate
            if (httpRequest.Headers.TryGetValue("Accept-Encoding", out var encoding) && (encoding.Contains("gzip") || encoding.Contains("deflate")))
            {
                if (!httpResponse.Headers.ContainsKey("Content-Encoding"))
                    httpResponse.Headers.Append("Content-Encoding", "gzip");

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

        /// <summary>
        /// Returns the serializer used by the client, that should be used on the server.
        /// </summary>
        public virtual (int ClientBatchSize, ISerializerFactory ClientSerializer) GetClientSerializer(string serializerInfoString)
        {
            try
            {
                if (string.IsNullOrEmpty(serializerInfoString))
                    throw new Exception("Serializer header is null, coming from http header");

                var serializerInfo = JsonSerializer.Deserialize<SerializerInfo>(serializerInfoString);

                var clientSerializerFactory = this.WebServerOptions.SerializerFactories.FirstOrDefault(sf => sf.Key == serializerInfo.SerializerKey);
                clientSerializerFactory ??= SerializersFactory.JsonSerializerFactory;

                return (serializerInfo.ClientBatchSize, clientSerializerFactory);
            }
            catch
            {
                throw new Exception("Serializer header is incorrect, coming from http header");
            }
        }

        /// <summary>
        /// Returns the converter used by the client, that should be used on the server.
        /// </summary>
        public virtual IConverter GetClientConverter(string cliConverterKey)
        {
            try
            {
                if (string.IsNullOrEmpty(cliConverterKey))
                    return null;

                var clientConverter = this.WebServerOptions.Converters.First(c => c.Key.Equals(cliConverterKey, StringComparison.OrdinalIgnoreCase));

                return clientConverter;
            }
            catch
            {
                throw new HttpConverterNotConfiguredException(this.WebServerOptions.Converters.Select(sf => sf.Key));
            }
        }

        /// <summary>
        /// Ensure we have the latest version of the server side.
        /// </summary>
        protected internal virtual async Task<HttpMessageEnsureScopesResponse> EnsureScopesAsync(HttpContext httpContext, HttpMessageEnsureScopesRequest httpMessage, SessionCache sessionCache,
                                            IProgress<ProgressArgs> progress, CancellationToken cancellationToken = default)
        {
            if (httpMessage == null)
                throw new ArgumentException("EnsureScopesAsync message could not be null");

            if (this.Setup == null)
                throw new ArgumentException("You need to set the tables to sync on server side");

            var context = httpMessage.SyncContext;

            ScopeInfo serverScopeInfo;
            bool shouldProvision;
            (context, serverScopeInfo, shouldProvision) = await this.RemoteOrchestrator.InternalEnsureScopeInfoAsync(context, this.Setup, false, default, default, progress, cancellationToken).ConfigureAwait(false);

            // TODO : Is it used ?
            httpContext.Session.Set(httpMessage.SyncContext.ScopeName, serverScopeInfo.Schema);

            // Provision if needed
            if (shouldProvision)
            {
                // 2) Provision
                var provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;
                (context, serverScopeInfo) = await this.RemoteOrchestrator.InternalProvisionServerAsync(serverScopeInfo, context, provision, false, default, default, progress, cancellationToken).ConfigureAwait(false);
            }

            // Create http response
            var httpResponse = new HttpMessageEnsureScopesResponse(context, serverScopeInfo);

            return httpResponse;
        }

        /// <summary>
        /// Get estimated changes count only.
        /// </summary>
        protected internal virtual async Task<HttpMessageSendChangesResponse> GetEstimatedChangesCountAsync(HttpContext httpContext, HttpMessageSendChangesRequest httpMessage,
                        IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            var changes = await this.RemoteOrchestrator.GetEstimatedChangesCountAsync(httpMessage.ScopeInfoClient).ConfigureAwait(false);

            var changesResponse = new HttpMessageSendChangesResponse(httpMessage.SyncContext)
            {
                ServerChangesSelected = changes.ServerChangesSelected,
                ClientChangesApplied = new DatabaseChangesApplied(),
                ServerStep = HttpStep.GetMoreChanges,
                ConflictResolutionPolicy = this.Options.ConflictResolutionPolicy,
                IsLastBatch = true,
                RemoteClientTimestamp = changes.RemoteClientTimestamp,
            };

            return changesResponse;
        }

        /// <summary>
        /// Get remote client timestamp.
        /// </summary>
        protected internal virtual async Task<HttpMessageRemoteTimestampResponse> GetRemoteClientTimestampAsync(HttpContext httpContext, HttpMessageRemoteTimestampRequest httpMessage,
                IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            var ts = await this.RemoteOrchestrator.GetLocalTimestampAsync(httpMessage.SyncContext.ScopeName).ConfigureAwait(false);

            return new HttpMessageRemoteTimestampResponse(httpMessage.SyncContext, ts);
        }

        /// <summary>
        /// Get overriden operation to send to the client.
        /// </summary>
        protected internal virtual async Task<HttpMessageOperationResponse> GetOperationAsync(HttpContext httpContext, HttpMessageOperationRequest httpMessage,
             IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            var context = httpMessage.SyncContext;

            ScopeInfo serverScopeInfo;

            (context, serverScopeInfo, _) = await this.RemoteOrchestrator.InternalEnsureScopeInfoAsync(context, this.Setup, false, default, default, progress, cancellationToken).ConfigureAwait(false);

            SyncOperation operation;
            (context, operation) = await this.RemoteOrchestrator.InternalGetOperationAsync(serverScopeInfo, httpMessage.ScopeInfoFromClient, httpMessage.ScopeInfoClient, context, default, default, progress, cancellationToken).ConfigureAwait(false);

            return new HttpMessageOperationResponse(context, operation);
        }

        /// <summary>
        /// End the session.
        /// </summary>
        protected internal virtual async Task<HttpMessageEndSessionResponse> EndSessionAsync(HttpContext httpContext, HttpMessageEndSessionRequest httpMessage,
             IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            var context = httpMessage.SyncContext;

            var result = new SyncResult(context.SessionId)
            {
                ChangesAppliedOnClient = httpMessage.ChangesAppliedOnClient,
                ChangesAppliedOnServer = httpMessage.ChangesAppliedOnServer,
                ClientChangesSelected = httpMessage.ClientChangesSelected,
                CompleteTime = httpMessage.CompleteTime,
                ScopeName = context.ScopeName,
                ServerChangesSelected = httpMessage.ServerChangesSelected,
                SnapshotChangesAppliedOnClient = httpMessage.SnapshotChangesAppliedOnClient,
                StartTime = httpMessage.StartTime,
            };

            SyncException syncException = null;
            if (httpMessage.SyncExceptionMessage != null)
                syncException = new SyncException(httpMessage.SyncExceptionMessage);

            context = await this.RemoteOrchestrator.InternalEndSessionAsync(context, result, null, syncException, progress, cancellationToken).ConfigureAwait(false);

            return new HttpMessageEndSessionResponse(context);
        }

        /// <summary>
        /// Gets the snapshot summary.
        /// </summary>
        protected internal virtual async Task<HttpMessageSummaryResponse> GetSnapshotSummaryAsync(HttpContext httpContext, HttpMessageSendChangesRequest httpMessage, SessionCache sessionCache,
                        IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            // Get context from request message
            var context = httpMessage.SyncContext;

            ScopeInfo sScopeInfo;
            (context, sScopeInfo, _) = await this.RemoteOrchestrator.InternalEnsureScopeInfoAsync(context, this.Setup, false, default, default, progress, cancellationToken).ConfigureAwait(false);

            // TODO : Is it used ?
            httpContext.Session.Set(httpMessage.SyncContext.ScopeName, sScopeInfo.Schema);

            // get snapshot info
            ServerSyncChanges serverSyncChanges;
            (context, serverSyncChanges) =
                 await this.RemoteOrchestrator.InternalGetSnapshotAsync(sScopeInfo, context, default, default, progress, cancellationToken).ConfigureAwait(false);

            var summaryResponse = new HttpMessageSummaryResponse(context)
            {
                BatchInfo = serverSyncChanges.ServerBatchInfo,
                RemoteClientTimestamp = serverSyncChanges.RemoteClientTimestamp,
                ClientChangesApplied = new DatabaseChangesApplied(),
                ServerChangesSelected = serverSyncChanges.ServerChangesSelected,
                ConflictResolutionPolicy = this.Options.ConflictResolutionPolicy,
                Step = HttpStep.GetSummary,
            };

            // Save the server batch info object to cache
            sessionCache.RemoteClientTimestamp = serverSyncChanges.RemoteClientTimestamp;
            sessionCache.ServerBatchInfo = serverSyncChanges.ServerBatchInfo;
            sessionCache.ServerChangesSelected = serverSyncChanges.ServerChangesSelected;

            return summaryResponse;
        }

        /// <summary>
        /// Gets the snapshot.
        /// </summary>
        protected internal virtual async Task<HttpMessageSendChangesResponse> GetSnapshotAsync(HttpContext httpContext, HttpMessageSendChangesRequest httpMessage, SessionCache sessionCache,
                            IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            ScopeInfo sScopeInfo;

            (_, sScopeInfo, _) = await this.RemoteOrchestrator.InternalEnsureScopeInfoAsync(httpMessage.SyncContext, this.Setup, false, default, default, progress, cancellationToken).ConfigureAwait(false);

            // TODO : Is it used ?
            httpContext.Session.Set(httpMessage.SyncContext.ScopeName, sScopeInfo.Schema);

            // get changes
            var snap = await this.RemoteOrchestrator.GetSnapshotAsync(sScopeInfo).ConfigureAwait(false);

            // Save the server batch info object to cache
            sessionCache.RemoteClientTimestamp = snap.RemoteClientTimestamp;
            sessionCache.ServerBatchInfo = snap.ServerBatchInfo;
            sessionCache.ServerChangesSelected = snap.ServerChangesSelected;

            // httpContext.Session.Set(sessionId, sessionCache);

            // if no snapshot, return empty response
            if (snap.ServerBatchInfo == null)
            {
                var changesResponse = new HttpMessageSendChangesResponse(httpMessage.SyncContext)
                {
                    ServerStep = HttpStep.GetSnapshot,
                    BatchIndex = 0,
                    BatchCount = 0,
                    IsLastBatch = true,
                    RemoteClientTimestamp = 0,
                    Changes = null,
                };
                return changesResponse;
            }

            sessionCache.RemoteClientTimestamp = snap.RemoteClientTimestamp;
            sessionCache.ServerBatchInfo = snap.ServerBatchInfo;

            // Get the firt response to send back to client
            return await this.GetChangesResponseAsync(httpContext, httpMessage.SyncContext, snap.RemoteClientTimestamp, snap.ServerBatchInfo, null, snap.ServerChangesSelected, 0).ConfigureAwait(false);
        }

        /// <summary>
        /// Apply changes to the server and then get the changes to send back to the client.
        /// </summary>
        protected internal virtual async Task<HttpMessageSummaryResponse> ApplyThenGetChangesAsync2(HttpContext httpContext, HttpMessageSendChangesRequest httpMessage, SessionCache sessionCache,
                        int clientBatchSize, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            // Overriding batch size options value, coming from client
            // having changes from server in batch size or not is decided by the client.
            // Basically this options is not used on the server, since it's always overriden by the client
            this.Options.BatchSize = clientBatchSize;

            var context = httpMessage.SyncContext;
            ScopeInfo sScopeInfo;
            (context, sScopeInfo, _) = await this.RemoteOrchestrator.InternalEnsureScopeInfoAsync(
                context, this.Setup, false, default, default, progress, cancellationToken).ConfigureAwait(false);

            // TODO : Is it used ?
            httpContext.Session.Set(context.ScopeName, sScopeInfo.Schema);

            // ------------------------------------------------------------
            // FIRST STEP : receive client changes
            // ------------------------------------------------------------

            // We are receiving changes from client
            // BatchInfo containing all BatchPartInfo objects
            // Retrieve batchinfo instance if exists
            // Get batch info from session cache if exists, otherwise create it
            sessionCache.ClientBatchInfo ??= new BatchInfo(this.Options.BatchDirectory, info: "REMOTEGETCHANGES");

            if (httpMessage.Changes != null && httpMessage.Changes.HasRows)
            {
                using var localSerializer = new LocalJsonSerializer(this.RemoteOrchestrator, context);

                // we have only one table here
                var containerTable = httpMessage.Changes.Tables[0];
                var schemaTable = BaseOrchestrator.CreateChangesTable(sScopeInfo.Schema.Tables[containerTable.TableName, containerTable.SchemaName]);

                var setupTable = new SetupTable(containerTable.TableName, containerTable.SchemaName);

                var tableName = setupTable.GetFullName().Replace(".", "_").Replace(" ", "_");

                var fileName = BatchInfo.GenerateNewFileName(httpMessage.BatchIndex.ToString(CultureInfo.InvariantCulture), tableName, LocalJsonSerializer.Extension, "CLICHANGES");
                var fullPath = Path.Combine(sessionCache.ClientBatchInfo.GetDirectoryFullPath(), fileName);

                SyncRowState syncRowState = SyncRowState.None;
                if (containerTable.Rows != null && containerTable.Rows.Count > 0)
                {
                    var sr = new SyncRow(schemaTable, containerTable.Rows[0]);
                    syncRowState = sr.RowState;
                }

                // open the file and write table header
                await localSerializer.OpenFileAsync(fullPath, schemaTable, syncRowState).ConfigureAwait(false);

                foreach (var row in containerTable.Rows)
                {
                    var syncRow = new SyncRow(schemaTable, row);

                    if (this.clientConverter != null && syncRow.Length > 0)
                        this.clientConverter.AfterDeserialized(syncRow, schemaTable);

                    await localSerializer.WriteRowToFileAsync(syncRow, schemaTable).ConfigureAwait(false);
                }

                var bpi = new BatchPartInfo
                {
                    FileName = fileName,
                    TableName = containerTable.TableName,
                    SchemaName = containerTable.SchemaName,
                    RowsCount = containerTable.Rows.Count,
                    IsLastBatch = httpMessage.IsLastBatch,
                    Index = httpMessage.BatchIndex,
                };

                sessionCache.ClientBatchInfo.RowsCount += bpi.RowsCount;
                sessionCache.ClientBatchInfo.BatchPartsInfo.Add(bpi);
            }

            // Clear the httpMessage set
            if (httpMessage.Changes != null)
                httpMessage.Changes.Clear();

            // Until we don't have received all the batches, wait for more
            if (!httpMessage.IsLastBatch)
                return new HttpMessageSummaryResponse(httpMessage.SyncContext) { Step = HttpStep.SendChangesInProgress };

            // ------------------------------------------------------------
            // SECOND STEP : apply then return server changes
            // ------------------------------------------------------------
            ServerSyncChanges serverSyncChanges;
            context = httpMessage.SyncContext;
            var clientSyncChanges = new ClientSyncChanges(httpMessage.ClientLastSyncTimestamp, sessionCache.ClientBatchInfo, null, null);

            // get changes
            (context, serverSyncChanges, _) = await this.RemoteOrchestrator.InternalApplyThenGetChangesAsync(
                                               httpMessage.ScopeInfoClient,
                                               sScopeInfo,
                                               context,
                                               clientSyncChanges,
                                               default, default, progress, cancellationToken).ConfigureAwait(false);

            // Set session cache infos
            sessionCache.RemoteClientTimestamp = serverSyncChanges.RemoteClientTimestamp;
            sessionCache.ServerBatchInfo = serverSyncChanges.ServerBatchInfo;
            sessionCache.ServerChangesSelected = serverSyncChanges.ServerChangesSelected;
            sessionCache.ClientChangesApplied = serverSyncChanges.ServerChangesApplied;

            // delete the folder (not the BatchPartInfo, because we have a reference on it)
            var cleanFolder = this.Options.CleanFolder;

            if (cleanFolder)
                cleanFolder = await this.RemoteOrchestrator.InternalCanCleanFolderAsync(httpMessage.SyncContext.ScopeName, context.Parameters, sessionCache.ClientBatchInfo, default, cancellationToken).ConfigureAwait(false);

            if (cleanFolder)
                sessionCache.ClientBatchInfo.TryRemoveDirectory();

            // we do not need client batch info now
            sessionCache.ClientBatchInfo = null;

            // Retro compatiblité to version < 0.9.3
            if (serverSyncChanges.ServerBatchInfo.BatchPartsInfo == null)
                serverSyncChanges.ServerBatchInfo.BatchPartsInfo = [];

            var summaryResponse = new HttpMessageSummaryResponse(httpMessage.SyncContext)
            {
                BatchInfo = serverSyncChanges.ServerBatchInfo,
                Step = HttpStep.GetSummary,
                RemoteClientTimestamp = serverSyncChanges.RemoteClientTimestamp,
                ClientChangesApplied = serverSyncChanges.ServerChangesApplied,
                ServerChangesSelected = serverSyncChanges.ServerChangesSelected,
                ConflictResolutionPolicy = this.Options.ConflictResolutionPolicy,
            };

            // Get the firt response to send back to client
            return summaryResponse;
        }

        /// <summary>
        /// Get batch changes.
        /// </summary>
        protected internal virtual Task<HttpMessageSendChangesResponse> GetMoreChangesAsync(HttpContext httpContext, HttpMessageGetMoreChangesRequest httpMessage,
            SessionCache sessionCache, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        => this.GetChangesResponseAsync(httpContext, httpMessage.SyncContext, sessionCache.RemoteClientTimestamp,
                sessionCache.ServerBatchInfo, sessionCache.ClientChangesApplied,
                sessionCache.ServerChangesSelected, httpMessage.BatchIndexRequested);

        /// <summary>
        /// Get changes from server.
        /// </summary>
        protected internal virtual async Task<HttpMessageSendChangesResponse> GetChangesResponseAsync(HttpContext httpContext, SyncContext context, long remoteClientTimestamp, BatchInfo serverBatchInfo,
                              DatabaseChangesApplied clientChangesApplied, DatabaseChangesSelected serverChangesSelected, int batchIndexRequested)
        {
            ScopeInfo sScopeInfo;

            (context, sScopeInfo, _) = await this.RemoteOrchestrator.InternalEnsureScopeInfoAsync(
                context, this.Setup, false, default, default, default, default).ConfigureAwait(false);

            // TODO : Is it used ?
            httpContext.Session.Set(context.ScopeName, sScopeInfo.Schema);

            // 1) Create the http message content response
            var changesResponse = new HttpMessageSendChangesResponse(context)
            {
                ServerChangesSelected = serverChangesSelected,
                ClientChangesApplied = clientChangesApplied,
                ServerStep = HttpStep.GetMoreChanges,
                ConflictResolutionPolicy = this.Options.ConflictResolutionPolicy,
            };

            if (serverBatchInfo == null)
                throw new Exception("serverBatchInfo is Null and should not be ....");

            // If nothing to do, just send back
            if (serverBatchInfo.BatchPartsInfo == null || serverBatchInfo.BatchPartsInfo.Count <= 0)
            {
                changesResponse.Changes = new ContainerSet();
                changesResponse.BatchIndex = 0;
                changesResponse.BatchCount = serverBatchInfo.BatchPartsInfo == null ? 0 : serverBatchInfo.BatchPartsInfo.Count;
                changesResponse.IsLastBatch = true;
                changesResponse.RemoteClientTimestamp = remoteClientTimestamp;
                return changesResponse;
            }

            // Get the batch part index requested
            var batchPartInfo = serverBatchInfo.BatchPartsInfo.First(d => d.Index == batchIndexRequested);

            // Get the updatable schema for the only table contained in the batchpartinfo
            var schemaTable = BaseOrchestrator.CreateChangesTable(sScopeInfo.Schema.Tables[batchPartInfo.TableName, batchPartInfo.SchemaName]);

            // Generate the ContainerSet containing rows to send to the user
            var containerSet = new ContainerSet();
            var containerTable = new ContainerTable(schemaTable);
            var fullPath = Path.Combine(serverBatchInfo.GetDirectoryFullPath(), batchPartInfo.FileName);
            containerSet.Tables.Add(containerTable);

            // read rows from file
            using var localSerializer = new LocalJsonSerializer(this.RemoteOrchestrator, context);
            foreach (var row in localSerializer.GetRowsFromFile(fullPath, schemaTable))
            {
                if (row != null && row.Length > 0 && this.clientConverter != null)
                    this.clientConverter.BeforeSerialize(row, schemaTable);

                containerTable.Rows.Add(row.ToArray());
            }

            // generate the response
            changesResponse.Changes = containerSet;
            changesResponse.BatchIndex = batchIndexRequested;
            changesResponse.BatchCount = serverBatchInfo.BatchPartsInfo.Count;
            changesResponse.IsLastBatch = batchPartInfo.IsLastBatch;
            changesResponse.RemoteClientTimestamp = remoteClientTimestamp;
            changesResponse.ServerStep = batchPartInfo.IsLastBatch ? HttpStep.GetMoreChanges : HttpStep.GetChangesInProgress;

            return changesResponse;
        }

        /// <summary>
        /// Send an end download changes message.
        /// </summary>
        protected internal virtual async Task<HttpMessageSendChangesResponse> SendEndDownloadChangesAsync(
            HttpContext httpContext, HttpMessageGetMoreChangesRequest httpMessage,
            SessionCache sessionCache, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            var batchPartInfo = sessionCache.ServerBatchInfo.BatchPartsInfo.FirstOrDefault(d => d.Index == httpMessage.BatchIndexRequested);

            // we can try to clean if batchinfo is empty of if we found the last one AND we have the option.
            var cleanFolder = (batchPartInfo == null || batchPartInfo.IsLastBatch) && this.Options.CleanFolder;

            if (cleanFolder)
                cleanFolder = await this.RemoteOrchestrator.InternalCanCleanFolderAsync(httpMessage.SyncContext.ScopeName, httpMessage.SyncContext.Parameters, sessionCache.ServerBatchInfo, default, cancellationToken).ConfigureAwait(false);

            if (cleanFolder)
                sessionCache.ServerBatchInfo.TryRemoveDirectory();
            return new HttpMessageSendChangesResponse(httpMessage.SyncContext) { ServerStep = HttpStep.SendEndDownloadChanges };
        }

        private static async Task UpgradeAsync(RemoteOrchestrator remoteOrchestrator)
        {
            if (checkUpgradeDone)
                return;

            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);
            var needToUpgrade = await remoteOrchestrator.NeedsToUpgradeAsync(context).ConfigureAwait(false);

            if (needToUpgrade)
                await remoteOrchestrator.InternalUpgradeAsync(context).ConfigureAwait(false);

            checkUpgradeDone = true;
        }
    }
}