//using Dotmim.Sync.Serialization;

//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Net.Http;
//using System.Text;
//using System.Text.Json;
//using System.Threading;
//using System.Threading.Tasks;
//using System.Net.Http.Headers;

//namespace Dotmim.Sync.Web.Client
//{
//    /// <summary>
//    /// Object in charge to send requests
//    /// </summary>
//    public class HttpRequestHandler : IDisposable
//    {
//        internal Dictionary<string, string> CustomHeaders { get; } = new Dictionary<string, string>();

//        internal Dictionary<string, string> ScopeParameters { get; } = new Dictionary<string, string>();

//        internal CookieHeaderValue Cookie { get; set; }

//        private WebRemoteOrchestrator orchestrator;

//        public HttpRequestHandler(WebRemoteOrchestrator orchestrator)
//            => this.orchestrator = orchestrator;


//        private string BuildUri(string baseUri)
//        {
//            var requestUri = new StringBuilder();
//            requestUri.Append(baseUri);
//            requestUri.Append(baseUri.EndsWith("/", StringComparison.CurrentCultureIgnoreCase) ? string.Empty : "/");

//            // Add params if any
//            if (ScopeParameters != null && ScopeParameters.Count > 0)
//            {
//                string prefix = "?";
//                foreach (var kvp in ScopeParameters)
//                {
//                    requestUri.AppendFormat("{0}{1}={2}", prefix, Uri.EscapeDataString(kvp.Key),
//                                            Uri.EscapeDataString(kvp.Value));
//                    if (prefix.Equals("?"))
//                        prefix = "&";
//                }
//            }

//            return requestUri.ToString();
//        }

//        public async Task<T> ProcessRequestAsync<T>(HttpClient client, string baseUri, IScopeMessage message, HttpStep step,
//                                    ISerializerFactory serializerFactory, IConverter converter, int batchSize, SyncPolicy policy,
//                                    CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null) where T : IScopeMessage
//        {
//            if (client is null)
//                throw new ArgumentNullException(nameof(client));

//            if (baseUri == null)
//                throw new ArgumentException("BaseUri is not defined");

//            HttpResponseMessage response = null;
//            try
//            {
//                if (cancellationToken.IsCancellationRequested)
//                    cancellationToken.ThrowIfCancellationRequested();

//                var requestUri = BuildUri(baseUri);

//                // Execute my OpenAsync in my policy context
//                response = await policy.ExecuteAsync(ct => this.SendAsync(client, requestUri.ToString(),
//                    step, message, batchSize, converter, serializerFactory, ct), progress, cancellationToken);

//                // Ensure we have a cookie
//                this.EnsureCookie(response?.Headers);

//                if (response.Content == null)
//                    throw new HttpEmptyResponseContentException();

//                T messageResponse = default;
//                var serializer = serializerFactory.GetSerializer();

//                using (var streamResponse = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
//                {
//                    if (streamResponse.CanRead)
//                        messageResponse = await serializer.DeserializeAsync<T>(streamResponse);
//                }


//                await this.InterceptAsync(new HttpGettingResponseMessageArgs(response, this.ServiceUri.ToString(),
//                    HttpStep.SendChangesInProgress, context, summaryResponseContent, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);


//                return messageResponse;
//            }
//            catch (HttpSyncWebException)
//            {
//                throw;
//            }
//            catch (SyncException se)
//            {
//                throw new HttpSyncWebException(se.Message);
//            }
//            catch (Exception e)
//            {
//                if (response == null || response.Content == null)
//                    throw new HttpSyncWebException(e.Message);

//                var exrror = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

//                throw new HttpSyncWebException(exrror);
//            }
//        }

//        /// <summary>
//        /// Process a request message with HttpClient object. 
//        /// </summary>
//        public async Task<HttpResponseMessage> ProcessRequestAsync(HttpClient client, string baseUri, IScopeMessage message, HttpStep step,
//            ISerializerFactory serializerFactory, IConverter converter, int batchSize, SyncPolicy policy,
//            CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
//        {
//            if (client is null)
//                throw new ArgumentNullException(nameof(client));

//            if (baseUri == null)
//                throw new ArgumentException("BaseUri is not defined");

//            HttpResponseMessage response = null;
//            try
//            {
//                if (cancellationToken.IsCancellationRequested)
//                    cancellationToken.ThrowIfCancellationRequested();

//                var requestUri = BuildUri(baseUri);

//                // Execute my OpenAsync in my policy context
//                response = await policy.ExecuteAsync(ct => this.SendAsync(client, requestUri.ToString(),
//                    step, message, batchSize, converter, serializerFactory, ct), progress, cancellationToken);

//                // Ensure we have a cookie
//                this.EnsureCookie(response?.Headers);


//                if (response.Content == null)
//                    throw new HttpEmptyResponseContentException();

//                return response;
//            }
//            catch (HttpSyncWebException)
//            {
//                throw;
//            }
//            catch (SyncException se)
//            {
//                throw new HttpSyncWebException(se.Message);
//            }
//            catch (Exception e)
//            {
//                if (response == null || response.Content == null)
//                    throw new HttpSyncWebException(e.Message);

//                var exrror = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

//                throw new HttpSyncWebException(exrror);
//            }

//        }

//        private void EnsureCookie(HttpResponseHeaders headers)
//        {
//            if (headers == null)
//                return;
//            if (!headers.TryGetValues("Set-Cookie", out var tmpList))
//                return;

//            var cookieList = tmpList.ToList();

//            // var cookieList = response.Headers.GetValues("Set-Cookie").ToList();
//            if (cookieList != null && cookieList.Count > 0)
//            {
//                //try to parse the very first cookie
//                if (CookieHeaderValue.TryParse(cookieList[0], out var cookie))
//                    this.Cookie = cookie;
//            }
//        }


//        private async Task<HttpResponseMessage> SendAsync(HttpClient client, string requestUri,
//            HttpStep step, IScopeMessage message, int batchSize, IConverter converter, ISerializerFactory serializerFactory,  CancellationToken cancellationToken)
//        {

//            // serialize message
//            var serializer = serializerFactory.GetSerializer();

//            var contentType = serializerFactory.Key == SerializersFactory.JsonSerializerFactory.Key ? "application/json" : null;
//            var ser = JsonSerializer.Serialize(new SerializerInfo(serializerFactory.Key, batchSize));

//            // Create the request message
//            var requestMessage = new HttpRequestMessage(HttpMethod.Post, requestUri);

//            // Adding the serialization format used and session id and scope name
//            requestMessage.Headers.Add("dotmim-sync-session-id", message.SyncContext.SessionId.ToString());
//            requestMessage.Headers.Add("dotmim-sync-scope-id", message.SyncContext.ClientId.ToString());
//            requestMessage.Headers.Add("dotmim-sync-scope-name", message.SyncContext.ScopeName);
//            requestMessage.Headers.Add("dotmim-sync-step", ((int)step).ToString());
//            requestMessage.Headers.Add("dotmim-sync-serialization-format", ser);
//            requestMessage.Headers.Add("dotmim-sync-version", SyncVersion.Current.ToString());

//            // if client specifies a converter, add it as header
//            if (converter != null)
//                requestMessage.Headers.Add("dotmim-sync-converter", converter.Key);

//            // Adding others headers
//            if (this.CustomHeaders != null && this.CustomHeaders.Count > 0)
//                foreach (var kvp in this.CustomHeaders)
//                    if (!requestMessage.Headers.Contains(kvp.Key))
//                        requestMessage.Headers.Add(kvp.Key, kvp.Value);

//            var args = new HttpSendingRequestMessageArgs(requestMessage, message.SyncContext, message, orchestrator.GetServiceHost());
//            await this.orchestrator.InterceptAsync(args, progress: default, cancellationToken).ConfigureAwait(false);

//            var binaryData = await serializer.SerializeAsync(args.Data);
//            requestMessage = args.Request;

//            // Check if data is null
//            binaryData = binaryData == null ? new byte[] { } : binaryData;

//            // calculate hash
//            var hash = HashAlgorithm.SHA256.Create(binaryData);
//            var hashString = Convert.ToBase64String(hash);
//            requestMessage.Headers.Add("dotmim-sync-hash", hashString);

//            // get byte array content
//            requestMessage.Content = new ByteArrayContent(binaryData);

//            // If Json, specify header
//            if (!string.IsNullOrEmpty(contentType) && !requestMessage.Content.Headers.Contains("content-type"))
//                requestMessage.Content.Headers.Add("content-type", contentType);

//            // Eventually, send the request
//            var response = await client.SendAsync(requestMessage, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);

//            if (cancellationToken.IsCancellationRequested)
//                cancellationToken.ThrowIfCancellationRequested();

//            // throw exception if response is not successfull
//            // get response from server
//            if (!response.IsSuccessStatusCode && response.Content != null)
//                await HandleSyncError(response);

//            return response;

//        }

//        /// <summary>
//        /// Handle a request error
//        /// </summary>
//        
//        private async Task HandleSyncError(HttpResponseMessage response)
//        {
//            try
//            {
//                HttpSyncWebException syncException = null;

//                if (!TryGetHeaderValue(response.Headers, "dotmim-sync-error", out string syncErrorTypeName))
//                {
//                    var exceptionString = await response.Content.ReadAsStringAsync();

//                    if (string.IsNullOrEmpty(exceptionString))
//                        exceptionString = response.ReasonPhrase;

//                    syncException = new HttpSyncWebException(exceptionString);
//                }
//                else
//                {
//                    using var streamResponse = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);

//                    if (streamResponse.CanRead)
//                    {
//                        // Error are always json formatted
//                        var webSyncErrorSerializer = new JsonObjectSerializer();
//                        var webError = await webSyncErrorSerializer.DeserializeAsync<WebSyncException>(streamResponse);

//                        if (webError != null)
//                        {
//                            var exceptionString = webError.Message;

//                            if (string.IsNullOrEmpty(exceptionString))
//                                exceptionString = response.ReasonPhrase;

//                            syncException = new HttpSyncWebException(exceptionString)
//                            {
//                                DataSource = webError.DataSource,
//                                InitialCatalog = webError.InitialCatalog,
//                                Number = webError.Number,
//                                SyncStage = webError.SyncStage,
//                                TypeName = webError.TypeName
//                            };

//                        }
//                        else
//                        {
//                            syncException = new HttpSyncWebException(response.ReasonPhrase);
//                        }

//                    }
//                    else
//                    {
//                        syncException = new HttpSyncWebException(response.ReasonPhrase);

//                    }
//                }

//                syncException.ReasonPhrase = response.ReasonPhrase;
//                syncException.StatusCode = response.StatusCode;

//                throw syncException;


//            }
//            catch (SyncException)
//            {
//                throw;
//            }
//            catch (Exception ex)
//            {
//                throw new SyncException(ex);
//            }


//        }

//        public static bool TryGetHeaderValue(HttpResponseHeaders n, string key, out string header)
//        {
//            if (n.TryGetValues(key, out var vs))
//            {
//                header = vs.First();
//                return true;
//            }

//            header = null;
//            return false;
//        }


//        #region IDisposable Support
//        private bool disposedValue = false; // To detect redundant calls

//        protected virtual void Dispose(bool disposing)
//        {
//            if (!disposedValue)
//            {
//                if (disposing)
//                {
//                }
//                disposedValue = true;
//            }
//        }


//        // This code added to correctly implement the disposable pattern.
//        public void Dispose() =>
//            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
//            Dispose(true);
//        #endregion


//    }
//}
