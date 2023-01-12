using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Newtonsoft.Json;

namespace Dotmim.Sync.Web.Client
{
    public partial class WebRemoteOrchestrator : RemoteOrchestrator
    {

        public Dictionary<string, string> CustomHeaders  = new Dictionary<string, string>();
        public Dictionary<string, string> ScopeParameters = new Dictionary<string, string>();

        /// <summary>
        /// Gets or Sets custom converter for all rows
        /// </summary>
        public IConverter Converter { get; set; }

        /// <summary>
        /// Max threads used to get parts from server
        /// </summary>
        public int MaxDownladingDegreeOfParallelism { get; }

        /// <summary>
        /// Gets or Sets serializer used to serialize and deserialize rows coming from server
        /// </summary>
        public ISerializerFactory SerializerFactory { get; set; }
        /// <summary>
        /// Gets or Sets a custom sync policy
        /// </summary>
        public SyncPolicy SyncPolicy { get; set; }

        /// <summary>
        /// Gets or Sets the service uri used to reach the server api.
        /// </summary>
        public string ServiceUri { get; set; }

        /// <summary>
        /// Gets or Sets the HttpClient instanced used for this web client orchestrator
        /// </summary>
        public HttpClient HttpClient { get; set; }
        public CookieHeaderValue Cookie { get; private set; }

        public string GetServiceHost()
        {
            var uri = new Uri(this.ServiceUri);

            if (uri == null)
                return "Undefined";

            return uri.Host;
        }

        /// <summary>
        /// Gets a new web proxy orchestrator
        /// </summary>
        public WebRemoteOrchestrator(string serviceUri,
            IConverter customConverter = null,
            HttpClient client = null,
            SyncPolicy syncPolicy = null,
            int maxDownladingDegreeOfParallelism = 4)
            : base(null, new SyncOptions())
        {

            // if no HttpClient provisionned, create a new one
            if (client == null)
            {
                var handler = new HttpClientHandler();

                // Activated by default
                if (handler.SupportsAutomaticDecompression)
                    handler.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;

                this.HttpClient = new HttpClient(handler);
            }
            else
            {
                this.HttpClient = client;
            }

            this.SyncPolicy = this.EnsurePolicy(syncPolicy);
            this.Converter = customConverter;
            this.MaxDownladingDegreeOfParallelism = maxDownladingDegreeOfParallelism <= 0 ? -1 : maxDownladingDegreeOfParallelism;
            this.ServiceUri = serviceUri;
            this.SerializerFactory = SerializersCollection.JsonSerializerFactory;
        }

        /// <summary>
        /// Adds some scope parameters
        /// </summary>
        public void AddScopeParameter(string key, string value)
        {
            if (this.ScopeParameters.ContainsKey(key))
                this.ScopeParameters[key] = value;
            else
                this.ScopeParameters.Add(key, value);

        }

        /// <summary>
        /// Adds some custom headers
        /// </summary>
        public void AddCustomHeader(string key, string value)
        {
            if (this.CustomHeaders.ContainsKey(key))
                this.CustomHeaders[key] = value;
            else
                this.CustomHeaders.Add(key, value);

        }

        /// <summary>
        /// Ensure we have policy. Create a new one, if not provided
        /// </summary>
        private SyncPolicy EnsurePolicy(SyncPolicy policy)
        {
            if (policy != default)
                return policy;

            // Defining my retry policy
            policy = SyncPolicy.WaitAndRetry(2,
            (retryNumber) =>
            {
                return TimeSpan.FromMilliseconds(500 * retryNumber);
            },
            (ex, arg) =>
            {
                var webEx = ex as SyncException;

                // handle session lost
                return webEx == null || webEx.TypeName != nameof(HttpSessionLostException);

            }, async (ex, cpt, ts, arg) =>
            {
                await this.InterceptAsync(new HttpSyncPolicyArgs(10, cpt, ts, this.GetServiceHost()), default).ConfigureAwait(false);
            });


            return policy;

        }
        private async Task SerializeAsync(HttpResponseMessage response, string fileName, string directoryFullPath, BaseOrchestrator orchestrator = null)
        {
            if (!Directory.Exists(directoryFullPath))
                Directory.CreateDirectory(directoryFullPath);

            var fullPath = Path.Combine(directoryFullPath, fileName);
            using var streamResponse = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
            using var fileStream = new FileStream(fullPath, FileMode.CreateNew, FileAccess.ReadWrite);
            await streamResponse.CopyToAsync(fileStream).ConfigureAwait(false);

        }

        private static async Task<HttpMessageSendChangesResponse> DeserializeAsync(ISerializerFactory serializerFactory, string fileName, string directoryFullPath, BaseOrchestrator orchestrator = null)
        {
            var fullPath = Path.Combine(directoryFullPath, fileName);
            using var fileStream = new FileStream(fullPath, FileMode.Open, FileAccess.Read);
            var httpMessageContent = await serializerFactory.GetSerializer().DeserializeAsync<HttpMessageSendChangesResponse>(fileStream);
            return httpMessageContent;
        }


        private string BuildUri(string baseUri)
        {
            var requestUri = new StringBuilder();
            requestUri.Append(baseUri);
            requestUri.Append(baseUri.EndsWith("/", StringComparison.CurrentCultureIgnoreCase) ? string.Empty : "/");

            // Add params if any
            if (ScopeParameters != null && ScopeParameters.Count > 0)
            {
                string prefix = "?";
                foreach (var kvp in ScopeParameters)
                {
                    requestUri.AppendFormat("{0}{1}={2}", prefix, Uri.EscapeDataString(kvp.Key),
                                            Uri.EscapeDataString(kvp.Value));
                    if (prefix.Equals("?"))
                        prefix = "&";
                }
            }

            return requestUri.ToString();
        }

        /// <summary>
        /// This ProcessRequestAsync\<T\> Will deserialize the message and then send back it to caller
        /// </summary>
        public async Task<T> ProcessRequestAsync<T>(SyncContext context, IScopeMessage message, HttpStep step, int batchSize,
            CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null) where T : IScopeMessage
        {
            if (this.HttpClient is null)
                throw new ArgumentNullException(nameof(this.HttpClient));

            if (this.ServiceUri == null)
                throw new ArgumentException("ServiceUri is not defined");

            HttpResponseMessage response = null;
            try
            {
                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Execute my OpenAsync in my policy context
                response = await this.SyncPolicy.ExecuteAsync(ct => this.SendAsync(step, message, batchSize, ct), cancellationToken, progress);

                // Ensure we have a cookie
                this.EnsureCookie(response?.Headers);

                if (response.Content == null)
                    throw new HttpEmptyResponseContentException();

                T messageResponse = default;
                var serializer = this.SerializerFactory.GetSerializer();

                using (var streamResponse = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
                {
                    if (streamResponse.CanRead)
                        messageResponse = await serializer.DeserializeAsync<T>(streamResponse);
                }
                context = messageResponse?.SyncContext;

                await this.InterceptAsync(new HttpGettingResponseMessageArgs(response, this.ServiceUri.ToString(),
                    HttpStep.SendChangesInProgress, context, messageResponse, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);


                return messageResponse;
            }
            catch (HttpSyncWebException)
            {
                throw;
            }
            catch (SyncException se)
            {
                throw new HttpSyncWebException(se.Message);
            }
            catch (Exception e)
            {
                if (response == null || response.Content == null)
                    throw new HttpSyncWebException(e.Message);

                var exrror = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                throw new HttpSyncWebException(exrror);
            }
        }

        /// <summary>
        /// This ProcessRequestAsync will not deserialize the message and then send back directly the HttpResponseMessage
        /// </summary>

        public async Task<HttpResponseMessage> ProcessRequestAsync(IScopeMessage message, HttpStep step,int batchSize, 
            CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (this.HttpClient is null)
                throw new ArgumentNullException(nameof(this.HttpClient));

            if (this.ServiceUri == null)
                throw new ArgumentException("ServiceUri is not defined");

            HttpResponseMessage response = null;
            try
            {
                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Execute my OpenAsync in my policy context
                response = await this.SyncPolicy.ExecuteAsync(ct => this.SendAsync(step, message, batchSize, ct), cancellationToken, progress); ;

                // Ensure we have a cookie
                this.EnsureCookie(response?.Headers);

                if (response.Content == null)
                    throw new HttpEmptyResponseContentException();

                return response;
            }
            catch (HttpSyncWebException)
            {
                throw;
            }
            catch (SyncException se)
            {
                throw new HttpSyncWebException(se.Message);
            }
            catch (Exception e)
            {
                if (response == null || response.Content == null)
                    throw new HttpSyncWebException(e.Message);

                var exrror = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                throw new HttpSyncWebException(exrror);
            }

        }

        private void EnsureCookie(HttpResponseHeaders headers)
        {
            if (headers == null)
                return;
            if (!headers.TryGetValues("Set-Cookie", out var tmpList))
                return;

            var cookieList = tmpList.ToList();

            // var cookieList = response.Headers.GetValues("Set-Cookie").ToList();
            if (cookieList != null && cookieList.Count > 0)
            {
                //try to parse the very first cookie
                if (CookieHeaderValue.TryParse(cookieList[0], out var cookie))
                    this.Cookie = cookie;
            }
        }


        private async Task<HttpResponseMessage> SendAsync(HttpStep step, IScopeMessage message, int batchSize, CancellationToken cancellationToken)
        {
            var serializer = this.SerializerFactory.GetSerializer();

            var contentType = this.SerializerFactory.Key == SerializersCollection.JsonSerializerFactory.Key ? "application/json" : null;
            var ser = JsonConvert.SerializeObject(new { f = this.SerializerFactory.Key, s = batchSize });

            var requestUri = BuildUri(this.ServiceUri);

            // Create the request message
            var requestMessage = new HttpRequestMessage(HttpMethod.Post, requestUri);

            // Adding the serialization format used and session id and scope name
            requestMessage.Headers.Add("dotmim-sync-session-id", message.SyncContext.SessionId.ToString());
            requestMessage.Headers.Add("dotmim-sync-scope-id", message.SyncContext.ClientId.ToString());
            requestMessage.Headers.Add("dotmim-sync-scope-name", message.SyncContext.ScopeName);
            requestMessage.Headers.Add("dotmim-sync-step", ((int)step).ToString());
            requestMessage.Headers.Add("dotmim-sync-serialization-format", ser);
            requestMessage.Headers.Add("dotmim-sync-version", SyncVersion.Current.ToString());

            // if client specifies a converter, add it as header
            if (this.Converter != null)
                requestMessage.Headers.Add("dotmim-sync-converter", this.Converter.Key);

            // Adding others headers
            if (this.CustomHeaders != null && this.CustomHeaders.Count > 0)
                foreach (var kvp in this.CustomHeaders)
                    if (!requestMessage.Headers.Contains(kvp.Key))
                        requestMessage.Headers.Add(kvp.Key, kvp.Value);

            var args = new HttpSendingRequestMessageArgs(requestMessage, message.SyncContext, message, GetServiceHost());
            await this.InterceptAsync(args, progress: default, cancellationToken).ConfigureAwait(false);

            var binaryData = await serializer.SerializeAsync(args.Data);
            requestMessage = args.Request;

            // Check if data is null
            binaryData = binaryData == null ? new byte[] { } : binaryData;

            // calculate hash
            var hash = HashAlgorithm.SHA256.Create(binaryData);
            var hashString = Convert.ToBase64String(hash);
            requestMessage.Headers.Add("dotmim-sync-hash", hashString);

            // get byte array content
            requestMessage.Content = new ByteArrayContent(binaryData);

            // If Json, specify header
            if (!string.IsNullOrEmpty(contentType) && !requestMessage.Content.Headers.Contains("content-type"))
                requestMessage.Content.Headers.Add("content-type", contentType);

            // Eventually, send the request
            var response = await this.HttpClient.SendAsync(requestMessage, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);

            if (cancellationToken.IsCancellationRequested)
                cancellationToken.ThrowIfCancellationRequested();

            // throw exception if response is not successfull
            // get response from server
            if (!response.IsSuccessStatusCode && response.Content != null)
                await HandleSyncError(response);

            return response;

        }

        /// <summary>
        /// Handle a request error
        /// </summary>
        /// <returns></returns>
        private async Task HandleSyncError(HttpResponseMessage response)
        {
            try
            {
                HttpSyncWebException syncException = null;

                if (!TryGetHeaderValue(response.Headers, "dotmim-sync-error", out string syncErrorTypeName))
                {
                    var exceptionString = await response.Content.ReadAsStringAsync();

                    if (string.IsNullOrEmpty(exceptionString))
                        exceptionString = response.ReasonPhrase;

                    syncException = new HttpSyncWebException(exceptionString);
                }
                else
                {
                    using var streamResponse = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);

                    if (streamResponse.CanRead)
                    {
                        // Error are always json formatted
                        var webSyncErrorSerializer = new JsonObjectSerializer();
                        var webError = await webSyncErrorSerializer.DeserializeAsync<WebSyncException>(streamResponse);

                        if (webError != null)
                        {
                            var exceptionString = webError.Message;

                            if (string.IsNullOrEmpty(exceptionString))
                                exceptionString = response.ReasonPhrase;

                            syncException = new HttpSyncWebException(exceptionString)
                            {
                                DataSource = webError.DataSource,
                                InitialCatalog = webError.InitialCatalog,
                                Number = webError.Number,
                                SyncStage = webError.SyncStage,
                                TypeName = webError.TypeName
                            };

                        }
                        else
                        {
                            syncException = new HttpSyncWebException(response.ReasonPhrase);
                        }

                    }
                    else
                    {
                        syncException = new HttpSyncWebException(response.ReasonPhrase);

                    }
                }

                syncException.ReasonPhrase = response.ReasonPhrase;
                syncException.StatusCode = response.StatusCode;

                throw syncException;


            }
            catch (SyncException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new SyncException(ex);
            }


        }

        public static bool TryGetHeaderValue(HttpResponseHeaders n, string key, out string header)
        {
            if (n.TryGetValues(key, out var vs))
            {
                header = vs.First();
                return true;
            }

            header = null;
            return false;
        }



        public override string ToString() => !String.IsNullOrEmpty(this.ServiceUri) ? this.ServiceUri : base.ToString();
    }
}
