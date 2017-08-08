using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Microsoft.Net.Http.Headers;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


namespace Dotmim.Sync.Proxy
{
    /// <summary>
    /// Object in charge to send requests
    /// </summary>
    public class HttpRequestHandler : IDisposable
    {
        private Uri baseUri;
        private Dictionary<string, string> scopeParameters;
        private CancellationToken cancellationToken;
        private Dictionary<string, string> customHeaders;
        private HttpClientHandler handler;
        private SerializationFormat serializationFormat;
        private String cookieName;
        private String cookieValue;
        private HttpClient client;

        public HttpRequestHandler(Uri serviceUri, SerializationFormat serializationFormat, CancellationToken cancellationToken)
        {
            this.baseUri = serviceUri;
            this.serializationFormat = serializationFormat;
            this.cancellationToken = cancellationToken;
        }
        public HttpRequestHandler(Uri serviceUri, SerializationFormat serializationFormat,
            HttpClientHandler handler,
            Dictionary<string, string> scopeParameters,
            Dictionary<string, string> customHeaders,
            CancellationToken cancellationToken)
        {
            this.baseUri = serviceUri;
            this.handler = handler;
            this.scopeParameters = scopeParameters;
            this.customHeaders = customHeaders;
            this.cancellationToken = cancellationToken;
            this.serializationFormat = serializationFormat;
        }
        public HttpRequestHandler(Uri serviceUri, SerializationFormat serializationFormat) : this(serviceUri, serializationFormat, CancellationToken.None) { }
        public HttpRequestHandler(Uri serviceUri, SerializationFormat serializationFormat, HttpClientHandler handler) : this(serviceUri, serializationFormat, handler, null, null, CancellationToken.None) { }
        public HttpRequestHandler(Uri serviceUri, SerializationFormat serializationFormat, HttpClientHandler handler, Dictionary<string, string> scopeParameters) : this(serviceUri, serializationFormat, handler, scopeParameters, null, CancellationToken.None) { }
        public HttpRequestHandler(Uri serviceUri, SerializationFormat serializationFormat, HttpClientHandler handler, Dictionary<string, string> scopeParameters, Dictionary<string, string> customHeaders) : this(serviceUri, serializationFormat, handler, scopeParameters, customHeaders, CancellationToken.None) { }


        /// <summary>
        /// Process a request message with HttpClient object. 
        /// </summary>
        public async Task<T> ProcessRequest<T>(T content, CancellationToken cancellationToken)
        {
            HttpResponseMessage response = null;
            T dmSetResponse = default(T);
            try
            {
                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                var requestUri = new StringBuilder();
                requestUri.Append(this.baseUri.ToString());
                requestUri.Append(this.baseUri.ToString().EndsWith("/", StringComparison.CurrentCultureIgnoreCase) ? string.Empty : "/");

                // Add params if any
                if (scopeParameters != null)
                {
                    string prefix = "?";
                    foreach (var kvp in scopeParameters)
                    {
                        requestUri.AppendFormat("{0}{1}={2}", prefix, Uri.EscapeUriString(kvp.Key),
                                                Uri.EscapeUriString(kvp.Value));
                        if (prefix.Equals("?"))
                            prefix = "&";
                    }
                }

                // default handler if no one specified
                HttpClientHandler httpClientHandler = this.handler ?? new HttpClientHandler();


                // serialize dmSet content to bytearraycontent
                var serializer = BaseConverter<T>.GetConverter(serializationFormat);
                var binaryData = serializer.Serialize(content);
                ByteArrayContent arrayContent = new ByteArrayContent(binaryData);

                // do not dispose HttpClient for performance issue
                if (client == null)
                    client = new HttpClient(httpClientHandler);

                if (this.cookieName != null && this.cookieValue != null)
                {
                    // add it to the default header
                    client.DefaultRequestHeaders.Add("Cookie", new CookieHeaderValue(this.cookieName, this.cookieValue).ToString());
                }

                HttpRequestMessage requestMessage = new HttpRequestMessage(HttpMethod.Post, requestUri.ToString())
                {
                    Content = arrayContent
                };

                if (this.customHeaders != null)
                    foreach (var kvp in this.customHeaders)
                        requestMessage.Headers.Add(kvp.Key, kvp.Value);

                //request.AddHeader("content-type", "application/json");
                if (serializationFormat == SerializationFormat.Json && !requestMessage.Content.Headers.Contains("content-type"))
                    requestMessage.Content.Headers.Add("content-type", "application/json");

                response = await client.SendAsync(requestMessage, cancellationToken);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // get response from server
                if (!response.IsSuccessStatusCode)
                    throw new Exception(response.ReasonPhrase);

                // try to set the cookie for http session
                var headers = response?.Headers;
                if (headers != null)
                {
                    if (headers.TryGetValues("Set-Cookie", out IEnumerable<string> tmpList))
                    {
                        var cookieList = tmpList.ToList();
                        // var cookieList = response.Headers.GetValues("Set-Cookie").ToList();
                        if (cookieList != null && cookieList.Count > 0)
                        {
                            // Get the first cookie
                            var cookie = SetCookieHeaderValue.ParseList(cookieList).First();

                            this.cookieName = cookie.Name.Value;
                            this.cookieValue = cookie.Value.Value;
                        }
                    }

                }

                using (var streamResponse = await response.Content.ReadAsStreamAsync())
                    if (streamResponse.CanRead && streamResponse.Length > 0)
                        dmSetResponse = serializer.Deserialize(streamResponse);

                return dmSetResponse;

            }
            catch (Exception e)
            {
                if (response.Content == null)
                    throw e;

                try
                {
                    var exrror = await response.Content.ReadAsStringAsync();
                    WebSyncException webSyncException = JsonConvert.DeserializeObject<WebSyncException>(exrror);

                    if (webSyncException != null)
                        throw webSyncException;

                }
                catch (WebSyncException)
                {
                    throw;
                }
                catch (Exception)
                {
                    // no need to do something here, just rethrow the initial error
                }

                throw e;

            }

        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    if (client != null)
                        client.Dispose();
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
