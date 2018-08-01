using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

#if NETSTANDARD
using Microsoft.Net.Http.Headers;
#else
using System.Net.Http.Headers;
#endif



namespace Dotmim.Sync.Web
{
    /// <summary>
    /// Object in charge to send requests
    /// </summary>
    public class HttpRequestHandler : IDisposable
    {
        private HttpClient client;

        internal Dictionary<string, string> CustomHeaders { get; } = new Dictionary<string, string>();

        internal Dictionary<string, string> ScopeParameters { get; } = new Dictionary<string, string>();

        internal Uri BaseUri { get; set; }

        internal CancellationToken CancellationToken { get; set; }

        internal HttpClientHandler Handler { get; set; }

        internal CookieHeaderValue Cookie { get; set; }

        public HttpRequestHandler()
        {
            this.CancellationToken = CancellationToken.None;
        }

        public HttpRequestHandler(Uri serviceUri, CancellationToken cancellationToken)
        {
            this.BaseUri = serviceUri;
            this.CancellationToken = cancellationToken;
        }


        /// <summary>
        /// Process a request message with HttpClient object. 
        /// </summary>
        public async Task<T> ProcessRequest<T>(T content, SerializationFormat serializationFormat,  CancellationToken cancellationToken)
        {
            if (this.BaseUri == null)
                throw new ArgumentException("BaseUri is not defined");

            HttpResponseMessage response = null;
            T responseMessage = default(T);
            try
            {
                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                var requestUri = new StringBuilder();
                requestUri.Append(this.BaseUri.ToString());
                requestUri.Append(this.BaseUri.ToString().EndsWith("/", StringComparison.CurrentCultureIgnoreCase) ? string.Empty : "/");

                // Add params if any
                if (ScopeParameters != null && ScopeParameters.Count > 0)
                {
                    string prefix = "?";
                    foreach (var kvp in ScopeParameters)
                    {
                        requestUri.AppendFormat("{0}{1}={2}", prefix, Uri.EscapeUriString(kvp.Key),
                                                Uri.EscapeUriString(kvp.Value));
                        if (prefix.Equals("?"))
                            prefix = "&";
                    }
                }

                // default handler if no one specified
                HttpClientHandler httpClientHandler = this.Handler ?? new HttpClientHandler();

                // serialize dmSet content to bytearraycontent
                var serializer = BaseConverter<T>.GetConverter(serializationFormat);
                var binaryData = serializer.Serialize(content);
                ByteArrayContent arrayContent = new ByteArrayContent(binaryData);

                // do not dispose HttpClient for performance issue
                if (client == null)
                    client = new HttpClient(httpClientHandler);

                // add it to the default header
                if (this.Cookie != null)
                    client.DefaultRequestHeaders.Add("Cookie", this.Cookie.ToString());

                HttpRequestMessage requestMessage = new HttpRequestMessage(HttpMethod.Post, requestUri.ToString())
                {
                    Content = arrayContent
                };

                // Adding the serialization format used
                requestMessage.Headers.Add("dotmim-sync-serialization-format", serializationFormat.ToString());

                // Adding others headers
                if (this.CustomHeaders != null && this.CustomHeaders.Count > 0)
                    foreach (var kvp in this.CustomHeaders)
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
#if NETSTANDARD
                            // Get the first cookie
                            this.Cookie = CookieHeaderValue.ParseList(cookieList).FirstOrDefault();
#else
                            //try to parse the very first cookie
                            if (CookieHeaderValue.TryParse(cookieList[0], out var cookie))
                                this.Cookie = cookie;
#endif
                        }
                    }

                }

                using (var streamResponse = await response.Content.ReadAsStreamAsync())
                    if (streamResponse.CanRead && streamResponse.Length > 0)
                        responseMessage = serializer.Deserialize(streamResponse);

                return responseMessage;

            }
            catch (TaskCanceledException ex)
            {
                throw ex;
            }
            catch (Exception e)
            {
                if (response == null || response.Content == null)
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
                catch (Exception )
                {
                    throw;
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
