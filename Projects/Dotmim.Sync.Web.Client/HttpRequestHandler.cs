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
using System.Net;
using Dotmim.Sync.Serialization.Serializers;

#if NETSTANDARD
using Microsoft.Net.Http.Headers;
#else
using System.Net.Http.Headers;
#endif

namespace Dotmim.Sync.Web.Client
{
    /// <summary>
    /// Object in charge to send requests
    /// </summary>
    public class HttpRequestHandler : IDisposable
    {

        internal Dictionary<string, string> CustomHeaders { get; } = new Dictionary<string, string>();

        internal Dictionary<string, string> ScopeParameters { get; } = new Dictionary<string, string>();

        internal CookieHeaderValue Cookie { get; set; }


        ///// <summary>
        ///// Ensure that Gzip stream is supported. If not (or if client disable it, explicitly) plain text is used
        ///// </summary>
        ///// <param name="overridenValue"></param>
        //private void EnsureGzipStream(bool overridenValue = true)
        //{
        //    if (!this.Handler.SupportsAutomaticDecompression || !overridenValue)
        //        this.Handler.AutomaticDecompression = DecompressionMethods.None;
        //    else
        //        this.Handler.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;
        //}

        /// <summary>
        /// Process a request message with HttpClient object. 
        /// </summary>
        public async Task<U> ProcessRequestAsync<T, U>(HttpClient client, string baseUri, T content, HttpStep step, Guid sessionId,
            ISerializerFactory serializerFactory, int batchSize, CancellationToken cancellationToken)
        {
            if (client is null)
                throw new ArgumentNullException(nameof(client));

            if (baseUri == null)
                throw new ArgumentException("BaseUri is not defined");

            HttpResponseMessage response = null;
            var responseMessage = default(U);
            try
            {
                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                var requestUri = new StringBuilder();
                requestUri.Append(baseUri);
                requestUri.Append(baseUri.EndsWith("/", StringComparison.CurrentCultureIgnoreCase) ? string.Empty : "/");

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

                // serialize dmSet content to bytearraycontent
                var serializer = serializerFactory.GetSerializer<T>();
                var binaryData = serializer.Serialize(content);
                var arrayContent = new ByteArrayContent(binaryData);

                // reinit client
                client.DefaultRequestHeaders.Clear();

                // add it to the default header
                if (this.Cookie != null)
                    client.DefaultRequestHeaders.Add("Cookie", this.Cookie.ToString());

                // Create the request message
                var requestMessage = new HttpRequestMessage(HttpMethod.Post, requestUri.ToString()) { Content = arrayContent };

                // Adding the serialization format used and session id
                requestMessage.Headers.Add("dotmim-sync-session-id", sessionId.ToString());
                requestMessage.Headers.Add("dotmim-sync-step", ((int)step).ToString());

                // serialize the serialization format and the batchsize we want.
                var ser = JsonConvert.SerializeObject(new { f = serializerFactory.Key, s = batchSize });
                requestMessage.Headers.Add("dotmim-sync-serialization-format", ser);

                // Adding others headers
                if (this.CustomHeaders != null && this.CustomHeaders.Count > 0)
                    foreach (var kvp in this.CustomHeaders)
                        if (!requestMessage.Headers.Contains(kvp.Key))
                            requestMessage.Headers.Add(kvp.Key, kvp.Value);

                // If Json, specify header
                if (serializerFactory.Key == SerializersCollection.JsonSerializer.Key && !requestMessage.Content.Headers.Contains("content-type"))
                    requestMessage.Content.Headers.Add("content-type", "application/json");

                // Eventually, send the request
                response = await client.SendAsync(requestMessage, cancellationToken).ConfigureAwait(false);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // get response from server
                if (!response.IsSuccessStatusCode && response.Content != null)
                {
                    var exrror = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                    var syncException = JsonConvert.DeserializeObject<SyncException>(exrror);

                    if (syncException != null)
                        throw syncException;
                }

                // try to set the cookie for http session
                var headers = response?.Headers;

                if (headers != null)
                {
                    if (headers.TryGetValues("Set-Cookie", out var tmpList))
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

                if (response.Content == null)
                    throw new Exception("Can't have an empty body");

                var serverSerializer = serializerFactory.GetSerializer<U>();

                using (var streamResponse = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
                    if (streamResponse.CanRead && streamResponse.Length > 0)
                        responseMessage = serverSerializer.Deserialize(streamResponse);


                return responseMessage;
            }
            catch (TaskCanceledException)
            {
                throw;
            }
            catch (SyncException)
            {
                throw;
            }
            catch (Exception e)
            {
                if (response == null || response.Content == null)
                    throw e;

                var exrror = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                throw new SyncException(exrror);
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
