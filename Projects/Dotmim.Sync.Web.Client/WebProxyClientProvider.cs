//using Dotmim.Sync.Batch;
//using Dotmim.Sync.Data;
//using Dotmim.Sync.Data.Surrogate;
//using Dotmim.Sync.Enumerations;
//using Dotmim.Sync.Messages;
//using Newtonsoft.Json.Linq;
//using System;
//using System.Collections.Generic;
//using System.IO;
//using System.Linq;
//using System.Net.Http;
//using System.Threading;
//using System.Threading.Tasks;
//#if NETSTANDARD
//using Microsoft.Net.Http.Headers;
//#else
//using System.Net.Http.Headers;
//#endif

//namespace Dotmim.Sync.Web.Client
//{

//    /// <summary>
//    /// Class used when you have to deal with a Web Server
//    /// </summary>
//    public class WebProxyClientProvider : IDisposable
//    {
//        private readonly HttpRequestHandler httpRequestHandler;

//        public Dictionary<string, string> CustomHeaders => this.httpRequestHandler.CustomHeaders;
//        public Dictionary<string, string> ScopeParameters => this.httpRequestHandler.ScopeParameters;


//        /// <summary>
//        /// Gets the options used on this provider
//        /// </summary>
//        public SyncOptions Options { get; private set; } = new SyncOptions();

//        /// <summary>
//        /// Gets the options used on this provider
//        /// </summary>
//        public SyncSchema Configuration { get; private set; } = new SyncSchema();

//        /// <summary>
//        /// Set Options parameters
//        /// </summary>
//        public void SetOptions(Action<SyncOptions> options)
//            => options?.Invoke(this.Options);

//        /// <summary>
//        /// Set Configuration parameters
//        /// </summary>
//        public void SetSchema(Action<SyncSchema> configuration)
//            => configuration?.Invoke(this.Configuration);

//      /// <summary>
//        ///  The proxy client does not report any progress
//        /// </summary>
//        /// <param name="progress"></param>
//        public void SetProgress(IProgress<ProgressArgs> progress) { }


//        /// <summary>
//        ///  The proxy client does not use any interceptor
//        /// </summary>
//        public void On(InterceptorBase interceptorBase) { }

//        /// <summary>
//        /// Set an interceptor to get info on the current sync process
//        /// </summary>
//        public void On<T>(Action<T> interceptorAction) where T : ProgressArgs { }

//        /// <summary>
//        /// The proxy client does not interecot changes failed
//        /// Failed changes are handled by the server side only
//        /// </summary>
//        public void InterceptApplyChangesFailed(Func<ApplyChangesFailedArgs, Task> action) { }

//        /// <summary>
//        /// The proxy client does not interecot changes failed
//        /// Failed changes are handled by the server side only
//        /// </summary>
//        public void InterceptApplyChangesFailed(Action<ApplyChangesFailedArgs> action) { }

//        /// <summary>
//        /// Gets or Sets the service uri to the server side
//        /// </summary>
//        public Uri ServiceUri
//        {
//            get => this.httpRequestHandler.BaseUri;
//            set => this.httpRequestHandler.BaseUri = value;
//        }

//        public CancellationToken CancellationToken
//        {
//            get => this.httpRequestHandler.CancellationToken;
//            set => this.httpRequestHandler.CancellationToken = value;
//        }
//        public HttpClientHandler Handler
//        {
//            get => this.httpRequestHandler.Handler;
//            set => this.httpRequestHandler.Handler = value;
//        }
//        public CookieHeaderValue Cookie
//        {
//            get => this.httpRequestHandler.Cookie;
//            set => this.httpRequestHandler.Cookie = value;
//        }

//        public WebProxyClientProvider() => this.httpRequestHandler = new HttpRequestHandler();
//        /// <summary>
//        /// Use this Constructor if you are on the Client Side, only
//        /// </summary>
//        public WebProxyClientProvider(Uri serviceUri) => this.httpRequestHandler = new HttpRequestHandler(serviceUri, CancellationToken.None);


//        public void AddScopeParameter(string key, string value)
//        {
//            if (this.httpRequestHandler.ScopeParameters.ContainsKey(key))
//                this.httpRequestHandler.ScopeParameters[key] = value;
//            else
//                this.httpRequestHandler.ScopeParameters.Add(key, value);

//        }

//        public void AddCustomHeader(string key, string value)
//        {
//            if (this.httpRequestHandler.CustomHeaders.ContainsKey(key))
//                this.httpRequestHandler.CustomHeaders[key] = value;
//            else
//                this.httpRequestHandler.CustomHeaders.Add(key, value);

//        }


//        /// <summary>
//        /// Use this constructor when you are on the Remote Side, only
//        /// </summary>
//        public WebProxyClientProvider(Uri serviceUri,
//                                      Dictionary<string, string> scopeParameters = null,
//                                      Dictionary<string, string> customHeaders = null)
//        {
//            this.httpRequestHandler = new HttpRequestHandler(serviceUri, CancellationToken.None);

//            foreach (var sp in scopeParameters)
//                this.AddScopeParameter(sp.Key, sp.Value);

//            foreach (var ch in customHeaders)
//                this.AddCustomHeader(ch.Key, ch.Value);
//        }


//        #region IDisposable Support
//        private bool disposedValue = false; // To detect redundant calls

//        protected virtual void Dispose(bool disposing)
//        {
//            if (!this.disposedValue)
//            {
//                if (disposing)
//                {
//                    if (this.httpRequestHandler != null)
//                        this.httpRequestHandler.Dispose();
//                }
//                this.disposedValue = true;
//            }
//        }

//        // This code added to correctly implement the disposable pattern.
//        public void Dispose() =>
//            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
//            this.Dispose(true);

//        #endregion
//    }
//}
