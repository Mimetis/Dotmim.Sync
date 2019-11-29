using System;
using System.Collections.Generic;
using Dotmim.Sync.Batch;
using System.Threading.Tasks;
using System.IO;
using System.Linq;
using System.Net.Http.Headers;
using System.Threading;
using Newtonsoft.Json;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Data;
using Dotmim.Sync.Messages;
using Dotmim.Sync.Web.Client;
using Newtonsoft.Json.Linq;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Session;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Dotmim.Sync.Cache;

namespace Dotmim.Sync.Web.Server
{

    /// <summary>
    /// Class used when you have to deal with a Web Server
    /// </summary>
    public class WebProxyServerOrchestrator
    {

        private static WebProxyServerOrchestrator defaultInstance = new WebProxyServerOrchestrator();


        /// <summary>
        /// Default constructor for DI
        /// </summary>
        public WebProxyServerOrchestrator() { }


        /// <summary>
        /// Create a new WebProxyServerProvider with a first instance of an in memory CoreProvider
        /// Use this method to create your WebProxyServerProvider if you don't use the DI stuff from ASP.NET
        /// </summary>
        public static WebProxyServerOrchestrator Create(HttpContext context, CoreProvider provider, Action<SyncSchema> conf, Action<SyncOptions> options)
        {
            if (!TryGetHeaderValue(context.Request.Headers, "dotmim-sync-session-id", out var sessionId))
                throw new SyncException($"Can't find any session id in the header");

            // Check if we have already a cached Sync Memory provider
            var syncMemoryOrchestrator = GetCachedOrchestrator(context, sessionId);

            // we don't have any provider for this session id, so create it
            if (syncMemoryOrchestrator == null)
                AddNewOrchestratorToCache(context, provider, conf, options, sessionId);

            return defaultInstance;
        }

        /// <summary>
        /// Retrieve from cache the selected provider depending on the session id
        /// </summary>
        public WebServerOrchestrator GetLocalOrchestrator(HttpContext context)
        {
            if (!TryGetHeaderValue(context.Request.Headers, "dotmim-sync-session-id", out var sessionId))
                return null;

            var webServerOrchestrator = GetCachedOrchestrator(context, sessionId);

            if (webServerOrchestrator != null)
                return webServerOrchestrator;

            return null;
        }

        /// <summary>
        /// Call this method to handle requests on the server, sent by the client
        /// </summary>
        public Task HandleRequestAsync(HttpContext context) =>
            HandleRequestAsync(context, null, CancellationToken.None);

        /// <summary>
        /// Call this method to handle requests on the server, sent by the client
        /// </summary>
        public Task HandleRequestAsync(HttpContext context, Action<RemoteOrchestrator> action) =>
            HandleRequestAsync(context, action, CancellationToken.None);

        /// <summary>
        /// Call this method to handle requests on the server, sent by the client
        /// </summary>
        public Task HandleRequestAsync(HttpContext context, CancellationToken token) =>
            HandleRequestAsync(context, null, token);

        /// <summary>
        /// Call this method to handle requests on the server, sent by the client
        /// </summary>
        public async Task HandleRequestAsync(HttpContext context, Action<RemoteOrchestrator> action, CancellationToken cancellationToken)
        {
            var httpRequest = context.Request;
            var httpResponse = context.Response;
            var streamArray = GetBody(httpRequest);

            string clientSerializationFormat = "json";
            // Get the serialization format
            if (TryGetHeaderValue(context.Request.Headers, "dotmim-sync-serialization-format", out var vs))
                clientSerializationFormat = vs.ToLowerInvariant();

            if (!TryGetHeaderValue(context.Request.Headers, "dotmim-sync-session-id", out var sessionId))
                throw new SyncException($"Can't find any session id in the header");

            WebServerOrchestrator remoteOrchestrator = null;
            
            try
            {

                // get cached provider instance if not defined byt web proxy server provider
                if (remoteOrchestrator == null)
                    remoteOrchestrator = GetCachedOrchestrator(context, sessionId);

                if (remoteOrchestrator == null)
                    remoteOrchestrator = AddNewOrchestratorToCacheFromDI(context, sessionId);

                var clientSerializer = remoteOrchestrator.Options.GetSerializer<HttpMessage>(clientSerializationFormat);
                var httpMessage = clientSerializer.Deserialize(streamArray);
                var syncSessionId = httpMessage.SyncContext.SessionId.ToString();


                if (!httpMessage.SyncContext.SessionId.Equals(Guid.Parse(sessionId)))
                    throw new SyncException($"Session id is not matching correctly between header and message");

                // action from user if available
                action?.Invoke(remoteOrchestrator);

                var httpMessageResponse =
                    await remoteOrchestrator.GetResponseMessageAsync(httpMessage, cancellationToken).ConfigureAwait(false);

                // Adding the serialization format used and session id
                httpResponse.Headers.Add("dotmim-sync-session-id", sessionId.ToString());
                httpResponse.Headers.Add("dotmim-sync-serialization-format", remoteOrchestrator.Options.Serializers.CurrentKey);

                var serverSerializer = remoteOrchestrator.Options.GetSerializer<HttpMessage>() ;

                var binaryData = serverSerializer.Serialize(httpMessageResponse);

                await GetBody(httpResponse).WriteAsync(binaryData, 0, binaryData.Length).ConfigureAwait(false);

            }
            catch (Exception ex)
            {
                await WriteExceptionAsync(httpResponse, ex, remoteOrchestrator?.Provider?.ProviderTypeName ?? "ServerLocalProvider");
            }
            finally
            {
                //if (httpMessage != null && httpMessage.Step == HttpStep.EndSession)
                //    Cleanup(context.RequestServices.GetService(typeof(IMemoryCache)), syncSessionId);
            }
        }

        /// <summary>
        /// Get an instance of SyncMemoryProvider depending on session id. If the entry for session id does not exists, create a new one
        /// </summary>
        private static WebServerOrchestrator GetCachedOrchestrator(HttpContext context, string syncSessionId)
        {
            WebServerOrchestrator remoteOrchestrator;

            var cache = context.RequestServices.GetService<IMemoryCache>();
            if (cache == null)
                throw new SyncException("Cache is not configured! Please add memory cache, distributed or not (see https://docs.microsoft.com/en-us/aspnet/core/performance/caching/response?view=aspnetcore-2.2)");

            if (string.IsNullOrWhiteSpace(syncSessionId))
                throw new ArgumentNullException(nameof(syncSessionId));

            // get the sync provider associated with the session id
            remoteOrchestrator = cache.Get(syncSessionId) as WebServerOrchestrator;

            return remoteOrchestrator;
        }

        /// <summary>
        /// Add a new instance of SyncMemoryProvider, created by DI
        /// </summary>
        /// <returns></returns>
        private static WebServerOrchestrator AddNewOrchestratorToCacheFromDI(HttpContext context, string syncSessionId)
        {
            var cache = context.RequestServices.GetService<IMemoryCache>();

            if (cache == null)
                throw new SyncException("Cache is not configured! Please add memory cache, distributed or not (see https://docs.microsoft.com/en-us/aspnet/core/performance/caching/response?view=aspnetcore-2.2)");

            var remoteOrchestrator = DependencyInjection.GetNewOrchestrator();
            cache.Set(syncSessionId, remoteOrchestrator, TimeSpan.FromHours(1));

            return remoteOrchestrator;
        }


        private static WebServerOrchestrator AddNewOrchestratorToCache(HttpContext context, CoreProvider provider, Action<SyncSchema> schema, Action<SyncOptions> options, string sessionId)
        {
            WebServerOrchestrator remoteOrchestrator;
            var cache = context.RequestServices.GetService<IMemoryCache>();

            if (cache == null)
                throw new SyncException("Cache is not configured! Please add memory cache, distributed or not (see https://docs.microsoft.com/en-us/aspnet/core/performance/caching/response?view=aspnetcore-2.2)");

            remoteOrchestrator = new WebServerOrchestrator(provider);

            var syncSchema = new SyncSchema();
            schema(syncSchema);
            remoteOrchestrator.Schema = syncSchema;

            var syncOptions = new SyncOptions();
            options(syncOptions);
            remoteOrchestrator.Options = syncOptions;

            cache.Set(sessionId, remoteOrchestrator, TimeSpan.FromHours(1));
            return remoteOrchestrator;
        }



        /// <summary>
        /// Clean up memory cache object for specified session id
        /// </summary>
        private static void Cleanup(object memoryCache, string syncSessionId)
        {
            if (memoryCache == null || string.IsNullOrWhiteSpace(syncSessionId)) return;
            Task.Run(() =>
            {
                try
                {
                    (memoryCache as IMemoryCache)?.Remove(syncSessionId);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
            });
        }

        /// <summary>
        /// Write exception to output message
        /// </summary>
        public static async Task WriteExceptionAsync(HttpResponse httpResponse, Exception ex, string providerTypeName)
        {
            // Check if it's an unknown error, not managed (yet)
            if (!(ex is SyncException syncException))
                syncException = new SyncException(ex.Message, SyncStage.None, SyncExceptionType.Unknown);

            var webXMessage = JsonConvert.SerializeObject(syncException);

            httpResponse.StatusCode = StatusCodes.Status400BadRequest;
            httpResponse.ContentLength = webXMessage.Length;
            await httpResponse.WriteAsync(webXMessage);
            Console.WriteLine(syncException);
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

        public Stream GetBody(HttpRequest r) => r.Body;
        public Stream GetBody(HttpResponse r) => r.Body;

    }

}
