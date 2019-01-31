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
    public class WebProxyServerProvider
    {

        /// <summary>
        /// TODO : How to handle a direct instance of webproxy provider, if no memorycache available ?
        /// </summary>
        public WebProxyServerProvider(IProvider provider, SyncConfiguration configuration)
        {
        }


        public static bool IsSessionEnabled(HttpContext context)
        {
            // try to get the session store service from DI
            var sessionStore = context.RequestServices.GetService(typeof(ISessionStore));
            return sessionStore != null;
        }

        /// <summary>
        /// Call this method to handle requests on the server, sent by the client
        /// </summary>
        public async Task HandleRequestAsync(HttpContext context) =>
            await HandleRequestAsync(context, null, CancellationToken.None);

        /// <summary>
        /// Call this method to handle requests on the server, sent by the client
        /// </summary>
        public async Task HandleRequestAsync(HttpContext context, Action<SyncMemoryProvider> action) =>
            await HandleRequestAsync(context, action, CancellationToken.None);

        /// <summary>
        /// Call this method to handle requests on the server, sent by the client
        /// </summary>
        public async Task HandleRequestAsync(HttpContext context, CancellationToken token) =>
            await HandleRequestAsync(context, null, token);

        /// <summary>
        /// Call this method to handle requests on the server, sent by the client
        /// </summary>
        public async Task HandleRequestAsync(HttpContext context, Action<SyncMemoryProvider> action, CancellationToken cancellationToken)
        {
            var httpRequest = context.Request;
            var httpResponse = context.Response;
            var streamArray = GetBody(httpRequest);

            var serializationFormat = SerializationFormat.Json;
            // Get the serialization format
            if (TryGetHeaderValue(context.Request.Headers, "dotmim-sync-serialization-format", out var vs))
                serializationFormat = vs.ToLowerInvariant() == "json" ? SerializationFormat.Json : SerializationFormat.Binary;

            SyncMemoryProvider syncMemoryProvider = null;
            var syncSessionId = "";
            HttpMessage httpMessage = null;
            try
            {
                var serializer = BaseConverter<HttpMessage>.GetConverter(serializationFormat);
                httpMessage = serializer.Deserialize(streamArray);
                syncSessionId = httpMessage.SyncContext.SessionId.ToString();

                // get cached provider instance
                syncMemoryProvider = GetCachedProviderInstance(context, syncSessionId);

                if (syncMemoryProvider == null)
                    throw new SyncException($"Can't find any cached provider for session id {syncSessionId}");

                // action from user if available
                action?.Invoke(syncMemoryProvider);

                // get cache manager
                syncMemoryProvider.LocalProvider.CacheManager = GetCacheManagerInstance(context, syncSessionId);

                var httpMessageResponse =
                    await syncMemoryProvider.GetResponseMessageAsync(httpMessage, cancellationToken);

                var binaryData = serializer.Serialize(httpMessageResponse);
                await GetBody(httpResponse).WriteAsync(binaryData, 0, binaryData.Length);

            }
            catch (Exception ex)
            {
                await WriteExceptionAsync(httpResponse, ex, syncMemoryProvider?.LocalProvider?.ProviderTypeName ?? "ServerLocalProvider");
            }
            finally
            {
                if (httpMessage != null && httpMessage.Step == HttpStep.EndSession)
                    Cleanup(context.RequestServices.GetService(typeof(IMemoryCache)), syncSessionId);
            }
        }

        /// <summary>
        /// Get an instance of SyncMemoryProvider depending on session id. If the entry for session id does not exists, create a new one
        /// </summary>
        private static SyncMemoryProvider GetCachedProviderInstance(HttpContext context, string syncSessionId)
        {
            SyncMemoryProvider syncMemoryProvider;

            if (!(context.RequestServices.GetService(typeof(MemoryCache)) is IMemoryCache cache))
                throw new SyncException("Cache is not configured! Please add memory cache, distributed or not (see https://docs.microsoft.com/en-us/aspnet/core/performance/caching/response?view=aspnetcore-2.2)");

            if (string.IsNullOrWhiteSpace(syncSessionId))
                throw new ArgumentNullException(nameof(syncSessionId));

            // get the sync provider associated with the session id
            syncMemoryProvider = (SyncMemoryProvider)cache.Get(syncSessionId);
            if (syncMemoryProvider != null)
                return syncMemoryProvider;

            syncMemoryProvider = DependencyInjection.GetNewWebProxyServerProvider();
            cache.Set(syncSessionId, syncMemoryProvider, TimeSpan.FromHours(1));
            return syncMemoryProvider;
        }

        /// <summary>
        /// Get an instance of SyncMemoryProvider depending on session id. If the entry for session id does not exists, create a new one
        /// </summary>
        private static ICache GetCacheManagerInstance(HttpContext context, string syncSessionId)
        {
            InMemoryCache memoryCache;

            if (!(context.RequestServices.GetService(typeof(MemoryCache)) is IMemoryCache cache))
                throw new SyncException("Cache is not configured! Please add memory cache, distributed or not (see https://docs.microsoft.com/en-us/aspnet/core/performance/caching/response?view=aspnetcore-2.2)");

            if (string.IsNullOrWhiteSpace(syncSessionId))
                throw new ArgumentNullException(nameof(syncSessionId));

            // get the sync provider associated with the session id
            memoryCache = (InMemoryCache)cache.Get(syncSessionId + "_session");
            if (memoryCache != null)
                return memoryCache;

            memoryCache = new InMemoryCache();
            cache.Set(syncSessionId + "_session", memoryCache, TimeSpan.FromHours(1));
            return memoryCache;
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
                try
                {
                    (memoryCache as IMemoryCache)?.Remove(syncSessionId + "_session");
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


        public bool TryGetHeaderValue(IHeaderDictionary n, string key, out string header)
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
