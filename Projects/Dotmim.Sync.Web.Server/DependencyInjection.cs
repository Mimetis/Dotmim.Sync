using Dotmim.Sync;
using Dotmim.Sync.Web.Server;
using System;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
[assembly:InternalsVisibleTo("Dotmim.Sync.Tests")]

namespace Microsoft.Extensions.DependencyInjection
{
    public static class DependencyInjection
    {
        internal static bool RegisterAsSingleton;
        
        private static Type _providerType;
        private static string _connectionString;
        private static SyncConfiguration _syncConfiguration;

        /// <summary>
        /// Add the server provider (inherited from CoreProvider) and register in the DI a WebProxyServerProvider.
        /// Use the WebProxyServerProvider in your controller, by inject it.
        /// </summary>
        /// <typeparam name="TProvider">Provider inherited from CoreProvider (SqlSyncProvider, MySqlSyncProvider, OracleSyncProvider) Should have [CanBeServerProvider=true] </typeparam>
        /// <param name="serviceCollection"></param>
        /// <param name="connectionString">Provider connection string</param>
        /// <param name="action">Configuration server side. Adding at least tables to be synchronized</param>
        /// <param name="registerAsSingleton">WebProxyServerProvider registration</param>
        public static IServiceCollection AddSyncServer<TProvider>(
                    this IServiceCollection serviceCollection,
                    string connectionString,
                    Action<SyncConfiguration> action,
                    bool registerAsSingleton = true) where TProvider : CoreProvider, new()
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException(nameof(connectionString));
            
            if (action == null)
                throw new ArgumentNullException(nameof(action));

            RegisterAsSingleton = registerAsSingleton;

            if (registerAsSingleton)
            {
                var provider = new TProvider();
                var syncConfiguration = new SyncConfiguration();
                action.Invoke(syncConfiguration);

                provider.ConnectionString = connectionString;

                var webProvider = new WebProxyServerProvider(provider)
                {
                    // Sets the configuration, owned by the server side.
                    Configuration = syncConfiguration,
                    // since we will register this proxy as a singleton, just signal it
                    IsRegisterAsSingleton = true
                };
                serviceCollection.AddSingleton(webProvider);
            }
            else
            {
                _providerType = typeof(TProvider);
                _connectionString = connectionString;
                _syncConfiguration = new SyncConfiguration();
                action.Invoke(_syncConfiguration);

                serviceCollection.AddOptions();
                serviceCollection.TryAdd(ServiceDescriptor.Singleton<SyncMemoryCache, SyncMemoryCache>());
            }

            return serviceCollection;
        }

        internal static WebProxyServerProvider GetNewWebProxyServerProvider()
        {
            if (RegisterAsSingleton) throw new Exception("Do not use this method if RegisterAsSingleton is true!");
            
            var provider = (CoreProvider) Activator.CreateInstance(_providerType);
            provider.ConnectionString = _connectionString;
            var webProvider = new WebProxyServerProvider(provider)
            {
                // Sets the configuration, owned by the server side.
                Configuration = _syncConfiguration,
                IsRegisterAsSingleton = false
            };
            return webProvider;
        }
        
        public class SyncMemoryCache : MemoryCache
        {
            public SyncMemoryCache(IOptions<MemoryCacheOptions> optionsAccessor) : base(optionsAccessor)
            {
            }
        }
    }
}

