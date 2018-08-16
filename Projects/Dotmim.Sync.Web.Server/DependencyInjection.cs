using Dotmim.Sync;
using Dotmim.Sync.Web.Server;
using System;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class DependencyInjection
    {
        /// <summary>
        /// Add the server provider (inherited from CoreProvider) and register in the DI a WebProxyServerProvider.
        /// Use the WebProxyServerProvider in your controller, by inject it.
        /// </summary>
        /// <typeparam name="TProvider">Provider inherited from CoreProvider (SqlSyncProvider, MySqlSyncProvider, OracleSyncProvider) Shoud have [CanBeServerProvider=true] </typeparam>
        /// <param name="serviceCollection"></param>
        /// <param name="connectionString">Provider connection string</param>
        /// <param name="action">Configuration server side. Adding at least tables to be synchronized</param>
        public static IServiceCollection AddSyncServer<TProvider>(
                    this IServiceCollection serviceCollection,
                    string connectionString,
                    Action<SyncConfiguration> action) where TProvider : CoreProvider, new()
        {
            if (action == null)
                throw new ArgumentNullException(nameof(action));

            var provider = new TProvider();
            SyncConfiguration syncConfiguration = new SyncConfiguration();
            action?.Invoke(syncConfiguration);

            provider.ConnectionString = connectionString;

            var webProvider = new WebProxyServerProvider(provider)
            {
                // Sets the configuration, owned by the server side.
                Configuration = syncConfiguration,
                // since we will register this proxy as a singleton, just signal it
                IsRegisterAsSingleton = true
            };

            
            serviceCollection.AddSingleton(webProvider);

            return serviceCollection;
        }


        

    }
}

