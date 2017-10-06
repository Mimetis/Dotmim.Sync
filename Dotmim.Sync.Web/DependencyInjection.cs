using Dotmim.Sync;
using Dotmim.Sync.Web;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System;
using System.Collections.Generic;
using System.Text;

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

            provider.SetConfiguration(syncConfiguration);
            provider.ConnectionString = connectionString;

            var webProvider = new WebProxyServerProvider(provider)
            {
                // since we will register this proxy as a singleton, just signal it
                IsRegisterAsSingleton = true
            };

            serviceCollection.AddSingleton(webProvider);

            return serviceCollection;
        }


        

    }
}

