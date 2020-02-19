using Dotmim.Sync;
using Dotmim.Sync.Web.Server;
using System;
using System.Runtime.CompilerServices;


[assembly: InternalsVisibleTo("Dotmim.Sync.Tests")]

namespace Microsoft.Extensions.DependencyInjection
{
    public static class DependencyInjection
    {

        /// <summary>
        /// Add the server provider (inherited from CoreProvider) and register in the DI a WebProxyServerProvider.
        /// Use the WebProxyServerProvider in your controller, by inject it.
        /// </summary>
        /// <param name="providerType">Provider inherited from CoreProvider (SqlSyncProvider, MySqlSyncProvider, OracleSyncProvider) Should have [CanBeServerProvider=true] </param>
        /// <param name="serviceCollection">services collections</param>
        /// <param name="connectionString">Provider connection string</param>
        /// <param name="setup">Configuration server side. Adding at least tables to be synchronized</param>
        /// <param name="options">Options, not shared with client, but only applied locally. Can be null</param>

        public static IServiceCollection AddSyncServer(this IServiceCollection serviceCollection, Type providerType,
                                                        string connectionString, SyncSetup setup = null, WebServerOptions options = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException(nameof(connectionString));

            //serviceCollection.AddOptions();

            // create cache instance
            var webServerProperties = new WebServerProperties
            {
                ProviderType = providerType,
                Options = options ?? new WebServerOptions(),
                ConnectionString = connectionString,
                Setup = setup
            };

            // Add this to the service pool injection
            serviceCollection.AddSingleton(webServerProperties);

            // Add this to the service pool injection
            serviceCollection.AddScoped<WebProxyServerOrchestrator>();

            return serviceCollection;
        }

        public static IServiceCollection AddSyncServer<TProvider>(this IServiceCollection serviceCollection, string connectionString, SyncSetup setup, WebServerOptions options = null) where TProvider : CoreProvider, new()
            => serviceCollection.AddSyncServer(typeof(TProvider), connectionString, setup, options);

        public static IServiceCollection AddSyncServer<TProvider>(this IServiceCollection serviceCollection, string connectionString, string[] tables, WebServerOptions options = null) where TProvider : CoreProvider, new()
            => serviceCollection.AddSyncServer(typeof(TProvider), connectionString, new SyncSetup(tables), options);

    }
}

