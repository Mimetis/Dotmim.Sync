using Dotmim.Sync;
using Dotmim.Sync.Web.Server;
using System;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
[assembly: InternalsVisibleTo("Dotmim.Sync.Tests")]

namespace Microsoft.Extensions.DependencyInjection
{
    public static class DependencyInjection
    {
        private static Type providerType;
        private static string connectionString;
        private static Action<SyncSchema> configuration;
        private static Action<SyncOptions> options;

        /// <summary>
        /// Add the server provider (inherited from CoreProvider) and register in the DI a WebProxyServerProvider.
        /// Use the WebProxyServerProvider in your controller, by inject it.
        /// </summary>
        /// <typeparam name="TProvider">Provider inherited from CoreProvider (SqlSyncProvider, MySqlSyncProvider, OracleSyncProvider) Should have [CanBeServerProvider=true] </typeparam>
        /// <param name="serviceCollection"></param>
        /// <param name="connectionString">Provider connection string</param>
        /// <param name="configuration">Configuration server side. Adding at least tables to be synchronized</param>
        /// <param name="options">Options, not shared with client, but only applied locally. Can be null</param>
        public static IServiceCollection AddSyncServer<TProvider>(
                    this IServiceCollection serviceCollection,
                    string connectionString,
                    Action<SyncSchema> configuration,
                    Action<SyncOptions> options = null) where TProvider : CoreProvider, new()
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException(nameof(connectionString));

            providerType = typeof(TProvider);
            DependencyInjection.connectionString = connectionString;
            DependencyInjection.options = options;
            DependencyInjection.configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));

            serviceCollection.AddOptions();
            serviceCollection.AddSingleton(new WebProxyServerProvider());

            return serviceCollection;
        }

        /// <summary>
        /// Create a new instance of Sync Memory Provider
        /// </summary>
        internal static WebServerOrchestrator GetNewOrchestrator()
        {
            var provider = (CoreProvider)Activator.CreateInstance(providerType);
            provider.ConnectionString = connectionString;

            var webProvider = new WebServerOrchestrator(provider);

            // Sets the options / configurations
            webProvider.SetSchema(configuration);
            webProvider.SetOptions(options);

            return webProvider;
        }


    }
}

