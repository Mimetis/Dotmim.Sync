using Dotmim.Sync;
using Dotmim.Sync.Web.Server;
using System;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.AspNetCore.Hosting;

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
                                                        string connectionString, string scopeName = SyncOptions.DefaultScopeName, SyncSetup setup = null, SyncOptions options = null, WebServerOptions webServerOptions = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException(nameof(connectionString));

            // Create default web server options
            if (webServerOptions == null)
                webServerOptions = new WebServerOptions();

            options = options ?? new SyncOptions();
            setup = setup ?? throw new ArgumentNullException(nameof(setup));

            var serviceProvider = serviceCollection.BuildServiceProvider();

            // Get all registered server providers with schema and options
            var webServerProperties = serviceProvider.GetService<WebServerProperties>();

            // On first time, inject the singleton in the service collection provider
            if (webServerProperties == null)
            {
                var cache = serviceProvider.GetService<IMemoryCache>();
                var env = serviceProvider.GetService<IHostingEnvironment>();

                webServerProperties = new WebServerProperties(cache, env);
                serviceCollection.AddSingleton(webServerProperties);
            }

            // Check if we don't have already added this scope name provider to the remote orchestrator list
            if (webServerProperties.Contains(scopeName))
                throw new ArgumentException($"Orchestrator with scope name {scopeName} already exists in the service collection");

            // Create provider
            var provider = (CoreProvider)Activator.CreateInstance(providerType);
            provider.ConnectionString = connectionString;
            provider.Options = options;

            // Create orchestrator
            var webServerOrchestrator = new WebServerOrchestrator(provider, options, webServerOptions, setup, webServerProperties.Cache, scopeName);

            // add it to the singleton collection
            webServerProperties.Add(webServerOrchestrator);

            return serviceCollection;
        }

        public static IServiceCollection AddSyncServer(this IServiceCollection serviceCollection, WebServerOrchestrator webServerOrchestrator)
        {

            var serviceProvider = serviceCollection.BuildServiceProvider();

            // Get all registered server providers with schema and options
            var webServerProperties = serviceProvider.GetService<WebServerProperties>();

            if (webServerProperties == null)
            {
                var cache = serviceProvider.GetService<IMemoryCache>();
                var env = serviceProvider.GetService<IHostingEnvironment>();

                webServerProperties = new WebServerProperties(cache, env); 
                serviceCollection.AddSingleton(webServerProperties);
            }
           
            // Check if we don't have already added this scope name provider to the remote orchestrator list
            if (webServerProperties.Contains(webServerOrchestrator.ScopeName))
                throw new ArgumentException($"Orchestrator with scope name {webServerOrchestrator.ScopeName} already exists in the service collection");

            // add it to the singleton collection
            webServerProperties.Add(webServerOrchestrator);

            return serviceCollection;
        }


        public static IServiceCollection AddSyncServer<TProvider>(this IServiceCollection serviceCollection, string connectionString, string scopeName = SyncOptions.DefaultScopeName, SyncSetup setup = null, SyncOptions options = null, WebServerOptions webServerOptions = null) where TProvider : CoreProvider, new()
        => serviceCollection.AddSyncServer(typeof(TProvider), connectionString, scopeName, setup, options, webServerOptions);

        public static IServiceCollection AddSyncServer<TProvider>(this IServiceCollection serviceCollection, string connectionString, SyncSetup setup = null, SyncOptions options = null, WebServerOptions webServerOptions = null) where TProvider : CoreProvider, new()
             => serviceCollection.AddSyncServer(typeof(TProvider), connectionString, SyncOptions.DefaultScopeName, setup, options, webServerOptions);

        public static IServiceCollection AddSyncServer<TProvider>(this IServiceCollection serviceCollection, string connectionString, string scopeName = SyncOptions.DefaultScopeName, string[] tables = default, SyncOptions options = null, WebServerOptions webServerOptions = null) where TProvider : CoreProvider, new()
            => serviceCollection.AddSyncServer(typeof(TProvider), connectionString, scopeName, new SyncSetup(tables), options, webServerOptions);

        public static IServiceCollection AddSyncServer<TProvider>(this IServiceCollection serviceCollection, string connectionString, string[] tables = default, SyncOptions options = null, WebServerOptions webServerOptions = null) where TProvider : CoreProvider, new()
            => serviceCollection.AddSyncServer(typeof(TProvider), connectionString, SyncOptions.DefaultScopeName, new SyncSetup(tables), options, webServerOptions);

    }
}

