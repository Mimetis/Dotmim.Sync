using Dotmim.Sync;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

[assembly: InternalsVisibleTo("Dotmim.Sync.Tests")]

namespace Microsoft.Extensions.DependencyInjection
{
    /// <summary>
    /// Dependency injection extensions for the Dotmim.Sync.Web.Server library.
    /// </summary>
    public static class DependencyInjection
    {
        /// <summary>
        /// Add the server provider (inherited from CoreProvider) and register in the DI a WebServerAgent.
        /// Use the WebServerAgent in your controller, by inject it.
        /// </summary>
        /// <param name="serviceCollection">services collections.</param>
        /// <param name="providerType">Provider inherited from CoreProvider (SqlSyncProvider, MySqlSyncProvider, OracleSyncProvider) Should have [CanBeServerProvider=true]. </param>
        /// <param name="connectionString">Provider connection string.</param>
        /// <param name="setup">Configuration server side. Adding at least tables to be synchronized.</param>
        /// <param name="options">Options, not shared with client, but only applied locally. Can be null.</param>
        /// <param name="webServerOptions">Specific web server options.</param>
        /// <param name="scopeName">Scope name.</param>
        /// <param name="identifier">Can be use to differentiate configuration where you are using the same provider in a multiple databases scenario.</param>
        [Obsolete("Use AddSyncServer(CoreProvider provider) instead, as it offers more possibilities to configure your provider, if needed.")]
        public static IServiceCollection AddSyncServer(this IServiceCollection serviceCollection, Type providerType,
                                                        string connectionString, SyncSetup setup = null, SyncOptions options = null,
                                                        WebServerOptions webServerOptions = null, string scopeName = null, string identifier = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException(nameof(connectionString));

            webServerOptions ??= new WebServerOptions();
            options ??= new SyncOptions();
            setup = setup ?? throw new ArgumentNullException(nameof(setup));
            scopeName ??= SyncOptions.DefaultScopeName;

            // Create provider
            var provider = (CoreProvider)Activator.CreateInstance(providerType);
            provider.ConnectionString = connectionString;

            // Create orchestrator
            serviceCollection.AddScoped(sp => new WebServerAgent(provider, setup, options, webServerOptions, scopeName, identifier));

            return serviceCollection;
        }

        /// <inheritdoc cref="AddSyncServer(IServiceCollection, CoreProvider, string[], SyncOptions, WebServerOptions, string, string)" />
        [Obsolete("Use AddSyncServer(CoreProvider provider) instead, as it offers you to configure your provider, if needed.")]
        public static IServiceCollection AddSyncServer<TProvider>(this IServiceCollection serviceCollection, string connectionString, SyncSetup setup = null, SyncOptions options = null, WebServerOptions webServerOptions = null, string identifier = null)
            where TProvider : CoreProvider, new()
            => serviceCollection.AddSyncServer(typeof(TProvider), connectionString, setup, options, webServerOptions, identifier);

        /// <inheritdoc cref="AddSyncServer(IServiceCollection, CoreProvider, string[], SyncOptions, WebServerOptions, string, string)" />
        [Obsolete("Use AddSyncServer(CoreProvider provider) instead, as it offers you to configure your provider, if needed.")]
        public static IServiceCollection AddSyncServer<TProvider>(this IServiceCollection serviceCollection, string connectionString, string[] tables = default, SyncOptions options = null, WebServerOptions webServerOptions = null, string identifier = null)
            where TProvider : CoreProvider, new()
            => serviceCollection.AddSyncServer(typeof(TProvider), connectionString, new SyncSetup(tables), options, webServerOptions, identifier);

        /// <summary>
        /// Add the server provider (inherited from CoreProvider) and register in the DI as a new WebServerAgent.
        /// In Your controller, inject a WebServerAgent to get your agent.
        /// </summary>
        /// <param name="serviceCollection">services collections.</param>
        /// <param name="provider">Provider inherited from CoreProvider (SqlSyncProvider, MySqlSyncProvider, OracleSyncProvider) Should have [CanBeServerProvider=true]. </param>
        /// <param name="setup">Configuration server side. Adding at least tables to be synchronized.</param>
        /// <param name="options">Options, not shared with client, but only applied locally. Can be null.</param>
        /// <param name="webServerOptions">Specific web server options.</param>
        /// <param name="scopeName">scope name.</param>
        /// <param name="identifier">Can be use to differentiate configuration where you are using the same provider in a multiple databases scenario.</param>
        public static IServiceCollection AddSyncServer(this IServiceCollection serviceCollection, CoreProvider provider,
                                                        SyncSetup setup = null, SyncOptions options = null,
                                                        WebServerOptions webServerOptions = null, string scopeName = null, string identifier = null)
        {
            Guard.ThrowIfNull(provider);

            webServerOptions ??= new WebServerOptions();
            options ??= new SyncOptions();
            setup = setup ?? throw new ArgumentNullException(nameof(setup));
            scopeName ??= SyncOptions.DefaultScopeName;

            // Create orchestrator
            serviceCollection.AddScoped(sp => new WebServerAgent(provider, setup, options, webServerOptions, scopeName, identifier));

            return serviceCollection;
        }

        /// <inheritdoc cref="AddSyncServer(IServiceCollection, CoreProvider, SyncSetup, SyncOptions, WebServerOptions, string, string)" />
        public static IServiceCollection AddSyncServer(this IServiceCollection serviceCollection, CoreProvider provider, string[] tables = default, SyncOptions options = null, WebServerOptions webServerOptions = null, string scopeName = null, string identifier = null)
                => serviceCollection.AddSyncServer(provider, new SyncSetup(tables), options, webServerOptions, scopeName, identifier);

        /// <inheritdoc cref="WebServerAgent.WriteHelloAsync(HttpContext, CancellationToken)"/>
        public static Task WriteHelloAsync(this HttpContext context, WebServerAgent webServerAgent, CancellationToken cancellationToken = default)
            => webServerAgent.WriteHelloAsync(context, cancellationToken);

        /// <inheritdoc cref="WebServerAgent.WriteHelloAsync(HttpContext, IEnumerable{WebServerAgent}, CancellationToken)"/>
        public static Task WriteHelloAsync(this HttpContext context, IEnumerable<WebServerAgent> webServerAgents, CancellationToken cancellationToken = default)
            => WebServerAgent.WriteHelloAsync(context, webServerAgents, cancellationToken);

        /// <summary>
        /// Get Scope Name sent by the client.
        /// </summary>
        public static string GetScopeName(this HttpContext httpContext) => WebServerAgent.TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-scope-name", out var val) ? val : null;

        /// <summary>
        /// Get the DMS version used by the Client.
        /// </summary>
        public static string GetVersion(this HttpContext httpContext) => WebServerAgent.TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-version", out var val) ? val : null;

        /// <summary>
        /// Get Scope Name sent by the client.
        /// </summary>
        public static Guid? GetClientScopeId(this HttpContext httpContext) => WebServerAgent.TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-scope-id", out var val) ? string.IsNullOrEmpty(val) ? null : new Guid(val) : null;

        /// <summary>
        /// Get the current client session id.
        /// </summary>
        public static string GetClientSessionId(this HttpContext httpContext) => WebServerAgent.TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-session-id", out var val) ? val : null;

        /// <summary>
        /// Get the current Step.
        /// </summary>
        public static HttpStep GetCurrentStep(this HttpContext httpContext) => WebServerAgent.TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-step", out var val) ? string.IsNullOrEmpty(val) ? HttpStep.None : (HttpStep)SyncTypeConverter.TryConvertTo<int>(val) : HttpStep.None;

        /// <summary>
        /// Get the identifier that can be used in multi sync providers.
        /// </summary>
        public static string GetIdentifier(this HttpContext httpContext) => WebServerAgent.TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-identifier", out var val) ? val : null;
    }
}