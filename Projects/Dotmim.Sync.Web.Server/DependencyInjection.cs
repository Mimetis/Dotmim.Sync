using Dotmim.Sync;
using Dotmim.Sync.Web.Server;
using System;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using System.Threading;
using Dotmim.Sync.Web.Client;

[assembly: InternalsVisibleTo("Dotmim.Sync.Tests")]

namespace Microsoft.Extensions.DependencyInjection
{
    public static class DependencyInjection
    {
        /// <summary>
        /// Add the server provider (inherited from CoreProvider) and register in the DI a WebServerAgent.
        /// Use the WebServerAgent in your controller, by inject it.
        /// </summary>
        /// <param name="providerType">Provider inherited from CoreProvider (SqlSyncProvider, MySqlSyncProvider, OracleSyncProvider) Should have [CanBeServerProvider=true] </param>
        /// <param name="serviceCollection">services collections</param>
        /// <param name="connectionString">Provider connection string</param>
        /// <param name="setup">Configuration server side. Adding at least tables to be synchronized</param>
        /// <param name="options">Options, not shared with client, but only applied locally. Can be null</param>

        public static IServiceCollection AddSyncServer(this IServiceCollection serviceCollection, Type providerType,
                                                        string connectionString, SyncSetup setup = null, SyncOptions options = null, 
                                                        WebServerOptions webServerOptions = null, string identifier = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException(nameof(connectionString));

            webServerOptions ??= new WebServerOptions();
            options ??= new SyncOptions();
            setup = setup ?? throw new ArgumentNullException(nameof(setup));

            // Create provider
            var provider = (CoreProvider)Activator.CreateInstance(providerType);
            provider.ConnectionString = connectionString;

            // Create orchestrator
            serviceCollection.AddScoped(sp => new WebServerAgent(provider, setup, options, webServerOptions, identifier));

            return serviceCollection;

        }

        public static IServiceCollection AddSyncServer<TProvider>(this IServiceCollection serviceCollection, string connectionString, SyncSetup setup = null, SyncOptions options = null, WebServerOptions webServerOptions = null, string identifier = null) where TProvider : CoreProvider, new()
        => serviceCollection.AddSyncServer(typeof(TProvider), connectionString, setup, options, webServerOptions, identifier);
        public static IServiceCollection AddSyncServer<TProvider>(this IServiceCollection serviceCollection, string connectionString, string[] tables = default, SyncOptions options = null, WebServerOptions webServerOptions = null, string identifier = null) where TProvider : CoreProvider, new()
            => serviceCollection.AddSyncServer(typeof(TProvider), connectionString, new SyncSetup(tables), options, webServerOptions, identifier);

    }
}
namespace Dotmim.Sync
{
    public static class DependencyInjection
    {
        public static Task WriteHelloAsync(this HttpContext context, WebServerAgent webServerAgent, CancellationToken cancellationToken = default) => webServerAgent.WriteHelloAsync(context, cancellationToken);
        public static Task WriteHelloAsync(this HttpContext context, IEnumerable<WebServerAgent> webServerAgents, CancellationToken cancellationToken = default) => WebServerAgent.WriteHelloAsync(context, webServerAgents, cancellationToken);


        /// <summary>
        /// Get Scope Name sent by the client
        /// </summary>
        public static string GetScopeName(this HttpContext httpContext) => WebServerAgent.TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-scope-name", out var val) ? val : null;

        /// <summary>
        /// Get the DMS version used by the Client
        /// </summary>
        public static string GetVersion(this HttpContext httpContext) => WebServerAgent.TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-version", out var val) ? val : null;

        /// <summary>
        /// Get Scope Name sent by the client
        /// </summary>
        public static Guid? GetClientScopeId(this HttpContext httpContext) => WebServerAgent.TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-scope-id", out var val) ? string.IsNullOrEmpty(val) ? null : new Guid(val) : null;

        /// <summary>
        /// Get the current client session id
        /// </summary>
        public static string GetClientSessionId(this HttpContext httpContext) => WebServerAgent.TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-session-id", out var val) ? val : null;

        /// <summary>
        /// Get the current Step
        /// </summary>
        public static HttpStep GetCurrentStep(this HttpContext httpContext) => WebServerAgent.TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-step", out var val) ? string.IsNullOrEmpty(val) ? HttpStep.None : (HttpStep)Convert.ToInt32(val) : HttpStep.None;

        /// <summary>
        /// Get the identifier that can be used in multi sync providers
        /// </summary>
        public static string GetIdentifier(this HttpContext httpContext) => WebServerAgent.TryGetHeaderValue(httpContext.Request.Headers, "dotmim-sync-identifier", out var val) ? val : null;


    }
}

