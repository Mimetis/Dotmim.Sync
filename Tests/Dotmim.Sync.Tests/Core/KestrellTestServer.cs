using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dotmim.Sync.MySql;

namespace Dotmim.Sync.Tests.Core
{
    public delegate Task ResponseDelegate(string serviceUri);

    /// <summary>
    /// This is a test server for Kestrell
    /// Actually we can use Microsoft.AspNetCore.TestHost
    /// But I can't manage to find a way to perform through Fiddler
    /// </summary>
    public class KestrellTestServer : IDisposable
    {
        private readonly IWebHostBuilder builder;
        private readonly bool useFiddler = false;
        private IWebHost host;

        public KestrellTestServer(bool useFiddler = false)
        {
            var hostBuilder = new WebHostBuilder()
                .UseKestrel()
                .UseUrls("http://127.0.0.1:0/")
                .ConfigureServices(services =>
                {
                    services.AddMemoryCache();
                    services.AddSession(options =>
                    {
                        // Set a long timeout for easy testing.
                        options.IdleTimeout = TimeSpan.FromDays(10);
                        options.Cookie.HttpOnly = true;
                    });
                }) ;
            this.useFiddler = useFiddler;
            this.builder = hostBuilder;

        }

        //public KestrellTestServer(CoreProvider coreProvider, Action<SyncConfiguration> action, bool registerAsSingleton, bool useFiddler = false)
        //{
        //    var hostBuilder = new WebHostBuilder()
        //        .UseKestrel()
        //        .UseUrls("http://127.0.0.1:0/")
        //        .ConfigureServices(services =>
        //        {
        //            services.AddDistributedMemoryCache();
        //            services.AddSession(options =>
        //            {
        //                // Set a long timeout for easy testing.
        //                options.IdleTimeout = TimeSpan.FromDays(10);
        //                options.Cookie.HttpOnly = true;
        //            });


        //            // call AddSyncServer method
        //            var addSyncServerMethod = typeof(DependencyInjection)
        //                .GetMethod(nameof(DependencyInjection.AddSyncServer), new[] { services.GetType(), typeof(string), typeof(Action<SyncConfiguration>), typeof(bool) })
        //                .MakeGenericMethod(coreProvider.GetType());
        //            addSyncServerMethod.Invoke(this, new object [] { services, coreProvider.ConnectionString, action, registerAsSingleton });
        //        });
        //    this.useFiddler = useFiddler;
        //    this.builder = hostBuilder;
        //}

        public async Task Run(RequestDelegate serverHandler, ResponseDelegate clientHandler)
        {
            this.builder.Configure(app =>
            {
                app.Run(async context => await serverHandler(context));
            });

            this.host = this.builder.Build();
            this.host.Start();
            var localHost = $"http://localhost";

            if (this.useFiddler)
                localHost = $"{localHost}.fiddler";

            var serviceUrl = $"{localHost}:{this.host.GetPort()}/";

            await clientHandler(serviceUrl);
        }

        public async void Dispose()
        {
            await this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual async Task Dispose(bool cleanup)
        {
            if (this.host != null)
            {
                await this.host.StopAsync();
                this.host.Dispose();
            }
        }
    }
    public static class IWebHostPortExtensions
    {
        public static string GetHost(this IWebHost host, bool isHttps = false) => host.GetUri(isHttps).Host;

        public static int GetPort(this IWebHost host) => host.GetPorts().First();

        public static int GetPort(this IWebHost host, string scheme) => host.GetUris()
                .Where(u => u.Scheme.Equals(scheme, StringComparison.OrdinalIgnoreCase))
                .Select(u => u.Port)
                .First();

        public static IEnumerable<int> GetPorts(this IWebHost host) => host.GetUris()
                .Select(u => u.Port);

        public static IEnumerable<Uri> GetUris(this IWebHost host) => host.ServerFeatures.Get<IServerAddressesFeature>().Addresses
                .Select(a => new Uri(a));

        public static Uri GetUri(this IWebHost host, bool isHttps = false)
        {
            var uri = host.GetUris().First();

            if (isHttps && uri.Scheme == "http")
            {
                var uriBuilder = new UriBuilder(uri)
                {
                    Scheme = "https",
                };

                if (uri.Port == 80)
                {
                    uriBuilder.Port = 443;
                }

                return uriBuilder.Uri;
            }

            return uri;
        }
    }
}
