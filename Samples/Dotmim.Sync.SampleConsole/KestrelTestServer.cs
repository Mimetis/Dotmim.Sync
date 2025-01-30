using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Session;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.SampleConsole
{
    public delegate Task ResponseDelegate(string serviceUri);

    /// <summary>
    /// This is a test server for Kestrel
    /// Actually we can use Microsoft.AspNetCore.TestHost
    /// But I can't manage to find a way to perform through Fiddler
    /// </summary>
    public class KestrelTestServer : IDisposable
    {
        IWebHostBuilder builder;
        IWebHost host;
        bool useFiddler;

        public KestrelTestServer(Action<IServiceCollection> configureServices = null, bool useFidller = false)
        {
            var hostBuilder = new WebHostBuilder()
                .UseKestrel()
                .UseUrls("http://127.0.0.1:0/")
                .ConfigureServices(services =>
                {
                    services.AddMemoryCache();
                    services.AddDistributedMemoryCache();
                    services.AddSession(options =>
                    {
                        // Set a long timeout for easy testing.
                        options.IdleTimeout = TimeSpan.FromDays(10);
                        options.Cookie.HttpOnly = true;
                    });

                    configureServices?.Invoke(services);

                });
            this.builder = hostBuilder;

            this.useFiddler = useFidller;
        }

        public async Task Run(RequestDelegate serverHandler, ResponseDelegate clientHandler)
        {
            this.builder.Configure(app =>
            {
                app.UseSession();
                app.Run(async context => await serverHandler(context));

            });

            var fiddler = useFiddler ? ".fiddler" : "";
            this.host = this.builder.Build();
            this.host.Start();
            string serviceUrl = $"http://localhost{fiddler}:{this.host.GetPort()}/";
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
        public static string GetHost(this IWebHost host, bool isHttps = false)
        {
            return host.GetUri(isHttps).Host;
        }

        public static int GetPort(this IWebHost host)
        {
            return host.GetPorts().First();
        }

        public static int GetPort(this IWebHost host, string scheme)
        {
            return host.GetUris()
                .Where(u => u.Scheme.Equals(scheme, StringComparison.OrdinalIgnoreCase))
                .Select(u => u.Port)
                .First();
        }

        public static IEnumerable<int> GetPorts(this IWebHost host)
        {
            return host.GetUris()
                .Select(u => u.Port);
        }

        public static IEnumerable<Uri> GetUris(this IWebHost host)
        {
            return host.ServerFeatures.Get<IServerAddressesFeature>().Addresses
                .Select(a => new Uri(a));
        }

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
