using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Session;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.VisualStudio.TestPlatform.CommunicationUtilities;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Tests
{
    public delegate Task ResponseDelegate(string serviceUri);

    /// <summary>
    /// This is a test server for Kestrell
    /// Actually we can use Microsoft.AspNetCore.TestHost
    /// But I can't manage to find a way to perform through Fiddler
    /// </summary>
    public class KestrellTestServer : IDisposable
    {
        IWebHostBuilder builder;
        private bool useFiddler;
        IWebHost host;

        public KestrellTestServer(bool useFidller = false)
        {
            initBuilder();
            this.useFiddler = useFidller;
        }

        private void initBuilder()
        {
            var hostBuilder = new WebHostBuilder()
            .UseKestrel()
            .UseUrls("http://127.0.0.1:0/")
            .ConfigureServices(services =>
            {
                services.AddDistributedMemoryCache();
                services.AddSession(options =>
                {
                    // Set a long timeout for easy testing.
                    options.IdleTimeout = TimeSpan.FromDays(10);
                    options.Cookie.HttpOnly = true;
                });
            });
            this.builder = hostBuilder;
        }

        public void AddSyncServer(Type providerType, string connectionString, SyncSetup setup = null, SyncOptions options = null,
            WebServerOptions webServerOptions = null, string scopeName = null, string identifier = null)
        {
            this.builder.ConfigureServices(services =>
            {
                services.AddSyncServer(providerType, connectionString, setup, options, webServerOptions, scopeName, identifier);
            });
        }


        public string Run(RequestDelegate serverHandler = null)
        {
            // Create server web proxy
            serverHandler ??= new RequestDelegate(async context =>
                {
                    var allWebServerAgents = context.RequestServices.GetService(typeof(IEnumerable<WebServerAgent>)) as IEnumerable<WebServerAgent>;
                    var identifier = context.GetIdentifier();
                    var scopeName = context.GetScopeName();

                    IEnumerable<WebServerAgent> webServerAgents = null;

                    if (string.IsNullOrEmpty(identifier))
                        webServerAgents = allWebServerAgents.Where(wsa => string.IsNullOrEmpty(wsa.Identifier));
                    else
                        webServerAgents = allWebServerAgents.Where(wsa => wsa.Identifier == identifier);

                    var webServerAgent = webServerAgents.FirstOrDefault(wsa => wsa.ScopeName == scopeName);

                    if (webServerAgent == null)
                    {
                        context.Response.StatusCode = StatusCodes.Status400BadRequest;
                        await HttpResponseWritingExtensions.WriteAsync(context.Response, $"There is no web server agent configured for this scope name {scopeName} and identifier {identifier}.");
                    }
                    else
                    {
                        await webServerAgent.HandleRequestAsync(context);
                    }
                });

            this.builder.Configure(app =>
            {
                app.UseSession();
                app.Run(async context => await serverHandler(context));
            });

            var fiddler = useFiddler ? ".fiddler" : "";

            this.host = this.builder.Build();
            this.host.Start();
            string serviceUrl = $"http://localhost{fiddler}:{this.host.GetPort()}/";

            return serviceUrl;
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
                this.host = null;
                this.builder = null;
            }
        }

        public async Task StopAsync()
        {
            await this.host.StopAsync();
            this.host.Dispose();
            this.host = null;
            this.builder = null;
            this.initBuilder();
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
