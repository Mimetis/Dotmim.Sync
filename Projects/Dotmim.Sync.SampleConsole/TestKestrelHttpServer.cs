using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Session;
using System.Linq;
using System.Net;
using System.Threading;
using Microsoft.AspNetCore.Hosting.Server.Features;
using System.Diagnostics;

namespace Dotmim.Sync.SampleConsole
{
    public delegate Task ResponseDelegate(string serviceUri);
    public static class TestKestrelHttpServer
    {
        public async static Task LaunchKestrellAsync(RequestDelegate serverHandler, ResponseDelegate clientHandler)
        {
            var hostBuilder = new WebHostBuilder()
            .UseKestrel()
            .UseUrls("http://127.0.0.1:0/")
            .Configure(app =>
                {
                    app.UseSession();
                    app.Run(async context => await serverHandler(context));
                }
            )
            .ConfigureServices(services => {
                services.AddDistributedMemoryCache();
                services.AddSession(options =>
                {
                    // Set a long timeout for easy testing.
                    options.IdleTimeout = TimeSpan.FromDays(10);
                    options.CookieHttpOnly = true;
                }); ;
            });

            using (var host = hostBuilder.Build())
            {
                host.Start();
                string serviceUrl = $"http://localhost.fiddler:{host.GetPort()}/";
                await clientHandler(serviceUrl);

            }
        }
    }

    //public async static Task TestKestrell()
    //{

    //    var headerName = "Header-Value";
    //    var headerValue = "1";

    //    var hostBuilder = new WebHostBuilder()
    //        .UseKestrel()
    //        .UseUrls("http://127.0.0.1:0/")
    //        .Configure(app =>
    //        {
    //            app.Run(async context =>
    //            {
    //                context.Response.Headers.Add(headerName, headerValue);

    //                await context.Response.WriteAsync("");
    //            });
    //        });

    //    using (var host = hostBuilder.Build())
    //    {
    //        host.Start();

    //        using (var client = new HttpClient())
    //        {
    //            var response = await client.GetAsync($"http://localhost:{host.GetPort()}/");
    //            response.EnsureSuccessStatusCode();

    //            var headers = response.Headers;

    //            if (headers.Contains(headerName))
    //                Debug.WriteLine($"Containing Header {headerName} : {headers.GetValues(headerName).Single()}");


    //        }
    //    }
    //}

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
