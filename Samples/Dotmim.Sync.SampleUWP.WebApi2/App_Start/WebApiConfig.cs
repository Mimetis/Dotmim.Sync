using System.Configuration;
using System.Web.Http;
using System.Web.Http.Dispatcher;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web;
using Microsoft.Extensions.DependencyInjection;
using UWPSyncSampleWebServer.Controllers;

namespace UWPSyncSampleWebServer
{
    public static class WebApiConfig
    {
        public static void Register(HttpConfiguration config)
        {
            // initilaize dotmim.sync
            config.UseDotmimSync();

            // Web API routes
            config.MapHttpAttributeRoutes();

            config.Routes.MapHttpRoute(
                name: "DefaultApi",
                routeTemplate: "api/{controller}/{id}",
                defaults: new { id = RouteParameter.Optional }
            );
        }

        /// <summary>
        /// Integrates Web API 2 with Microsoft.Extensions.DependencyInjection
        /// see: https://gist.github.com/jt000/0b57f811807d119090f1184bb3460dee
        /// </summary>
        public static void UseDotmimSync(this HttpConfiguration config)
        {
            // setup DI for Dotmim.Sync....
            var services = new ServiceCollection();

            var connectionString = ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString;
            // register sql provider
            services.AddSyncServer<SqlSyncProvider>(connectionString, configuration =>
            {
                var s = new string[] { "Employees" };
                configuration.Add(s);
                configuration.DownloadBatchSizeInKB = 1000;
            });

            // register controller
            services.AddScoped<ValuesController>(sp => new ValuesController(sp.GetRequiredService<WebProxyServerProvider>()));

            // create the serviceprovider and replace the default implementation
            var provider = services.BuildServiceProvider();
            config.Services.Replace(typeof(IHttpControllerActivator), new ServiceProviderControllerActivator(provider));

        }
    }
}
