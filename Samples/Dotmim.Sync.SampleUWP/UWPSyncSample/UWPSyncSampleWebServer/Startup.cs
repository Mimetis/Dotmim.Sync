using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Dotmim.Sync;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using UWPSyncSampleWebServer.Context;

namespace UWPSyncSampleWebServer
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddMvc();

            // Mandatory to be able to handle multiple sessions
            services.AddMemoryCache();

            // Get a connection string for your server data source
            var connectionString = Configuration.GetSection("ConnectionStrings")["DefaultConnection"];

            // Set the web server Options
            var options = new WebServerOptions()
            {
                BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "server"),
            };

            // Create the setup used for your sync process
            var tables = new string[] {"Employee" };

            var setup = new SyncSetup(tables);

            // add a SqlSyncProvider acting as the server hub
            services.AddSyncServer<SqlSyncProvider>(connectionString, setup, options);

        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseMvc();
        }
    }
}
