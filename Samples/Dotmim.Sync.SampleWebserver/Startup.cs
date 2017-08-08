using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Dotmim.Sync.SqlServer;

namespace Dotmim.Sync.SampleWebserver
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
            services.AddDistributedMemoryCache();
            services.AddSession(options =>
            {
                // Set a long timeout for easy testing.
                options.IdleTimeout = TimeSpan.FromDays(10);
                options.CookieHttpOnly = true;
            });
            services.Configure<Data>(Configuration.GetSection("Data"));

            var connectionString = Configuration.GetSection("Data").Get<Data>().ConnectionString;

            services.AddSyncServer<SqlSyncProvider>(connectionString, configuration =>
            {
                configuration.Tables = new string[] { "ServiceTickets" };
                
            });
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, ILoggerFactory loggerFactory)
        {
            app.UseSession();
            app.UseMvc();
            
            
        }
    }
}
