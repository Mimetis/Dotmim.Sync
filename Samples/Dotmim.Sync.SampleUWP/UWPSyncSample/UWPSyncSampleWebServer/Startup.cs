using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dotmim.Sync.SqlServer;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

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

            // For UWP Sample
            // Make Sure this database is created on you sqlexpress instance
            var connectionString = @"Data Source=localhost\mssqlexpress; Initial Catalog=AdventureWorks; Integrated Security=true;";
            services.AddSyncServer<SqlSyncProvider>(connectionString, configuration =>
            {
                var s = new string[] { "Customer" };
                configuration.Add(s);
                configuration.DownloadBatchSizeInKB = 1000;
            });


            // For console sample app
            // make sure the Northwind database is up and running on your sqlexpress instance
            //var connectionString = @"Data Source=localhost\sqlexpress; Initial Catalog=Northwind; Integrated Security=true;";
            //services.AddSyncServer<SqlSyncProvider>(connectionString, configuration =>
            //{
            //    var s = new string[] { "Customers", "Region" };
            //    configuration.Add(s);
            //    configuration.DownloadBatchSizeInKB = 1000;
            //});
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
