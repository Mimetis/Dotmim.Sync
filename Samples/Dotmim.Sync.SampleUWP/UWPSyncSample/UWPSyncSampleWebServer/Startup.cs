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
using Microsoft.Extensions.Hosting;
using Microsoft.EntityFrameworkCore;

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
            var connectionString = Configuration.GetConnectionString("AdventureWorksConnection");

            services.AddSingleton<ContosoContext>();

            // Sync options
            var syncOptions = new SyncOptions
            {
                SnapshotsDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "Snapshots"),
                BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "Server"),
                BatchSize = 10000,
            };

            // Create the setup used for your sync process
            //var tables = new string[] { "Employees" };

            var tables = new string[] {"ProductDescription", "ProductCategory",
                                    "ProductModel", "Product",
                                    "Address", "Customer", "CustomerAddress",
                                    "SalesOrderHeader", "SalesOrderDetail" };

            var setup = new SyncSetup(tables);

            // add a SqlSyncProvider acting as the server hub
            services.AddSyncServer<SqlSyncProvider>(connectionString, setup, syncOptions);

        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseHttpsRedirection();

            app.UseRouting();

            app.UseAuthorization();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });
        }
    }
}
