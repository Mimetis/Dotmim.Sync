using System;
using System.IO;
using Dotmim.Sync;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Server
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
            services.AddControllers();
            services.AddMemoryCache();

            services.AddSingleton(Configuration);

            var connectionString = Configuration.GetConnectionString("DefaultConnection");
            var syncSetup = new SyncSetup(new string[] { "Customer" });
            services.AddSingleton(syncSetup);
            var snapshotDirectoryName = "snapshots";
            var snapshotDirectory = Path.Combine(Environment.CurrentDirectory, snapshotDirectoryName);
            var syncOptions = new SyncOptions
            {
                SnapshotsDirectory = snapshotDirectory,
                BatchSize = 500
            };
            services.AddTransient(x => new SyncOptions
            {
                SnapshotsDirectory = snapshotDirectory,
                BatchSize = 500
            });
            var webServerOptions = new WebServerOptions();
            services.AddSyncServer<SqlSyncProvider>(connectionString, syncSetup, syncOptions, webServerOptions);
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
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
