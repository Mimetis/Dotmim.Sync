using Dotmim.Sync;
using Dotmim.Sync.SqlServer;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;

namespace HelloWebSyncServer
{
    public class Startup
    {
        public Startup(IConfiguration configuration) => this.Configuration = configuration;

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddControllers();

            services.AddDistributedMemoryCache();
            services.AddSession(options => options.IdleTimeout = TimeSpan.FromMinutes(30));

            // [Required]: Get a connection string to your server data source
            var connectionString = this.Configuration.GetSection("ConnectionStrings")["SqlConnection"];

            // var connectionString = Configuration.GetSection("ConnectionStrings")["MySqlConnection"];
            var options = new SyncOptions
            {
                SnapshotsDirectory = "C:\\Tmp\\Snapshots",
                BatchSize = 2000,
            };

            // [Required] Tables involved in the sync process:
            var tables = new string[]
            {
                "ProductCategory", "ProductModel", "Product",
                "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail",
            };

            // [Required]: Add a SqlSyncProvider acting as the server hub.
            var provider = new SqlSyncProvider(connectionString);
            services.AddSyncServer(provider, tables, options);
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
                app.UseDeveloperExceptionPage();

            app.UseHttpsRedirection();
            app.UseRouting();
            app.UseAuthorization();
            app.UseSession();
            app.UseEndpoints(endpoints => endpoints.MapControllers());
        }
    }
}