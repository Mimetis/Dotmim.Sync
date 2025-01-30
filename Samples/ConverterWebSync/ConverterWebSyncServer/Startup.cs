using ConverterWebSyncServer.Converters;
using ConverterWebSyncServer.Serializer;
using Dotmim.Sync;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;

namespace ConverterWebSyncServer
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            this.Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddControllers();

            services.AddDistributedMemoryCache();
            services.AddSession(options => options.IdleTimeout = TimeSpan.FromMinutes(30));

            // [Required]: Get a connection string to your server data source
            var connectionString = this.Configuration.GetSection("ConnectionStrings")["SqlConnection"];

            // [Required] Tables involved in the sync process:
            var tables = new string[]
            {
                "ProductCategory", "ProductModel", "Product",
                "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail",
            };

            // To add a converter, create an instance and add it to the special WebServerOptions
            var webServerOptions = new WebServerOptions();
            webServerOptions.Converters.Add(new CustomConverter());
            webServerOptions.SerializerFactories.Add(new CustomMessagePackSerializerFactory());

            var options = new SyncOptions { };

            // [Required]: Add a SqlSyncProvider acting as the server hub.
            var provider = new SqlSyncChangeTrackingProvider(connectionString);
            services.AddSyncServer(provider, tables, options, webServerOptions);
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

            app.UseSession();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });
        }
    }
}