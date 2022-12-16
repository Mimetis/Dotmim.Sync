using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.MySql;
using Dotmim.Sync.PostgreSql;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Extensions;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Controllers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace HelloWebSyncServer
{
    public class Startup
    {
        public Startup(IConfiguration configuration) => Configuration = configuration;

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddControllers();

            services.AddDistributedMemoryCache();
            services.AddSession(options => options.IdleTimeout = TimeSpan.FromMinutes(30));

            // [Required]: Get a connection string to your server data source
            //var connectionString = Configuration.GetSection("ConnectionStrings")["SqlConnection"];
            var connectionString = Configuration.GetSection("ConnectionStrings")["NpgsqlConnection"];
            //var connectionString = Configuration.GetSection("ConnectionStrings")["MySqlConnection"];

            var options = new SyncOptions
            {
                SnapshotsDirectory = "C:\\Tmp\\Snapshots",
                BatchSize = 2000,
            };

            // [Required] Tables involved in the sync process:
            //var tables = new string[] { "ProductCategory", "ProductModel", "Product", "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" };
            //var tables = new string[] { "humanresources.department", "humanresources.employeedepartmenthistory", "humanresources.employee", "humanresources.jobcandidate", "person.person", "person.address" };
            var tables = new string[] { "public.Items", "public.SaleInvoices", "public.SaleInvoiceItem" };

            // [Required]: Add a SqlSyncProvider acting as the server hub.
            //services.AddSyncServer<SqlSyncProvider>(connectionString, tables, options);
            services.AddSyncServer<NpgsqlSyncProvider>(connectionString, tables, options);
            //services.AddSyncServer<MySqlSyncProvider>(connectionString, tables, options);
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
