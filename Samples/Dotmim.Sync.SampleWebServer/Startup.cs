using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Hosting;

namespace Dotmim.Sync.SampleWebServer
{
    public class Startup
    {
        public Startup(IConfiguration configuration) => this.Configuration = configuration;

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddControllers();
            // Mandatory to be able to handle multiple sessions
            services.AddMemoryCache();

            // Get a connection string for your server data source
            var connectionString = Configuration.GetSection("ConnectionStrings")["DefaultConnection"];

            var tables = new string[] { "Customer" };

            var syncSetup = new SyncSetup(tables);
            //syncSetup.Tables["Customer"].SyncDirection = SyncDirection.UploadOnly;
            syncSetup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerId", "FirstName", "LastName" });

            services.AddSyncServer<SqlSyncChangeTrackingProvider>(connectionString, syncSetup);


            // Example1(services);

        }

        private void Example1(IServiceCollection services)
        {
            // Get a connection string for your server data source
            var connectionString = Configuration.GetSection("ConnectionStrings")["DefaultConnection"];

            // Set the web server Options
            var options = new SyncOptions()
            {
                BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "server"),
                SnapshotsDirectory = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "Snapshots")
            };


            // Create the setup used for your sync process
            var tables = new string[] {"ProductCategory",
                    "ProductDescription", "ProductModel",
                    "Product", "ProductModelProductDescription",
                    "Address", "Customer", "CustomerAddress",
                    "SalesOrderHeader", "SalesOrderDetail" };

            var setup = new SyncSetup(tables)
            {
                // optional :
                StoredProceduresPrefix = "server",
                StoredProceduresSuffix = "",
                TrackingTablesPrefix = "server",
                TrackingTablesSuffix = ""
            };

            setup.Filters.Add("Customer", "CompanyName");

            var addressCustomerFilter = new SetupFilter("CustomerAddress");
            addressCustomerFilter.AddParameter("CompanyName", "Customer");
            addressCustomerFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            addressCustomerFilter.AddWhere("CompanyName", "Customer", "CompanyName");
            setup.Filters.Add(addressCustomerFilter);

            var addressFilter = new SetupFilter("Address");
            addressFilter.AddParameter("CompanyName", "Customer");
            addressFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "AddressId", "Address", "AddressId");
            addressFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            addressFilter.AddWhere("CompanyName", "Customer", "CompanyName");
            setup.Filters.Add(addressFilter);

            var orderHeaderFilter = new SetupFilter("SalesOrderHeader");
            orderHeaderFilter.AddParameter("CompanyName", "Customer");
            orderHeaderFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
            orderHeaderFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            orderHeaderFilter.AddWhere("CompanyName", "Customer", "CompanyName");
            setup.Filters.Add(orderHeaderFilter);

            var orderDetailsFilter = new SetupFilter("SalesOrderDetail");
            orderDetailsFilter.AddParameter("CompanyName", "Customer");
            orderDetailsFilter.AddJoin(Join.Left, "SalesOrderHeader").On("SalesOrderDetail", "SalesOrderID", "SalesOrderHeader", "SalesOrderID");
            orderDetailsFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
            orderDetailsFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            orderDetailsFilter.AddWhere("CompanyName", "Customer", "CompanyName");
            setup.Filters.Add(orderDetailsFilter);

            // add a SqlSyncProvider acting as the server hub
            services.AddSyncServer<SqlSyncProvider>(connectionString, setup, options);

            // Create the setup used for your sync process
            tables = new string[] { "ProductCategory" };

            setup = new SyncSetup(tables)
            {
                // optional :
                StoredProceduresPrefix = "server",
                StoredProceduresSuffix = "",
                TrackingTablesPrefix = "server",
                TrackingTablesSuffix = ""
            };

            // add a SqlSyncProvider acting as the server hub
            services.AddSyncServer<SqlSyncProvider>(connectionString, "ProdScope", setup, options);
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
