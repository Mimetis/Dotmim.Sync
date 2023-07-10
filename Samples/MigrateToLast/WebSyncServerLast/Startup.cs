using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace WebSyncServerLast
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

            var connectionString = Configuration.GetSection("ConnectionStrings")["SqlConnection"];

            var options = new SyncOptions
            {
                BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), "server")
            };

            // Create the setup used for your sync process
            var tables = new string[] {"ProductCategory",
                "ProductModel", "Product",
                "Address", "Customer", "CustomerAddress",
                "SalesOrderHeader", "SalesOrderDetail" };

            var setup = new SyncSetup(tables)
            {
                // optional :
                StoredProceduresPrefix = "s",
                StoredProceduresSuffix = "",
                TrackingTablesPrefix = "s",
                TrackingTablesSuffix = ""
            };

            // Create a filter on table Address on City Washington
            // Optional : Sub filter on PostalCode, for testing purpose
            var addressFilter = new SetupFilter("Address");
            addressFilter.AddParameter("City", "Address", true);
            addressFilter.AddParameter("postal", DbType.String, true, null, 20);
            addressFilter.AddWhere("City", "Address", "City");
            addressFilter.AddWhere("PostalCode", "Address", "postal");
            setup.Filters.Add(addressFilter);

            var addressCustomerFilter = new SetupFilter("CustomerAddress");
            addressCustomerFilter.AddParameter("City", "Address", true);
            addressCustomerFilter.AddParameter("postal", DbType.String, true, null, 20);
            addressCustomerFilter.AddJoin(Join.Left, "Address")
                .On("CustomerAddress", "AddressId", "Address", "AddressId");
            addressCustomerFilter.AddWhere("City", "Address", "City");
            addressCustomerFilter.AddWhere("PostalCode", "Address", "postal");
            setup.Filters.Add(addressCustomerFilter);

            var customerFilter = new SetupFilter("Customer");
            customerFilter.AddParameter("City", "Address", true);
            customerFilter.AddParameter("postal", DbType.String, true, null, 20);
            customerFilter.AddJoin(Join.Left, "CustomerAddress")
                .On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            customerFilter.AddJoin(Join.Left, "Address")
                .On("CustomerAddress", "AddressId", "Address", "AddressId");
            customerFilter.AddWhere("City", "Address", "City");
            customerFilter.AddWhere("PostalCode", "Address", "postal");
            setup.Filters.Add(customerFilter);

            var orderHeaderFilter = new SetupFilter("SalesOrderHeader");
            orderHeaderFilter.AddParameter("City", "Address", true);
            orderHeaderFilter.AddParameter("postal", DbType.String, true, null, 20);
            orderHeaderFilter.AddJoin(Join.Left, "CustomerAddress")
                .On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
            orderHeaderFilter.AddJoin(Join.Left, "Address")
                .On("CustomerAddress", "AddressId", "Address", "AddressId");
            orderHeaderFilter.AddWhere("City", "Address", "City");
            orderHeaderFilter.AddWhere("PostalCode", "Address", "postal");
            setup.Filters.Add(orderHeaderFilter);

            var orderDetailsFilter = new SetupFilter("SalesOrderDetail");
            orderDetailsFilter.AddParameter("City", "Address", true);
            orderDetailsFilter.AddParameter("postal", DbType.String, true, null, 20);
            orderDetailsFilter.AddJoin(Join.Left, "SalesOrderHeader")
                .On("SalesOrderHeader", "SalesOrderID", "SalesOrderDetail", "SalesOrderID");
            orderDetailsFilter.AddJoin(Join.Left, "CustomerAddress")
                .On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
            orderDetailsFilter.AddJoin(Join.Left, "Address")
                .On("CustomerAddress", "AddressId", "Address", "AddressId");
            orderDetailsFilter.AddWhere("City", "Address", "City");
            orderDetailsFilter.AddWhere("PostalCode", "Address", "postal");
            setup.Filters.Add(orderDetailsFilter);

            // add a SqlSyncProvider with filters
            services.AddSyncServer<SqlSyncProvider>(connectionString, setup, options);

            // add a SqlSyncProvider with another scope
            services.AddSyncServer<SqlSyncProvider>(connectionString,
                new string[] { "BuildVersion", "ErrorLog" }, options, null, "logs");
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
