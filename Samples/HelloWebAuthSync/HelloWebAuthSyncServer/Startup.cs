using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IdentityModel.Tokens.Jwt;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dotmim.Sync;
using Dotmim.Sync.MySql;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Tokens;

namespace HelloWebSyncServer
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

            services.AddDistributedMemoryCache();
            services.AddSession(options => options.IdleTimeout = TimeSpan.FromMinutes(30));

            // Adding a default authentication system
            JwtSecurityTokenHandler.DefaultInboundClaimTypeMap.Clear(); // => remove default claims

            services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
                    .AddJwtBearer(options =>
                    {
                        options.RequireHttpsMetadata = false;
                        options.SaveToken = true;
                        options.TokenValidationParameters = new TokenValidationParameters
                        {
                            ValidIssuer = "Dotmim.Sync.Bearer",
                            ValidAudience = "Dotmim.Sync.Bearer",
                            IssuerSigningKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes("SOME_RANDOM_KEY_DO_NOT_SHARE_YOUR_KEY"))
                        };

                        options.Events = new JwtBearerEvents
                        {
                            OnAuthenticationFailed = context =>
                            {
                                Debug.WriteLine("OnAuthenticationFailed: " + context.Exception.Message);
                                return Task.CompletedTask;
                            },
                            OnTokenValidated = context =>
                            {
                                Debug.WriteLine("OnTokenValidated: " + context.SecurityToken);
                                return Task.CompletedTask;
                            }
                        };
                    });

            // [Required]: Get a connection string to your server data source
            var connectionString = Configuration.GetSection("ConnectionStrings")["SqlConnection"];

            // [Required] Tables involved in the sync process:
            var setup = new SyncSetup("ProductCategory", "Product");

            var pcFilter = new SetupFilter("ProductCategory");
            pcFilter.AddParameter("IsActive", "ProductCategory", true);
            pcFilter.AddParameter("ProductCategoryID", "ProductCategory", true);
            pcFilter.AddWhere("IsActive", "ProductCategory", "IsActive");
            pcFilter.AddWhere("ProductCategoryID", "ProductCategory", "ProductCategoryID");
            setup.Filters.Add(pcFilter);

            setup.Filters.Add("Product", "ProductCategoryID");

            var provider = new SqlSyncProvider(connectionString);
            services.AddSyncServer(provider, setup);
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

            app.UseAuthentication();
            app.UseAuthorization();
            app.UseSession();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });
        }
    }
}
