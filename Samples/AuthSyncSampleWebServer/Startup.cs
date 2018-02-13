using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;
using Dotmim.Sync.SqlServer;

namespace AuthSyncSampleWebServer
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
            var connectionString = Configuration.GetSection("ConnectionStrings")["DefaultConnection"];
            services.AddSyncServer<SqlSyncProvider>(connectionString, configuration =>
            {
                var s = new string[] { "Employees" };
                configuration.Add(s);
                configuration.DownloadBatchSizeInKB = 1000;
            });

            // Use bearer schema for authentication
            services.AddAuthentication(sharedOptions =>
            {
                sharedOptions.DefaultScheme = JwtBearerDefaults.AuthenticationScheme;
                sharedOptions.DefaultAuthenticateScheme = JwtBearerDefaults.AuthenticationScheme;
                sharedOptions.DefaultChallengeScheme = JwtBearerDefaults.AuthenticationScheme;

            })
             .AddJwtBearer(options =>
             {
                 var azureAdOptions = Configuration.GetSection("AzureAdV2").Get<AzureAdOptions>();

                 options.Audience = azureAdOptions.ClientId;
                 options.Authority = azureAdOptions.Authority;

                 options.TokenValidationParameters = new TokenValidationParameters()
                 {
                     ValidateAudience = false,
                     ValidateIssuer = false,
                     ValidateIssuerSigningKey = false
                 };
             });

            services.AddMvc();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
                app.UseBrowserLink();
                app.UseDatabaseErrorPage();
            }
            else
            {
                app.UseExceptionHandler("/Home/Error");
            }

            app.UseStaticFiles();

            app.UseAuthentication();

            app.UseMvc(routes =>
            {
                routes.MapRoute(
                    name: "default",
                    template: "{controller=Home}/{action=Index}/{id?}");
            });
        }
    }


    public class AzureAdOptions
    {
        public string ClientId { get; set; }
        public string Authority { get; set; }
    }
}
