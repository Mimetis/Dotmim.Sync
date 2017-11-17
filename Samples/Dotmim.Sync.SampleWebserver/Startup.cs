using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Dotmim.Sync.SqlServer;

namespace Dotmim.Sync.SampleWebserver
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

            var connectionString = Configuration["Data:ConnectionString"];
            services.AddSyncServer<SqlSyncProvider>(connectionString, configuration =>
            {
                var s = new string[] { "C4File" };

                //var s = new string[] {
                //        "Analysis", "Event", "FileData", "HCategory", "PermissionPolicyUser",
                //        "Resource", "XPObjectType", "XpoStateMachine", "C4File", "PermissionPolicyRole",
                //        "ReportDataV2", "ResourceResources_EventEvents", "XpoState",
                //        "PermissionPolicyNavigationPermissionsObject", "PermissionPolicyTypePermissionsObject",
                //        "PermissionPolicyUserUsers_PermissionPolicyRoleRoles","XpoStateAppearance", "XpoTransition",
                //        "PermissionPolicyMemberPermissionsObject", "PermissionPolicyObjectPermissionsObject"};

                configuration.Add(s);
            });
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, ILoggerFactory loggerFactory)
        {
            app.UseMvc();


        }
    }
}
