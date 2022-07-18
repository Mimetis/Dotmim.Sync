using Dotmim.Sync;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using McMaster.Extensions.CommandLineUtils;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WebSyncClient
{
 
    [Command("Last", Description = "v0.95: This command will execute a sync to the server with version 0.95")]
    public class SyncLastCommand
    {
        public SyncLastCommand(IConfiguration configuration, IOptions<ApiOptions> apiOptions)
        {
            ApiOptions = apiOptions.Value;
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }
        public ApiOptions ApiOptions { get; }

        [Option("-s")]
        public ScopeNames Scope { get; set; } = ScopeNames.DefaultScope;

        [Option("-r")]
        public bool Reinitialize { get; set; } = false;


        public async Task<int> OnExecuteAsync(CommandLineApplication app)
        {
            var serverOrchestrator = new WebClientOrchestrator(ApiOptions.SyncAddressLast);

            // Second provider is using plain old Sql Server provider, relying on triggers and tracking tables to create the sync environment
            var connectionString = Configuration.GetConnectionString(ProviderType.Sql, "Client");

            var clientProvider = new SqlSyncProvider(connectionString);

            try
            {
                if (Scope == ScopeNames.Logs)
                    await SyncServices.SynchronizeLogsAsync(serverOrchestrator, clientProvider, new SyncOptions(), Reinitialize);
                else
                    await SyncServices.SynchronizeDefaultAsync(serverOrchestrator, clientProvider, new SyncOptions(), Reinitialize);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return 0;
            }
            return 1;
        }


    

    }
}
