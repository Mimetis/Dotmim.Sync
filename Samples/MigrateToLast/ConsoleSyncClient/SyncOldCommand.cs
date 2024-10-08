﻿using Dotmim.Sync;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using System;
using System.Threading.Tasks;

namespace WebSyncClient
{

    [Command("old", Description = "v0.94: This command will execute a simple sync to the server version 0.94")]
    public class SyncOldCommand
    {

        public SyncOldCommand(IConfiguration configuration, IOptions<ApiOptions> apiOptions)
        {
            this.ApiOptions = apiOptions.Value;
            this.Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        public ApiOptions ApiOptions { get; }

        [Option("-s")]
        public ScopeNames Scope { get; } = ScopeNames.DefaultScope;

        [Option("-r")]
        public bool Reinitialize { get; set; } = false;

        public async Task<int> OnExecuteAsync(CommandLineApplication app)
        {
            var serverOrchestrator = new WebClientOrchestrator(this.ApiOptions.SyncAddressOld);

            // Second provider is using plain old Sql Server provider, relying on triggers and tracking tables to create the sync environment
            var connectionString = this.Configuration.GetConnectionString(ProviderType.Sql, "Client");

            var clientProvider = new SqlSyncProvider(connectionString);

            try
            {
                if (this.Scope == ScopeNames.Logs)
                    await SyncServices.SynchronizeLogsAsync(serverOrchestrator, clientProvider, new SyncOptions(), this.Reinitialize).ConfigureAwait(false);
                else
                    await SyncServices.SynchronizeDefaultAsync(serverOrchestrator, clientProvider, new SyncOptions(), this.Reinitialize).ConfigureAwait(false);
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