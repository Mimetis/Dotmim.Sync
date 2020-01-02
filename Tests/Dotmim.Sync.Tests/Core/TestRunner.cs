using Dotmim.Sync.MySql;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Test;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Tests.Core
{
    public class TestRunner
    {
        private readonly ProviderFixture providerFixture;
        private readonly CoreProvider serverProvider;

        public Action<IRemoteOrchestrator> BeginRun { get; set; }
        public Action<IRemoteOrchestrator> EndRun { get; set; }


        public TestRunner(ProviderFixture providerFixture, CoreProvider serverProvider)
        {
            this.providerFixture = providerFixture;
            this.serverProvider = serverProvider;
        }

        public async Task<List<ProviderRun>> RunTestsAsync(SyncOptions options = null, bool reuseAgent = true)
        {
            return await RunTestsAsync(this.providerFixture.Tables, null, options, reuseAgent);
        }

        public async Task<List<ProviderRun>> RunTestsAsync(string[] tables, SyncSet schema = null,
            SyncOptions options = null, bool reuseAgent = true)
        {
            foreach (var tra in this.providerFixture.ClientRuns)
            {
                try
                {
                    tra.BeginRun = this.BeginRun;
                    tra.EndRun = this.EndRun;

                    await tra.RunAsync(this.providerFixture, tables, options, reuseAgent) ;
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }
                finally
                {
                    tra.BeginRun = null;
                    tra.EndRun = null;
                }
            }

            return this.providerFixture.ClientRuns;
        }
    }

}
