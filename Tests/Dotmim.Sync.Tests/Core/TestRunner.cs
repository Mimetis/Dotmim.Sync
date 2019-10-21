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

        public Action<IProvider> BeginRun { get; set; }
        public Action<IProvider> EndRun { get; set; }


        public TestRunner(ProviderFixture providerFixture, CoreProvider serverProvider)
        {
            this.providerFixture = providerFixture;
            this.serverProvider = serverProvider;
        }

        public async Task<List<ProviderRun>> RunTestsAsync()
        {
            return await RunTestsAsync(this.providerFixture.Tables, null, true);
        }
        public async Task<List<ProviderRun>> RunTestsAsync(bool reuseAgent)
        {
            return await RunTestsAsync(this.providerFixture.Tables, null, reuseAgent);
        }

        public async Task<List<ProviderRun>> RunTestsAsync(Action<SyncSchema> conf, bool reuseAgent = true)
        {
            return await RunTestsAsync(this.providerFixture.Tables, conf, reuseAgent);
        }

        public async Task<List<ProviderRun>> RunTestsAsync(string[] tables = null, Action<SyncSchema> conf = null,
                                                           bool reuseAgent = true)
        {
            foreach (var tra in this.providerFixture.ClientRuns)
            {
                try
                {
                    tra.BeginRun = this.BeginRun;
                    tra.EndRun = this.EndRun;

                    await tra.RunAsync(this.providerFixture, tables, conf, reuseAgent);
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
