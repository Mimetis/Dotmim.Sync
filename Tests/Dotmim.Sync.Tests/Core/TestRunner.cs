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
        private readonly ProviderFixture<CoreProvider> providerFixture;

        public Action<IProvider> BeginRun { get; set; }
        public Action<IProvider> EndRun { get; set; }

        public TestRunner(ProviderFixture<CoreProvider> providerFixture)
        {
            this.providerFixture = providerFixture;
        }

        public async Task<List<ProviderRun>> RunTestsAsync()
        {
            return await RunTestsAsync(null, this.providerFixture.Tables, null, true);
        }
        public async Task<List<ProviderRun>> RunTestsAsync(bool reuseAgent)
        {
            return await RunTestsAsync(null, this.providerFixture.Tables, null, reuseAgent);
        }

        public async Task<List<ProviderRun>> RunTestsAsync(SyncConfiguration conf, bool reuseAgent = true)
        {
            return await RunTestsAsync(null, this.providerFixture.Tables, conf, reuseAgent);
        }

        public async Task<List<ProviderRun>> RunTestsAsync(string scopeName = null, string[] tables = null, SyncConfiguration conf = null,
                                                           bool reuseAgent = true)
        {
            foreach (var tra in this.providerFixture.ClientRuns)
            {
                try
                {
                    tra.BeginRun = this.BeginRun;
                    tra.EndRun = this.EndRun;

                    await tra.RunAsync(this.providerFixture, scopeName, tables, conf, reuseAgent);
                }
                finally
                {
                    tra.BeginRun = null;
                    tra.EndRun = null;
                }
            }

            return this.providerFixture.ClientRuns;
        }

        //public async Task<List<ProviderRun>> RunTestsAsync(string scopeName = null, string[] tables = null, SyncConfiguration conf = null,
        //bool reuseAgent = true)
        //{
        //    // server proxy
        //    var proxyServerProvider = new WebProxyServerProvider(this.providerFixture.ServerProvider);
        //    var proxyClientProvider = new WebProxyClientProvider();

        //    var syncTables = tables ?? this.providerFixture.Tables;

        //    // local test, through tcp
        //    foreach (var tra in this.providerFixture.ClientRuns.Where(cp => cp.IsHttp == false))
        //    {
        //        var localAgent = tra.Agent;

        //        // create agent
        //        if (localAgent == null || !reuseAgent)
        //            localAgent = new SyncAgent(tra.Provider, this.providerFixture.ServerProvider, syncTables);

        //        // cache agent for reuse on a second loop in the same test
        //        tra.Agent = localAgent;

        //        // copy conf settings
        //        if (conf != null)
        //            this.providerFixture.CopyConfiguration(localAgent.Configuration, conf);

        //        // sync
        //        try
        //        {
        //            // Delegate to the user if need to work on the agent
        //            InterceptAgent?.Invoke(this, localAgent);

        //            tra.Results = await localAgent.SynchronizeAsync();
        //        }
        //        catch (Exception ex)
        //        {
        //            tra.Exception = ex;

        //        }
        //    }

        //    // -----------------------------------------------------------------------
        //    // HTTP
        //    // -----------------------------------------------------------------------

        //    // tests through http proxy
        //    foreach (var tra in this.providerFixture.ClientRuns.Where(cp => cp.IsHttp == true))
        //    {
        //        var syncHttpTables = tables ?? this.providerFixture.Tables;

        //        // client handler
        //        using (var server = new KestrellTestServer())
        //        {
        //            // server handler
        //            var serverHandler = new RequestDelegate(async context =>
        //            {
        //                SyncConfiguration syncConfiguration = new SyncConfiguration(syncHttpTables);

        //                // copy conf settings
        //                if (conf != null)
        //                    this.providerFixture.CopyConfiguration(syncConfiguration, conf);

        //                // set proxy conf
        //                proxyServerProvider.Configuration = syncConfiguration;

        //                // sync
        //                try
        //                {
        //                    // delegate to user if need to work on proxy before handle request
        //                    InterceptProxyServer?.Invoke(this, proxyServerProvider);

        //                    await proxyServerProvider.HandleRequestAsync(context);

        //                }
        //                catch (Exception ew)
        //                {
        //                    Debug.WriteLine(ew);
        //                }
        //            });

        //            var clientHandler = new ResponseDelegate(async (serviceUri) =>
        //            {
        //                var proxyAgent = tra.Agent;

        //                // create agent
        //                if (proxyAgent == null || !reuseAgent)
        //                    proxyAgent = new SyncAgent(tra.Provider, proxyClientProvider);

        //                // cache agent for reuse on a second loop in the same test
        //                tra.Agent = proxyAgent;

        //                ((WebProxyClientProvider)tra.Agent.RemoteProvider).ServiceUri = new Uri(serviceUri);

        //                try
        //                {
        //                    // Delegate to the user if need to work on the agent
        //                    InterceptAgent?.Invoke(this, proxyAgent);

        //                    tra.Results = await proxyAgent.SynchronizeAsync();
        //                }
        //                catch (Exception ew)
        //                {
        //                    tra.Exception = ew;
        //                }
        //            });
        //            await server.Run(serverHandler, clientHandler);
        //        }

        //    }

        //    return this.providerFixture.ClientRuns;
        //}

    }

}
