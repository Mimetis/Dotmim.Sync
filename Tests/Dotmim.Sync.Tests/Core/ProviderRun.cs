using Dotmim.Sync.MySql;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Tests.Core
{
    public class ProviderRun
    {
        /// <summary>
        /// Gets or Sets the database name used to generate correct connection string
        /// </summary>
        public String DatabaseName { get; set; }

        /// <summary>
        /// Get or Sets Connection on inner Provider.
        /// </summary>
        public String ConnectionString
        {
            get
            {
                switch (ProviderType)
                {
                    case ProviderType.Sql:
                        return ((SqlSyncProvider)Provider)?.ConnectionString;
                    case ProviderType.MySql:
                        return ((MySqlSyncProvider)Provider)?.ConnectionString;
                    case ProviderType.Sqlite:
                        return ((SqliteSyncProvider)Provider)?.ConnectionString;
                }

                return null;
            }
            set
            {
                switch (ProviderType)
                {
                    case ProviderType.Sql:
                        ((SqlSyncProvider)Provider).ConnectionString = value;
                        break;
                    case ProviderType.MySql:
                        ((MySqlSyncProvider)Provider).ConnectionString = value;
                        break;
                    case ProviderType.Sqlite:
                        ((SqliteSyncProvider)Provider).ConnectionString = value;
                        break;
                }
            }
        }
        public ProviderType ProviderType { get; set; }
        public IProvider Provider { get; set; }
        public Boolean IsHttp { get; set; }
        public SyncContext Results { get; set; }
        public SyncAgent Agent { get; set; }
        public Exception Exception { get; set; }

        public ProviderRun(string databaseName, IProvider clientProvider, bool isHttp, ProviderType providerType)
        {
            if (string.IsNullOrEmpty(databaseName))
                throw new ArgumentNullException(nameof(databaseName));

            this.DatabaseName = databaseName;
            this.Provider = clientProvider ?? throw new ArgumentNullException(nameof(clientProvider));
            this.IsHttp = isHttp;
            this.ProviderType = providerType;
        }


        //public event EventHandler<SyncAgent> OnRunning;

        public Action<IProvider> BeginRun { get; set; }
        public Action<IProvider> EndRun { get; set; }

        public async Task<ProviderRun> RunAsync(ProviderFixture<CoreProvider> serverFixture, string scopeName = null, string[] tables = null, SyncConfiguration conf = null,
        bool reuseAgent = true)
        {
            // server proxy
            var proxyServerProvider = new WebProxyServerProvider(serverFixture.ServerProvider);
            var proxyClientProvider = new WebProxyClientProvider();

            var syncTables = tables ?? serverFixture.Tables;

            // local test, through tcp
            if (!IsHttp)
            {
                // create agent
                if (this.Agent == null || !reuseAgent)
                    this.Agent = new SyncAgent(Provider, serverFixture.ServerProvider, syncTables);

                // copy conf settings
                if (conf != null)
                    serverFixture.CopyConfiguration(this.Agent.Configuration, conf);

                // sync
                try
                {
                    BeginRun?.Invoke(this.Agent.RemoteProvider);
                    Results = await this.Agent.SynchronizeAsync();
                    EndRun?.Invoke(this.Agent.RemoteProvider);
                }
                catch (Exception ex)
                {
                    Exception = ex;

                }
            }

            // -----------------------------------------------------------------------
            // HTTP
            // -----------------------------------------------------------------------

            // tests through http proxy
            if (IsHttp)
            {
                var syncHttpTables = tables ?? serverFixture.Tables;

                // client handler
                using (var server = new KestrellTestServer())
                {
                    // server handler
                    var serverHandler = new RequestDelegate(async context =>
                    {
                        SyncConfiguration syncConfiguration = new SyncConfiguration(syncHttpTables);

            // copy conf settings
            if (conf != null)
                            serverFixture.CopyConfiguration(syncConfiguration, conf);

            // set proxy conf
            proxyServerProvider.Configuration = syncConfiguration;

            // sync
            try
                        {
                            BeginRun?.Invoke(proxyServerProvider.LocalProvider);
                            await proxyServerProvider.HandleRequestAsync(context);
                            EndRun?.Invoke(proxyServerProvider.LocalProvider);

                        }
                        catch (Exception ew)
                        {
                            Debug.WriteLine(ew);
                        }
                    });

                    var clientHandler = new ResponseDelegate(async (serviceUri) =>
                    {
                        // create agent
                        if (this.Agent == null || !reuseAgent)
                            this.Agent = new SyncAgent(Provider, proxyClientProvider);

                        ((WebProxyClientProvider)this.Agent.RemoteProvider).ServiceUri = new Uri(serviceUri);

                        try
                        {
                            Results = await Agent.SynchronizeAsync();
                        }
                        catch (Exception ew)
                        {
                            Exception = ew;
                        }
                    });
                    await server.Run(serverHandler, clientHandler);
                }

            }
            return this;
        }
    }
}
