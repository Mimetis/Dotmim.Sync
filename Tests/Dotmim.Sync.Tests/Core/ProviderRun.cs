using Dotmim.Sync.MySql;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
                switch (ClientProviderType)
                {
                    case ProviderType.Sql:
                        return ((SqlSyncProvider)ClientProvider)?.ConnectionString;
                    case ProviderType.MySql:
                        return ((MySqlSyncProvider)ClientProvider)?.ConnectionString;
                    case ProviderType.Sqlite:
                        return ((SqliteSyncProvider)ClientProvider)?.ConnectionString;
                }

                return null;
            }
            set
            {
                switch (ClientProviderType)
                {
                    case ProviderType.Sql:
                        ((SqlSyncProvider)ClientProvider).ConnectionString = value;
                        break;
                    case ProviderType.MySql:
                        ((MySqlSyncProvider)ClientProvider).ConnectionString = value;
                        break;
                    case ProviderType.Sqlite:
                        ((SqliteSyncProvider)ClientProvider).ConnectionString = value;
                        break;
                }
            }
        }
        public ProviderType ClientProviderType { get; set; }
        public IProvider ClientProvider { get; set; }
        public NetworkType NetworkType { get; set; }
        public SyncContext Results { get; set; }
        public SyncAgent Agent { get; set; }
        public Exception Exception { get; set; }

        public ProviderRun(string databaseName, IProvider clientProvider, ProviderType clientProviderType, NetworkType networkType)
        {
            if (string.IsNullOrEmpty(databaseName))
                throw new ArgumentNullException(nameof(databaseName));

            this.DatabaseName = databaseName;
            this.ClientProvider = clientProvider ?? throw new ArgumentNullException(nameof(clientProvider));
            this.NetworkType = networkType;
            this.ClientProviderType = clientProviderType;
        }


        //public event EventHandler<SyncAgent> OnRunning;

        public Action<IProvider> BeginRun { get; set; }
        public Action<IProvider> EndRun { get; set; }


        public async Task<ProviderRun> RunAsync(CoreProvider serverProvider, ProviderFixture<CoreProvider> serverFixture, string scopeName = null, string[] tables = null, SyncConfiguration conf = null,
        bool reuseAgent = true)
        {
            // server proxy
            var proxyServerProvider = new WebProxyServerProvider(serverProvider);
            var proxyClientProvider = new WebProxyClientProvider();

            var syncTables = tables ?? serverFixture.Tables;

            // local test, through tcp
            if (NetworkType == NetworkType.Tcp)
            {
                // create agent
                if (this.Agent == null || !reuseAgent)
                    this.Agent = new SyncAgent(ClientProvider, serverProvider, syncTables);

                // copy conf settings
                if (conf != null)
                    serverFixture.CopyConfiguration(this.Agent.Configuration, conf);

                // Add Filers
                if (serverFixture.Filters != null && serverFixture.Filters.Count > 0)
                    serverFixture.Filters.ForEach(f => {
                        if (!Agent.Configuration.Filters.Contains(f))
                            Agent.Configuration.Filters.Add(f);
                    });

                // Add Filers values
                if (serverFixture.FilterParameters != null && serverFixture.FilterParameters.Count > 0)
                    foreach(var syncParam in serverFixture.FilterParameters)
                        if (!this.Agent.Parameters.Contains(syncParam))
                            this.Agent.Parameters.Add(syncParam);

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
            if (NetworkType == NetworkType.Http)
            {

                // client handler
                using (var server = new KestrellTestServer())
                {
                    // server handler
                    var serverHandler = new RequestDelegate(async context =>
                    {
                        SyncConfiguration syncConfiguration = new SyncConfiguration(syncTables);// set proxy conf

                        // copy conf settings

                        if (conf != null)
                            serverFixture.CopyConfiguration(syncConfiguration, conf);


                        // set proxy conf
                        proxyServerProvider.Configuration = syncConfiguration;

                        // Add Filers
                        if (serverFixture.Filters != null && serverFixture.Filters.Count > 0)
                            serverFixture.Filters.ForEach(f => {
                                if (!proxyServerProvider.Configuration.Filters.Contains(f))
                                    proxyServerProvider.Configuration.Filters.Add(f);
                            });


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
                            this.Agent = new SyncAgent(ClientProvider, proxyClientProvider);

                        if (serverFixture.FilterParameters != null && serverFixture.FilterParameters.Count > 0)
                            foreach (var syncParam in serverFixture.FilterParameters)
                                if (!this.Agent.Parameters.Contains(syncParam))
                                    this.Agent.Parameters.Add(syncParam);

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
