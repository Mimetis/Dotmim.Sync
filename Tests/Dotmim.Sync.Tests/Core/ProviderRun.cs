using Dotmim.Sync.MySql;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Dotmim.Sync.Tests.Core
{
    public class ProviderRun
    {
        /// <summary>
        /// Gets or Sets the database name used to generate correct connection string
        /// </summary>
        public string DatabaseName { get; set; }

        /// <summary>
        /// Get or Sets Connection on inner Provider.
        /// </summary>
        public string ConnectionString
        {
            get
            {
                switch (this.ClientProviderType)
                {
                    case ProviderType.Sql:
                        return ((SqlSyncProvider)this.ClientProvider)?.ConnectionString;
                    case ProviderType.MySql:
                        return ((MySqlSyncProvider)this.ClientProvider)?.ConnectionString;
                    case ProviderType.Sqlite:
                        return ((SqliteSyncProvider)this.ClientProvider)?.ConnectionString;
                }

                return null;
            }
            set
            {
                switch (this.ClientProviderType)
                {
                    case ProviderType.Sql:
                        ((SqlSyncProvider)this.ClientProvider).ConnectionString = value;
                        break;
                    case ProviderType.MySql:
                        ((MySqlSyncProvider)this.ClientProvider).ConnectionString = value;
                        break;
                    case ProviderType.Sqlite:
                        ((SqliteSyncProvider)this.ClientProvider).ConnectionString = value;
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
            if (this.NetworkType == NetworkType.Tcp)
            {
                // create agent
                if (this.Agent == null || !reuseAgent)
                    this.Agent = new SyncAgent(this.ClientProvider, serverProvider, syncTables);

                // copy conf settings
                if (conf != null)
                    serverFixture.CopyConfiguration(this.Agent.Configuration, conf);

                // Add Filers
                if (serverFixture.Filters != null && serverFixture.Filters.Count > 0)
                    serverFixture.Filters.ForEach(f =>
                    {
                        if (!this.Agent.Configuration.Filters.Contains(f))
                            this.Agent.Configuration.Filters.Add(f);
                    });

                // Add Filers values
                if (serverFixture.FilterParameters != null && serverFixture.FilterParameters.Count > 0)
                    foreach (var syncParam in serverFixture.FilterParameters)
                        if (!this.Agent.Parameters.Contains(syncParam))
                            this.Agent.Parameters.Add(syncParam);

                // sync
                try
                {
                    this.BeginRun?.Invoke(this.Agent.RemoteProvider);
                    this.Results = await this.Agent.SynchronizeAsync();
                    this.EndRun?.Invoke(this.Agent.RemoteProvider);
                }
                catch (Exception ex)
                {
                    this.Exception = ex;
                    Console.WriteLine(ex);
                }
            }

            // -----------------------------------------------------------------------
            // HTTP
            // -----------------------------------------------------------------------

            // tests through http proxy
            if (this.NetworkType == NetworkType.Http)
            {

                // client handler
                using (var server = new KestrellTestServer())
                {
                    // server handler
                    var serverHandler = new RequestDelegate(async context =>
                    {

                        var syncConfiguration = new SyncConfiguration(syncTables);// set proxy conf
                        // copy conf settings

                        if (conf != null)
                            serverFixture.CopyConfiguration(syncConfiguration, conf);


                        // set proxy conf
                        proxyServerProvider.Configuration = syncConfiguration;

                        // test if <> directory name works
                        proxyServerProvider.Configuration.BatchDirectory = Path.Combine(proxyServerProvider.Configuration.BatchDirectory, "server");

                        // Add Filers
                        if (serverFixture.Filters != null && serverFixture.Filters.Count > 0)
                            serverFixture.Filters.ForEach(f =>
                            {
                                if (!proxyServerProvider.Configuration.Filters.Contains(f))
                                    proxyServerProvider.Configuration.Filters.Add(f);
                            });


                        // sync
                        try
                        {
                            this.BeginRun?.Invoke(proxyServerProvider.LocalProvider);
                            await proxyServerProvider.HandleRequestAsync(context);
                            this.EndRun?.Invoke(proxyServerProvider.LocalProvider);

                        }
                        catch (Exception ew)
                        {
                            Console.WriteLine(ew);
                        }
                    });

                    var clientHandler = new ResponseDelegate(async (serviceUri) =>
                    {
                        // create agent
                        if (this.Agent == null || !reuseAgent)
                            this.Agent = new SyncAgent(this.ClientProvider, proxyClientProvider);

                        if (serverFixture.FilterParameters != null && serverFixture.FilterParameters.Count > 0)
                            foreach (var syncParam in serverFixture.FilterParameters)
                                if (!this.Agent.Parameters.Contains(syncParam))
                                    this.Agent.Parameters.Add(syncParam);

                        ((WebProxyClientProvider)this.Agent.RemoteProvider).ServiceUri = new Uri(serviceUri);

                        try
                        {
                            this.Results = await this.Agent.SynchronizeAsync();
                        }
                        catch (Exception ew)
                        {
                            this.Exception = ew;
                            Console.WriteLine(ew);
                        }
                    });
                    await server.Run(serverHandler, clientHandler);
                }

            }
            return this;
        }
    }
}
