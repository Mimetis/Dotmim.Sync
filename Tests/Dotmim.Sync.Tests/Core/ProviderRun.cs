using Dotmim.Sync.MySql;
using Dotmim.Sync.Serialization;
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
        public CoreProvider ClientProvider { get; set; }
        public NetworkType NetworkType { get; set; }
        public SyncContext Results { get; set; }
        public SyncAgent Agent { get; set; }
        public Exception Exception { get; set; }

        public ProviderRun(string databaseName, CoreProvider clientProvider, ProviderType clientProviderType, NetworkType networkType)
        {
            if (string.IsNullOrEmpty(databaseName))
                throw new ArgumentNullException(nameof(databaseName));

            this.DatabaseName = databaseName;
            this.ClientProvider = clientProvider ?? throw new ArgumentNullException(nameof(clientProvider));
            this.NetworkType = networkType;
            this.ClientProviderType = clientProviderType;
        }


        //public event EventHandler<SyncAgent> OnRunning;

        public Action<IRemoteOrchestrator> BeginRun { get; set; }
        public Action<IRemoteOrchestrator> EndRun { get; set; }


        public async Task<ProviderRun> RunAsync(ProviderFixture serverFixture, string[] tables = null,
            SyncSchema schema = null, SyncOptions options = null, bool reuseAgent = true)
        {
            var syncTables = tables ?? serverFixture.Tables;

            // local test, through tcp
            if (this.NetworkType == NetworkType.Tcp)
            {
                // create agent
                if (this.Agent == null || !reuseAgent)
                    this.Agent = new SyncAgent(this.ClientProvider, serverFixture.ServerProvider, syncTables, options);

                if (options != null)
                    this.Agent.Options = options;

                // copy conf settings
                if (schema != null)
                    this.Agent.Schema = schema;

                // Add Filers
                if (serverFixture.Filters != null && serverFixture.Filters.Count > 0)
                    serverFixture.Filters.ForEach(f => this.Agent.Schema.Filters.Add(f));

                // Add Filers values
                if (serverFixture.FilterParameters != null && serverFixture.FilterParameters.Count > 0)
                    foreach (var syncParam in serverFixture.FilterParameters)
                        if (!this.Agent.Parameters.Contains(syncParam))
                            this.Agent.Parameters.Add(syncParam);

                // sync
                try
                {
                    this.BeginRun?.Invoke(this.Agent.RemoteOrchestrator);
                    this.Results = await this.Agent.SynchronizeAsync();
                    this.EndRun?.Invoke(this.Agent.RemoteOrchestrator);
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
                using (var server = new KestrellTestServer())
                {
                    // server handler
                    var serverHandler = new RequestDelegate(async context =>
                    {
                        // test if <> directory name works
                        var serverOptions = new WebServerOptions
                        {
                            BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "server")
                        };

                        // get policy from test options
                        if (options != null)
                            serverOptions.ConflictResolutionPolicy = options.ConflictResolutionPolicy;

                        // sync
                        try
                        {

                            var proxyServerOrchestrator = WebProxyServerOrchestrator.Create(
                                context, serverFixture.ServerProvider, schema, serverOptions);

                            var webServerOrchestrator = proxyServerOrchestrator.GetLocalOrchestrator(context);

                            webServerOrchestrator.Schema.Add(syncTables);

                            // Add Filers
                            if (serverFixture.Filters != null && serverFixture.Filters.Count > 0)
                                serverFixture.Filters.ForEach(f =>
                                {
                                    if (!webServerOrchestrator.Schema.Filters.Contains(f))
                                        webServerOrchestrator.Schema.Filters.Add(f);
                                });


                            this.BeginRun?.Invoke(webServerOrchestrator);
                            await proxyServerOrchestrator.HandleRequestAsync(context);
                            this.EndRun?.Invoke(webServerOrchestrator);

                        }
                        catch (Exception ew)
                        {
                            Console.WriteLine(ew);
                        }
                    });

                    var clientHandler = new ResponseDelegate(async (serviceUri) =>
                    {

                        // server proxy
                        var proxyClientOrchestrator = new WebClientOrchestrator(serviceUri);

                        // create agent
                        if (this.Agent == null || !reuseAgent)
                            this.Agent = new SyncAgent(this.ClientProvider, proxyClientOrchestrator, null, options);

                        if (options != null)
                            this.Agent.Options = options;

                        // copy conf settings
                        if (schema != null)
                            this.Agent.Schema = schema;

                        // Just set the correct serviceUri if my kestrell server changed it
                        ((WebClientOrchestrator)this.Agent.RemoteOrchestrator).ServiceUri = serviceUri;

                        if (serverFixture.FilterParameters != null && serverFixture.FilterParameters.Count > 0)
                            foreach (var syncParam in serverFixture.FilterParameters)
                                if (!this.Agent.Parameters.Contains(syncParam))
                                    this.Agent.Parameters.Add(syncParam);

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
