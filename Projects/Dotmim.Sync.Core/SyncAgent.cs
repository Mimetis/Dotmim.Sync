using Dotmim.Sync.Core.Builders;
using Dotmim.Sync.Core.Context;
using Dotmim.Sync.Core.Scope;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync.Core
{

    /// <summary>
    /// Sync agent. It's the sync orchestrator
    /// Knows both the Sync Server provider and the Sync Client provider
    /// </summary>
    public class SyncAgent : IDisposable
    {

        string[] configurationTables;
        string scopeName;
        bool isConfigured;

        /// <summary>
        /// Defines the state that a synchronization session is in.
        /// </summary>
        public SyncSessionState SessionState { get; set; }

        /// <summary>
        /// Get the provider for the Client Side
        /// </summary>
        public CoreProvider LocalProvider { get; }

        /// <summary>
        /// Get the provider for the Server Side
        /// </summary>
        public CoreProvider RemoteProvider { get; }


        // Scope informaitons. 
        // On Server, we have tow scopes available : Server Scope and Client (server timestamp) scope
        // On Client, we have only the client scope
        public Dictionary<string, ScopeInfo> Scopes { get; set; }

        /// <summary>
        /// Gets or Sets the configuration option needed by the synchronization
        /// </summary>
        public ServiceConfiguration Configuration { get; set; }

        /// <summary>
        /// Occurs during progress
        /// </summary>
        public event EventHandler<ScopeProgressEventArgs> SyncProgress = null;


        public event EventHandler<SyncSessionState> SessionStateChanged = null;

        /// <summary>
        /// SyncAgent manage both server and client provider
        /// It's the main object to launch the Sync process
        /// </summary>
        public SyncAgent(string scopeName, CoreProvider localProvider, CoreProvider remoteProvider)
        {
            this.LocalProvider = localProvider ?? throw new ArgumentNullException("ClientProvider");
            this.RemoteProvider = remoteProvider ?? throw new ArgumentNullException("ServerProvider");
            this.scopeName = scopeName ?? throw new ArgumentNullException("scopeName");

            this.LocalProvider.SyncProgress += ClientProvider_SyncProgress;
            this.RemoteProvider.SyncProgress += ServerProvider_SyncProgress;
        }

        /// <summary>
        /// SyncAgent manage both server and client provider
        /// It's the main object to launch the Sync process
        /// </summary>
        public SyncAgent(CoreProvider clientProvider, CoreProvider serverProvider)
            : this("DefaultScope", clientProvider, serverProvider)
        {
        }

        /// <summary>
        /// SyncAgent manage both server and client provider
        /// The Configuration object contains the DmSet Schema for the synchronization
        /// </summary>
        public SyncAgent(string scopeName, CoreProvider clientProvider, CoreProvider serverProvider, ServiceConfiguration configuration)
            : this(scopeName, clientProvider, serverProvider)
        {

            if (configuration == null)
                throw new ArgumentNullException("configuration");

            if (configuration.ScopeSet == null || configuration.ScopeSet.Tables.Count <= 0)
                throw new ArgumentNullException("configuration.ScopeSet");

            this.configurationTables = null;

            // Set the Server configuration
            this.Configuration = configuration;
        }

        /// <summary>
        /// SyncAgent manage both server and client provider
        /// The Configuration object contains the DmSet Schema for the synchronization
        /// </summary>
        public SyncAgent(CoreProvider clientProvider, CoreProvider serverProvider, ServiceConfiguration configuration)
          : this("DefaultScope", clientProvider, serverProvider, configuration)
        {
        }

        /// <summary>
        /// SyncAgent manage both server and client provider
        /// the tables array represents the tables you want to sync
        /// </summary>
        public SyncAgent(string scopeName, CoreProvider clientProvider, CoreProvider serverProvider, string[] tables)
        : this(scopeName, clientProvider, serverProvider)
        {
            if (tables == null || tables.Length <= 0)
                throw new ArgumentException("you need to pass at lease one table name");

            this.configurationTables = tables;

        }

        /// <summary>
        /// SyncAgent manage both server and client provider
        /// the tables array represents the tables you want to sync
        /// </summary>
        public SyncAgent(CoreProvider clientProvider, CoreProvider serverProvider, string[] tables)
        : this("DefaultScope", clientProvider, serverProvider, tables)
        {
        }


        /// <summary>
        /// Main action : Launch the synchronization
        /// </summary>
        public SyncContext SynchronizeAsync()
        {
            if (string.IsNullOrEmpty(this.scopeName))
                throw new Exception("Scope Name is mandatory");

            // Context, containing statistics
            SyncContext context = new SyncContext(Guid.NewGuid());
            // set start time
            context.SyncStartTime = DateTime.Now;

            this.SessionState = SyncSessionState.Synchronizing;
            this.SessionStateChanged?.Invoke(this, this.SessionState);

            try
            {
                // Begin Session / Read the adapters
                this.RemoteProvider.BeginSession();
                this.LocalProvider.BeginSession();

                // ----------------------------------------
                // 1) Read scope info
                // ----------------------------------------
                //ScopeInfo scopeInfoClient;
                //ScopeInfo scopeInfoServerClient;
                //ScopeInfo scopeInfoServer;

                //// Read the client provider scope info and actually create a new one if needed
                //scopeInfoClient = this.LocalProvider.EnsureScopes(scopeName, null)[0];

                //// Read the server provider and create a new one if needed, and then add the client scope in its server version
                //var scopes = this.RemoteProvider.EnsureScopes(scopeName, scopeInfoClient.Name);
                //scopeInfoServer = scopes[0];
                //scopeInfoServerClient = scopes[1];

                // Ensures local scopes are created fo both provieders
                this.LocalProvider.EnsureScopes(scopeName);
                this.RemoteProvider.EnsureScopes(scopeName, this.LocalProvider.ClientScopeInfo.Name);

                // set the correct ScopeInfo to localProvider since the ServerScope is not the good one
                this.LocalProvider.ServerScopeInfo = this.RemoteProvider.ServerScopeInfo;

                // ----------------------------------------
                // 2) Build Configuration Object
                // ----------------------------------------

                // Construct the configuration
                if (!isConfigured)
                {
                    if (this.configurationTables != null && this.configurationTables.Length > 0)
                        this.Configuration = this.RemoteProvider.BuildConfiguration(this.configurationTables);
                    else if (this.Configuration != null)
                        this.Configuration = this.RemoteProvider.BuildConfiguration(this.Configuration);
                    else
                        throw new Exception("Configuration is not good enough today");

                    // Here, the configuration is correct, tables schema is ready and all dmTables ar schema complete
                    // So we can pass the configuration to the client side
                    this.LocalProvider.BuildConfiguration(this.Configuration);

                    isConfigured = true;
                }


                // Set the schema for information purpose
                context.ScopeSet = this.Configuration.ScopeSet.Clone();
  
                // ----------------------------------------
                // 3) Ensure databases are ready
                // ----------------------------------------

                // Server should have already the schema
                this.RemoteProvider.EnsureDatabase(DbBuilderOption.CreateOrUseExistingSchema | DbBuilderOption.CreateOrUseExistingTrackingTables);
                // Client could have, or not, the tables
                this.LocalProvider.EnsureDatabase(DbBuilderOption.CreateOrUseExistingSchema | DbBuilderOption.CreateOrUseExistingTrackingTables);

                // ----------------------------------------
                // 5) Get changes and apply them
                // ----------------------------------------

                // Get Changes from client to server
                // Since we are on the client, we need to compare to its own scope timestamp
                var setClient = this.LocalProvider.GetChangeBatch();
                // scopeinfoclient it's the local scopename

                // Apply on the Server Side
                // Since we are on the server, we need to check the server client timestamp (not the client timestamp which is completely different)
                this.RemoteProvider.ApplyChanges(this.RemoteProvider.ClientScopeInfo, setClient);

                // Get changes from server
                var setServer = this.RemoteProvider.GetChangeBatch();

                // Apply on client side
                // On the client side we should be able to direct write, without check the server scope
                //var scope = new ScopeInfo { Name = scopeName, LastTimestamp = 0 };

                // Apply local changes
                this.LocalProvider.ApplyChanges(this.LocalProvider.ServerScopeInfo, setServer);

                long serverTimestamp = this.RemoteProvider.GetLocalTimestamp();
                long clientTimestamp = this.LocalProvider.GetLocalTimestamp();

                //update scopes
                this.RemoteProvider.WriteScopes();
                this.LocalProvider.WriteScopes();


                // Begin Session / Read the adapters
                this.RemoteProvider.EndSession();
                this.LocalProvider.EndSession();


            }
            catch (Exception)
            {

                throw;
            }
            finally
            {
                this.SessionState = SyncSessionState.Ready;
                this.SessionStateChanged?.Invoke(this, this.SessionState);
                context.SyncCompleteTime = DateTime.Now;
            }

            return context;

        }


        void ServerProvider_SyncProgress(object sender, ScopeProgressEventArgs e)
        {
           this.SyncProgress?.Invoke(this, e);
        }

        void ClientProvider_SyncProgress(object sender, ScopeProgressEventArgs e)
        {
           this.SyncProgress?.Invoke(this, e);
        }



        // --------------------------------------------------------------------
        // Dispose
        // --------------------------------------------------------------------

        /// <summary>
        /// Releases all resources used by the <see cref="T:Microsoft.Synchronization.Data.DbSyncBatchInfo" />.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Releases the unmanaged resources used 
        /// by the <see cref="T:Microsoft.Synchronization.Data.DbSyncBatchInfo" /> and optionally releases the managed resources.
        /// </summary>
        protected virtual void Dispose(bool cleanup)
        {
            this.LocalProvider.SyncProgress -= ClientProvider_SyncProgress;
            this.RemoteProvider.SyncProgress -= ServerProvider_SyncProgress;

        }
    }
}
