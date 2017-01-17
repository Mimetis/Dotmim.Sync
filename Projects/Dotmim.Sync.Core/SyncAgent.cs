using Dotmim.Sync.Core.Builders;
using Dotmim.Sync.Core.Context;
using Dotmim.Sync.Core.Scope;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
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

        string scopeName;
        private IResponseHandler clientProvider;
        private IResponseHandler serverProvider;

        /// <summary>
        /// Defines the state that a synchronization session is in.
        /// </summary>
        public SyncSessionState SessionState { get; set; }

        /// <summary>
        /// Get the provider for the Client Side
        /// </summary>
        public IResponseHandler LocalProvider { get; }

        /// <summary>
        /// Get the provider for the Server Side
        /// </summary>
        public IResponseHandler RemoteProvider { get; }

        // Scope informaitons. 
        // On Server, we have tow scopes available : Server Scope and Client (server timestamp) scope
        // On Client, we have only the client scope
        public Dictionary<string, ScopeInfo> Scopes { get; set; }


        /// <summary>
        /// Occurs during progress
        /// </summary>
        public event EventHandler<ScopeProgressEventArgs> SyncProgress = null;


        public event EventHandler<SyncSessionState> SessionStateChanged = null;

        /// <summary>
        /// SyncAgent manage both server and client provider
        /// It's the main object to launch the Sync process
        /// </summary>
        public SyncAgent(string scopeName, IResponseHandler localProvider, IResponseHandler remoteProvider)
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
        public SyncAgent(IResponseHandler localProvider, IResponseHandler remoteProvider)
            : this("DefaultScope", localProvider, remoteProvider)
        {
        }

        /// <summary>
        /// SyncAgent manage both server and client provider
        /// the tables array represents the tables you want to sync
        /// Don't work on the proxy provider
        /// </summary>
        public SyncAgent(string scopeName, IResponseHandler clientProvider, IResponseHandler serverProvider, string[] tables)
        : this(scopeName, clientProvider, serverProvider)
        {
            if (tables == null || tables.Length <= 0)
                throw new ArgumentException("you need to pass at lease one table name");

            var remoteCoreProvider = this.RemoteProvider as CoreProvider;

            if (remoteCoreProvider == null)
                throw new ArgumentException("Since the remote provider is a web proxy, you have to configure the server side");

            remoteCoreProvider.Configuration = new ServiceConfiguration(tables);
        }

        /// <summary>
        /// SyncAgent manage both server and client provider
        /// the tables array represents the tables you want to sync
        /// Don't work on the proxy provider
        /// </summary>
        public SyncAgent(IResponseHandler clientProvider, IResponseHandler serverProvider, string[] tables)
        : this("DefaultScope", clientProvider, serverProvider, tables)
        {
        }


        /// <summary>
        /// SyncAgent manage both server and client provider
        /// the Configuration object represents the server configuration 
        /// Don't work on the proxy provider
        /// </summary>
        public SyncAgent(string scopeName, IResponseHandler clientProvider, IResponseHandler serverProvider, ServiceConfiguration configuration)
        : this(scopeName, clientProvider, serverProvider)
        {
            if (configuration.Tables == null || configuration.Tables.Length <= 0)
                throw new ArgumentException("you need to pass at least one table name");

            var remoteCoreProvider = this.RemoteProvider as CoreProvider;

            if (remoteCoreProvider == null)
                throw new ArgumentException("Since the remote provider is a web proxy, you have to configure the server side");

            remoteCoreProvider.Configuration = configuration;

        }

        /// <summary>
        /// SyncAgent manage both server and client provider
        /// the Configuration object represents the server configuration 
        /// Don't work on the proxy provider
        /// </summary>
        public SyncAgent(IResponseHandler clientProvider, IResponseHandler serverProvider, ServiceConfiguration configuration)
        : this("DefaultScope", clientProvider, serverProvider, configuration)
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
            ScopeInfo localScopeInfo = null, serverScopeInfo = null, serverLocalScopeReferenceInfo = null;
            try
            {
                // Begin Session / Read the adapters
                this.RemoteProvider.BeginSession();
                this.LocalProvider.BeginSession();

                // ----------------------------------------
                // 1) Read scope info
                // ----------------------------------------
                // Ensures local scopes are created fo both provieders
                (serverScopeInfo, localScopeInfo) = this.LocalProvider.EnsureScopes(scopeName);

                (serverScopeInfo, serverLocalScopeReferenceInfo) = this.RemoteProvider.EnsureScopes(scopeName, localScopeInfo.Name);

                // set the correct ScopeInfo to localProvider since the ServerScope is not the good one
                ((CoreProvider)this.LocalProvider).ServerScopeInfo = serverScopeInfo;

                // ----------------------------------------
                // 2) Build Configuration Object
                // ----------------------------------------

                // Applying Configuration on remote provider
                // Actually should take the remote configuration already available on the remote machine
                this.RemoteProvider.ApplyConfiguration();

                // Get Configuration from remote provider
                ServiceConfiguration configuration = this.RemoteProvider.GetConfiguration();

                // Invert policy on the client
                var configurationLocale = configuration.Clone();
                var policy = configuration.ConflictResolutionPolicy;
                if (policy == ConflictResolutionPolicy.ServerWins)
                    configurationLocale.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;
                if (policy == ConflictResolutionPolicy.ClientWins)
                    configurationLocale.ConflictResolutionPolicy = ConflictResolutionPolicy.ServerWins;

                // Apply on local Provider
                this.LocalProvider.ApplyConfiguration(configurationLocale);

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
                var clientBatchInfo = this.LocalProvider.GetChangeBatch();

                // Apply on the Server Side
                // Since we are on the server, we need to check the server client timestamp (not the client timestamp which is completely different)
                this.RemoteProvider.ApplyChanges(serverLocalScopeReferenceInfo, clientBatchInfo);

                // Get changes from server
                var serverBatchInfo = this.RemoteProvider.GetChangeBatch();

                // Apply on client side
                // On the client side we should be able to direct write, without check the server scope
                //var scope = new ScopeInfo { Name = scopeName, LastTimestamp = 0 };

                // Apply local changes
                this.LocalProvider.ApplyChanges(serverScopeInfo, serverBatchInfo);

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
            //this.SyncProgress?.Invoke(this, e);
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
