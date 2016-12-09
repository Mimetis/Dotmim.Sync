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
        /// <summary>
        /// Defines the state that a synchronization session is in.
        /// </summary>
        public SyncSessionState SessionState { get; set; }

        /// <summary>
        /// Get the provider for the Client Side
        /// </summary>
        public CoreProvider ClientProvider { get; }

        /// <summary>
        /// Get the provider for the Server Side
        /// </summary>
        public CoreProvider ServerProvider { get; }

           /// <summary>
        /// Sync Agent configuration
        /// </summary>
        public ServiceConfiguration Configuration { get; }


        /// <summary>
        /// Occurs during progress
        /// </summary>
        public event EventHandler<ScopeProgressEventArgs> SyncProgress = null;


        public event EventHandler<SyncSessionState> SessionStateChanged = null;

        /// <summary>
        /// Constructor. Should manage both server and client provider
        /// </summary>
        public SyncAgent(CoreProvider clientProvider, CoreProvider serverProvider)
        {
            this.ClientProvider = clientProvider ?? throw new Exception("ClientProvider is mandatory");
            this.ServerProvider = serverProvider ?? throw new Exception("ServerProvider is mandatory");

            this.Configuration = new ServiceConfiguration();

            this.ClientProvider.SyncProgress += ClientProvider_SyncProgress;
            this.ServerProvider.SyncProgress += ServerProvider_SyncProgress;
        }

        void ServerProvider_SyncProgress(object sender, ScopeProgressEventArgs e)
        {
            this.SyncProgress?.Invoke(this, e);
        }

        void ClientProvider_SyncProgress(object sender, ScopeProgressEventArgs e)
        {
            this.SyncProgress?.Invoke(this, e);
        }

        /// <summary>
        /// Main action : Launch the synchronization
        /// </summary>
        public SyncContext SynchronizeAsync()
        {

            if (string.IsNullOrEmpty(this.Configuration.ScopeName))
                throw new Exception("Server Scopename is mandatory");

            // Context, containing statistics
            SyncContext context = new SyncContext(Guid.NewGuid());
            // set start time
            context.SyncStartTime = DateTime.Now;

            this.SessionState = SyncSessionState.Synchronizing;
            this.SessionStateChanged?.Invoke(this, this.SessionState);

            try
            {
                ScopeInfo serverScope = null;
                ScopeInfo serverClientScope = null;
                ScopeInfo clientScope = null;

                // Begin Session / Read the adapters
                this.ServerProvider.BeginSession();
                this.ClientProvider.BeginSession();

                // Read the server scope info
                serverScope = this.ServerProvider.ReadScopeInfo(this.Configuration.ScopeName);

                if (serverScope == null)
                    throw new Exception($"Scope {this.Configuration.ScopeName} does not exist in the server database");

                // Get the config from the server side
                this.ServerProvider.ReadScopeConfig(serverScope);

                // Set the client provider scopeconfigdata
                this.ClientProvider.ScopeConfigData = this.ServerProvider.ScopeConfigData;

                // Try to read the client provider scope info
                clientScope = this.ClientProvider.ReadScopeInfo();

                // Check if client scope name is null
                // if null Create a new one
                if (clientScope == null)
                {
                    // Create a new Guid and use that as the client Id.
                    Guid clientId = Guid.NewGuid();
                    var scopeName = string.Format(CultureInfo.InvariantCulture, "{0}_{1}", serverScope.Name, clientId);

                    // Write the scopeName both to the client and server
                    this.ServerProvider.WriteScopeInfo(scopeName);
                    clientScope = this.ClientProvider.WriteScopeInfo(scopeName);
                }

                // the client scope from the server
                // dont raise any event on the client side scope (since the information are related to server side)
                serverClientScope = this.ServerProvider.ReadScopeInfo(clientScope.Name);


                // Get Changes from client to server
                // Since we are on the client, we need to compare to its own scope timestamp
                var setClient = this.ClientProvider.GetChangeBatch(clientScope);

                // Apply on the Server Side
                // Since we are on the server, we need to check the server client timestamp (not the client timestamp which is completely different)
                this.ServerProvider.ApplyChanges(serverClientScope, setClient);

                // Get changes from server
                var setServer = this.ServerProvider.GetChangeBatch(serverClientScope);
                
                // Apply on client side
                // On the client side we should be able to direct write, without check the server scope
                var scope = new ScopeInfo
                {
                    Name = serverScope.Name,
                    LastTimestamp = 0
                };

                this.ClientProvider.ApplyChanges(scope, setServer);

                long serverTimestamp = this.ServerProvider.ReadLocalTimestamp();
                long clientTimestamp = this.ClientProvider.ReadLocalTimestamp();

                this.ServerProvider.WriteScopeInfo(clientScope.Name);
                this.ClientProvider.WriteScopeInfo(clientScope.Name);

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
            this.ClientProvider.SyncProgress -= ClientProvider_SyncProgress;
            this.ServerProvider.SyncProgress -= ServerProvider_SyncProgress;

        }
    }
}
