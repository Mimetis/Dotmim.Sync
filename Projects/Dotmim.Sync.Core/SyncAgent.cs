using Dotmim.Sync.Core.Batch;
using Dotmim.Sync.Core.Builders;
using Dotmim.Sync.Core.Context;
using Dotmim.Sync.Core.Log;
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
        ServiceConfiguration serviceConfiguration;

        /// <summary>
        /// Defines the state that a synchronization session is in.
        /// </summary>
        public SyncSessionState SessionState { get; set; }

        /// <summary>
        /// Gets or Sets the provider for the Client Side
        /// </summary>
        public IResponseHandler LocalProvider { get;  set; }

        /// <summary>
        /// Get or Sets the provider for the Server Side
        /// </summary>
        public IResponseHandler RemoteProvider { get; set; }

        // Scope informaitons. 
        // On Server, we have tow scopes available : Server Scope and Client (server timestamp) scope
        // On Client, we have only the client scope
        public Dictionary<string, ScopeInfo> Scopes { get; set; }


        /// <summary>
        /// Occurs during progress
        /// </summary>
        public event EventHandler<SyncProgressEventArgs> SyncProgress = null;


        public event EventHandler<SyncSessionState> SessionStateChanged = null;


        public SyncAgent(string[] tables)
        {
            this.serviceConfiguration = new ServiceConfiguration(tables);

        }

        public SyncAgent(ServiceConfiguration configuration)
        {
            this.serviceConfiguration = configuration;

        }

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

            this.serviceConfiguration = new ServiceConfiguration(tables);
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

            this.serviceConfiguration = configuration;

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



        public async Task<SyncContext> SynchronizeAsync()
        {
            return await this.SynchronizeAsync(CancellationToken.None);
        }

        /// <summary>
        /// Main action : Launch the synchronization
        /// </summary>
        public async Task<SyncContext> SynchronizeAsync(CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(this.scopeName))
                throw new Exception("Scope Name is mandatory");

            // Context, used to back and forth data between servers
            SyncContext context = new SyncContext(Guid.NewGuid());
            // set start time
            context.StartTime = DateTime.Now;

            this.SessionState = SyncSessionState.Synchronizing;
            this.SessionStateChanged?.Invoke(this, this.SessionState);
            ScopeInfo localScopeInfo = null, serverScopeInfo = null, serverLocalScopeReferenceInfo = null;

            // Stats computed
            ChangesStatistics changesStatistics = new ChangesStatistics();
            try
            {
                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Setting the cancellation token
                this.LocalProvider.SetCancellationToken(cancellationToken);
                this.RemoteProvider.SetCancellationToken(cancellationToken);

                // Begin Session / Read the adapters
                context = await this.RemoteProvider.BeginSessionAsync(context);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                context = await this.LocalProvider.BeginSessionAsync(context);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();
                // ----------------------------------------
                // 1) Read scope info
                // ----------------------------------------

                // get the scope from local provider 
                List<ScopeInfo> localScopes;
                List<ScopeInfo> serverScopes;
                (context,  localScopes) = await this.LocalProvider.EnsureScopesAsync(context, scopeName);
                if (localScopes.Count != 1)
                    throw new Exception("On Local provider, we should have only one scope info");

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                localScopeInfo = localScopes[0];

                (context, serverScopes) = await this.RemoteProvider.EnsureScopesAsync(context, scopeName, localScopeInfo.Id);

                if (serverScopes.Count != 2)
                    throw new Exception("On Remote provider, we should have two scopes (one for server and one for client side)");

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                serverScopeInfo = serverScopes.First(s => s.Id != localScopeInfo.Id);
                serverLocalScopeReferenceInfo = serverScopes.First(s => s.Id == localScopeInfo.Id);

                // ----------------------------------------
                // 2) Build Configuration Object
                // ----------------------------------------

                // Get Configuration from remote provider
                (context, this.serviceConfiguration) = await this.RemoteProvider.EnsureConfigurationAsync(context, this.serviceConfiguration);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Invert policy on the client
                var configurationLocale = this.serviceConfiguration.Clone();
                var policy = this.serviceConfiguration.ConflictResolutionPolicy;
                if (policy == ConflictResolutionPolicy.ServerWins)
                    configurationLocale.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;
                if (policy == ConflictResolutionPolicy.ClientWins)
                    configurationLocale.ConflictResolutionPolicy = ConflictResolutionPolicy.ServerWins;

                // Apply on local Provider
                ServiceConfiguration configuration;
                (context, configuration) = await this.LocalProvider.EnsureConfigurationAsync(context, configurationLocale);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // ----------------------------------------
                // 3) Ensure databases are ready
                // ----------------------------------------

                // Server should have already the schema
                context = await this.RemoteProvider.EnsureDatabaseAsync(context, serverScopeInfo, DbBuilderOption.CreateOrUseExistingSchema | DbBuilderOption.CreateOrUseExistingTrackingTables);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Client could have, or not, the tables
                context = await this.LocalProvider.EnsureDatabaseAsync(context, localScopeInfo, DbBuilderOption.CreateOrUseExistingSchema | DbBuilderOption.CreateOrUseExistingTrackingTables);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // ----------------------------------------
                // 5) Get changes and apply them
                // ----------------------------------------
                BatchInfo clientBatchInfo;
                BatchInfo serverBatchInfo;
                ChangesStatistics clientStatistics = null;
                ChangesStatistics serverStatistics = null;
                ChangesStatistics tmpClientStatistics = null;
                ChangesStatistics tmpServerStatistics = null;


                // Generate the client batchinfo with all files involved
                (context, clientBatchInfo, clientStatistics) = await this.LocalProvider.GetChangeBatchAsync(context, localScopeInfo);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Apply on the Server Side
                // Since we are on the server, we need to check the server client timestamp (not the client timestamp which is completely different)
                (context, serverStatistics) = await this.RemoteProvider.ApplyChangesAsync(context, serverLocalScopeReferenceInfo, clientBatchInfo);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Get changes from server
                (context, serverBatchInfo, tmpServerStatistics) = await this.RemoteProvider.GetChangeBatchAsync(context, serverScopeInfo);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Update server stats
                if (serverStatistics == null)
                    serverStatistics = tmpServerStatistics;
                else
                    clientStatistics.SelectedChanges = tmpServerStatistics.SelectedChanges;

                // Apply local changes
                (context, tmpClientStatistics)  = await this.LocalProvider.ApplyChangesAsync(context, serverScopeInfo, serverBatchInfo);
      
                if (clientStatistics == null)
                    clientStatistics = tmpClientStatistics;
                else
                    clientStatistics.AppliedChanges = tmpClientStatistics.AppliedChanges;

                context.TotalChangesDownloaded = clientStatistics.TotalAppliedChanges;
                context.TotalChangesUploaded = serverStatistics.TotalAppliedChanges;
                context.TotalSyncErrors = clientStatistics.TotalAppliedChangesFailed;

                long serverTimestamp, clientTimestamp;

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                (context, serverTimestamp) = await this.RemoteProvider.GetLocalTimestampAsync(context);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                (context, clientTimestamp) = await this.LocalProvider.GetLocalTimestampAsync(context);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                context.CompleteTime = DateTime.Now;

                serverScopeInfo.IsNewScope = false;
                serverLocalScopeReferenceInfo.IsNewScope = false;
                localScopeInfo.IsNewScope = false;

                serverScopeInfo.LastSync = context.CompleteTime;
                serverLocalScopeReferenceInfo.LastSync = context.CompleteTime;
                localScopeInfo.LastSync = context.CompleteTime;

                serverScopeInfo.IsLocal = true;
                serverLocalScopeReferenceInfo.IsLocal = false;

                context = await this.RemoteProvider.WriteScopesAsync(context, new List <ScopeInfo> { serverScopeInfo, serverLocalScopeReferenceInfo });

                serverScopeInfo.IsLocal = false;
                localScopeInfo.IsLocal = true;

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                context = await this.LocalProvider.WriteScopesAsync(context, new List <ScopeInfo> { localScopeInfo, serverScopeInfo });

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Begin Session / Read the adapters
                context = await this.RemoteProvider.EndSessionAsync(context);
                context = await this.LocalProvider.EndSessionAsync(context);
            }
            catch (OperationCanceledException oce)
            {
                var error = SyncException.CreateOperationCanceledException(context.SyncStage, oce);
                HandleSyncError(error);
                context.Error = error;
            }
            catch (SyncException sex)
            {
                HandleSyncError(sex);
                context.Error = sex;
            }
            catch (Exception ex)
            {
                var error = SyncException.CreateUnknowException(context.SyncStage, ex);
                HandleSyncError(error);
                context.Error = error;
            }
            finally
            {
                // if EndSessionAsync() was never called, try a last time
                try
                {
                    context = await this.RemoteProvider.EndSessionAsync(context);
                    context = await this.LocalProvider.EndSessionAsync(context);

                }
                catch (Exception)
                {
                    // no raise
                }

                this.SessionState = SyncSessionState.Ready;
                this.SessionStateChanged?.Invoke(this, this.SessionState);
            }

            return context;

        }

        private static void HandleSyncError(SyncException sex)
        {
            switch (sex.SyncStage)
            {
                case SyncStage.BeginSession:
                    Logger.Current.Info(sex.ToString());
                    break;
                case SyncStage.EnsureMetadata:
                    break;
                case SyncStage.SelectedChanges:
                    break;
                case SyncStage.AppliedChanges:
                    break;
                case SyncStage.ApplyingInserts:
                    break;
                case SyncStage.ApplyingUpdates:
                    break;
                case SyncStage.ApplyingDeletes:
                    break;
                case SyncStage.WriteMetadata:
                    break;
                case SyncStage.EndSession:
                    break;
                case SyncStage.CleanupMetadata:
                    break;
                default:
                    break;
            }

            // try to end sessions on both


        }

        void ServerProvider_SyncProgress(object sender, SyncProgressEventArgs e)
        {
            //this.SyncProgress?.Invoke(this, e);
        }

        void ClientProvider_SyncProgress(object sender, SyncProgressEventArgs e)
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
