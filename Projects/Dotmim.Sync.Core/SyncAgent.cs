using Dotmim.Sync.Batch;
using Dotmim.Sync.Filter;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;

namespace Dotmim.Sync
{

    /// <summary>
    /// Sync agent. It's the sync orchestrator
    /// Knows both the Sync Server provider and the Sync Client provider
    /// </summary>
    public class SyncAgent : IDisposable
    {

        string scopeName;
        public SyncConfiguration Configuration { get; set; }

        /// <summary>
        /// Defines the state that a synchronization session is in.
        /// </summary>
        public SyncSessionState SessionState { get; set; }

        /// <summary>
        /// Gets or Sets the provider for the Client Side
        /// </summary>
        public IProvider LocalProvider { get; set; }

        /// <summary>
        /// Get or Sets the provider for the Server Side
        /// </summary>
        public IProvider RemoteProvider { get; set; }

        // Scope informaitons. 
        // On Server, we have tow scopes available : Server Scope and Client (server timestamp) scope
        // On Client, we have only the client scope
        public Dictionary<string, ScopeInfo> Scopes { get; set; }

        /// <summary>
        /// Get or Sets the Sync parameter to pass to Remote provider for filtering rows
        /// </summary>
        public SyncParameterCollection Parameters { get; set; } = new SyncParameterCollection();


        // Read only SyncProgress for all SyncStage
        public event EventHandler<ProgressEventArgs> SyncProgress = null;

        // Types events for each SyncStage
        public event EventHandler<BeginSessionEventArgs> BeginSession = null;
        public event EventHandler<EndSessionEventArgs> EndSession = null;
        public event EventHandler<ScopeEventArgs> ScopeLoading = null;
        public event EventHandler<ScopeEventArgs> ScopeSaved = null;
        public event EventHandler<DatabaseApplyingEventArgs> DatabaseApplying = null;
        public event EventHandler<DatabaseAppliedEventArgs> DatabaseApplied = null;
        public event EventHandler<DatabaseTableApplyingEventArgs> DatabaseTableApplying = null;
        public event EventHandler<DatabaseTableAppliedEventArgs> DatabaseTableApplied = null;
        public event EventHandler<ConfigurationApplyingEventArgs> ConfigurationApplying = null;
        public event EventHandler<ConfigurationAppliedEventArgs> ConfigurationApplied = null;
        public event EventHandler<TableChangesSelectingEventArgs> TableChangesSelecting = null;
        public event EventHandler<TableChangesSelectedEventArgs> TableChangesSelected = null;
        public event EventHandler<TableChangesApplyingEventArgs> TableChangesApplying = null;
        public event EventHandler<TableChangesAppliedEventArgs> TableChangesApplied = null;


        /// <summary>
        /// Occurs when a conflict is raised on the server side
        /// </summary>
        public event EventHandler<ApplyChangeFailedEventArgs> ApplyChangedFailed = null;

        /// <summary>
        /// Occurs when sync is starting, ending
        /// </summary>
        public event EventHandler<SyncSessionState> SessionStateChanged = null;


        public SyncAgent(string[] tables)
        {
            this.Configuration = new SyncConfiguration(tables);
        }

        public SyncAgent(SyncConfiguration configuration)
        {
            this.Configuration = configuration;

        }

        /// <summary>
        /// SyncAgent manage both server and client provider
        /// It's the main object to launch the Sync process
        /// </summary>
        public SyncAgent(string scopeName, IProvider localProvider, IProvider remoteProvider)
        {
            this.LocalProvider = localProvider ?? throw new ArgumentNullException("ClientProvider");
            this.RemoteProvider = remoteProvider ?? throw new ArgumentNullException("ServerProvider");
            this.scopeName = scopeName ?? throw new ArgumentNullException("scopeName");

            this.LocalProvider.SyncProgress += (s, e) => this.SyncProgress?.Invoke(s, e);
            this.LocalProvider.BeginSession += (s, e) => this.BeginSession?.Invoke(s, e);
            this.LocalProvider.EndSession += (s, e) => this.EndSession?.Invoke(s, e);
            this.LocalProvider.TableChangesApplied += (s, e) => this.TableChangesApplied?.Invoke(s, e);
            this.LocalProvider.TableChangesApplying += (s, e) => this.TableChangesApplying?.Invoke(s, e);
            this.LocalProvider.TableChangesSelected += (s, e) => this.TableChangesSelected?.Invoke(s, e);
            this.LocalProvider.TableChangesSelecting += (s, e) => this.TableChangesSelecting?.Invoke(s, e);
            this.LocalProvider.ConfigurationApplied += (s, e) => this.ConfigurationApplied?.Invoke(s, e);
            this.LocalProvider.ConfigurationApplying += (s, e) => this.ConfigurationApplying?.Invoke(s, e);
            this.LocalProvider.DatabaseApplied += (s, e) => this.DatabaseApplied?.Invoke(s, e);
            this.LocalProvider.DatabaseApplying += (s, e) => this.DatabaseApplying?.Invoke(s, e);
            this.LocalProvider.DatabaseTableApplied += (s, e) => this.DatabaseTableApplied?.Invoke(s, e);
            this.LocalProvider.DatabaseTableApplying += (s, e) => this.DatabaseTableApplying?.Invoke(s, e);
            this.LocalProvider.ScopeLoading += (s, e) => this.ScopeLoading?.Invoke(s, e);
            this.LocalProvider.ScopeSaved += (s, e) => this.ScopeSaved?.Invoke(s, e);

            this.RemoteProvider.ApplyChangedFailed += RemoteProvider_ApplyChangedFailed;
        }



        /// <summary>
        /// SyncAgent used in a web proxy sync session. No need to set tables, it's done from the server web api side.
        /// </summary>
        public SyncAgent(IProvider localProvider, IProvider remoteProvider)
            : this("DefaultScope", localProvider, remoteProvider)
        {
        }

        /// <summary>
        /// SyncAgent manage both server and client provider
        /// the tables array represents the tables you want to sync
        /// Don't work on the proxy provider
        /// </summary>
        public SyncAgent(string scopeName, IProvider clientProvider, IProvider serverProvider, string[] tables)
        : this(scopeName, clientProvider, serverProvider)
        {
            if (tables == null || tables.Length <= 0)
                throw new ArgumentException("you need to pass at lease one table name");

            var remoteCoreProvider = this.RemoteProvider as CoreProvider;

            if (remoteCoreProvider == null)
                throw new ArgumentException("Since the remote provider is a web proxy, you have to configure the server side");

            if (!remoteCoreProvider.CanBeServerProvider)
                throw new NotSupportedException();

            this.Configuration = new SyncConfiguration(tables);
        }

        /// <summary>
        /// SyncAgent manage both server and client provider
        /// the tables array represents the tables you want to sync
        /// Don't work on the proxy provider
        /// </summary>
        public SyncAgent(IProvider clientProvider, IProvider serverProvider, string[] tables)
        : this("DefaultScope", clientProvider, serverProvider, tables)
        {
        }


        /// <summary>
        /// SyncAgent manage both server and client provider
        /// the Configuration object represents the server configuration 
        /// Don't work on the proxy provider
        /// </summary>
        public SyncAgent(string scopeName, IProvider clientProvider, IProvider serverProvider, SyncConfiguration configuration)
        : this(scopeName, clientProvider, serverProvider)
        {
            if (configuration.Count <= 0)
                throw new ArgumentNullException("No tables specified");

            var remoteCoreProvider = this.RemoteProvider as CoreProvider;

            if (remoteCoreProvider == null)
                throw new ArgumentNullException("Since the remote provider is a web proxy, you have to configure the server side");

            if (!remoteCoreProvider.CanBeServerProvider)
                throw new NotSupportedException();

            this.Configuration = configuration;

        }

        /// <summary>
        /// SyncAgent manage both server and client provider
        /// the Configuration object represents the server configuration 
        /// Don't work on the proxy provider
        /// </summary>
        public SyncAgent(IProvider clientProvider, IProvider serverProvider, SyncConfiguration configuration)
        : this("DefaultScope", clientProvider, serverProvider, configuration)
        {
        }

        /// <summary>
        /// Launch a normal synchronization
        /// </summary>
        public async Task<SyncContext> SynchronizeAsync()
        {
            return await this.SynchronizeAsync(SyncType.Normal, CancellationToken.None);
        }

        /// <summary>
        /// Launch a normal synchronization with a cancellation token
        /// </summary>
        public async Task<SyncContext> SynchronizeAsync(CancellationToken cancellationToken)
        {
            return await this.SynchronizeAsync(SyncType.Normal, cancellationToken);
        }

        /// <summary>
        /// Launch a synchronization with the specified mode
        /// </summary>
        public async Task<SyncContext> SynchronizeAsync(SyncType syncType)
        {
            return await this.SynchronizeAsync(syncType, CancellationToken.None);

        }
 
        /// <summary>
        /// Launch a synchronization with the specified mode
        /// </summary>
        public async Task<SyncContext> SynchronizeAsync(SyncType syncType, CancellationToken cancellationToken)
        {

            if (string.IsNullOrEmpty(this.scopeName))
                throw new ArgumentNullException("scopeName", "Scope Name is mandatory");

            // Context, used to back and forth data between servers
            SyncContext context = new SyncContext(Guid.NewGuid())
            {
                // set start time
                StartTime = DateTime.Now,

                // if any parameters, set in context
                Parameters = this.Parameters,

                // set sync type (Normal, Reinitialize, ReinitializeWithUpload)
                SyncType = syncType
            };

            this.SessionState = SyncSessionState.Synchronizing;
            this.SessionStateChanged?.Invoke(this, this.SessionState);

            ScopeInfo localScopeInfo = null, 
                      serverScopeInfo = null, 
                      localScopeReferenceInfo = null, 
                      scope = null;

            Guid fromId = Guid.Empty;
            long lastSyncTS = 0L;
            bool isNew = true;

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
                (context, localScopes) = await this.LocalProvider.EnsureScopesAsync(context, scopeName);

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
                localScopeReferenceInfo = serverScopes.First(s => s.Id == localScopeInfo.Id);

                // ----------------------------------------
                // 2) Build Configuration Object
                // ----------------------------------------

                // Get Configuration from remote provider
                (context, this.Configuration) = await this.RemoteProvider.EnsureConfigurationAsync(context, this.Configuration);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Invert policy on the client
                var configurationLocale = this.Configuration.Clone();
                var policy = this.Configuration.ConflictResolutionPolicy;
                if (policy == ConflictResolutionPolicy.ServerWins)
                    configurationLocale.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;
                if (policy == ConflictResolutionPolicy.ClientWins)
                    configurationLocale.ConflictResolutionPolicy = ConflictResolutionPolicy.ServerWins;

                // Apply on local Provider
                SyncConfiguration configuration;
                (context, configuration) = await this.LocalProvider.EnsureConfigurationAsync(context, configurationLocale);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // ----------------------------------------
                // 3) Ensure databases are ready
                // ----------------------------------------

                // Server should have already the schema
                context = await this.RemoteProvider.EnsureDatabaseAsync(context, serverScopeInfo);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Client could have, or not, the tables
                context = await this.LocalProvider.EnsureDatabaseAsync(context, localScopeInfo);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // ----------------------------------------
                // 5) Get changes and apply them
                // ----------------------------------------
                BatchInfo clientBatchInfo;
                BatchInfo serverBatchInfo;

                ChangesSelected clientChangesSelected = null;
                ChangesSelected serverChangesSelected = null;
                ChangesApplied clientChangesApplied = null;
                ChangesApplied serverChangesApplied = null;

                // fromId : not really needed on this case, since updated / inserted / deleted row has marked null
                // otherwise, lines updated by server or others clients are already syncked
                fromId = localScopeInfo.Id;
                // lastSyncTS : get lines inserted / updated / deteleted after the last sync commited
                lastSyncTS = localScopeInfo.LastTimestamp;
                // isNew : If isNew, lasttimestamp is not correct, so grab all
                isNew = localScopeInfo.IsNewScope;
                //Direction set to Upload
                context.SyncWay = SyncWay.Upload;

                scope = new ScopeInfo { Id = fromId, IsNewScope = isNew, LastTimestamp = lastSyncTS };
                (context, clientBatchInfo, clientChangesSelected) = await this.LocalProvider.GetChangeBatchAsync(context, scope);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Apply on the Server Side
                // Since we are on the server, 
                // we need to check the server client timestamp (not the client timestamp which is completely different)

                // fromId : When applying rows, make sure it's identified as applied by this client scope
                fromId = localScopeInfo.Id;
                // lastSyncTS : apply lines only if thye are not modified since last client sync
                lastSyncTS = localScopeReferenceInfo.LastTimestamp;
                // isNew : not needed
                isNew = false;
                scope = new ScopeInfo { Id = fromId, IsNewScope = isNew, LastTimestamp = lastSyncTS };

                (context, serverChangesApplied) = await this.RemoteProvider.ApplyChangesAsync(context, scope, clientBatchInfo);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();
                // Get changes from server

                // fromId : Make sure we don't select lines on server that has been already updated by the client
                fromId = localScopeInfo.Id;
                // lastSyncTS : apply lines only if thye are not modified since last client sync
                lastSyncTS = localScopeReferenceInfo.LastTimestamp;
                // isNew : make sure we take all lines if it's the first time we get 
                isNew = localScopeReferenceInfo.IsNewScope;
                scope = new ScopeInfo { Id = fromId, IsNewScope = isNew, LastTimestamp = lastSyncTS };
                //Direction set to Download
                context.SyncWay = SyncWay.Download;

                (context, serverBatchInfo, serverChangesSelected) = await this.RemoteProvider.GetChangeBatchAsync(context, scope);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Apply local changes

                // fromId : When applying rows, make sure it's identified as applied by this server scope
                fromId = serverScopeInfo.Id;
                // lastSyncTS : apply lines only if they are not modified since last client sync
                lastSyncTS = localScopeInfo.LastTimestamp;
                // isNew : if IsNew, don't apply deleted rows from server
                isNew = localScopeInfo.IsNewScope;
                scope = new ScopeInfo { Id = fromId, IsNewScope = isNew, LastTimestamp = lastSyncTS };

                (context, clientChangesApplied) = await this.LocalProvider.ApplyChangesAsync(context, scope, serverBatchInfo);

                context.TotalChangesDownloaded = clientChangesApplied.TotalAppliedChanges ;
                context.TotalChangesUploaded = clientChangesSelected.TotalChangesSelected;
                context.TotalSyncErrors = clientChangesApplied.TotalAppliedChangesFailed;

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
                localScopeReferenceInfo.IsNewScope = false;
                localScopeInfo.IsNewScope = false;

                serverScopeInfo.LastSync = context.CompleteTime;
                localScopeReferenceInfo.LastSync = context.CompleteTime;
                localScopeInfo.LastSync = context.CompleteTime;

                serverScopeInfo.IsLocal = true;
                localScopeReferenceInfo.IsLocal = false;

                context = await this.RemoteProvider.WriteScopesAsync(context, new List<ScopeInfo> { serverScopeInfo, localScopeReferenceInfo });

                serverScopeInfo.IsLocal = false;
                localScopeInfo.IsLocal = true;

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                context = await this.LocalProvider.WriteScopesAsync(context, new List<ScopeInfo> { localScopeInfo, serverScopeInfo });

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

            }
            catch (SyncException se)
            {
                Debug.WriteLine($"Sync Exception: {se.Message}. Type:{se.Type}. On provider: {se.ProviderName}.");
                throw;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Unknwon Exception: {ex.Message}.");
                throw new SyncException(ex, SyncStage.None, string.Empty);
            }
            finally
            {
                // End the current session
                context = await this.RemoteProvider.EndSessionAsync(context);
                context = await this.LocalProvider.EndSessionAsync(context);

                this.SessionState = SyncSessionState.Ready;
                this.SessionStateChanged?.Invoke(this, this.SessionState);
            }

            return context;
        }


        private void RemoteProvider_ApplyChangedFailed(object sender, ApplyChangeFailedEventArgs e)
        {
            this.ApplyChangedFailed?.Invoke(this, e);
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
            this.LocalProvider.BeginSession -= (s, e) => this.BeginSession?.Invoke(s, e);
            this.LocalProvider.EndSession -= (s, e) => this.EndSession?.Invoke(s, e);
            this.LocalProvider.TableChangesApplied -= (s, e) => this.TableChangesApplied?.Invoke(s, e);
            this.LocalProvider.TableChangesApplying -= (s, e) => this.TableChangesApplying?.Invoke(s, e);
            this.LocalProvider.TableChangesSelected -= (s, e) => this.TableChangesSelected?.Invoke(s, e);
            this.LocalProvider.TableChangesSelecting -= (s, e) => this.TableChangesSelecting?.Invoke(s, e);
            this.LocalProvider.ConfigurationApplied -= (s, e) => this.ConfigurationApplied?.Invoke(s, e);
            this.LocalProvider.ConfigurationApplying -= (s, e) => this.ConfigurationApplying?.Invoke(s, e);
            this.LocalProvider.DatabaseApplied -= (s, e) => this.DatabaseApplied?.Invoke(s, e);
            this.LocalProvider.DatabaseApplying -= (s, e) => this.DatabaseApplying?.Invoke(s, e);
            this.LocalProvider.ScopeLoading -= (s, e) => this.ScopeLoading?.Invoke(s, e);
            this.LocalProvider.ScopeSaved -= (s, e) => this.ScopeSaved?.Invoke(s, e);

            this.RemoteProvider.ApplyChangedFailed -= RemoteProvider_ApplyChangedFailed;
        }
    }
}
