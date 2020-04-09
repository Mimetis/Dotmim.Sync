using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Sync agent. It's the sync orchestrator
    /// Knows both the Sync Server provider and the Sync Client provider
    /// </summary>
    public class SyncAgent : IDisposable
    {
        private IProgress<ProgressArgs> remoteProgress = null;
        private bool syncInProgress;

        /// <summary>
        /// Gets or Sets the scope name, defining the tables involved in the sync
        /// </summary>
        public string ScopeName { get; set; }

        /// <summary>
        /// Defines the state that a synchronization session is in.
        /// </summary>
        public SyncSessionState SessionState { get; set; } = SyncSessionState.Ready;

        /// <summary>
        /// Gets or Sets the local orchestrator
        /// </summary>
        public LocalOrchestrator LocalOrchestrator { get; set; }

        /// <summary>
        /// Get or Sets the remote orchestrator
        /// </summary>
        public RemoteOrchestrator RemoteOrchestrator { get; set; }

        /// <summary>
        /// Get or Sets the Sync parameters to pass to Remote provider for filtering rows
        /// </summary>
        public SyncParameters Parameters { get; private set; } = new SyncParameters();

        /// <summary>
        /// Occurs when sync is starting, ending
        /// </summary>
        public event EventHandler<SyncSessionState> SessionStateChanged = null;

        /// <summary>
        /// Gets the setup used for this sync
        /// </summary>
        public SyncSetup Setup => this.LocalOrchestrator?.Setup;

        /// <summary>
        /// Gets the options used on this sync process.
        /// </summary>
        public SyncOptions Options => this.LocalOrchestrator?.Options;

        public SyncSet Schema { get; private set; }

        /// <summary>
        /// Set interceptors on the LocalOrchestrator
        /// </summary>
        public void SetInterceptors(Interceptors interceptors) => this.LocalOrchestrator.On(interceptors);

        /// <summary>
        /// If you want to see remote progress as well (only available RemoteOrchestrator)
        /// </summary>
        /// <param name="remoteProgress"></param>
        public void AddRemoteProgress(IProgress<ProgressArgs> remoteProgress) => this.remoteProgress = remoteProgress;

        /// <summary>
        /// Shortcut to Apply changed failed if remote orchestrator supports it
        /// </summary>
        public void OnApplyChangesFailed(Func<ApplyChangesFailedArgs, Task> func)
        {
            var remoteOrchestrator = this.RemoteOrchestrator as RemoteOrchestrator;

            if (remoteOrchestrator == null)
                throw new InvalidRemoteOrchestratorException();

            remoteOrchestrator.OnApplyChangesFailed(func);

        }
            

        /// <summary>
        /// Shortcut to Apply changed failed if remote orchestrator supports it
        /// </summary>
        public void OnApplyChangesFailed(Action<ApplyChangesFailedArgs> action)
        {
            var remoteOrchestrator = this.RemoteOrchestrator as RemoteOrchestrator;

            if (remoteOrchestrator == null)
                throw new InvalidRemoteOrchestratorException();

            remoteOrchestrator.OnApplyChangesFailed(action);

        }


        /// <summary>
        /// Lock sync to prevent multi call to sync at the same time
        /// </summary>
        private void LockSync()
        {
            lock (this)
            {
                if (this.syncInProgress)
                    throw new AlreadyInProgressException();

                this.syncInProgress = true;
            }
        }

        /// <summary>
        /// Unlock sync to be able to launch a new sync
        /// </summary>
        private void UnlockSync()
        {
            // Enf sync from local provider
            lock (this)
            {
                this.syncInProgress = false;
            }
        }


        private SyncAgent(string scopeName) => this.ScopeName = scopeName;

        // 1
        /// <summary>
        /// Create an agent based on TCP connection
        /// </summary>
        /// <param name="clientProvider">local provider to your client database</param>
        /// <param name="serverProvider">local provider to your server database</param>
        /// <param name="tables">tables list</param>
        /// <param name="scopeName">scope name</param>
        public SyncAgent(CoreProvider clientProvider, CoreProvider serverProvider, string[] tables, string scopeName = SyncOptions.DefaultScopeName)
            : this(clientProvider, serverProvider, new SyncOptions(), new SyncSetup(tables), scopeName)
        {
        }

        // 2
        /// <summary>
        /// Create an agent based on TCP connection
        /// </summary>
        /// <param name="clientProvider">local provider to your client database</param>
        /// <param name="serverProvider">local provider to your server database</param>
        /// <param name="scopeName">scope name</param>
        public SyncAgent(CoreProvider clientProvider, CoreProvider serverProvider, string scopeName = SyncOptions.DefaultScopeName)
            : this(clientProvider, serverProvider, new SyncOptions(), new SyncSetup(), scopeName)
        {
        }

        // 3
        /// <summary>
        /// Create an agent based on TCP connection
        /// </summary>
        /// <param name="clientProvider">local provider to your client database</param>
        /// <param name="serverProvider">local provider to your server database</param>
        /// <param name="options">sync options</param>
        /// <param name="tables">tables list</param>
        /// <param name="scopeName">scope name</param>
        public SyncAgent(CoreProvider clientProvider, CoreProvider serverProvider, SyncOptions options, string[] tables, string scopeName = SyncOptions.DefaultScopeName)
            : this(clientProvider, serverProvider, options, new SyncSetup(tables), scopeName)
        {
        }

        // 4
        /// <summary>
        /// Create an agent based on TCP connection
        /// </summary>
        /// <param name="clientProvider">local provider to your client database</param>
        /// <param name="serverProvider">local provider to your server database</param>
        /// <param name="options">Sync options.</param>
        /// <param name="setup">Contains list of your tables.</param>
        /// <param name="scopeName">scope name</param>
        public SyncAgent(CoreProvider clientProvider, CoreProvider serverProvider, SyncOptions options, SyncSetup setup, string scopeName = SyncOptions.DefaultScopeName)
            : this(scopeName)
        {
            if (clientProvider is null)
                throw new ArgumentNullException(nameof(clientProvider));
            if (serverProvider is null)
                throw new ArgumentNullException(nameof(serverProvider));
            if (options == null)
                throw new ArgumentNullException(nameof(options));
            if (setup == null)
                throw new ArgumentNullException(nameof(setup));

            // Affect local and remote orchestrators
            this.LocalOrchestrator = new LocalOrchestrator(clientProvider, options, setup, scopeName);
            this.RemoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup, scopeName);

            this.EnsureOptionsAndSetupInstances();
        }

        // 5
        /// <summary>
        /// Create an agent based on TCP connection
        /// </summary>
        /// <param name="clientProvider">local provider to your client database</param>
        /// <param name="remoteOrchestrator">remote orchestrator</param>
        /// <param name="tables">tables list</param>
        /// <param name="scopeName">scope name</param>
        public SyncAgent(CoreProvider clientProvider, RemoteOrchestrator remoteOrchestrator, string[] tables, string scopeName = SyncOptions.DefaultScopeName)
            : this(clientProvider, remoteOrchestrator, new SyncOptions(), new SyncSetup(tables), scopeName)
        {
        }


        // 6
        /// <summary>
        /// Create an agent that will manages a complete sync between one client and one server
        /// </summary>
        /// <param name="clientProvider">local provider to your client database</param>
        /// <param name="remoteOrchestrator">remote orchestrator</param>
        /// <param name="scopeName">scope name</param>
        public SyncAgent(CoreProvider clientProvider, RemoteOrchestrator remoteOrchestrator, string scopeName = SyncOptions.DefaultScopeName)
            : this(clientProvider, remoteOrchestrator, new SyncOptions(), new SyncSetup(), scopeName)
        {

        }


        // 7
        /// <summary>
        /// Create an agent based on TCP connection
        /// </summary>
        /// <param name="clientProvider">local provider to your client database</param>
        /// <param name="remoteOrchestrator">remote orchestrator</param>
        /// <param name="options">sync options</param>
        /// <param name="tables">tables list</param>
        /// <param name="scopeName">scope name</param>
        public SyncAgent(CoreProvider clientProvider, RemoteOrchestrator remoteOrchestrator, SyncOptions options, string[] tables, string scopeName = SyncOptions.DefaultScopeName)
            : this(clientProvider, remoteOrchestrator, options, new SyncSetup(tables), scopeName)
        {
        }

        // 8
        /// <summary>
        /// Create an agent based on TCP connection
        /// </summary>
        /// <param name="clientProvider">local provider to your client database</param>
        /// <param name="remoteOrchestrator">remote orchestrator</param>
        /// <param name="options">sync options</param>
        /// <param name="tables">tables list</param>
        /// <param name="scopeName">scope name</param>
        public SyncAgent(CoreProvider clientProvider, RemoteOrchestrator remoteOrchestrator, SyncOptions options, string scopeName = SyncOptions.DefaultScopeName)
            : this(clientProvider, remoteOrchestrator, options, new SyncSetup(), scopeName)
        {
        }

        // 9
        /// <summary>
        /// Create an agent based on TCP connection
        /// </summary>
        /// <param name="clientProvider">local provider to your client database</param>
        /// <param name="remoteOrchestrator">remote orchestrator</param>
        /// <param name="options">Sync options.</param>
        /// <param name="setup">Contains list of your tables.</param>
        /// <param name="scopeName">scope name</param>
        public SyncAgent(CoreProvider clientProvider, RemoteOrchestrator remoteOrchestrator, SyncOptions options, SyncSetup setup, string scopeName = SyncOptions.DefaultScopeName)
            : this(scopeName)
        {
            if (clientProvider is null)
                throw new ArgumentNullException(nameof(clientProvider));
            if (remoteOrchestrator is null)
                throw new ArgumentNullException(nameof(remoteOrchestrator));
            if (options == null)
                throw new ArgumentNullException(nameof(options));
            if (setup == null)
                throw new ArgumentNullException(nameof(setup));

            // Override remote orchestrator options, setup and scope name
            remoteOrchestrator.Options = options;
            remoteOrchestrator.Setup = setup;
            remoteOrchestrator.ScopeName = scopeName;

            var localOrchestrator = new LocalOrchestrator(clientProvider, options, setup, scopeName);

            this.LocalOrchestrator = localOrchestrator;
            this.RemoteOrchestrator = remoteOrchestrator;

            this.EnsureOptionsAndSetupInstances();
        }
        // 10
        /// <summary>
        /// Create an agent based on 2 orchestrators
        /// </summary>
        /// <param name="localOrchestrator">local orchestrator</param>
        /// <param name="remoteOrchestrator">remote orchestrator</param>
        /// <param name="scopeName">scope name</param>
        public SyncAgent(LocalOrchestrator localOrchestrator, RemoteOrchestrator remoteOrchestrator, string scopeName = SyncOptions.DefaultScopeName)
            : this(scopeName)
        {
            if (localOrchestrator is null)
                throw new ArgumentNullException(nameof(localOrchestrator));
            if (remoteOrchestrator is null)
                throw new ArgumentNullException(nameof(remoteOrchestrator));

            this.LocalOrchestrator = localOrchestrator;
            this.RemoteOrchestrator = remoteOrchestrator;

            this.EnsureOptionsAndSetupInstances();
        }



        /// <summary>
        /// Ensure Options and Setup instances are the same on local orchestrator and remote orchestrator
        /// </summary>
        private void EnsureOptionsAndSetupInstances()
        {
            // if we have a remote orchestrator with different options, raise an error
            if (this.RemoteOrchestrator.Options != null && this.RemoteOrchestrator.Options != this.LocalOrchestrator.Options)
                throw new OptionsReferencesAreNotSameExecption();
            else if (this.RemoteOrchestrator.Options == null)
                this.RemoteOrchestrator.Options = this.LocalOrchestrator.Options;

            // if we have a remote orchestrator with different options, raise an error
            if (this.RemoteOrchestrator.Setup != null && this.RemoteOrchestrator.Setup != this.LocalOrchestrator.Setup)
                throw new SetupReferencesAreNotSameExecption();
            else if (this.RemoteOrchestrator.Setup == null)
                this.RemoteOrchestrator.Setup = this.LocalOrchestrator.Setup;

        }

        /// <summary>
        /// Launch a normal synchronization without any IProgess or CancellationToken
        /// </summary>
        public Task<SyncResult> SynchronizeAsync() => SynchronizeAsync(SyncType.Normal, CancellationToken.None);

        /// <summary>
        /// Launch a normal synchronization without any IProgess or CancellationToken
        /// </summary>
        public Task<SyncResult> SynchronizeAsync(IProgress<ProgressArgs> progress) => SynchronizeAsync(SyncType.Normal, CancellationToken.None, progress);

        /// <summary>
        /// Launch a synchronization with a SyncType specified
        /// </summary>
        public Task<SyncResult> SynchronizeAsync(SyncType syncType, IProgress<ProgressArgs> progress = null) => SynchronizeAsync(syncType, CancellationToken.None, progress);

        /// <summary>
        /// Launch a synchronization with the specified mode
        /// </summary>
        public async Task<SyncResult> SynchronizeAsync(SyncType syncType, CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            // checkpoints dates
            var startTime = DateTime.UtcNow;
            var completeTime = DateTime.UtcNow;

            // for view purpose, if needed
            if (this.LocalOrchestrator?.Provider != null)
                this.LocalOrchestrator.Provider.Options = this.Options;

            if (this.RemoteOrchestrator?.Provider != null)
                this.RemoteOrchestrator.Provider.Options = this.Options;

            // Lock sync to prevent multi call to sync at the same time
            LockSync();

            // Context, used to back and forth data between servers
            var context = new SyncContext(Guid.NewGuid(), this.ScopeName)
            {
                // if any parameters, set in context
                Parameters = this.Parameters,
                // set sync type (Normal, Reinitialize, ReinitializeWithUpload)
                SyncType = syncType
            };

            // Result, with sync results stats.
            var result = new SyncResult(context.SessionId)
            {
                // set start time
                StartTime = startTime,
                CompleteTime = completeTime,
            };

            this.SessionState = SyncSessionState.Synchronizing;
            this.SessionStateChanged?.Invoke(this, this.SessionState);

            try
            {
                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                ServerScopeInfo serverScopeInfo = null;

                this.LocalOrchestrator.SetContext(context);
                this.RemoteOrchestrator.SetContext(context);
                this.LocalOrchestrator.StartTime = startTime;
                this.RemoteOrchestrator.StartTime = startTime;

                // Begin session
                await this.LocalOrchestrator.BeginSessionAsync(cancellationToken, progress);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // On local orchestrator, get scope info
                var clientScopeInfo = await this.LocalOrchestrator.GetClientScopeAsync(cancellationToken, progress);

                // if client is new or else schema does not exists
                // We need to get it from server
                if (clientScopeInfo.IsNewScope || string.IsNullOrEmpty(clientScopeInfo.Schema))
                {
                    // Ensure schema is defined on remote side
                    // This action will create schema on server if needed
                    var (schema, version) = await this.RemoteOrchestrator.EnsureSchemaAsync(cancellationToken, remoteProgress);
                    clientScopeInfo.Schema = JsonConvert.SerializeObject(schema);
                    clientScopeInfo.Version = version;

                    // Set schema for agent, just to let the opportunity to user to use it.
                    this.Schema = schema;
                    this.Schema.EnsureSchema();

                    // Provision local database
                    var provision = SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;
                    await this.LocalOrchestrator.ProvisionAsync(schema, provision, cancellationToken, progress).ConfigureAwait(false);
                }
                else
                {
                    // on remote orchestrator get scope info as well
                    serverScopeInfo = await this.RemoteOrchestrator.GetServerScopeAsync(cancellationToken, remoteProgress);

                    // If coming from Web remote, the schema is null, so just compare version
                    // if server schema is null, assuming they are identical, unless version are differents
                    bool schemaAreTheSame = true;
                    if (serverScopeInfo.Schema != null)
                    {
                        var serverSchema = JsonConvert.DeserializeObject<SyncSet>(serverScopeInfo.Schema);
                        serverSchema.EnsureSchema();
                        var localSchema = JsonConvert.DeserializeObject<SyncSet>(clientScopeInfo.Schema);
                        localSchema.EnsureSchema();

                        schemaAreTheSame = serverSchema == localSchema;
                    }

                    var serverVersion = serverScopeInfo.Version;
                    var localVersion = clientScopeInfo.Version;

                    // Compare both string
                    if (!schemaAreTheSame || serverVersion != localVersion)
                        throw new ArgumentException("Schema from server is not the same as Client schema, should Deprovision then relaunch");

                    // Get schema
                    this.Schema = JsonConvert.DeserializeObject<SyncSet>(clientScopeInfo.Schema);
                    this.Schema.EnsureSchema();

                }


                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Before call the changes from localorchestrator, check if we are outdated
                if (serverScopeInfo != null && context.SyncType != SyncType.Reinitialize && context.SyncType != SyncType.ReinitializeWithUpload)
                {
                    var isOutDated = await this.LocalOrchestrator.IsOutDated(clientScopeInfo, serverScopeInfo);

                    // if client does not change SyncType to Reinitialize / ReinitializeWithUpload on SyncInterceptor, we raise an error
                    // otherwise, we are outdated, but we can continue, because we have a new mode.
                    if (isOutDated)
                        Debug.WriteLine($"Client id outdated, but we change mode to {context.SyncType}");
                }


                // On local orchestrator, get local changes
                var clientChanges = await this.LocalOrchestrator.GetChangesAsync(clientScopeInfo, cancellationToken, progress);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Get if we need to get all rows from the datasource
                var fromScratch = clientScopeInfo.IsNewScope || context.SyncType == SyncType.Reinitialize || context.SyncType == SyncType.ReinitializeWithUpload;

                // IF is new and we have a snapshot directory, try to apply a snapshot
                if (fromScratch)
                {
                    // Get snapshot files
                    var serverSnapshotChanges = await this.RemoteOrchestrator.GetSnapshotAsync(this.Schema, cancellationToken, remoteProgress);

                    // Apply snapshot
                    if (serverSnapshotChanges.ServerBatchInfo != null)
                    {
                        (result.SnapshotChangesAppliedOnClient, clientScopeInfo) = await this.LocalOrchestrator.ApplySnapshotAsync(
                            clientScopeInfo, serverSnapshotChanges.ServerBatchInfo, clientChanges.ClientTimestamp, serverSnapshotChanges.RemoteClientTimestamp, cancellationToken, progress);
                    }
                }

                // Hook !! (I'm Evil inside :) )
                var serverChanges = await this.RemoteOrchestrator.ApplyThenGetChangesAsync(clientScopeInfo, clientChanges.ClientBatchInfo, cancellationToken, remoteProgress);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Policy is always Server policy, so reverse this policy to get the client policy
                var reverseConflictResolutionPolicy = serverChanges.ServerPolicy == ConflictResolutionPolicy.ServerWins ? ConflictResolutionPolicy.ClientWins : ConflictResolutionPolicy.ServerWins;

                var clientChangesApplied = await this.LocalOrchestrator.ApplyChangesAsync(
                    clientScopeInfo, this.Schema, serverChanges.ServerBatchInfo,
                    clientChanges.ClientTimestamp, serverChanges.RemoteClientTimestamp, reverseConflictResolutionPolicy,
                    cancellationToken, progress);

                completeTime = DateTime.UtcNow;
                this.LocalOrchestrator.CompleteTime = completeTime;
                this.RemoteOrchestrator.CompleteTime = completeTime;

                result.CompleteTime = completeTime;

                // All clients changes selected
                result.ClientChangesSelected = clientChanges.ClientChangesSelected;
                result.ServerChangesSelected = serverChanges.ServerChangesSelected;
                result.ChangesAppliedOnClient = clientChangesApplied.ChangesApplied;
                result.ChangesAppliedOnServer = serverChanges.ClientChangesApplied;

                // Begin session
                await this.LocalOrchestrator.EndSessionAsync(cancellationToken, progress);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

            }
            catch (SyncException se)
            {
                Debug.WriteLine($"Sync Exception: {se.Message}. TypeName:{se.TypeName}.");
                throw;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Unknwon Exception: {ex.Message}.");
                throw new SyncException(ex, SyncStage.None);
            }
            finally
            {
                // End the current session
                this.SessionState = SyncSessionState.Ready;
                this.SessionStateChanged?.Invoke(this, this.SessionState);
                // unlock sync since it's over
                UnlockSync();
            }

            return result;
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
        /// </summary>
        protected virtual void Dispose(bool cleanup)
        {

        }
    }
}
