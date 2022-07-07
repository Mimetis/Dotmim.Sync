using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
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
    public partial class SyncAgent : IDisposable
    {
        private bool syncInProgress;
        private bool checkUpgradeDone = false;

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
        /// Occurs when sync is starting, ending
        /// </summary>
        public event EventHandler<SyncSessionState> SessionStateChanged = null;

        /// <summary>
        /// Gets the options used on this sync process.
        /// </summary>
        public SyncOptions Options => this.LocalOrchestrator?.Options;

        /// <summary>
        /// Set interceptors on the LocalOrchestrator
        /// </summary>
        public void SetInterceptors(Interceptors interceptors) => this.LocalOrchestrator.On(interceptors);

        /// <summary>
        /// Shortcut to Apply changed failed if remote orchestrator supports it
        /// </summary>
        public void OnApplyChangesFailed(Action<ApplyChangesFailedArgs> action)
        {
            if (this.RemoteOrchestrator == null)
                throw new InvalidRemoteOrchestratorException();

            this.RemoteOrchestrator.OnApplyChangesFailed(action);
        }


        /// <summary>
        /// Lock sync to prevent multi call to sync at the same time
        /// </summary>
        private void LockSync()
        {
            lock (this)
            {
                if (syncInProgress)
                    throw new AlreadyInProgressException();

                syncInProgress = true;
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
                syncInProgress = false;
            }
        }


        private SyncAgent() { }

        // 4
        /// <summary>
        /// Creates a synchronization agent that will handle a full synchronization between a client and a server.
        /// </summary>
        /// <param name="clientProvider">Local Provider connecting to your client database</param>
        /// <param name="serverProvider">Local Provider connecting to your server database</param>
        /// <param name="options">Sync Options defining options used by your local and remote provider</param>
        public SyncAgent(CoreProvider clientProvider, CoreProvider serverProvider, SyncOptions options = default)
            : this()
        {
            if (clientProvider is null)
                throw new ArgumentNullException(nameof(clientProvider));
            if (serverProvider is null)
                throw new ArgumentNullException(nameof(serverProvider));
            if (options == null)
                options = new SyncOptions();

            // Affect local and remote orchestrators
            this.LocalOrchestrator = new LocalOrchestrator(clientProvider, options);
            this.RemoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            this.EnsureOptionsAndSetupInstances();
        }

        // 9
        /// <summary>
        /// Creates a synchronization agent that will handle a full synchronization between a client and a server.
        /// </summary>
        /// <param name="clientProvider">local provider to your client database</param>
        /// <param name="remoteOrchestrator">Remote Orchestrator already configured with a SyncProvider</param>
        /// <param name="options">Sync Options defining options used by your local provider (and remote provider if remoteOrchestrator is not a WebClientOrchestrator)</param>
        public SyncAgent(CoreProvider clientProvider, RemoteOrchestrator remoteOrchestrator, SyncOptions options = default)
            : this()
        {
            if (clientProvider is null)
                throw new ArgumentNullException(nameof(clientProvider));
            if (remoteOrchestrator is null)
                throw new ArgumentNullException(nameof(remoteOrchestrator));
            if (options == default)
                options = new SyncOptions();

            // Override remote orchestrator options, setup and scope name
            remoteOrchestrator.Options = options;

            var localOrchestrator = new LocalOrchestrator(clientProvider, options);

            this.LocalOrchestrator = localOrchestrator;
            this.RemoteOrchestrator = remoteOrchestrator;
            this.EnsureOptionsAndSetupInstances();
        }

        // 10
        /// <summary>
        /// Creates a synchronization agent that will handle a full synchronization between a client and a server.
        /// </summary>
        /// <param name="localOrchestrator">Local Orchestrator already configured with a SyncProvider</param>
        /// <param name="remoteOrchestrator">Remote Orchestrator already configured with a SyncProvider</param>
        /// <param name="scopeName">scope name</param>
        public SyncAgent(LocalOrchestrator localOrchestrator, RemoteOrchestrator remoteOrchestrator) : this()
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

        }



        /// <summary>
        /// Launch a synchronization with the specified mode
        /// </summary>
        public async Task<SyncResult> SynchronizeAsync(string scopeName, SyncSetup setup, SyncType syncType, SyncParameters parameters, CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            // checkpoints dates
            var startTime = DateTime.UtcNow;
            var completeTime = DateTime.UtcNow;

            // Create a logger
            var logger = this.Options.Logger ?? new SyncLogger().AddDebug();

            // Lock sync to prevent multi call to sync at the same time
            LockSync();

            // Context, used to back and forth data between servers
            var context = new SyncContext(Guid.NewGuid(), scopeName)
            {
                // if any parameters, set in context
                Parameters = parameters,
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
            //await Task.Run(async () =>
            //{
            try
            {
                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                if (setup != null)
                {
                    var remoteOrchestratorType = this.RemoteOrchestrator.GetType();
                    var providerType = remoteOrchestratorType.Name;
                    if (providerType.ToLowerInvariant() == "webclientorchestrator")
                        throw new Exception("Do not set Tables (or SyncSetup) from your client. Please use SyncAgent, without any Tables or SyncSetup. The tables will come from the server side");
                }

                // Begin session
                context = await this.LocalOrchestrator.InternalBeginSessionAsync(context, cancellationToken, progress).ConfigureAwait(false);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // on remote orchestrator, get Server scope
                ServerScopeInfo serverScopeInfo;
                (context, serverScopeInfo) = await this.RemoteOrchestrator.InternalGetServerScopeInfoAsync(context, setup, default, default, cancellationToken, progress).ConfigureAwait(false);

                // If we just have create the server scope, we need to provision it
                // the WebServerAgent will do this setp on the GetServrScopeInfoAsync task, just before
                // So far, on Http mode, this if() will not be called
                if (serverScopeInfo != null && serverScopeInfo.IsNewScope)
                {
                    // 2) Provision
                    var provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;
                    (context, serverScopeInfo) = await this.RemoteOrchestrator.InternalProvisionServerAsync(serverScopeInfo, context, provision, false, default, default, cancellationToken, progress).ConfigureAwait(false);
                }

                // no need to check on every call to SynchronizeAsync
                if (!checkUpgradeDone)
                {
                    var needToUpgrade = await this.LocalOrchestrator.NeedsToUpgradeAsync(default, default, cancellationToken, progress).ConfigureAwait(false);

                    if (needToUpgrade)
                        await this.LocalOrchestrator.UpgradeAsync(default, default, cancellationToken, progress).ConfigureAwait(false);

                    checkUpgradeDone = true;
                }

                // On local orchestrator, get scope info.
                ClientScopeInfo clientScopeInfo;
                (context, clientScopeInfo) = await this.LocalOrchestrator.InternalGetClientScopeInfoAsync(context, default, default, cancellationToken, progress).ConfigureAwait(false);

                if (setup != null && clientScopeInfo.Setup != null && !clientScopeInfo.Setup.EqualsByProperties(setup))
                    throw new Exception("Seems you are trying another Setup tables that what is stored in your client scope database. Please create a new scope or deprovision and provision again your scope");

                // Register local scope id
                context.ClientScopeId = clientScopeInfo.Id;

                // if client is new or schema does not exists or scope name is a new one
                // We need to get the scope from server
                if (clientScopeInfo.IsNewScope || clientScopeInfo.Schema == null)
                {
                    // Provision local database
                    var provision = SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;
                    (context, clientScopeInfo) = await this.LocalOrchestrator.InternalProvisionClientAsync(serverScopeInfo, clientScopeInfo, context, provision, false, default, default, cancellationToken, progress).ConfigureAwait(false);
                }

                if (setup == null)
                    setup = clientScopeInfo.Setup;

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Before call the changes from localorchestrator, check if we are outdated
                if (serverScopeInfo != null && context.SyncType != SyncType.Reinitialize && context.SyncType != SyncType.ReinitializeWithUpload)
                {
                    bool isOutDated = false;
                    (context, isOutDated) = await this.LocalOrchestrator.IsOutDatedAsync(context, clientScopeInfo, serverScopeInfo).ConfigureAwait(false);

                    // if client does not change SyncType to Reinitialize / ReinitializeWithUpload on SyncInterceptor, we raise an error
                    // otherwise, we are outdated, but we can continue, because we have a new mode.
                    if (isOutDated)
                        Debug.WriteLine($"Client id outdated, but we change mode to {context.SyncType}");
                }

                context.ProgressPercentage = 0.1;

                // On local orchestrator, get local changes
                ClientSyncChanges clientChanges;
                (context, clientChanges) = await this.LocalOrchestrator.InternalGetChangesAsync(clientScopeInfo, context, default, default, cancellationToken, progress).ConfigureAwait(false);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // If we are in reinit mode, force scope last server sync timestamp to null
                if (context.SyncType == SyncType.Reinitialize || context.SyncType == SyncType.ReinitializeWithUpload)
                    clientScopeInfo.LastServerSyncTimestamp = null;

                // Get if we need to get all rows from the datasource
                var fromScratch = clientScopeInfo.IsNewScope || context.SyncType == SyncType.Reinitialize || context.SyncType == SyncType.ReinitializeWithUpload;

                // IF is new and we have a snapshot directory, try to apply a snapshot
                if (fromScratch)
                {
                    // Get snapshot files
                    long snapRemoteClientTimestamp;
                    BatchInfo snapServerBatchInfo;
                    DatabaseChangesSelected snapDatabaseChangesSelected;

                    (context, snapRemoteClientTimestamp, snapServerBatchInfo, snapDatabaseChangesSelected)
                        = await this.RemoteOrchestrator.InternalGetSnapshotAsync(serverScopeInfo, context, default, default, cancellationToken, progress).ConfigureAwait(false);

                    // Apply snapshot
                    if (snapServerBatchInfo != null)
                    {
                        (context, result.SnapshotChangesAppliedOnClient, clientScopeInfo) = await this.LocalOrchestrator.InternalApplySnapshotAsync(
                            clientScopeInfo, context, snapServerBatchInfo, clientChanges.ClientTimestamp, snapRemoteClientTimestamp, snapDatabaseChangesSelected, default, default, cancellationToken, progress).ConfigureAwait(false);
                    }
                }

                // Get if we have already applied a snapshot, so far we don't need to reset table even if we are i Reinitialize Mode
                var snapshotApplied = result.SnapshotChangesAppliedOnClient != null;

                context.ProgressPercentage = 0.3;

                // server changes selected
                ServerSyncChanges serverSyncChanges;
                // client changes applied on server
                DatabaseChangesApplied serverChangesApplied;
                ConflictResolutionPolicy serverResolutionPolicy;

                (context, serverSyncChanges, serverChangesApplied, serverResolutionPolicy) = await this.RemoteOrchestrator.InternalApplyThenGetChangesAsync(clientScopeInfo, context, clientChanges.ClientBatchInfo, default, default, cancellationToken, progress).ConfigureAwait(false);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Policy is always Server policy, so reverse this policy to get the client policy
                var reverseConflictResolutionPolicy = serverResolutionPolicy == ConflictResolutionPolicy.ServerWins ? ConflictResolutionPolicy.ClientWins : ConflictResolutionPolicy.ServerWins;

                // apply is 25%
                context.ProgressPercentage = 0.75;
                var clientChangesApplied = await this.LocalOrchestrator.InternalApplyChangesAsync(
                    clientScopeInfo, context, serverSyncChanges.ServerBatchInfo,
                    clientChanges.ClientTimestamp, serverSyncChanges.RemoteClientTimestamp, reverseConflictResolutionPolicy, snapshotApplied,
                    serverSyncChanges.ServerChangesSelected, default, default, cancellationToken, progress).ConfigureAwait(false);

                completeTime = DateTime.UtcNow;
                this.LocalOrchestrator.CompleteTime = completeTime;
                this.RemoteOrchestrator.CompleteTime = completeTime;

                result.CompleteTime = completeTime;

                // All clients changes selected
                result.ClientChangesSelected = clientChanges.ClientChangesSelected;
                result.ServerChangesSelected = serverSyncChanges.ServerChangesSelected;
                result.ChangesAppliedOnClient = clientChangesApplied.ChangesApplied;
                result.ChangesAppliedOnServer = serverChangesApplied;

                // Begin session
                context.ProgressPercentage = 1;
                context = await this.LocalOrchestrator.InternalEndSessionAsync(context, cancellationToken, progress).ConfigureAwait(false);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

            }

            catch (SyncException se)
            {
                this.Options.Logger.LogError(SyncEventsId.Exception, se, se.TypeName);
                throw;
            }
            catch (Exception ex)
            {
                this.Options.Logger.LogCritical(SyncEventsId.Exception, ex, ex.Message);
                throw new SyncException(ex, SyncStage.None);
            }
            finally
            {
                // End the current session
                this.SessionState = SyncSessionState.Ready;
                this.SessionStateChanged?.Invoke(this, this.SessionState);
                // unlock sync since it's over
                GC.Collect();
                GC.WaitForPendingFinalizers();
                UnlockSync();
            }
            //});

            return result;
        }



        /// <summary>
        /// Set local existing database as synced.
        /// Initial rows from server will not be downloaded when this method is called.
        /// You can mark local rows to be downloaded on next call to SynchronizeAsync()
        /// </summary>
        /// <param name="remoteClientTimestamp">
        /// Specify the server timestamp bound for retrieving rows from server
        /// If set to null, the highest value from server is retrieved. So far, no rows from server will be downloaded on next call to SynchronizeAsync()
        /// </param>
        /// <param name="markRows">
        /// Mark local rows to be uploaded on next call to SynchronizeAsync()
        /// </param>
        //public async Task SetSynchronizedAsync(long? remoteClientTimestamp = default, bool markRows = false, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        //{
        //    // checkpoints dates
        //    var startTime = DateTime.UtcNow;
        //    var completeTime = DateTime.UtcNow;

        //    // Create a logger
        //    var logger = this.Options.Logger ?? new SyncLogger().AddDebug();

        //    // Lock sync to prevent multi call to sync at the same time
        //    LockSync();

        //    // Context, used to back and forth data between servers
        //    var context = new SyncContext(Guid.NewGuid(), this.ScopeName)
        //    {
        //        // if any parameters, set in context
        //        Parameters = this.Parameters,
        //        // set sync type (Normal, Reinitialize, ReinitializeWithUpload)
        //        SyncType = SyncType.Normal
        //    };

        //    // Result, with sync results stats.
        //    var result = new SyncResult(context.SessionId)
        //    {
        //        // set start time
        //        StartTime = startTime,
        //        CompleteTime = completeTime,
        //    };

        //    this.SessionState = SyncSessionState.Synchronizing;
        //    this.SessionStateChanged?.Invoke(this, this.SessionState);


        //    await Task.Run(async () =>
        //    {
        //        try
        //        {
        //            if (cancellationToken.IsCancellationRequested)
        //                cancellationToken.ThrowIfCancellationRequested();

        //            // Internal set the good reference. Don't use the SetContext method here
        //            this.LocalOrchestrator.syncContext = context;
        //            this.RemoteOrchestrator.syncContext = context;
        //            this.LocalOrchestrator.StartTime = startTime;
        //            this.RemoteOrchestrator.StartTime = startTime;

        //            // Begin session
        //            await this.LocalOrchestrator.BeginSessionAsync(this.ScopeName, cancellationToken, progress).ConfigureAwait(false);

        //            if (cancellationToken.IsCancellationRequested)
        //                cancellationToken.ThrowIfCancellationRequested();

        //            // .....
        //            // Get the scope
        //            var clientScopeInfo = await this.LocalOrchestrator.GetClientScopeAsync(default, default, cancellationToken, progress).ConfigureAwait(false);

        //            //if (!clientScopeInfo.IsNewScope && clientScopeInfo.Schema != null)
        //            //    throw new CantInitiliazeAnAlreadySyncDatabaseException();

        //            context.ProgressPercentage = 0.2;

        //            // local database becomes a not new database
        //            var isNewScope = false;

        //            // get remote client timestamp 
        //            if (!remoteClientTimestamp.HasValue)
        //                remoteClientTimestamp = await this.RemoteOrchestrator.GetLocalTimestampAsync(default, default, cancellationToken, progress).ConfigureAwait(false);

        //            context.ProgressPercentage = 0.4;

        //            // Get the server scope info
        //            var serverScopeInfo = await this.RemoteOrchestrator.GetServerScopeAsync(default, default, cancellationToken, progress).ConfigureAwait(false);

        //            // if serverscopeinfo is a new, because we never run any sync before, grab schema and affect setup
        //            if (serverScopeInfo.Setup == null && serverScopeInfo.Schema == null)
        //            {
        //                // 1) Get Schema from remote provider
        //                var schema = await this.InternalGetSchemaAsync(scopeName, this.Setup, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //                // 2) Provision
        //                var provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;
        //                serverScopeInfo = await InternalProvisionAsync(scopeName, false, schema, provision, serverScopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false) as ServerScopeInfo;
        //            }

        //            // TODO : if serverScope.Schema is null, should we Provision here ?

        //            // Affect local setup since the setup could potentially comes from Web server
        //            // Affect local setup (equivalent to this.Setup)
        //            if (!this.Setup.EqualsByProperties(serverScopeInfo.Setup) && !this.Setup.HasTables)
        //                this.LocalOrchestrator.Setup = serverScopeInfo.Setup;

        //            context.ProgressPercentage = 0.6;

        //            // Provision the local database
        //            var provision = SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;
        //            clientScopeInfo = await this.LocalOrchestrator.ProvisionAsync(serverScopeInfo.Schema, provision, false, clientScopeInfo, default, default, cancellationToken, progress).ConfigureAwait(false);

        //            context.ProgressPercentage = 0.8;

        //            // Get the local timestamp
        //            var localTs = await this.LocalOrchestrator.GetLocalTimestampAsync(default, default, cancellationToken, progress).ConfigureAwait(false);

        //            if (markRows)
        //                await this.LocalOrchestrator.UpdateUntrackedRowsAsync(serverScopeInfo.Schema, default, default, cancellationToken, progress).ConfigureAwait(false);

        //            // generate the new scope item
        //            clientScopeInfo.IsNewScope = isNewScope;
        //            clientScopeInfo.LastSync = isNewScope ? null : DateTime.Now;
        //            foreach (var table in clientScopeInfo.Setup.Tables)
        //            {
        //                table.LastSyncTimestamp = localTs;
        //                table.LastServerSyncTimestamp = remoteClientTimestamp;
        //            }

        //            clientScopeInfo.LastSyncDuration = 1;
        //            clientScopeInfo.Setup = serverScopeInfo.Setup;
        //            clientScopeInfo.Schema = serverScopeInfo.Schema;

        //            await this.LocalOrchestrator.SaveClientScopeAsync(clientScopeInfo, default, default, cancellationToken, progress).ConfigureAwait(false);
        //            completeTime = DateTime.UtcNow;
        //            this.LocalOrchestrator.CompleteTime = completeTime;
        //            this.RemoteOrchestrator.CompleteTime = completeTime;

        //            result.CompleteTime = completeTime;

        //            // Begin session
        //            context.ProgressPercentage = 1;
        //            await this.LocalOrchestrator.EndSessionAsync(this.ScopeName, cancellationToken, progress).ConfigureAwait(false);

        //            if (cancellationToken.IsCancellationRequested)
        //                cancellationToken.ThrowIfCancellationRequested();


        //        }
        //        catch (SyncException se)
        //        {
        //            this.Options.Logger.LogError(SyncEventsId.Exception, se, se.TypeName);
        //            throw;
        //        }
        //        catch (Exception ex)
        //        {
        //            this.Options.Logger.LogCritical(SyncEventsId.Exception, ex, ex.Message);
        //            throw new SyncException(ex, SyncStage.None);
        //        }
        //        finally
        //        {
        //            // End the current session
        //            this.SessionState = SyncSessionState.Ready;
        //            this.SessionStateChanged?.Invoke(this, this.SessionState);
        //            // unlock sync since it's over
        //            GC.Collect();
        //            GC.WaitForPendingFinalizers();
        //            UnlockSync();
        //        }
        //    });
        //}

        // --------------------------------------------------------------------
        // Dispose
        // --------------------------------------------------------------------

        /// <summary>
        /// Releases all resources used by the <see cref="T:Microsoft.Synchronization.Data.DbSyncBatchInfo" />.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            //GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Releases the unmanaged resources used 
        /// </summary>
        protected virtual void Dispose(bool cleanup)
        {

        }
    }
}
