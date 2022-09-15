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
        /// Shortcut to Apply changed conflict occured if remote orchestrator supports it
        /// </summary>
        public void OnApplyChangesConflictOccured(Action<ApplyChangesConflictOccuredArgs> action)
        {
            if (this.RemoteOrchestrator == null)
                throw new InvalidRemoteOrchestratorException();

            this.RemoteOrchestrator.OnApplyChangesConflictOccured(action);
        }

        /// <summary>
        /// Shortcut to Apply changed conflict occured if remote orchestrator supports it
        /// </summary>
        public void OnApplyChangesConflictOccured(Func<ApplyChangesConflictOccuredArgs, Task> action)
        {
            if (this.RemoteOrchestrator == null)
                throw new InvalidRemoteOrchestratorException();

            this.RemoteOrchestrator.OnApplyChangesConflictOccured(action);
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
        /// <param name="options">Sync Options defining options used by your local provider (and remote provider if type of remoteOrchestrator is not a WebRemoteOrchestrator)</param>
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
            ClientSyncChanges clientSyncChanges = null;
            ServerSyncChanges serverSyncChanges = null;

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
                    if (providerType.ToLowerInvariant() == "webclientorchestrator" || providerType.ToLowerInvariant() == "webremotetorchestrator")
                        throw new Exception("Do not set Tables (or SyncSetup) from your client. Please use SyncAgent, without any Tables or SyncSetup. The tables will come from the server side");
                }

                // Begin session
                context = await this.LocalOrchestrator.InternalBeginSessionAsync(context, cancellationToken, progress).ConfigureAwait(false);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();


                // no need to check on every call to SynchronizeAsync
                if (!checkUpgradeDone)
                {
                    var needToUpgrade = await this.LocalOrchestrator.NeedsToUpgradeAsync().ConfigureAwait(false);
                    if (needToUpgrade)
                        throw new Exception("This version needs a client manual upgrade.");

                    checkUpgradeDone = true;
                }

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // on remote orchestrator, get Server scope
                ScopeInfo sScopeInfo;
                bool shouldProvision = false;
                (context, sScopeInfo, shouldProvision) = await this.RemoteOrchestrator.InternalEnsureScopeInfoAsync(context, setup, false, default, default, cancellationToken, progress).ConfigureAwait(false);

                bool isConflicting = false;
                (context, isConflicting, sScopeInfo) = await this.RemoteOrchestrator.InternalIsConflictingSetupAsync(context, setup, sScopeInfo).ConfigureAwait(false);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // On local orchestrator, get scope info.
                ScopeInfo cScopeInfo;
                (context, cScopeInfo) = await this.LocalOrchestrator.InternalEnsureScopeInfoAsync(context, default, default, cancellationToken, progress).ConfigureAwait(false);

                ScopeInfoClient cScopeInfoClient;
                (context, cScopeInfoClient) = await this.LocalOrchestrator.InternalEnsureScopeInfoClientAsync(context, default, default, cancellationToken, progress).ConfigureAwait(false);

                // Check if we have a problem with the SyncSetup local and the one coming from server
                // Let a chance to the user to update the local setup accordingly to the server one
                isConflicting = false;
                (context, isConflicting, cScopeInfo, sScopeInfo) = await this.LocalOrchestrator.InternalIsConflictingSetupAsync(context, setup, cScopeInfo, sScopeInfo).ConfigureAwait(false);

                if (isConflicting)
                {
                    context.ProgressPercentage = 1;
                    context = await this.LocalOrchestrator.InternalEndSessionAsync(context, result, null, null, cancellationToken, progress).ConfigureAwait(false);
                    return result;
                }

                // Register local scope id
                context.ClientId = cScopeInfoClient.Id;

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // we may have created the scope tables and fail before provision
                // check if we have some scope info clients already saved
                if (!shouldProvision)
                    shouldProvision = await this.RemoteOrchestrator.InternalShouldProvisionServerAsync(sScopeInfo, context, default, default, cancellationToken, progress).ConfigureAwait(false);

                // If we just have create the server scope, we need to provision it
                // the WebServerAgent will do this setp on the GetServrScopeInfoAsync task, just before
                // So far, on Http mode, this if() will not be called
                if (shouldProvision)
                {
                    // 2) Provision
                    var provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;
                    (context, sScopeInfo) = await this.RemoteOrchestrator.InternalProvisionServerAsync(sScopeInfo, context, provision, false, default, default, cancellationToken, progress).ConfigureAwait(false);
                }

                // Get operation from server
                SyncOperation operation;
                (context, operation) = await this.RemoteOrchestrator.InternalGetOperationAsync(sScopeInfo, cScopeInfo, cScopeInfoClient, context, default, default, cancellationToken, progress).ConfigureAwait(false);

                if (operation != SyncOperation.Normal)
                {
                    if (operation == SyncOperation.AbortSync)
                    {
                        context.ProgressPercentage = 1;
                        context = await this.LocalOrchestrator.InternalEndSessionAsync(context, result, null, null, cancellationToken, progress).ConfigureAwait(false);
                        return result;
                    }

                    // override order to Deprovision client
                    if (operation == SyncOperation.DeprovisionAndSync && cScopeInfo.Setup != null && cScopeInfo.Setup.HasTables)
                    {
                        var provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;
                        (context, _) = await this.LocalOrchestrator.InternalDeprovisionAsync(cScopeInfo, context, provision, default, default, cancellationToken, progress).ConfigureAwait(false);
                        (context, cScopeInfo) = await this.LocalOrchestrator.InternalProvisionClientAsync(sScopeInfo, cScopeInfo, context, provision, false, default, default, cancellationToken, progress).ConfigureAwait(false);
                    }

                    if (operation == SyncOperation.DropAllAndSync)
                    {
                        await this.LocalOrchestrator.DropAllAsync().ConfigureAwait(false);
                        // Recreated scope info
                        (context, cScopeInfo) = await this.LocalOrchestrator.InternalEnsureScopeInfoAsync(context, default, default, cancellationToken, progress).ConfigureAwait(false);
                    }

                    if (operation == SyncOperation.DropAllAndExit)
                    {
                        await this.LocalOrchestrator.DropAllAsync().ConfigureAwait(false);
                        context.ProgressPercentage = 1;
                        context = await this.LocalOrchestrator.InternalEndSessionAsync(context, result, null, null, cancellationToken, progress).ConfigureAwait(false);
                        return result;
                    }

                    if (operation == SyncOperation.Reinitialize)
                    {
                        context.SyncType = SyncType.Reinitialize;
                    }
                    else if (operation == SyncOperation.ReinitializeWithUpload)
                    {
                        context.SyncType = SyncType.ReinitializeWithUpload;
                    }
                }

                // if client is new or schema does not exists or scope name is a new one
                // We need to get the scope from server
                if (cScopeInfoClient.IsNewScope || cScopeInfo.Schema == null)
                {
                    // Provision local database
                    var provision = SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;
                    (context, cScopeInfo) = await this.LocalOrchestrator.InternalProvisionClientAsync(sScopeInfo, cScopeInfo, context, provision, false, default, default, cancellationToken, progress).ConfigureAwait(false);
                }

                setup ??= cScopeInfo.Setup;

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Before call the changes from localorchestrator, check if we are outdated
                if (sScopeInfo != null && context.SyncType != SyncType.Reinitialize && context.SyncType != SyncType.ReinitializeWithUpload)
                {
                    bool isOutDated = false;
                    (context, isOutDated) = await this.LocalOrchestrator.InternalIsOutDatedAsync(context, cScopeInfoClient, sScopeInfo).ConfigureAwait(false);

                    // if client does not change SyncType to Reinitialize / ReinitializeWithUpload on SyncInterceptor, we raise an error
                    // otherwise, we are outdated, but we can continue, because we have a new mode.
                    if (isOutDated)
                        Debug.WriteLine($"Client id outdated, but we change mode to {context.SyncType}");
                }

                context.ProgressPercentage = 0.1;

                // On local orchestrator, get local changes
                (context, clientSyncChanges) = await this.LocalOrchestrator.InternalGetChangesAsync(cScopeInfo, context, cScopeInfoClient,
                    default, default, cancellationToken, progress).ConfigureAwait(false);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // If we are in reinit mode, force scope last server sync timestamp to null
                if (context.SyncType == SyncType.Reinitialize || context.SyncType == SyncType.ReinitializeWithUpload)
                    cScopeInfoClient.LastServerSyncTimestamp = null;

                // Get if we need to get all rows from the datasource
                var fromScratch = cScopeInfoClient.IsNewScope || context.SyncType == SyncType.Reinitialize || context.SyncType == SyncType.ReinitializeWithUpload;

                // IF is new and we have a snapshot directory, try to apply a snapshot
                if (fromScratch)
                {
                    ServerSyncChanges snapshotServerSyncChanges;
                    (context, snapshotServerSyncChanges)
                        = await this.RemoteOrchestrator.InternalGetSnapshotAsync(sScopeInfo, context, default, default, cancellationToken, progress).ConfigureAwait(false);

                    // Apply snapshot
                    if (snapshotServerSyncChanges?.ServerBatchInfo != null)
                    {
                        (context, clientSyncChanges, cScopeInfoClient) = await this.LocalOrchestrator.InternalApplySnapshotAsync(
                                            cScopeInfo, cScopeInfoClient, context, snapshotServerSyncChanges, clientSyncChanges,
                                            default, default, cancellationToken, progress).ConfigureAwait(false);

                        result.SnapshotChangesAppliedOnClient = clientSyncChanges.ClientChangesApplied;
                    }
                }

                // Get if we have already applied a snapshot, so far we don't need to reset table even if we are i Reinitialize Mode
                var snapshotApplied = result.SnapshotChangesAppliedOnClient != null;

                context.ProgressPercentage = 0.3;

                ConflictResolutionPolicy serverResolutionPolicy;

                (context, serverSyncChanges, serverResolutionPolicy) =
                    await this.RemoteOrchestrator.InternalApplyThenGetChangesAsync(
                        cScopeInfoClient, cScopeInfo, context, clientSyncChanges, default, default, cancellationToken, progress).ConfigureAwait(false);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Policy is always Server policy, so reverse this policy to get the client policy
                var reverseConflictResolutionPolicy = serverResolutionPolicy == ConflictResolutionPolicy.ServerWins ? ConflictResolutionPolicy.ClientWins : ConflictResolutionPolicy.ServerWins;

                // apply is 25%
                context.ProgressPercentage = 0.75;

                (context, clientSyncChanges, cScopeInfoClient) = await this.LocalOrchestrator.InternalApplyChangesAsync(
                        cScopeInfo, cScopeInfoClient, context, serverSyncChanges, clientSyncChanges, reverseConflictResolutionPolicy, snapshotApplied, default, default, 
                        cancellationToken, progress).ConfigureAwait(false);

                // Update back failed rows that should be retried on next sync
                //context = await this.RemoteOrchestrator.InternalApplyBackFailedRowsAsync(cScopeInfo, context, clientSyncChanges.ClientChangesApplied,
                //    default, default, cancellationToken, progress).ConfigureAwait(false);

                completeTime = DateTime.UtcNow;
                this.LocalOrchestrator.CompleteTime = completeTime;
                this.RemoteOrchestrator.CompleteTime = completeTime;

                result.CompleteTime = completeTime;

                // All clients changes selected
                result.ClientChangesSelected = clientSyncChanges.ClientChangesSelected;
                result.ServerChangesSelected = serverSyncChanges.ServerChangesSelected;
                result.ChangesAppliedOnClient = clientSyncChanges.ClientChangesApplied;
                result.ChangesAppliedOnServer = serverSyncChanges.ServerChangesApplied;

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
                context.ProgressPercentage = 1;
                context = await this.LocalOrchestrator.InternalEndSessionAsync(context, result, clientSyncChanges, serverSyncChanges, cancellationToken, progress).ConfigureAwait(false);

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



        // <summary>
        // Set local existing database as synced.
        // Initial rows from server will not be downloaded when this method is called.
        // You can mark local rows to be downloaded on next call to SynchronizeAsync()
        // </summary>
        // <param name="remoteClientTimestamp">
        // Specify the server timestamp bound for retrieving rows from server
        // If set to null, the highest value from server is retrieved. So far, no rows from server will be downloaded on next call to SynchronizeAsync()
        // </param>
        // <param name="markRows">
        // Mark local rows to be uploaded on next call to SynchronizeAsync()
        // </param>
        //public async Task SetSynchronizedAsync(string scopeName = SyncOptions.DefaultScopeName, SyncSetup setup = default, long? remoteClientTimestamp = default, bool markRows = false, SyncParameters parameters = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        //{
        //    // checkpoints dates
        //    var startTime = DateTime.UtcNow;
        //    var completeTime = DateTime.UtcNow;

        //    // Create a logger
        //    var logger = this.Options.Logger ?? new SyncLogger().AddDebug();

        //    // Lock sync to prevent multi call to sync at the same time
        //    LockSync();

        //    // Context, used to back and forth data between servers
        //    var context = new SyncContext(Guid.NewGuid(), scopeName)
        //    {
        //        // if any parameters, set in context
        //        Parameters = parameters,
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


        //    try
        //    {
        //        if (cancellationToken.IsCancellationRequested)
        //            cancellationToken.ThrowIfCancellationRequested();

        //        // Begin session
        //        await this.LocalOrchestrator.BeginSessionAsync(scopeName, cancellationToken, progress).ConfigureAwait(false);

        //        if (cancellationToken.IsCancellationRequested)
        //            cancellationToken.ThrowIfCancellationRequested();

        //        // .....
        //        // Get the scope
        //        var clientScopeInfo = await this.LocalOrchestrator.GetClientScopeInfoAsync(scopeName, default, default, cancellationToken, progress).ConfigureAwait(false);

        //        context.ProgressPercentage = 0.2;

        //        // local database becomes a not new database
        //        var isNewScope = false;

        //        // on remote orchestrator, get Server scope
        //        ServerScopeInfo serverScopeInfo;
        //        (context, serverScopeInfo) = await this.RemoteOrchestrator.InternalGetServerScopeInfoAsync(context, setup, false, default, default, cancellationToken, progress).ConfigureAwait(false);

        //        context.ProgressPercentage = 0.4;

        //        // If we just have create the server scope, we need to provision it
        //        // the WebServerAgent will do this setp on the GetServrScopeInfoAsync task, just before
        //        // So far, on Http mode, this if() will not be called
        //        if (serverScopeInfo != null && serverScopeInfo.IsNewScope)
        //        {
        //            // 2) Provision
        //            var serverProvision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;
        //            (context, serverScopeInfo) = await this.RemoteOrchestrator.InternalProvisionServerAsync(serverScopeInfo, context, serverProvision, false, default, default, cancellationToken, progress).ConfigureAwait(false);
        //        }

        //        context.ProgressPercentage = 0.6;

        //        // get remote client timestamp 
        //        if (!remoteClientTimestamp.HasValue)
        //            remoteClientTimestamp = await this.RemoteOrchestrator.GetLocalTimestampAsync(scopeName, default, default, cancellationToken, progress).ConfigureAwait(false);


        //        // Provision local database
        //        var provision = SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;
        //        (context, clientScopeInfo) = await this.LocalOrchestrator.InternalProvisionClientAsync(serverScopeInfo, clientScopeInfo, context, provision, false, default, default, cancellationToken, progress).ConfigureAwait(false);

        //        if (setup == null)
        //            setup = clientScopeInfo.Setup;

        //        context.ProgressPercentage = 0.8;

        //        // Get the local timestamp
        //        var localTs = await this.LocalOrchestrator.GetLocalTimestampAsync(scopeName, default, default, cancellationToken, progress).ConfigureAwait(false);

        //        if (markRows)
        //            await this.LocalOrchestrator.UpdateUntrackedRowsAsync(scopeName, default, default, cancellationToken, progress).ConfigureAwait(false);

        //        // generate the new scope item
        //        clientScopeInfo.IsNewScope = isNewScope;
        //        clientScopeInfo.LastSync = isNewScope ? null : DateTime.Now;
        //        clientScopeInfo.LastServerSyncTimestamp = remoteClientTimestamp;
        //        clientScopeInfo.LastSyncTimestamp = localTs;
        //        clientScopeInfo.LastSyncDuration = 1;
        //        clientScopeInfo.Setup = serverScopeInfo.Setup;
        //        clientScopeInfo.Schema = serverScopeInfo.Schema;

        //        await this.LocalOrchestrator.SaveClientScopeInfoAsync(clientScopeInfo, default, default, cancellationToken, progress).ConfigureAwait(false);
        //        completeTime = DateTime.UtcNow;
        //        this.LocalOrchestrator.CompleteTime = completeTime;
        //        this.RemoteOrchestrator.CompleteTime = completeTime;

        //        result.CompleteTime = completeTime;

        //        // End session
        //        context.ProgressPercentage = 1;
        //        await this.LocalOrchestrator.EndSessionAsync(result, scopeName, cancellationToken, progress).ConfigureAwait(false);

        //        if (cancellationToken.IsCancellationRequested)
        //            cancellationToken.ThrowIfCancellationRequested();


        //    }
        //    catch (SyncException se)
        //    {
        //        this.Options.Logger.LogError(SyncEventsId.Exception, se, se.TypeName);
        //        throw;
        //    }
        //    catch (Exception ex)
        //    {
        //        this.Options.Logger.LogCritical(SyncEventsId.Exception, ex, ex.Message);
        //        throw new SyncException(ex, SyncStage.None);
        //    }
        //    finally
        //    {
        //        // End the current session
        //        this.SessionState = SyncSessionState.Ready;
        //        this.SessionStateChanged?.Invoke(this, this.SessionState);
        //        // unlock sync since it's over
        //        GC.Collect();
        //        GC.WaitForPendingFinalizers();
        //        UnlockSync();
        //    }
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
        public override string ToString()
        {
            var from = this.LocalOrchestrator?.ToString();
            var to = this.RemoteOrchestrator?.ToString();

            if (!string.IsNullOrEmpty(from) && !string.IsNullOrEmpty(to))
                return $"[{from}] => [{to}]";

            return base.ToString();
        }
    }
}
