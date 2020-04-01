using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public class LocalOrchestrator : BaseOrchestrator
    {
        public override SyncSide Side => SyncSide.ClientSide;

        /// <summary>
        /// Create a local orchestrator, used to orchestrates the whole sync on the client side
        /// </summary>
        public LocalOrchestrator(CoreProvider provider, SyncOptions options, SyncSetup setup, string scopeName = SyncOptions.DefaultScopeName)
           : base(provider, options, setup, scopeName)
        {
        }

        /// <summary>
        /// Called by the  to indicate that a 
        /// synchronization session has started.
        /// </summary>
        public async Task BeginSessionAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();

            ctx.SyncStage = SyncStage.BeginSession;

            // Progress & interceptor
            var sessionArgs = new SessionBeginArgs(ctx, null, null);
            await this.InterceptAsync(sessionArgs, cancellationToken).ConfigureAwait(false);
            this.ReportProgress(ctx, progress, sessionArgs);
        }

        /// <summary>
        /// Called when the sync is over
        /// </summary>
        public async Task EndSessionAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();

            ctx.SyncStage = SyncStage.EndSession;

            // Progress & interceptor
            var sessionArgs = new SessionEndArgs(ctx, null, null);
            await this.InterceptAsync(sessionArgs, cancellationToken).ConfigureAwait(false);
            this.ReportProgress(ctx, progress, sessionArgs);
        }

        /// <summary>
        /// Get the local configuration, ensures the local scope is created
        /// </summary>
        /// <returns>current context, the local scope info created or get from the database and the configuration from the client if changed </returns>
        public async Task<ScopeInfo> EnsureScopeAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();
            ScopeInfo localScope = null;
            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    ctx.SyncStage = SyncStage.ScopeLoading;

                    // Open connection
                    await this.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    using (var transaction = connection.BeginTransaction())
                    {
                        await this.InterceptAsync(new TransactionOpenedArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        await this.InterceptAsync(new ScopeLoadingArgs(ctx, this.ScopeName, this.Options.ScopeInfoTableName, connection, transaction), cancellationToken).ConfigureAwait(false);

                        ctx = await this.Provider.EnsureClientScopeAsync(ctx, this.Options.ScopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        (ctx, localScope) = await this.Provider.GetClientScopeAsync(ctx, this.Options.ScopeInfoTableName, this.ScopeName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        transaction.Commit();
                    }

                    ctx.SyncStage = SyncStage.ScopeLoaded;

                    await this.CloseConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    var scopeArgs = new ScopeLoadedArgs(ctx, localScope, connection);
                    await this.InterceptAsync(scopeArgs, cancellationToken).ConfigureAwait(false);
                    this.ReportProgress(ctx, progress, scopeArgs);

                }
                catch (Exception ex)
                {
                    RaiseError(ex);
                }
                finally
                {
                    if (connection != null && connection.State == ConnectionState.Open)
                        connection.Close();
                }
                return localScope;
            }
        }

        /// <summary>
        /// Input : localScopeInfo
        /// </summary>
        /// <returns></returns>
        public async Task<(long clientTimestamp, BatchInfo clientBatchInfo, DatabaseChangesSelected clientChangesSelected)>
            GetChangesAsync(ScopeInfo localScopeInfo, CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Output
            long clientTimestamp = 0L;
            BatchInfo clientBatchInfo = null;
            DatabaseChangesSelected clientChangesSelected = null;


            // Get context or create a new one
            var ctx = this.GetContext();
            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    ctx.SyncStage = SyncStage.ChangesSelecting;

                    // Check if we have a schema in local scope info
                    if (localScopeInfo == null || string.IsNullOrEmpty(localScopeInfo.Schema))
                        throw new ArgumentNullException("can't get changes if we don't have a scope info fill with a schema stored in it");

                    // Open connection
                    await this.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    using (var transaction = connection.BeginTransaction())
                    {
                        await this.InterceptAsync(new TransactionOpenedArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // Get concrete schema
                        var schema = JsonConvert.DeserializeObject<SyncSet>(localScopeInfo.Schema);

                        // On local, we don't want to chase rows from "others" 
                        // We just want our local rows, so we dont exclude any remote scope id, by setting scope id to NULL
                        Guid? remoteScopeId = null;
                        // lastSyncTS : get lines inserted / updated / deteleted after the last sync commited
                        var lastSyncTS = localScopeInfo.LastSyncTimestamp;
                        // isNew : If isNew, lasttimestamp is not correct, so grab all
                        var isNew = localScopeInfo.IsNewScope;
                        //Direction set to Upload
                        ctx.SyncWay = SyncWay.Upload;

                        // JUST before the whole process, get the timestamp, to be sure to 
                        // get rows inserted / updated elsewhere since the sync is not over
                        clientTimestamp = await this.Provider.GetLocalTimestampAsync(ctx, connection, transaction, cancellationToken, progress);

                        // Check if the provider is not outdated
                        var isOutdated = this.Provider.IsRemoteOutdated();

                        // Get a chance to make the sync even if it's outdated
                        if (isOutdated)
                        {
                            var outdatedArgs = new OutdatedArgs(ctx, connection, transaction);

                            // Interceptor
                            await this.InterceptAsync(outdatedArgs, cancellationToken).ConfigureAwait(false);

                            if (outdatedArgs.Action != OutdatedAction.Rollback)
                                ctx.SyncType = outdatedArgs.Action == OutdatedAction.Reinitialize ? SyncType.Reinitialize : SyncType.ReinitializeWithUpload;

                            if (outdatedArgs.Action == OutdatedAction.Rollback)
                                throw new OutOfDateException();
                        }

                        // Creating the message
                        var message = new MessageGetChangesBatch(remoteScopeId, localScopeInfo.Id, isNew, lastSyncTS, schema, this.Options.BatchSize, this.Options.BatchDirectory);

                        // Call interceptor
                        await this.InterceptAsync(new DatabaseChangesSelectingArgs(ctx, message, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // Locally, if we are new, no need to get changes
                        if (isNew)
                            (clientBatchInfo, clientChangesSelected) = await this.Provider.GetEmptyChangesAsync(message).ConfigureAwait(false);
                        else
                            (ctx, clientBatchInfo, clientChangesSelected) = await this.Provider.GetChangeBatchAsync(ctx, message, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        transaction.Commit();
                    }

                    // Event progress & interceptor
                    ctx.SyncStage = SyncStage.ChangesSelected;

                    await this.CloseConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    var tableChangesSelectedArgs = new DatabaseChangesSelectedArgs(ctx, clientTimestamp, clientBatchInfo, clientChangesSelected, connection);
                    this.ReportProgress(ctx, progress, tableChangesSelectedArgs);
                    await this.InterceptAsync(tableChangesSelectedArgs, cancellationToken).ConfigureAwait(false);

                }
                catch (Exception ex)
                {
                    RaiseError(ex);
                }
                finally
                {
                    if (connection != null && connection.State != ConnectionState.Closed)
                        connection.Close();
                }

                return (clientTimestamp, clientBatchInfo, clientChangesSelected);
            }
        }

        /// <summary>
        /// Apply changes locally
        /// </summary>
        /// <param name="scope">client scope</param>
        /// <param name="schema">schema used in the sync process</param>
        /// <param name="serverBatchInfo">batch from server to apply locally</param>
        /// <param name="clientTimestamp">last sync timestamp from client</param>
        /// <param name="remoteClientTimestamp">last sync timestamp from server</param>
        /// <param name="policy">policy used to apply locally. We don't use this.Option.ConflictResolutionPolicy because it could be the reverse policy</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <param name="progress">Progress args</param>
        /// <returns></returns>
        public async Task<(DatabaseChangesApplied ChangesApplied, ScopeInfo ClientScopeInfo)>
            ApplyChangesAsync(ScopeInfo scope, SyncSet schema, BatchInfo serverBatchInfo,
                              long clientTimestamp, long remoteClientTimestamp, ConflictResolutionPolicy policy,
                              CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();
            DatabaseChangesApplied clientChangesApplied = null;

            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    ctx.SyncStage = SyncStage.ChangesApplying;

                    // Open connection
                    await this.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    // Create a transaction
                    using (var transaction = connection.BeginTransaction())
                    {
                        await this.InterceptAsync(new TransactionOpenedArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // lastSyncTS : apply lines only if they are not modified since last client sync
                        var lastSyncTS = scope.LastSyncTimestamp;
                        // isNew : if IsNew, don't apply deleted rows from server
                        var isNew = scope.IsNewScope;

                        // Create the message containing everything needed to apply changes
                        var applyChanges = new MessageApplyChanges(scope.Id, Guid.Empty, isNew, lastSyncTS, schema, policy,
                                        this.Options.DisableConstraintsOnApplyChanges,
                                        this.Options.UseBulkOperations, this.Options.CleanMetadatas, this.Options.CleanFolder,
                                        serverBatchInfo);

                        // call interceptor
                        await this.InterceptAsync(new DatabaseChangesApplyingArgs(ctx, applyChanges, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // Call apply changes on provider
                        (ctx, clientChangesApplied) = await this.Provider.ApplyChangesAsync(ctx, applyChanges, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        // check if we need to delete metadatas
                        if (this.Options.CleanMetadatas && clientChangesApplied.TotalAppliedChanges > 0)
                            await this.Provider.DeleteMetadatasAsync(ctx, schema, lastSyncTS, connection, transaction, cancellationToken, progress);

                        // now the sync is complete, remember the time
                        this.CompleteTime = DateTime.UtcNow;

                        // generate the new scope item
                        scope.IsNewScope = false;
                        scope.LastSync = this.CompleteTime;
                        scope.LastSyncTimestamp = clientTimestamp;
                        scope.LastServerSyncTimestamp = remoteClientTimestamp;
                        scope.LastSyncDuration = this.CompleteTime.Value.Subtract(this.StartTime.Value).Ticks;

                        // Write scopes locally
                        ctx = await this.Provider.WriteClientScopeAsync(ctx, this.Options.ScopeInfoTableName, scope, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);
                        transaction.Commit();
                    }

                    ctx.SyncStage = SyncStage.ChangesApplied;

                    await this.CloseConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    var databaseChangesAppliedArgs = new DatabaseChangesAppliedArgs(ctx, clientChangesApplied, connection);
                    await this.InterceptAsync(databaseChangesAppliedArgs, cancellationToken).ConfigureAwait(false);
                    this.ReportProgress(ctx, progress, databaseChangesAppliedArgs);
                }
                catch (Exception ex)
                {
                    RaiseError(ex);
                }
                finally
                {
                    if (connection != null && connection.State != ConnectionState.Closed)
                        connection.Close();
                }
                return (clientChangesApplied, scope);
            }
        }


        public async Task<(DatabaseChangesApplied snapshotChangesApplied, ScopeInfo clientScopeInfo)> ApplySnapshotAsync(ScopeInfo clientScopeInfo, BatchInfo serverBatchInfo, long clientTimestamp, long remoteClientTimestamp, CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            if (serverBatchInfo == null || !await serverBatchInfo.HasDataAsync())
                return (new DatabaseChangesApplied(), clientScopeInfo);

            // Get context or create a new one
            var ctx = this.GetContext();

            ctx.SyncStage = SyncStage.SnapshotApplying;
            await this.InterceptAsync(new SnapshotApplyingArgs(ctx), cancellationToken).ConfigureAwait(false);

            // Get schema
            var schema = JsonConvert.DeserializeObject<SyncSet>(clientScopeInfo.Schema);

            if (schema == null)
                throw new ArgumentNullException(nameof(schema));

            // Applying changes and getting the new client scope info
            var (changesApplied, newClientScopeInfo) = await this.ApplyChangesAsync(clientScopeInfo, schema, serverBatchInfo,
                    clientTimestamp, remoteClientTimestamp, ConflictResolutionPolicy.ServerWins, cancellationToken, progress);

            // Because we have initialize everything here (if syncType != Normal)
            // We don't want to download everything from server, so change syncType to Normal
            ctx.SyncType = SyncType.Normal;

            // Progress & Interceptor
            ctx.SyncStage = SyncStage.SnapshotApplied;
            var snapshotAppliedArgs = new SnapshotAppliedArgs(ctx);
            this.ReportProgress(ctx, progress, snapshotAppliedArgs);
            await this.InterceptAsync(snapshotAppliedArgs, cancellationToken).ConfigureAwait(false);

            return (changesApplied, newClientScopeInfo);

        }

    }
}
