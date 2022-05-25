using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Net.Mime;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public partial class LocalOrchestrator : BaseOrchestrator
    {
        public override SyncSide Side => SyncSide.ClientSide;

        /// <summary>
        /// Create a local orchestrator, used to orchestrates the whole sync on the client side
        /// </summary>
        public LocalOrchestrator(CoreProvider provider, SyncOptions options) : base(provider, options)
        {
            if (provider == null)
                throw new MissingProviderException(nameof(LocalOrchestrator));

        }
        /// <summary>
        /// Create a local orchestrator, used to orchestrates the whole sync on the client side
        /// </summary>
        public LocalOrchestrator(CoreProvider provider) : base(provider, new SyncOptions())
        {
            if (provider == null)
                throw new MissingProviderException(nameof(LocalOrchestrator));
        }

        /// <summary>
        /// Called by the  to indicate that a 
        /// synchronization session has started.
        /// </summary>
        public virtual Task BeginSessionAsync(string scopeName = SyncOptions.DefaultScopeName, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            // Create a new context
            var context = new SyncContext(Guid.NewGuid(), scopeName);

            return InternalBeginSessionAsync(context, cancellationToken, progress);
        }

        internal async Task<SyncContext> InternalBeginSessionAsync(SyncContext context, CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            context.SyncStage = SyncStage.BeginSession;

            var connection = this.Provider.CreateConnection();

            // Progress & interceptor
            await this.InterceptAsync(new SessionBeginArgs(context, connection), progress, cancellationToken).ConfigureAwait(false);

            return context;

        }

        /// <summary>
        /// Called when the sync is over
        /// </summary>
        public Task EndSessionAsync(string scopeName = SyncOptions.DefaultScopeName, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            // Create a new context
            var ctx = new SyncContext(Guid.NewGuid(), scopeName);

            return InternalEndSessionAsync(ctx, cancellationToken, progress);
        }

        /// <summary>
        /// Called when the sync is over
        /// </summary>
        public async Task<SyncContext> InternalEndSessionAsync(SyncContext context, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            context.SyncStage = SyncStage.EndSession;

            var connection = this.Provider.CreateConnection();

            // Progress & interceptor
            await this.InterceptAsync(new SessionEndArgs(context, connection), progress, cancellationToken).ConfigureAwait(false);

            return context;
        }



        /// <summary>
        /// Apply changes locally
        /// </summary>
        internal async Task<(SyncContext context, DatabaseChangesApplied ChangesApplied, ClientScopeInfo ClientScopeInfo)>
            InternalApplyChangesAsync(ClientScopeInfo clientScopeInfo, SyncContext context, BatchInfo serverBatchInfo,
                              long clientTimestamp, long remoteClientTimestamp, ConflictResolutionPolicy policy, bool snapshotApplied, DatabaseChangesSelected allChangesSelected,
                              DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            try
            {
                // lastSyncTS : apply lines only if they are not modified since last client sync
                var lastTimestamp = clientScopeInfo.LastSyncTimestamp;
                // isNew : if IsNew, don't apply deleted rows from server
                var isNew = clientScopeInfo.IsNewScope;
                // We are in downloading mode

                // Create the message containing everything needed to apply changes
                var applyChanges = new MessageApplyChanges(clientScopeInfo.Id, Guid.Empty, isNew, lastTimestamp, clientScopeInfo.Schema, policy,
                                this.Options.DisableConstraintsOnApplyChanges, this.Options.CleanMetadatas, this.Options.CleanFolder, snapshotApplied,
                                serverBatchInfo);

                DatabaseChangesApplied clientChangesApplied;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                context.SyncWay = SyncWay.Download;

                // Call apply changes on provider
                (context, clientChangesApplied) = await this.InternalApplyChangesAsync(clientScopeInfo, context, applyChanges, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // check if we need to delete metadatas
                if (this.Options.CleanMetadatas && clientChangesApplied.TotalAppliedChanges > 0 && lastTimestamp.HasValue)
                {
                    List<ClientScopeInfo> allScopes;
                    (context, allScopes) = await this.InternalLoadAllClientScopesInfoAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    if (allScopes.Count > 0)
                    {
                        // Get the min value from LastSyncTimestamp from all scopes
                        var minLastTimeStamp = allScopes.Min(scope => scope.LastSyncTimestamp.HasValue ? scope.LastSyncTimestamp.Value : Int64.MaxValue);
                        minLastTimeStamp = minLastTimeStamp > lastTimestamp.Value ? lastTimestamp.Value : minLastTimeStamp;

                        (context, _) = await this.InternalDeleteMetadatasAsync(allScopes, context, minLastTimeStamp, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                    }
                }

                // now the sync is complete, remember the time
                this.CompleteTime = DateTime.UtcNow;

                // generate the new scope item
                clientScopeInfo.IsNewScope = false;
                clientScopeInfo.LastSync = this.CompleteTime;
                clientScopeInfo.LastSyncTimestamp = clientTimestamp;
                clientScopeInfo.LastServerSyncTimestamp = remoteClientTimestamp;
                clientScopeInfo.LastSyncDuration = this.CompleteTime.Value.Subtract(context.StartTime).Ticks;

                // Write scopes locally
                (context, clientScopeInfo) = await this.InternalSaveClientScopeInfoAsync(clientScopeInfo, context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return (context, clientChangesApplied, clientScopeInfo);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }


        /// <summary>
        /// Apply a snapshot locally
        /// </summary>
        internal async Task<(SyncContext context, DatabaseChangesApplied snapshotChangesApplied, ClientScopeInfo clientScopeInfo)>
            InternalApplySnapshotAsync(ClientScopeInfo clientScopeInfo, SyncContext context, BatchInfo serverBatchInfo, long clientTimestamp, long remoteClientTimestamp, DatabaseChangesSelected databaseChangesSelected,
            DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                if (serverBatchInfo == null)
                    return (context, new DatabaseChangesApplied(), clientScopeInfo);

                // Get context or create a new one
                context.SyncStage = SyncStage.SnapshotApplying;
                await this.InterceptAsync(new SnapshotApplyingArgs(context, this.Provider.CreateConnection()), progress, cancellationToken).ConfigureAwait(false);

                if (clientScopeInfo.Schema == null)
                    throw new ArgumentNullException(nameof(clientScopeInfo.Schema));

                // Applying changes and getting the new client scope info
                var (syncContext, changesApplied, newClientScopeInfo) = await this.InternalApplyChangesAsync(clientScopeInfo, context, serverBatchInfo,
                        clientTimestamp, remoteClientTimestamp, ConflictResolutionPolicy.ServerWins, false, databaseChangesSelected, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                await this.InterceptAsync(new SnapshotAppliedArgs(context, changesApplied), progress, cancellationToken).ConfigureAwait(false);

                // re-apply scope is new flag
                // to be sure we are calling the Initialize method, even for the delta
                // in that particular case, we want the delta rows coming from the current scope
                newClientScopeInfo.IsNewScope = true;

                return (context, changesApplied, newClientScopeInfo);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }


        ///// <summary>
        ///// Migrate an old setup configuration to a new one. This method is usefull if you are changing your SyncSetup when a database has been already configured previously
        ///// </summary>
        //public virtual async Task<ScopeInfo> MigrationAsync(ScopeInfo oldScopeInfo, ServerScopeInfo newScopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        //{
        //    try
        //    {
        //        await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Writing, SyncStage.Migrating, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
        //        // If schema does not have any table, just return
        //        if (newScopeInfo == null || newScopeInfo.Schema == null || newScopeInfo.Schema.Tables == null || !newScopeInfo.Schema.HasTables)
        //            throw new MissingTablesException();

        //        // Migrate the db structure
        //        await this.InternalMigrationAsync(this.GetContext(), oldScopeInfo, newScopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        ScopeInfo localScope = null;

        //        var exists = await this.InternalExistsScopeInfoTableAsync(this.GetContext(), DbScopeType.Client, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        if (!exists)
        //            await this.InternalCreateScopeInfoTableAsync(this.GetContext(), DbScopeType.Client, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        localScope = await this.InternalGetScopeAsync<ScopeInfo>(this.GetContext(), DbScopeType.Client, this.ScopeName, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        if (localScope == null)
        //        {
        //            localScope = await this.InternalCreateScopeAsync<ScopeInfo>(this.GetContext(), DbScopeType.Client, this.ScopeName, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
        //            localScope = await this.InternalSaveScopeAsync(localScope, DbScopeType.Client, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        }

        //        localScope.Setup = newScopeInfo.Setup;
        //        localScope.Schema = newScopeInfo.Schema;
        //        localScope.Name = newScopeInfo.Name;

        //        await this.InternalSaveScopeAsync(this.GetContext(), DbScopeType.Client, localScope, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        await runner.CommitAsync().ConfigureAwait(false);

        //        return localScope;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw GetSyncError(ex);
        //    }
        //}


    }
}
