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
        }

        /// <summary>
        /// Called by the  to indicate that a 
        /// synchronization session has started.
        /// </summary>
        public virtual Task BeginSessionAsync(string scopeName = SyncOptions.DefaultScopeName, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            // Create a new context
            var ctx = new SyncContext(Guid.NewGuid(), scopeName);

            return InternalBeginSessionAsync(ctx, cancellationToken, progress);
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
        /// Get changes from local database from a specific scope name
        /// </summary>
        public async Task<ClientSyncChanges>
            GetChangesAsync(string scopeName = SyncOptions.DefaultScopeName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Reading, SyncStage.ChangesSelecting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.Client, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    return default;

                ClientScopeInfo localScopeInfo;
                (context, localScopeInfo) = await this.InternalLoadClientScopeInfoAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (localScopeInfo == null)
                    return default;

                ClientSyncChanges clientChanges = null;
                (context, clientChanges) = await this.InternalGetChangesAsync(localScopeInfo, context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return clientChanges;

            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }
       
        
        
        /// <summary>
        /// Get changes from local database from a specific scope you already fetched from local database
        /// </summary>
        /// <returns></returns>
        internal virtual async Task<(SyncContext context, ClientSyncChanges syncChanges)>
            InternalGetChangesAsync(ClientScopeInfo clientScopeInfo, SyncContext context, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.Reading, SyncStage.ChangesSelecting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                // Output
                long clientTimestamp = 0L;
                BatchInfo clientBatchInfo = null;
                DatabaseChangesSelected clientChangesSelected = null;

                // If no schema in the client scope. Maybe the client scope table does not exists, or we never get the schema from server
                if (clientScopeInfo.Schema == null)
                    throw new MissingLocalOrchestratorSchemaException();

                // On local, we don't want to chase rows from "others" 
                // We just want our local rows, so we dont exclude any remote scope id, by setting scope id to NULL
                Guid? remoteScopeId = null;
                // lastSyncTS : get lines inserted / updated / deteleted after the last sync commited
                var lastTimestamp = clientScopeInfo.LastSyncTimestamp;
                // isNew : If isNew, lasttimestamp is not correct, so grab all
                var isNew = clientScopeInfo.IsNewScope;

                //Direction set to Upload
                context.SyncWay = SyncWay.Upload;

                // JUST before the whole process, get the timestamp, to be sure to 
                // get rows inserted / updated elsewhere since the sync is not over
                (context, clientTimestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // Locally, if we are new, no need to get changes
                if (isNew)
                    (clientBatchInfo, clientChangesSelected) = await this.InternalGetEmptyChangesAsync(clientScopeInfo, this.Options.BatchDirectory).ConfigureAwait(false);
                else
                    (context, clientBatchInfo, clientChangesSelected) = await this.InternalGetChangesAsync(clientScopeInfo, context, isNew, lastTimestamp, remoteScopeId, this.Provider.SupportsMultipleActiveResultSets,
                        this.Options.BatchDirectory, null, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                var changes = new ClientSyncChanges(clientTimestamp, clientBatchInfo, clientChangesSelected);

                await runner.CommitAsync().ConfigureAwait(false);

                return (context, changes);

            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }


        /// <summary>
        /// Get estimated changes from local database to be sent to the server
        /// </summary>
        /// <returns></returns>
        public async Task<ClientSyncChanges>
            GetEstimatedChangesCountAsync(string scopeName = SyncOptions.DefaultScopeName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Reading, SyncStage.MetadataCleaning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.Client, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    return default;

                ClientScopeInfo localScopeInfo;
                (context, localScopeInfo) = await this.InternalLoadClientScopeInfoAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (localScopeInfo == null)
                    return default;

                // If no schema in the client scope. Maybe the client scope table does not exists, or we never get the schema from server
                if (localScopeInfo.Schema == null)
                    throw new MissingLocalOrchestratorSchemaException();

                // On local, we don't want to chase rows from "others" 
                // We just want our local rows, so we dont exclude any remote scope id, by setting scope id to NULL
                Guid? remoteScopeId = null;
                var lastTimestamp = localScopeInfo.LastSyncTimestamp;
                var isNew = localScopeInfo.IsNewScope;

                //Direction set to Upload
                context.SyncWay = SyncWay.Upload;

                // Output
                // JUST before the whole process, get the timestamp, to be sure to 
                // get rows inserted / updated elsewhere since the sync is not over
                long clientTimestamp;
                (context, clientTimestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                DatabaseChangesSelected clientChangesSelected;

                // Locally, if we are new, no need to get changes
                if (isNew)
                    clientChangesSelected = new DatabaseChangesSelected();
                else
                    (context, clientChangesSelected) = await this.InternalGetEstimatedChangesCountAsync(
                        localScopeInfo, context,
                        isNew, lastTimestamp, remoteScopeId, this.Provider.SupportsMultipleActiveResultSets,
                        runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                var changes = new ClientSyncChanges(clientTimestamp, null, clientChangesSelected);

                return changes;

            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
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
