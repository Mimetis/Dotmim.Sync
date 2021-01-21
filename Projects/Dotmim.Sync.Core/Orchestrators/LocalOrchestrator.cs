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
        /// Get changes from local database
        /// </summary>
        /// <returns></returns>
        public Task<(long ClientTimestamp, BatchInfo ClientBatchInfo, DatabaseChangesSelected ClientChangesSelected)> GetChangesAsync(ScopeInfo localScopeInfo = null, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.ChangesSelecting, async (ctx, connection, transaction) =>
        {
            // Output
            long clientTimestamp = 0L;
            BatchInfo clientBatchInfo = null;
            DatabaseChangesSelected clientChangesSelected = null;

            // Get local scope, if not provided 
            if (localScopeInfo == null)
            {
                var scopeBuilder = this.Provider.GetScopeBuilder(this.Options.ScopeInfoTableName);

                var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.Client, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(ctx, DbScopeType.Client, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                localScopeInfo = await this.InternalGetScopeAsync<ScopeInfo>(ctx, DbScopeType.Client, this.ScopeName, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            }

            // If no schema in the client scope. Maybe the client scope table does not exists, or we never get the schema from server
            if (localScopeInfo.Schema == null)
                throw new MissingLocalOrchestratorSchemaException();

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
            clientTimestamp = await this.InternalGetLocalTimestampAsync(ctx, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            // Creating the message
            var message = new MessageGetChangesBatch(remoteScopeId, localScopeInfo.Id, isNew, lastSyncTS, localScopeInfo.Schema, this.Setup, this.Options.BatchSize, this.Options.BatchDirectory);

            // Locally, if we are new, no need to get changes
            if (isNew)
                (clientBatchInfo, clientChangesSelected) = await this.InternalGetEmptyChangesAsync(message).ConfigureAwait(false);
            else
                (ctx, clientBatchInfo, clientChangesSelected) = await this.InternalGetChangeBatchAsync(ctx, message, connection, transaction, cancellationToken, progress).ConfigureAwait(false);


            return (clientTimestamp, clientBatchInfo, clientChangesSelected);

        }, cancellationToken);



        /// <summary>
        /// Get estimated changes from local database to be sent to the server
        /// </summary>
        /// <returns></returns>
        public Task<(long ClientTimestamp, DatabaseChangesSelected ClientChangesSelected)> GetEstimatedChangesCountAsync(ScopeInfo localScopeInfo = null, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => RunInTransactionAsync(SyncStage.ChangesSelecting, async (ctx, connection, transaction) =>
            {
                // Output
                long clientTimestamp = 0L;
                DatabaseChangesSelected clientChangesSelected = null;

                // Get local scope, if not provided 
                if (localScopeInfo == null)
                {
                    var scopeBuilder = this.Provider.GetScopeBuilder(this.Options.ScopeInfoTableName);

                    var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.Client, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (!exists)
                        await this.InternalCreateScopeInfoTableAsync(ctx, DbScopeType.Client, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    localScopeInfo = await this.InternalGetScopeAsync<ScopeInfo>(ctx, DbScopeType.Client, this.ScopeName, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                // If no schema in the client scope. Maybe the client scope table does not exists, or we never get the schema from server
                if (localScopeInfo.Schema == null)
                    throw new MissingLocalOrchestratorSchemaException();

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
                clientTimestamp = await this.InternalGetLocalTimestampAsync(ctx, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Creating the message
                // Since it's an estimated count, we don't need to create batches, so we hard code the batchsize to 0
                var message = new MessageGetChangesBatch(remoteScopeId, localScopeInfo.Id, isNew, lastSyncTS, localScopeInfo.Schema, this.Setup, 0, this.Options.BatchDirectory);

                // Locally, if we are new, no need to get changes
                if (isNew)
                    clientChangesSelected = new DatabaseChangesSelected();
                else
                    (ctx, clientChangesSelected) = await this.InternalGetEstimatedChangesCountAsync(ctx, message, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                return (clientTimestamp, clientChangesSelected);
            }, cancellationToken);

        /// <summary>
        /// Apply changes locally
        /// </summary>
        internal Task<(DatabaseChangesApplied ChangesApplied, ScopeInfo ClientScopeInfo)> ApplyChangesAsync(ScopeInfo scope, SyncSet schema, BatchInfo serverBatchInfo,
                              long clientTimestamp, long remoteClientTimestamp, ConflictResolutionPolicy policy,
                              CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.ChangesApplying, async (ctx, connection, transaction) =>
        {
            DatabaseChangesApplied clientChangesApplied = null;

            // lastSyncTS : apply lines only if they are not modified since last client sync
            var lastSyncTS = scope.LastSyncTimestamp;
            // isNew : if IsNew, don't apply deleted rows from server
            var isNew = scope.IsNewScope;
            // We are in downloading mode
            ctx.SyncWay = SyncWay.Download;


            // Create the message containing everything needed to apply changes
            var applyChanges = new MessageApplyChanges(scope.Id, Guid.Empty, isNew, lastSyncTS, schema, this.Setup, policy,
                            this.Options.DisableConstraintsOnApplyChanges,
                            this.Options.UseBulkOperations, this.Options.CleanMetadatas, this.Options.CleanFolder,
                            serverBatchInfo);


            // Call apply changes on provider
            (ctx, clientChangesApplied) = await this.InternalApplyChangesAsync(ctx, applyChanges, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (cancellationToken.IsCancellationRequested)
                cancellationToken.ThrowIfCancellationRequested();

            // check if we need to delete metadatas
            if (this.Options.CleanMetadatas && clientChangesApplied.TotalAppliedChanges > 0)
                await this.InternalDeleteMetadatasAsync(ctx, schema, this.Setup, lastSyncTS, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            // now the sync is complete, remember the time
            this.CompleteTime = DateTime.UtcNow;

            // generate the new scope item
            scope.IsNewScope = false;
            scope.LastSync = this.CompleteTime;
            scope.LastSyncTimestamp = clientTimestamp;
            scope.LastServerSyncTimestamp = remoteClientTimestamp;
            scope.LastSyncDuration = this.CompleteTime.Value.Subtract(this.StartTime.Value).Ticks;
            scope.Setup = this.Setup;

            // Write scopes locally
            var scopeBuilder = this.Provider.GetScopeBuilder(this.Options.ScopeInfoTableName);

            await this.InternalSaveScopeAsync(ctx, DbScopeType.Client, scope, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            return (clientChangesApplied, scope);

        }, cancellationToken);


        /// <summary>
        /// Apply a snapshot locally
        /// </summary>
        internal async Task<(DatabaseChangesApplied snapshotChangesApplied, ScopeInfo clientScopeInfo)> ApplySnapshotAsync(ScopeInfo clientScopeInfo, BatchInfo serverBatchInfo, long clientTimestamp, long remoteClientTimestamp, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (serverBatchInfo == null || !await serverBatchInfo.HasDataAsync(this))
                return (new DatabaseChangesApplied(), clientScopeInfo);

            // Get context or create a new one
            var ctx = this.GetContext();

            ctx.SyncStage = SyncStage.SnapshotApplying;
            await this.InterceptAsync(new SnapshotApplyingArgs(ctx), cancellationToken).ConfigureAwait(false);

            if (clientScopeInfo.Schema == null)
                throw new ArgumentNullException(nameof(clientScopeInfo.Schema));

            // Applying changes and getting the new client scope info
            var (changesApplied, newClientScopeInfo) = await this.ApplyChangesAsync(clientScopeInfo, clientScopeInfo.Schema, serverBatchInfo,
                    clientTimestamp, remoteClientTimestamp, ConflictResolutionPolicy.ServerWins, cancellationToken, progress).ConfigureAwait(false);

            // Because we have initialize everything here (if syncType != Normal)
            // We don't want to download everything from server, so change syncType to Normal
            ctx.SyncType = SyncType.Normal;

            var snapshotAppliedArgs = new SnapshotAppliedArgs(ctx, changesApplied);
            this.ReportProgress(ctx, progress, snapshotAppliedArgs);
            await this.InterceptAsync(snapshotAppliedArgs, cancellationToken).ConfigureAwait(false);

            return (changesApplied, newClientScopeInfo);

        }

        /// <summary>
        /// Delete all metadatas from tracking tables, based on min timestamp from scope info table
        /// </summary>
        public async Task<DatabaseMetadatasCleaned> DeleteMetadatasAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get the min timestamp, where we can without any problem, delete metadatas
            var clientScopeInfo = await this.GetClientScopeAsync(cancellationToken, progress).ConfigureAwait(false);

            if (clientScopeInfo.LastSyncTimestamp == 0)
                return new DatabaseMetadatasCleaned();

            return await base.DeleteMetadatasAsync(clientScopeInfo.LastSyncTimestamp, cancellationToken, progress).ConfigureAwait(false);
        }


        /// <summary>
        /// Migrate an old setup configuration to a new one. This method is usefull if you are changing your SyncSetup when a database has been already configured previously
        /// </summary>
        public virtual Task<bool> MigrationAsync(SyncSetup oldSetup, SyncSet schema, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.Migrating, async (ctx, connection, transaction) =>
        {
            // If schema does not have any table, just return
            if (schema == null || schema.Tables == null || !schema.HasTables)
                throw new MissingTablesException();

            // Migrate the db structure
            await this.InternalMigrationAsync(ctx, schema, oldSetup, this.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            // Get Scope Builder
            var scopeBuilder = this.Provider.GetScopeBuilder(this.Options.ScopeInfoTableName);

            ScopeInfo localScope = null;

            var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.Client, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (!exists)
                await this.InternalCreateScopeInfoTableAsync(ctx, DbScopeType.Client, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            localScope = await this.InternalGetScopeAsync<ScopeInfo>(ctx, DbScopeType.Client, this.ScopeName, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            localScope.Setup = this.Setup;
            localScope.Schema = schema;

            await this.InternalSaveScopeAsync(ctx, DbScopeType.Client, localScope, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            return true;
        });

        /// <summary>
        /// Update all untracked rows from the client database
        /// </summary>
        public virtual Task<bool> UpdateUntrackedRowsAsync(SyncSet schema, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.ChangesApplying, async (ctx, connection, transaction) =>
        {
            // If schema does not have any table, just return
            if (schema == null || schema.Tables == null || !schema.HasTables)
                throw new MissingTablesException();

            // Update untracked rows
            foreach (var table in schema.Tables)
            {
                var syncAdapter = this.GetSyncAdapter(table, this.Setup);
                await syncAdapter.UpdateUntrackedRowsAsync(connection, transaction).ConfigureAwait(false);
            }

            return true;

        }, cancellationToken);

    }
}
