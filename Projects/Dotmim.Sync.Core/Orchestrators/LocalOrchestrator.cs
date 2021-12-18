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

            var connection = this.Provider.CreateConnection();

            // Progress & interceptor
            await this.InterceptAsync(new SessionBeginArgs(ctx, connection), progress, cancellationToken).ConfigureAwait(false);
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
            var connection = this.Provider.CreateConnection();

            // Progress & interceptor
            await this.InterceptAsync(new SessionEndArgs(ctx, connection), progress, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Get changes from local database
        /// </summary>
        /// <returns></returns>
        public async Task<(long ClientTimestamp, BatchInfo ClientBatchInfo, DatabaseChangesSelected ClientChangesSelected)>
            GetChangesAsync(ScopeInfo localScopeInfo = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            try
            {
                await using var runner = await this.GetConnectionAsync(SyncStage.ChangesSelecting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                // Output
                long clientTimestamp = 0L;
                BatchInfo clientBatchInfo = null;
                DatabaseChangesSelected clientChangesSelected = null;

                // Get local scope, if not provided 
                if (localScopeInfo == null)
                {
                    var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                    var exists = await this.InternalExistsScopeInfoTableAsync(this.GetContext(), DbScopeType.Client, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (!exists)
                        await this.InternalCreateScopeInfoTableAsync(this.GetContext(), DbScopeType.Client, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    localScopeInfo = await this.InternalGetScopeAsync<ScopeInfo>(this.GetContext(), DbScopeType.Client, this.ScopeName, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
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
                this.GetContext().SyncWay = SyncWay.Upload;

                // JUST before the whole process, get the timestamp, to be sure to 
                // get rows inserted / updated elsewhere since the sync is not over
                clientTimestamp = await this.InternalGetLocalTimestampAsync(this.GetContext(), runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // Creating the message
                var message = new MessageGetChangesBatch(remoteScopeId, localScopeInfo.Id, isNew, lastSyncTS, localScopeInfo.Schema, this.Setup,
                                                         this.Options.BatchSize, this.Options.BatchDirectory, null, this.Provider.SupportsMultipleActiveResultSets, this.Options.LocalSerializerFactory);

                var ctx = this.GetContext();
                // Locally, if we are new, no need to get changes
                if (isNew)
                    (clientBatchInfo, clientChangesSelected) = await this.InternalGetEmptyChangesAsync(message).ConfigureAwait(false);
                else
                    (ctx, clientBatchInfo, clientChangesSelected) = await this.InternalGetChangesAsync(ctx, message, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return (clientTimestamp, clientBatchInfo, clientChangesSelected);

            }
            catch (Exception ex)
            {
                throw GetSyncError(ex);
            }
        }


        /// <summary>
        /// Get estimated changes from local database to be sent to the server
        /// </summary>
        /// <returns></returns>
        public async Task<(long ClientTimestamp, DatabaseChangesSelected ClientChangesSelected)>
            GetEstimatedChangesCountAsync(ScopeInfo localScopeInfo = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(SyncStage.MetadataCleaning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                // Get local scope, if not provided 
                if (localScopeInfo == null)
                {
                    var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                    var exists = await this.InternalExistsScopeInfoTableAsync(this.GetContext(), DbScopeType.Client, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (!exists)
                        await this.InternalCreateScopeInfoTableAsync(this.GetContext(), DbScopeType.Client, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    localScopeInfo = await this.InternalGetScopeAsync<ScopeInfo>(this.GetContext(), DbScopeType.Client, this.ScopeName, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
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
                this.GetContext().SyncWay = SyncWay.Upload;

                // Output
                // JUST before the whole process, get the timestamp, to be sure to 
                // get rows inserted / updated elsewhere since the sync is not over
                var clientTimestamp = await this.InternalGetLocalTimestampAsync(this.GetContext(), runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // Creating the message
                // Since it's an estimated count, we don't need to create batches, so we hard code the batchsize to 0
                var message = new MessageGetChangesBatch(remoteScopeId, localScopeInfo.Id, isNew, lastSyncTS, localScopeInfo.Schema, this.Setup, 0, this.Options.BatchDirectory, null, this.Provider.SupportsMultipleActiveResultSets, this.Options.LocalSerializerFactory);

                DatabaseChangesSelected clientChangesSelected;
                // Locally, if we are new, no need to get changes
                var ctx = this.GetContext();
                if (isNew)
                    clientChangesSelected = new DatabaseChangesSelected();
                else
                    (ctx, clientChangesSelected) = await this.InternalGetEstimatedChangesCountAsync(ctx, message, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return (clientTimestamp, clientChangesSelected);

            }
            catch (Exception ex)
            {
                throw GetSyncError(ex);
            }
        }

        /// <summary>
        /// Apply changes locally
        /// </summary>
        internal async Task<(DatabaseChangesApplied ChangesApplied, ScopeInfo ClientScopeInfo)>
            ApplyChangesAsync(ScopeInfo scope, SyncSet schema, BatchInfo serverBatchInfo,
                              long clientTimestamp, long remoteClientTimestamp, ConflictResolutionPolicy policy, bool snapshotApplied, DatabaseChangesSelected allChangesSelected,
                              DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            try
            {
                // lastSyncTS : apply lines only if they are not modified since last client sync
                var lastSyncTS = scope.LastSyncTimestamp;
                // isNew : if IsNew, don't apply deleted rows from server
                var isNew = scope.IsNewScope;
                // We are in downloading mode

                // Create the message containing everything needed to apply changes
                var applyChanges = new MessageApplyChanges(scope.Id, Guid.Empty, isNew, lastSyncTS, schema, this.Setup, policy,
                                this.Options.DisableConstraintsOnApplyChanges, this.Options.CleanMetadatas, this.Options.CleanFolder, snapshotApplied,
                                serverBatchInfo, this.Options.LocalSerializerFactory);

                DatabaseChangesApplied clientChangesApplied;


                await using var runner = await this.GetConnectionAsync(SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                var ctx = this.GetContext();
                ctx.SyncWay = SyncWay.Download;

                // Call apply changes on provider
                (ctx, clientChangesApplied) = await this.InternalApplyChangesAsync(ctx, applyChanges, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();


                // check if we need to delete metadatas
                if (this.Options.CleanMetadatas && clientChangesApplied.TotalAppliedChanges > 0 && lastSyncTS.HasValue)
                    await this.InternalDeleteMetadatasAsync(ctx, schema, this.Setup, lastSyncTS.Value, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

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
                var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                await this.InternalSaveScopeAsync(ctx, DbScopeType.Client, scope, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return (clientChangesApplied, scope);
            }
            catch (Exception ex)
            {
                throw GetSyncError(ex);
            }

        }


        /// <summary>
        /// Apply a snapshot locally
        /// </summary>
        internal async Task<(DatabaseChangesApplied snapshotChangesApplied, ScopeInfo clientScopeInfo)>
            ApplySnapshotAsync(ScopeInfo clientScopeInfo, BatchInfo serverBatchInfo, long clientTimestamp, long remoteClientTimestamp, DatabaseChangesSelected databaseChangesSelected,
            DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (serverBatchInfo == null)
                return (new DatabaseChangesApplied(), clientScopeInfo);

            // Get context or create a new one
            var ctx = this.GetContext();

            ctx.SyncStage = SyncStage.SnapshotApplying;
            await this.InterceptAsync(new SnapshotApplyingArgs(ctx, this.Provider.CreateConnection()), progress, cancellationToken).ConfigureAwait(false);

            if (clientScopeInfo.Schema == null)
                throw new ArgumentNullException(nameof(clientScopeInfo.Schema));

            // Applying changes and getting the new client scope info
            var (changesApplied, newClientScopeInfo) = await this.ApplyChangesAsync(clientScopeInfo, clientScopeInfo.Schema, serverBatchInfo,
                    clientTimestamp, remoteClientTimestamp, ConflictResolutionPolicy.ServerWins, false, databaseChangesSelected, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            await this.InterceptAsync(new SnapshotAppliedArgs(ctx, changesApplied), progress, cancellationToken).ConfigureAwait(false);

            // re-apply scope is new flag
            // to be sure we are calling the Initialize method, even for the delta
            // in that particular case, we want the delta rows coming from the current scope
            newClientScopeInfo.IsNewScope = true;

            return (changesApplied, newClientScopeInfo);

        }

        /// <summary>
        /// Delete all metadatas from tracking tables, based on min timestamp from scope info table
        /// </summary>
        public async Task<DatabaseMetadatasCleaned> DeleteMetadatasAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get the min timestamp, where we can without any problem, delete metadatas
            var clientScopeInfo = await this.GetClientScopeAsync(connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (clientScopeInfo.LastSyncTimestamp == 0)
                return new DatabaseMetadatasCleaned();

            return await base.DeleteMetadatasAsync(clientScopeInfo.LastSyncTimestamp, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
        }


        /// <summary>
        /// Migrate an old setup configuration to a new one. This method is usefull if you are changing your SyncSetup when a database has been already configured previously
        /// </summary>
        public virtual async Task<ScopeInfo> MigrationAsync(SyncSetup oldSetup, SyncSetup newSetup, SyncSet newSchema, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(SyncStage.Migrating, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                // If schema does not have any table, just return
                if (newSchema == null || newSchema.Tables == null || !newSchema.HasTables)
                    throw new MissingTablesException();

                // Migrate the db structure
                await this.InternalMigrationAsync(this.GetContext(), newSchema, oldSetup, newSetup, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // Get Scope Builder
                var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                ScopeInfo localScope = null;

                var exists = await this.InternalExistsScopeInfoTableAsync(this.GetContext(), DbScopeType.Client, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(this.GetContext(), DbScopeType.Client, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                localScope = await this.InternalGetScopeAsync<ScopeInfo>(this.GetContext(), DbScopeType.Client, this.ScopeName, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                localScope.Setup = newSetup;
                localScope.Schema = newSchema;

                await this.InternalSaveScopeAsync(this.GetContext(), DbScopeType.Client, localScope, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return localScope;
            }
            catch (Exception ex)
            {
                throw GetSyncError(ex);
            }
        }
    }
}
