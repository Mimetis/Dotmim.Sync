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

            this.logger.LogInformation(SyncEventsId.BeginSession, ctx);

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

            this.logger.LogInformation(SyncEventsId.BeginSession, ctx);

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

            // Call interceptor
            await this.InterceptAsync(new DatabaseChangesSelectingArgs(ctx, message, connection, transaction), cancellationToken).ConfigureAwait(false);

            // Locally, if we are new, no need to get changes
            if (isNew)
                (clientBatchInfo, clientChangesSelected) = await this.InternalGetEmptyChangesAsync(message).ConfigureAwait(false);
            else
                (ctx, clientBatchInfo, clientChangesSelected) = await this.InternalGetChangeBatchAsync(ctx, message, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            var tableChangesSelectedArgs = new DatabaseChangesSelectedArgs(ctx, clientTimestamp, clientBatchInfo, clientChangesSelected, connection);
            this.ReportProgress(ctx, progress, tableChangesSelectedArgs);
            await this.InterceptAsync(tableChangesSelectedArgs, cancellationToken).ConfigureAwait(false);

            return (clientTimestamp, clientBatchInfo, clientChangesSelected);

        },  cancellationToken);



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

                // Call interceptor
                await this.InterceptAsync(new DatabaseChangesSelectingArgs(ctx, message, connection, transaction), cancellationToken).ConfigureAwait(false);

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

            // call interceptor
            await this.InterceptAsync(new DatabaseChangesApplyingArgs(ctx, applyChanges, connection, transaction), cancellationToken).ConfigureAwait(false);

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

            await this.InternalUpsertScopeAsync(ctx, DbScopeType.Client, scope, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            this.logger.LogInformation(SyncEventsId.ApplyChanges, clientChangesApplied);


            var databaseChangesAppliedArgs = new DatabaseChangesAppliedArgs(ctx, clientChangesApplied, connection);
            await this.InterceptAsync(databaseChangesAppliedArgs, cancellationToken).ConfigureAwait(false);
            this.ReportProgress(ctx, progress, databaseChangesAppliedArgs);

            return (clientChangesApplied, scope);

        }, cancellationToken);


        /// <summary>
        /// Apply a snapshot locally
        /// </summary>
        internal async Task<(DatabaseChangesApplied snapshotChangesApplied, ScopeInfo clientScopeInfo)> ApplySnapshotAsync(ScopeInfo clientScopeInfo, BatchInfo serverBatchInfo, long clientTimestamp, long remoteClientTimestamp, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            // TODO : this value is ovewritten in this.ApplyChangesAsync, 10 lines later...
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            if (serverBatchInfo == null || !await serverBatchInfo.HasDataAsync(this))
                return (new DatabaseChangesApplied(), clientScopeInfo);

            // Get context or create a new one
            var ctx = this.GetContext();

            ctx.SyncStage = SyncStage.SnapshotApplying;
            await this.InterceptAsync(new SnapshotApplyingArgs(ctx), cancellationToken).ConfigureAwait(false);

            var connection = this.Provider.CreateConnection();

            this.logger.LogDebug(SyncEventsId.ApplySnapshot, new { connection.Database, ClientTimestamp = clientTimestamp, RemoteClientTimestamp = remoteClientTimestamp });

            if (clientScopeInfo.Schema == null)
                throw new ArgumentNullException(nameof(clientScopeInfo.Schema));

            // Applying changes and getting the new client scope info
            var (changesApplied, newClientScopeInfo) = await this.ApplyChangesAsync(clientScopeInfo, clientScopeInfo.Schema, serverBatchInfo,
                    clientTimestamp, remoteClientTimestamp, ConflictResolutionPolicy.ServerWins, cancellationToken, progress).ConfigureAwait(false);

            // Because we have initialize everything here (if syncType != Normal)
            // We don't want to download everything from server, so change syncType to Normal
            ctx.SyncType = SyncType.Normal;

            this.logger.LogInformation(SyncEventsId.ApplyChanges, changesApplied);

            var snapshotAppliedArgs = new SnapshotAppliedArgs(ctx);
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
        public virtual async Task MigrationAsync(SyncSetup oldSetup, SyncSet schema, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();

            using var connection = this.Provider.CreateConnection();

            try
            {
                ctx.SyncStage = SyncStage.Migrating;

                // If schema does not have any table, just return
                if (schema == null || schema.Tables == null || !schema.HasTables)
                    throw new MissingTablesException();

                // Open connection
                await this.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                SyncProvision provision = SyncProvision.None;

                // Create a transaction
                using (var transaction = connection.BeginTransaction())
                {
                    await this.InterceptAsync(new TransactionOpenedArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                    // Launch InterceptAsync on Migrating
                    await this.InterceptAsync(new DatabaseMigratingArgs(ctx, schema, oldSetup, this.Setup, connection, transaction), cancellationToken).ConfigureAwait(false);

                    this.logger.LogDebug(SyncEventsId.Migration, oldSetup);
                    this.logger.LogDebug(SyncEventsId.Migration, this.Setup);

                    // Migrate the db structure
                    await this.Provider.MigrationAsync(ctx, schema, oldSetup, this.Setup, true, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    // Now call the ProvisionAsync() to provision new tables
                    provision = SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

                    await this.InterceptAsync(new DatabaseProvisioningArgs(ctx, provision, schema, connection, transaction), cancellationToken).ConfigureAwait(false);

                    // Provision new tables if needed
                    // get Database builder
                    var builder = this.Provider.GetDatabaseBuilder();
                    builder.UseChangeTracking = this.Provider.UseChangeTracking;
                    builder.UseBulkProcedures = this.Provider.SupportBulkOperations;

                    // Initialize database if needed
                    await builder.EnsureDatabaseAsync(connection, transaction).ConfigureAwait(false);

                    // Ensure client scope exists. since we need it
                    var scopeBuilder = this.Provider.GetScopeBuilder(this.Options.ScopeInfoTableName);

                    var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.Client, scopeBuilder, connection, transaction, cancellationToken, progress);

                    if (!exists)
                        await this.InternalCreateScopeInfoTableAsync(ctx, DbScopeType.Client, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    // Sorting tables based on dependencies between them
                    var schemaTables = schema.Tables
                        .SortByDependencies(tab => tab.GetRelations()
                            .Select(r => r.GetParentTable()));

                    foreach (var schemaTable in schemaTables)
                    {
                        var tableBuilder = this.Provider.GetTableBuilder(schemaTable, this.Setup);

                        this.logger.LogDebug(SyncEventsId.Provision, schemaTable);

                        if (provision.HasFlag(SyncProvision.Table))
                            await this.InternalCreateTableAsync(ctx, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (provision.HasFlag(SyncProvision.TrackingTable))
                            await this.InternalCreateTrackingTableAsync(ctx, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (provision.HasFlag(SyncProvision.Triggers))
                        {
                            foreach (DbTriggerType triggerType in Enum.GetValues(typeof(DbTriggerType)))
                            {
                                var trigExists = await InternalExistsTriggerAsync(ctx, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                                // Drop trigger if already exists
                                if (trigExists)
                                    await InternalDropTriggerAsync(ctx, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                                await InternalCreateTriggerAsync(ctx, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                            }
                        }

                        if (provision.HasFlag(SyncProvision.StoredProcedures))
                        {
                            foreach (DbStoredProcedureType storedProcedureType in Enum.GetValues(typeof(DbStoredProcedureType)))
                            {
                                // if we are iterating on bulk, but provider do not support it, just loop through and continue
                                if ((storedProcedureType is DbStoredProcedureType.BulkTableType || storedProcedureType is DbStoredProcedureType.BulkUpdateRows || storedProcedureType is DbStoredProcedureType.BulkDeleteRows)
                                    && !this.Provider.SupportBulkOperations)
                                    continue;

                                var procExists = await InternalExistsStoredProcedureAsync(ctx, tableBuilder, storedProcedureType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                                // Drop storedProcedure if already exists
                                if (procExists)
                                    await InternalDropStoredProcedureAsync(ctx, tableBuilder, storedProcedureType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                                await InternalCreateStoredProcedureAsync(ctx, tableBuilder, storedProcedureType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                            }
                        }
                    }

                    ScopeInfo localScope = null;

                    localScope.Setup = this.Setup;
                    localScope.Schema = schema;

                    await this.InternalUpsertScopeAsync(ctx, DbScopeType.Client, localScope, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                    transaction.Commit();
                }

                await this.CloseConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                var args = new DatabaseProvisionedArgs(ctx, provision, schema, connection);
                await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);
                this.ReportProgress(ctx, progress, args);

                // InterceptAsync Migrated
                var args2 = new DatabaseMigratedArgs(ctx, schema, this.Setup);
                await this.InterceptAsync(args2, cancellationToken).ConfigureAwait(false);
                this.ReportProgress(ctx, progress, args2);

            }
            catch (Exception ex)
            {
                RaiseError(ex);
            }
            finally
            {
                await this.CloseConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Update all untracked rows from the client database
        /// </summary>
        public virtual Task<bool> UpdateUntrackedRowsAsync(SyncSet schema, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.Provisioning, async (ctx, connection, transaction) =>
        {
            // If schema does not have any table, just return
            if (schema == null || schema.Tables == null || !schema.HasTables)
                throw new MissingTablesException();

            // Update untracked rows
            foreach (var table in schema.Tables)
            {
                var syncAdapter = this.Provider.GetSyncAdapter(table, this.Setup);
                await syncAdapter.UpdateUntrackedRowsAsync(connection, transaction).ConfigureAwait(false);
            }

            return true;
        }, cancellationToken);

    }
}
