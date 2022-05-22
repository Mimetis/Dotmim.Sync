using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public partial class RemoteOrchestrator : BaseOrchestrator
    {

        /// <summary>
        /// Gets the sync side of this Orchestrator. RemoteOrchestrator is always used on server side
        /// </summary>
        public override SyncSide Side => SyncSide.ServerSide;


        /// <summary>
        /// Create a remote orchestrator, used to orchestrates the whole sync on the server side
        /// </summary>
        public RemoteOrchestrator(CoreProvider provider, SyncOptions options) : base(provider, options)
        {
            if (!this.Provider.CanBeServerProvider)
                throw new UnsupportedServerProviderException(this.Provider.GetProviderTypeName());
        }

        /// <summary>
        /// Create a remote orchestrator, used to orchestrates the whole sync on the server side
        /// </summary>
        public RemoteOrchestrator(CoreProvider provider) : base(provider, new SyncOptions())
        {
            if (!this.Provider.CanBeServerProvider)
                throw new UnsupportedServerProviderException(this.Provider.GetProviderTypeName());
        }

        /// <summary>
        /// Ensure the schema is readed from the server, based on the Setup instance.
        /// Creates all required tables (server_scope tables) and provision all tables (tracking, stored proc, triggers and so on...)
        /// Then return the schema readed
        /// </summary>
        /// <returns>current context, the local scope info created or get from the database and the configuration from the client if changed </returns>
        //internal virtual async Task<ServerScopeInfo> EnsureSchemaAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        //{
        //    try
        //    {
        //        await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Writing, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

        //        var exists = await this.InternalExistsScopeInfoTableAsync(this.GetContext(), DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
        //        if (!exists)
        //            await this.InternalCreateScopeInfoTableAsync(this.GetContext(), DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        exists = await this.InternalExistsScopeInfoTableAsync(this.GetContext(), DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
        //        if (!exists)
        //            await this.InternalCreateScopeInfoTableAsync(this.GetContext(), DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        var serverScopeInfo = await this.InternalGetScopeAsync<ServerScopeInfo>(this.GetContext(), DbScopeType.Server, this.ScopeName, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        SyncSet schema;
        //        // Let's compare this serverScopeInfo with the current Setup
        //        // If schema is null :
        //        // - Read the schema from database based on this.Setup
        //        // - Provision the database with this schema
        //        // - Write the scope with this.Setup and schema
        //        // If schema is not null :
        //        // - Compare saved setup with current setup
        //        // - If not equals:
        //        // - Read schema from database based on this.Setup
        //        if (serverScopeInfo.Schema == null)
        //        {
        //            // So far, we don't have already a database provisionned
        //            this.GetContext().SyncStage = SyncStage.Provisioning;

        //            // 1) Get Schema from remote provider
        //            schema = await this.InternalGetSchemaAsync(this.GetContext(), this.Setup, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //            // 2) Provision
        //            var provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;
        //            schema = await InternalProvisionAsync(this.GetContext(), false, schema, this.Setup, provision, serverScopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
        //        }
        //        else
        //        {
        //            // Setup stored on local or remote is different from the one provided.
        //            // So, we can migrate
        //            if (!serverScopeInfo.Setup.EqualsByProperties(this.Setup))
        //            {
        //                // 1) Get Schema from remote provider
        //                schema = await this.InternalGetSchemaAsync(this.GetContext(), this.Setup, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //                // Migrate the old setup (serverScopeInfo.Setup) to the new setup (this.Setup) based on the new schema 
        //                await this.InternalMigrationAsync(this.GetContext(), schema, serverScopeInfo.Setup, this.Setup, runner.Connection, runner.Transaction, cancellationToken, progress);

        //                serverScopeInfo.Setup = this.Setup;
        //                serverScopeInfo.Schema = schema;

        //                // Write scopes locally
        //                await this.InternalSaveScopeAsync(this.GetContext(), DbScopeType.Server, serverScopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
        //            }

        //            // Get the schema saved on server
        //            schema = serverScopeInfo.Schema;
        //        }

        //        await runner.CommitAsync().ConfigureAwait(false);

        //        return serverScopeInfo;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw GetSyncError(ex);
        //    }
        //}


        /// <summary>
        /// Migrate an old setup configuration to a new one. This method is usefull if you are changing your SyncSetup when a database has been already configured previously
        /// </summary>
        //public virtual async Task<ServerScopeInfo> MigrationAsync(SyncSetup oldSetup, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        //{
        //    try
        //    {
        //        await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Writing, SyncStage.Migrating, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

        //        SyncSet schema = await this.InternalGetSchemaAsync(this.GetContext(), this.Setup, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        // Migrate the db structure
        //        await this.InternalMigrationAsync(this.GetContext(), schema, oldSetup, this.Setup, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        var exists = await this.InternalExistsScopeInfoTableAsync(this.GetContext(), DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        if (!exists)
        //            await this.InternalCreateScopeInfoTableAsync(this.GetContext(), DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        var remoteScope = await this.InternalGetScopeAsync<ServerScopeInfo>(this.GetContext(), DbScopeType.Server, this.ScopeName, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        if (remoteScope == null)
        //            remoteScope = await this.InternalCreateScopeAsync<ServerScopeInfo>(this.GetContext(), DbScopeType.Server, this.ScopeName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

        //        remoteScope.Setup = this.Setup;
        //        remoteScope.Schema = schema;

        //        // Write scopes locally
        //        await this.InternalSaveScopeAsync(this.GetContext(), DbScopeType.Server, remoteScope, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        await runner.CommitAsync().ConfigureAwait(false);

        //        return remoteScope;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw GetSyncError(ex);
        //    }
        //}


        /// <summary>
        /// Apply changes on remote provider
        /// </summary>
        internal virtual async Task<(SyncContext context, ServerSyncChanges serverSyncChanges)>
            InternalApplyThenGetChangesAsync(ClientScopeInfo clientScope, SyncContext context, BatchInfo clientBatchInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            long remoteClientTimestamp = 0L;
            DatabaseChangesSelected serverChangesSelected = null;
            DatabaseChangesApplied clientChangesApplied = null;
            BatchInfo serverBatchInfo = null;

            //Direction set to Upload
            context.SyncWay = SyncWay.Upload;

            // Create two transactions
            // First one to commit changes
            // Second one to get changes now that everything is commited
            await using (var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false))
            {

                IScopeInfo serverClientScopeInfo;
                // Getting server scope assumes we have already created the schema on server
                // Scope name is the scope name coming from client
                // Since server can have multiples scopes
                (context, serverClientScopeInfo) = await this.InternalLoadServerScopeInfoAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // Should we ?
                if (serverClientScopeInfo == null || serverClientScopeInfo.Schema == null)
                    throw new MissingRemoteOrchestratorSchemaException();

                // Deserialiaze schema
                var schema = serverClientScopeInfo.Schema;

                // Create message containing everything we need to apply on server side
                var applyChanges = new MessageApplyChanges(Guid.Empty, clientScope.Id, false, clientScope.LastServerSyncTimestamp, schema, this.Options.ConflictResolutionPolicy,
                                this.Options.DisableConstraintsOnApplyChanges, this.Options.CleanMetadatas, this.Options.CleanFolder, false, clientBatchInfo);

                // Call provider to apply changes
                (context, clientChangesApplied) = await this.InternalApplyChangesAsync(serverClientScopeInfo, context, applyChanges, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await this.InterceptAsync(new TransactionCommitArgs(context, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                // commit first transaction
                await runner.CommitAsync().ConfigureAwait(false);
            }

            await using (var runner = await this.GetConnectionAsync(context, SyncMode.Reading, SyncStage.ChangesSelecting, connection, transaction, cancellationToken, progress).ConfigureAwait(false))
            {
                context.ProgressPercentage = 0.55;

                //Direction set to Download
                context.SyncWay = SyncWay.Download;

                // JUST Before get changes, get the timestamp, to be sure to 
                // get rows inserted / updated elsewhere since the sync is not over
                (context, remoteClientTimestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress);

                // Get if we need to get all rows from the datasource
                var fromScratch = clientScope.IsNewScope || context.SyncType == SyncType.Reinitialize || context.SyncType == SyncType.ReinitializeWithUpload;

                // When we get the chnages from server, we create the batches if it's requested by the client
                // the batch decision comes from batchsize from client
                (context, serverBatchInfo, serverChangesSelected) =
                    await this.InternalGetChangesAsync(clientScope, context, fromScratch, clientScope.LastServerSyncTimestamp, clientScope.Id,
                    this.Provider.SupportsMultipleActiveResultSets,
                    this.Options.BatchDirectory, null, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (runner.CancellationToken.IsCancellationRequested)
                    runner.CancellationToken.ThrowIfCancellationRequested();

                // generate the new scope item
                this.CompleteTime = DateTime.UtcNow;

                var scopeHistory = new ServerHistoryScopeInfo
                {
                    Id = clientScope.Id,
                    Name = clientScope.Name,
                    LastSyncTimestamp = remoteClientTimestamp,
                    LastSync = this.CompleteTime,
                    LastSyncDuration = this.CompleteTime.Value.Subtract(context.StartTime).Ticks,
                };

                // Write scopes locally
                await this.InternalSaveServerHistoryScopeAsync(scopeHistory, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // Commit second transaction for getting changes
                await this.InterceptAsync(new TransactionCommitArgs(context, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);
            }

            var serverSyncChanges = new ServerSyncChanges(
                remoteClientTimestamp, serverBatchInfo, serverChangesSelected, clientChangesApplied, this.Options.ConflictResolutionPolicy);


            return (context, serverSyncChanges);

        }

        /// <summary>
        /// Get changes from remote database
        /// </summary>
        public virtual async Task<(long RemoteClientTimestamp, BatchInfo ServerBatchInfo, DatabaseChangesSelected ServerChangesSelected)>
            GetChangesAsync(ClientScopeInfo clientScope, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            var context = new SyncContext(Guid.NewGuid(), clientScope.Name);
            try
            {

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Reading, SyncStage.ChangesSelecting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Before getting changes, be sure we have a remote schema available
                ServerScopeInfo serverScopeInfo;
                (context, serverScopeInfo) = await this.InternalGetServerScopeInfoAsync(context, clientScope.Setup, runner.Connection, runner.Transaction, cancellationToken, progress);
                // TODO : if serverScope.Schema is null, should we Provision here ?

                // Should we ?
                if (serverScopeInfo.Schema == null)
                    throw new MissingRemoteOrchestratorSchemaException();

                //Direction set to Download
                context.SyncWay = SyncWay.Download;

                // Output
                // JUST Before get changes, get the timestamp, to be sure to 
                // get rows inserted / updated elsewhere since the sync is not over
                long remoteClientTimestamp;
                (context, remoteClientTimestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress);

                // Get if we need to get all rows from the datasource
                var fromScratch = clientScope.IsNewScope || context.SyncType == SyncType.Reinitialize || context.SyncType == SyncType.ReinitializeWithUpload;

                BatchInfo serverBatchInfo;
                DatabaseChangesSelected serverChangesSelected;
                // When we get the chnages from server, we create the batches if it's requested by the client
                // the batch decision comes from batchsize from client
                (context, serverBatchInfo, serverChangesSelected) =
                    await this.InternalGetChangesAsync(clientScope, context, fromScratch, clientScope.LastServerSyncTimestamp,
                    clientScope.Id, this.Provider.SupportsMultipleActiveResultSets,
                    this.Options.BatchDirectory, null, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return (remoteClientTimestamp, serverBatchInfo, serverChangesSelected);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }

        /// <summary>
        /// Get estimated changes from remote database to be applied on client
        /// </summary>
        public virtual async Task<(long RemoteClientTimestamp, DatabaseChangesSelected ServerChangesSelected)>
                    GetEstimatedChangesCountAsync(ClientScopeInfo clientScope, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), clientScope.Name);

            try
            {

                await using var runner0 = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                ServerScopeInfo serverScopeInfo;
                (context, serverScopeInfo) = await this.InternalGetServerScopeInfoAsync(context, clientScope.Setup, runner0.Connection, runner0.Transaction, runner0.CancellationToken, runner0.Progress).ConfigureAwait(false);

                await runner0.CommitAsync().ConfigureAwait(false);

                // Should we ?
                if (serverScopeInfo.Schema == null)
                    throw new MissingRemoteOrchestratorSchemaException();

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Reading, SyncStage.ChangesSelecting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                //Direction set to Download
                context.SyncWay = SyncWay.Download;

                // Output
                // JUST Before get changes, get the timestamp, to be sure to 
                // get rows inserted / updated elsewhere since the sync is not over
                long remoteClientTimestamp;
                (context, remoteClientTimestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress);

                // Get if we need to get all rows from the datasource
                var fromScratch = clientScope.IsNewScope || context.SyncType == SyncType.Reinitialize || context.SyncType == SyncType.ReinitializeWithUpload;

                DatabaseChangesSelected serverChangesSelected;
                // When we get the chnages from server, we create the batches if it's requested by the client
                // the batch decision comes from batchsize from client
                (context, serverChangesSelected) =
                    await this.InternalGetEstimatedChangesCountAsync(serverScopeInfo, context, fromScratch, clientScope.LastServerSyncTimestamp, clientScope.Id, this.Provider.SupportsMultipleActiveResultSets, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                return (remoteClientTimestamp, serverChangesSelected);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }
    }
}