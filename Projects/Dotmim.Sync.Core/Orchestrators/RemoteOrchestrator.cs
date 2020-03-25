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
    public class RemoteOrchestrator : BaseOrchestrator, IRemoteOrchestrator
    {

        /// <summary>
        /// Gets the sync side of this Orchestrator. RemoteOrchestrator is always used on server side
        /// </summary>
        public override SyncSide Side => SyncSide.ServerSide;


        /// <summary>
        /// Create a remote orchestrator, used to orchestrates the whole sync on the server side
        /// </summary>
        public RemoteOrchestrator(CoreProvider provider, SyncOptions options, SyncSetup setup, string scopeName = SyncOptions.DefaultScopeName)
        {
            this.ScopeName = scopeName ?? throw new ArgumentNullException(nameof(scopeName));
            this.Provider = provider ?? throw new ArgumentNullException(nameof(provider));
            this.Options = options ?? throw new ArgumentNullException(nameof(options));
            this.Setup = setup ?? throw new ArgumentNullException(nameof(setup));

            this.Provider.Orchestrator = this;
        }


        /// <summary>
        /// Get the local configuration, ensures the local scope is created
        /// </summary>
        /// <returns>current context, the local scope info created or get from the database and the configuration from the client if changed </returns>
        public async Task<ServerScopeInfo> EnsureScopesAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();
            ServerScopeInfo serverScopeInfo;

            DbTransaction transaction = null;

            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    ctx.SyncStage = SyncStage.ScopeLoading;

                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.Provider.OnConnectionOpened(connection);

                    await this.InterceptAsync(new ConnectionOpenArgs(ctx, connection), cancellationToken).ConfigureAwait(false);

                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {
                        await this.InterceptAsync(new TransactionOpenArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        await this.InterceptAsync(new ScopeLoadingArgs(ctx, this.ScopeName, this.Options.ScopeInfoTableName, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // Create scope server
                        ctx = await this.Provider.EnsureServerScopeAsync(ctx, this.Options.ScopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Create scope history
                        ctx = await this.Provider.EnsureServerHistoryScopeAsync(ctx, this.Options.ScopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Get scope if exists
                        (ctx, serverScopeInfo) = await this.Provider.GetServerScopeAsync(ctx, this.Options.ScopeInfoTableName, this.ScopeName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        transaction.Commit();
                    }

                    ctx.SyncStage = SyncStage.ScopeLoaded;

                    var scopeArgs = new ServerScopeLoadedArgs(ctx, serverScopeInfo, connection, transaction);
                    await this.InterceptAsync(scopeArgs, cancellationToken).ConfigureAwait(false);
                    this.ReportProgress(ctx, progress, scopeArgs);

                    return serverScopeInfo;
                }
                catch (Exception ex)
                {

                    var syncException = new SyncException(ex, SyncStage.ScopeLoading);

                    // try to let the provider enrich the exception
                    this.Provider.EnsureSyncException(syncException);
                    syncException.Side = this.Side;
                    throw syncException;
                }
                finally
                {
                    if (transaction != null)
                        transaction.Dispose();

                    if (connection != null && connection.State == ConnectionState.Open)
                        connection.Close();

                    await this.InterceptAsync(new ConnectionCloseArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                    // Let provider knows a connection is closed
                    this.Provider.OnConnectionClosed(connection);
                }
            }
        }



        /// <summary>
        /// Get the scope infos from remote
        /// </summary>
        public async Task<(SyncSet Schema, string Version)> EnsureSchemaAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();

            ServerScopeInfo serverScopeInfo;
            DbTransaction transaction = null;
            SyncSet schema;

            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    // starting with scope loading
                    ctx.SyncStage = SyncStage.ScopeLoading;

                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.Provider.OnConnectionOpened(connection);

                    await this.InterceptAsync(new ConnectionOpenArgs(ctx, connection), cancellationToken).ConfigureAwait(false);

                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {
                        // Open the connection
                        // Interceptors
                        await this.InterceptAsync(new TransactionOpenArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // raise scope loading
                        await this.InterceptAsync(new ScopeLoadingArgs(ctx, this.ScopeName, this.Options.ScopeInfoTableName, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // Create scope server
                        ctx = await this.Provider.EnsureServerScopeAsync(ctx, this.Options.ScopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Create scope history
                        ctx = await this.Provider.EnsureServerHistoryScopeAsync(ctx, this.Options.ScopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Get scope if exists
                        (ctx, serverScopeInfo) = await this.Provider.GetServerScopeAsync(ctx, this.Options.ScopeInfoTableName, this.ScopeName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // raise scope loaded
                        ctx.SyncStage = SyncStage.ScopeLoaded;
                        var scopeArgs = new ServerScopeLoadedArgs(ctx, serverScopeInfo, connection, transaction);
                        await this.InterceptAsync(scopeArgs, cancellationToken).ConfigureAwait(false);
                        this.ReportProgress(ctx, progress, scopeArgs);

                        // Let's compare this serverScopeInfo with the current Setup
                        // If we don't have any version 
                        // OrElse version are different
                        if (string.IsNullOrEmpty(serverScopeInfo.Schema))
                        {
                            // So far, we don't have already a database provisionned
                            ctx.SyncStage = SyncStage.SchemaProvisioning;

                            // 1) Get Schema from remote provider
                            (ctx, schema) = await this.Provider.GetSchemaAsync(ctx, this.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                            // 2) Ensure databases are ready
                            var provision = SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

                            await this.InterceptAsync(new DatabaseProvisioningArgs(ctx, provision, schema, connection, transaction), cancellationToken).ConfigureAwait(false);

                            ctx = await this.Provider.ProvisionAsync(ctx, schema, provision, this.Options.ScopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                            // So far, we don't have already a database provisionned
                            ctx.SyncStage = SyncStage.SchemaProvisioned;

                            // Generated the first serverscope to be updated
                            serverScopeInfo.LastCleanupTimestamp = 0;
                            serverScopeInfo.Schema = JsonConvert.SerializeObject(schema);
                            serverScopeInfo.Version = "1";

                            // 3) Update server scope
                            ctx = await this.Provider.WriteServerScopeAsync(ctx, this.Options.ScopeInfoTableName, serverScopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                            var args = new DatabaseProvisionedArgs(ctx, provision, schema, connection, transaction);
                            await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);
                            this.ReportProgress(ctx, progress, args);
                        }
                        else
                        {
                            // Get the schema saved on server
                            schema = JsonConvert.DeserializeObject<SyncSet>(serverScopeInfo.Schema);
                        }

                        await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        transaction.Commit();
                    }

                    return (schema, serverScopeInfo.Version);
                }
                catch (Exception ex)
                {
                    var syncException = new SyncException(ex, ctx.SyncStage);

                    // try to let the provider enrich the exception
                    this.Provider.EnsureSyncException(syncException);
                    syncException.Side = this.Side;
                    throw syncException;
                }
                finally
                {
                    if (transaction != null)
                        transaction.Dispose();

                    if (connection != null && connection.State == ConnectionState.Open)
                        connection.Close();

                    await this.InterceptAsync(new ConnectionCloseArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                    // Let provider knows a connection is closed
                    this.Provider.OnConnectionClosed(connection);
                }
            }
        }


        /// <summary>
        /// Apply changes on remote provider
        /// </summary>
        /// <param name="clientScope">client scope</param>
        /// <param name="schema">schema used to apply and then get changes</param>
        /// <param name="clientBatchInfo">changes to apply</param>
        /// <param name="cancellationToken">cancellation token</param>
        /// <param name="progress">progress</param>
        /// <returns></returns>
        public async Task<(long remoteClientTimestamp,
              BatchInfo serverBatchInfo,
              ConflictResolutionPolicy serverPolicy,
              DatabaseChangesApplied clientChangesApplied,
              DatabaseChangesSelected serverChangesSelected)>
            ApplyThenGetChangesAsync(ScopeInfo clientScope, BatchInfo clientBatchInfo, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();

            long remoteClientTimestamp;
            DatabaseChangesSelected serverChangesSelected;
            DatabaseChangesApplied clientChangesApplied;
            BatchInfo serverBatchInfo;
            DbTransaction transaction = null;
            SyncSet schema;

            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    ctx.SyncStage = SyncStage.DatabaseChangesApplying;

                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.Provider.OnConnectionOpened(connection);

                    await this.InterceptAsync(new ConnectionOpenArgs(ctx, connection), cancellationToken).ConfigureAwait(false);

                    // Create two transactions
                    // First one to commit changes
                    // Second one to get changes now that everything is commited
                    using (transaction = connection.BeginTransaction())
                    {
                        await this.InterceptAsync(new TransactionOpenArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        ServerScopeInfo serverScopeInfo;
                        // Maybe here, get the schema from server, issue from client scope name
                        // Maybe then compare the schema version from client scope with schema version issued from server
                        // Maybe if different, raise an error ?
                        // Get scope if exists

                        // Getting server scope assumes we have already created the schema on server
                        (ctx, serverScopeInfo) = await this.Provider.GetServerScopeAsync(ctx, this.Options.ScopeInfoTableName, clientScope.Name,
                                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Should we ?
                        if (string.IsNullOrEmpty(serverScopeInfo.Schema))
                            throw new NullReferenceException("Server schema does not exist");

                        // Check if we have a version to control
                        if (!serverScopeInfo.Version.Equals(clientScope.Version, SyncGlobalization.DataSourceStringComparison))
                            throw new ArgumentException("Server schema version does not match client schema version");

                        // deserialiaze schema
                        schema = JsonConvert.DeserializeObject<SyncSet>(serverScopeInfo.Schema);

                        // Create message containing everything we need to apply on server side
                        var applyChanges = new MessageApplyChanges(Guid.Empty, clientScope.Id, false, clientScope.LastServerSyncTimestamp, schema, this.Options.ConflictResolutionPolicy,
                                        this.Options.DisableConstraintsOnApplyChanges, this.Options.UseBulkOperations, this.Options.CleanMetadatas, this.Options.CleanFolder, clientBatchInfo);

                        // call interceptor
                        await this.InterceptAsync(new DatabaseChangesApplyingArgs(ctx, applyChanges, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // Call provider to apply changes
                        (ctx, clientChangesApplied) = await this.Provider.ApplyChangesAsync(ctx, applyChanges, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // commit first transaction
                        transaction.Commit();
                    }

                    ctx.SyncStage = SyncStage.DatabaseChangesApplied;

                    var databaseChangesAppliedArgs = new DatabaseChangesAppliedArgs(ctx, clientChangesApplied, connection, transaction);
                    await this.InterceptAsync(databaseChangesAppliedArgs, cancellationToken).ConfigureAwait(false);
                    this.ReportProgress(ctx, progress, databaseChangesAppliedArgs, connection, transaction);

                    ctx.SyncStage = SyncStage.DatabaseChangesSelecting;

                    using (transaction = connection.BeginTransaction())
                    {
                        //Direction set to Download
                        ctx.SyncWay = SyncWay.Download;

                        // JUST Before get changes, get the timestamp, to be sure to 
                        // get rows inserted / updated elsewhere since the sync is not over
                        remoteClientTimestamp = this.Provider.GetLocalTimestampAsync(ctx, connection, transaction, cancellationToken, progress);

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

                        // Get if we need to get all rows from the datasource
                        var fromScratch = clientScope.IsNewScope || ctx.SyncType == SyncType.Reinitialize || ctx.SyncType == SyncType.ReinitializeWithUpload;

                        var message = new MessageGetChangesBatch(clientScope.Id, Guid.Empty, fromScratch, clientScope.LastServerSyncTimestamp, schema, this.Options.BatchSize, this.Options.BatchDirectory);

                        // Call interceptor
                        await this.InterceptAsync(new DatabaseChangesSelectingArgs(ctx, message, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // When we get the chnages from server, we create the batches if it's requested by the client
                        // the batch decision comes from batchsize from client
                        (ctx, serverBatchInfo, serverChangesSelected) =
                            await this.Provider.GetChangeBatchAsync(ctx, message, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        // generate the new scope item
                        this.CompleteTime = DateTime.UtcNow;

                        var scopeHistory = new ServerHistoryScopeInfo
                        {
                            Id = clientScope.Id,
                            Name = clientScope.Name,
                            LastSyncTimestamp = remoteClientTimestamp,
                            LastSync = this.CompleteTime,
                            LastSyncDuration = this.CompleteTime.Value.Subtract(this.StartTime.Value).Ticks,
                        };

                        // Write scopes locally
                        ctx = await this.Provider.WriteServerHistoryScopeAsync(ctx, this.Options.ScopeInfoTableName, scopeHistory, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Commit second transaction for getting changes
                        await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);
                       
                        transaction.Commit();
                    }

                    // Event progress & interceptor
                    ctx.SyncStage = SyncStage.DatabaseChangesSelected;

                    var tableChangesSelectedArgs = new DatabaseChangesSelectedArgs(ctx, remoteClientTimestamp, serverBatchInfo, serverChangesSelected, connection, transaction);
                    this.ReportProgress(ctx, progress, tableChangesSelectedArgs);
                    await this.InterceptAsync(tableChangesSelectedArgs, cancellationToken).ConfigureAwait(false);

                    return (remoteClientTimestamp, serverBatchInfo, this.Options.ConflictResolutionPolicy, clientChangesApplied, serverChangesSelected);
                }
                catch (Exception ex)
                {
                    var syncException = new SyncException(ex, SyncStage.DatabaseChangesApplying);
                    // try to let the provider enrich the exception
                    this.Provider.EnsureSyncException(syncException);
                    syncException.Side = SyncSide.ServerSide;
                    throw syncException;
                }
                finally
                {
                    if (transaction != null)
                        transaction.Dispose();

                    if (connection != null && connection.State != ConnectionState.Closed)
                        connection.Close();

                    await this.InterceptAsync(new ConnectionCloseArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                    // Let provider knows a connection is closed
                    this.Provider.OnConnectionClosed(connection);
                }
            }

        }



        public async Task CreateSnapshotAsync(SyncParameters syncParameters = null, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            if (string.IsNullOrEmpty(this.Options.SnapshotsDirectory) || this.Options.BatchSize <= 0)
                throw new SnapshotMissingMandatariesOptionsException();

            // Get context or create a new one
            var ctx = this.GetContext();

            // check parameters
            // If context has no parameters specified, and user specifies a parameter collection we switch them
            if ((ctx.Parameters == null || ctx.Parameters.Count <= 0) && syncParameters != null && syncParameters.Count > 0)
                ctx.Parameters = syncParameters;

            DbTransaction transaction = null;
            long remoteClientTimestamp;

            // Get Schema from remote provider
            var (schema, version) = await this.EnsureSchemaAsync(cancellationToken, progress).ConfigureAwait(false);

            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.Provider.OnConnectionOpened(connection);

                    await this.InterceptAsync(new ConnectionOpenArgs(ctx, connection), cancellationToken).ConfigureAwait(false);

                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {
                        await this.InterceptAsync(new TransactionOpenArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // JUST Before get changes, get the timestamp, to be sure to 
                        // get rows inserted / updated elsewhere since the sync is not over
                        remoteClientTimestamp = this.Provider.GetLocalTimestampAsync(ctx, connection, transaction, cancellationToken, progress);

                        // Create snapshot
                        await this.Provider.CreateSnapshotAsync(ctx, schema, connection, transaction, this.Options.SnapshotsDirectory,
                                this.Options.BatchSize, remoteClientTimestamp, cancellationToken, progress).ConfigureAwait(false);

                        await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);
                        transaction.Commit();
                    }
                }
                catch (Exception ex)
                {
                    var syncException = new SyncException(ex, SyncStage.SnapshotApplying);
                    // try to let the provider enrich the exception
                    this.Provider.EnsureSyncException(syncException);
                    syncException.Side = SyncSide.ServerSide;
                    throw syncException;
                }
                finally
                {
                    if (transaction != null)
                        transaction.Dispose();

                    if (connection != null && connection.State != ConnectionState.Closed)
                        connection.Close();

                    await this.InterceptAsync(new ConnectionCloseArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                    // Let provider knows a connection is closed
                    this.Provider.OnConnectionClosed(connection);
                }
            }
        }


        /// <summary>
        /// Get a snapshot
        /// </summary>
        public async Task<(long remoteClientTimestamp, BatchInfo serverBatchInfo)>
                GetSnapshotAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            // TODO: Get snapshot based on version and scopename

            // Get context or create a new one
            var ctx = this.GetContext();

            BatchInfo serverBatchInfo;
            try
            {
                if (string.IsNullOrEmpty(this.Options.SnapshotsDirectory))
                    return (0, null);

                //Direction set to Download
                ctx.SyncWay = SyncWay.Download;

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Get Schema from remote provider
                var (schema, version) = await this.EnsureSchemaAsync(cancellationToken, progress).ConfigureAwait(false);


                // When we get the chnages from server, we create the batches if it's requested by the client
                // the batch decision comes from batchsize from client
                (ctx, serverBatchInfo) =
                    await this.Provider.GetSnapshotAsync(ctx, schema, this.Options.SnapshotsDirectory, cancellationToken, progress);

            }
            catch (Exception ex)
            {
                var syncException = new SyncException(ex, SyncStage.SnapshotApplying);
                // try to let the provider enrich the exception
                this.Provider.EnsureSyncException(syncException);
                syncException.Side = SyncSide.ServerSide;
                throw syncException;
            }

            if (serverBatchInfo == null)
                return (0, null);

            return (serverBatchInfo.Timestamp, serverBatchInfo);
        }
    }
}