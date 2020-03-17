using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Messages;
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
    public class LocalOrchestrator : IOrchestrator
    {
        private SyncContext syncContext;

        /// <summary>
        /// Gets or Sets the provider used by this local orchestrator
        /// </summary>
        public CoreProvider Provider { get; set; }

        /// <summary>
        /// Gets the options used by this local orchestrator
        /// </summary>
        public SyncOptions Options { get; set; }

        /// <summary>
        /// Gets the Setup used by this local orchestrator
        /// </summary>
        public SyncSetup Setup { get; set; }

        /// <summary>
        /// Gets the scope name used by this local orchestrator
        /// </summary>
        public string ScopeName { get; set; }

 
        /// <summary>
        /// Default ctor
        /// </summary>
        public LocalOrchestrator() => this.ScopeName = SyncOptions.DefaultScopeName;

        /// <summary>
        /// Local orchestrator used as a client
        /// </summary>
        public LocalOrchestrator(string scopeName, CoreProvider provider, SyncOptions options = null, SyncSetup setup = null)
        {
            this.ScopeName = scopeName ?? throw new ArgumentNullException(nameof(scopeName));
            this.Provider = provider ?? throw new ArgumentNullException(nameof(provider));
            this.Options = options;
            this.Setup = setup;
        }

        /// <summary>
        /// Local orchestrator used as a client
        /// </summary>
        public LocalOrchestrator(CoreProvider provider, SyncOptions options = null, SyncSetup setup = null)
        {
            this.ScopeName = SyncOptions.DefaultScopeName;
            this.Provider = provider ?? throw new ArgumentNullException(nameof(provider));
            this.Options = options;
            this.Setup = setup;
        }


        /// <summary>
        /// Set an interceptor to get info on the current sync process
        /// </summary>
        public void On<T>(Func<T, Task> interceptorFunc) where T : ProgressArgs => this.Provider.On(interceptorFunc);

        /// <summary>
        /// Set an interceptor to get info on the current sync process
        /// </summary>
        public void On<T>(Action<T> interceptorAction) where T : ProgressArgs => this.Provider.On(interceptorAction);

        /// <summary>
        /// Set a collection of interceptors
        /// </summary>
        public void On(Interceptors interceptors) => this.Provider.On(interceptors);


        /// <summary>
        /// Sets the current context
        /// </summary>
        public void SetContext(SyncContext context) => this.syncContext = context;

        /// <summary>
        /// Gets the current context
        /// </summary>
        public SyncContext GetContext()
        {
            if (this.syncContext != null)
                return this.syncContext;

            // Context, used to back and forth data between servers
            var context = new SyncContext(Guid.NewGuid()) {  StartTime = DateTime.UtcNow, };

            this.SetContext(context);

            return this.syncContext;
        }

        /// <summary>
        /// Get the local configuration, ensures the local scope is created
        /// </summary>
        /// <returns>current context, the local scope info created or get from the database and the configuration from the client if changed </returns>
        public async Task<ScopeInfo> EnsureScopeAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            if (this.Provider == null)
                throw new ArgumentNullException(nameof(this.Provider));

            if (this.Options == null)
                throw new ArgumentNullException(nameof(this.Options));

            // Get context or create a new one
            var ctx = this.GetContext();

            // ----------------------------------------
            // 0) Begin Session 
            // ----------------------------------------

            // Locally, almost no probability to change the configuration on the Begin Session
            // Anyway, to stay consistent, raise a Begin Session anyway
            await this.Provider.BeginSessionAsync(ctx, cancellationToken, progress).ConfigureAwait(false);

            // ----------------------------------------
            // 1) Read scope info
            // ----------------------------------------
            DbTransaction transaction = null;

            using (var connection = this.Provider.CreateConnection())
            {
                // Encapsulate in a try catch for a better exception handling
                // Especially whe called from web proxy
                try
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.Provider.OnConnectionOpened(connection);

                    await this.Provider.InterceptAsync(new ConnectionOpenArgs(ctx, connection)).ConfigureAwait(false);

                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {

                        await this.Provider.InterceptAsync(new TransactionOpenArgs(ctx, connection, transaction)).ConfigureAwait(false);

                        // Create scope info table
                        ctx = await this.Provider.EnsureClientScopeAsync(
                                            ctx, this.Options.ScopeInfoTableName, this.ScopeName,
                                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);


                        // Get scope
                        ScopeInfo localScope;
                        (ctx, localScope) = await this.Provider.GetClientScopeAsync(
                                            ctx, this.Options.ScopeInfoTableName, this.ScopeName,
                                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();

                        return localScope;
                    }
                }
                catch (Exception ex)
                {

                    var syncException = new SyncException(ex, SyncStage.ScopeLoading);

                    // try to let the provider enrich the exception
                    this.Provider.EnsureSyncException(syncException);
                    syncException.Side = SyncExceptionSide.ClientSide;
                    throw syncException;
                }
                finally
                {
                    if (transaction != null)
                        transaction.Dispose();

                    if (connection != null && connection.State == ConnectionState.Open)
                        connection.Close();

                    await this.Provider.InterceptAsync(new ConnectionCloseArgs(ctx, connection, transaction)).ConfigureAwait(false);

                    // Let provider knows a connection is closed
                    this.Provider.OnConnectionClosed(connection);
                }
            }
        }


        /// <summary>
        /// Input : localScopeInfo
        /// </summary>
        /// <returns></returns>
        public async Task<(ScopeInfo localScopeInfo, long clientTimestamp, BatchInfo clientBatchInfo, DatabaseChangesSelected clientChangesSelected)>
            GetChangesAsync(SyncSet schema, CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            if (this.Provider == null)
                throw new ArgumentNullException(nameof(this.Provider));

            if (this.Options == null)
                throw new ArgumentNullException(nameof(this.Options));

            // Get context or create a new one
            var ctx = this.GetContext();

            DbTransaction transaction = null;
            ScopeInfo clientScopeInfo;

            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.Provider.OnConnectionOpened(connection);

                    await this.Provider.InterceptAsync(new ConnectionOpenArgs(ctx, connection)).ConfigureAwait(false);

                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {
                        await this.Provider.InterceptAsync(new TransactionOpenArgs(ctx, connection, transaction)).ConfigureAwait(false);

                        // Output
                        long clientTimestamp;
                        BatchInfo clientBatchInfo;
                        DatabaseChangesSelected clientChangesSelected;


                        // Starts sync by :
                        // - Getting local config we have set by code
                        // - Ensure local scope is created (table and values)
                        clientScopeInfo = await this.EnsureScopeAsync(cancellationToken, progress);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        // If we have a stored schema, make a comparison with the one extract from setup
                        if (!string.IsNullOrEmpty(clientScopeInfo.Schema))
                        {
                            var currentSchema = JsonConvert.DeserializeObject<SyncSet>(clientScopeInfo.Schema);

                            var migrationTools = new DbMigrationTools();

                            var isIdentical = currentSchema == schema;

                            if (!isIdentical)
                            {
                                // Create a migration args where use can apply or not the migration
                                var migrationArgs = new MigrationArgs(ctx, migrationTools, currentSchema, schema, connection, transaction);

                                // Intercept and let user decides and apply migration
                                await this.Provider.InterceptAsync(migrationArgs).ConfigureAwait(false);
                            }
                        }
                        else
                        {
                            // Ensure databases are ready
                            ctx = await this.Provider.EnsureDatabaseAsync(ctx, schema, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                        }


                        // On local, we don't want to chase rows from "others" 
                        // We just want our local rows, so we dont exclude any remote scope id, by setting scope id to NULL
                        Guid? remoteScopeId = null;
                        // lastSyncTS : get lines inserted / updated / deteleted after the last sync commited
                        var lastSyncTS = clientScopeInfo.LastSyncTimestamp;
                        // isNew : If isNew, lasttimestamp is not correct, so grab all
                        var isNew = clientScopeInfo.IsNewScope;
                        //Direction set to Upload
                        ctx.SyncWay = SyncWay.Upload;

                        // JUST before the whole process, get the timestamp, to be sure to 
                        // get rows inserted / updated elsewhere since the sync is not over
                        clientTimestamp = this.Provider.GetLocalTimestampAsync(ctx, connection, transaction, cancellationToken, progress);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        // Creating the message
                        var message = new MessageGetChangesBatch(remoteScopeId, clientScopeInfo.Id, isNew, lastSyncTS, schema, this.Options.BatchSize, this.Options.BatchDirectory);

                        // Locally, if we are new, no need to get changes
                        if (isNew)
                            (clientBatchInfo, clientChangesSelected) = await this.Provider.GetEmptyChangesAsync(message).ConfigureAwait(false);
                        else
                            (ctx, clientBatchInfo, clientChangesSelected) =
                                await this.Provider.GetChangeBatchAsync(ctx, message,
                                        connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();

                        return (clientScopeInfo, clientTimestamp, clientBatchInfo, clientChangesSelected);

                    }
                }
                catch (Exception ex)
                {
                    // try to let the provider enrich the exception
                    var syncException = new SyncException(ex, SyncStage.TableChangesSelected);

                    this.Provider.EnsureSyncException(syncException);
                    syncException.Side = SyncExceptionSide.ClientSide;
                    throw syncException;
                }
                finally
                {
                    if (transaction != null)
                        transaction.Dispose();

                    if (connection != null && connection.State != ConnectionState.Closed)
                        connection.Close();

                    await this.Provider.InterceptAsync(new ConnectionCloseArgs(ctx, connection, transaction)).ConfigureAwait(false);

                    // Let provider knows a connection is closed
                    this.Provider.OnConnectionClosed(connection);
                }
            }


        }


        public async Task<DatabaseChangesApplied>
            ApplyChangesAsync(ScopeInfo scope, SyncSet schema, BatchInfo serverBatchInfo,
                              long clientTimestamp, long remoteClientTimestamp,
                              CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            if (this.Provider == null)
                throw new ArgumentNullException(nameof(this.Provider));

            if (this.Options == null)
                throw new ArgumentNullException(nameof(this.Options));


            // Get context or create a new one
            var ctx = this.GetContext();

            DbTransaction transaction = null;

            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.Provider.OnConnectionOpened(connection);

                    await this.Provider.InterceptAsync(new ConnectionOpenArgs(ctx, connection)).ConfigureAwait(false);

                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {

                        await this.Provider.InterceptAsync(new TransactionOpenArgs(ctx, connection, transaction)).ConfigureAwait(false);
                        DatabaseChangesApplied clientChangesApplied;

                        // lastSyncTS : apply lines only if they are not modified since last client sync
                        var lastSyncTS = scope.LastSyncTimestamp;
                        // isNew : if IsNew, don't apply deleted rows from server
                        var isNew = scope.IsNewScope;

                        // Policy is always Server policy, so reverse this policy to get the client policy
                        var clientPolicy = this.Options.ConflictResolutionPolicy == ConflictResolutionPolicy.ServerWins ? ConflictResolutionPolicy.ClientWins : ConflictResolutionPolicy.ServerWins;


                        (ctx, clientChangesApplied) =
                            await this.Provider.ApplyChangesAsync(ctx,
                                new MessageApplyChanges(scope.Id, Guid.Empty, isNew, lastSyncTS, schema, clientPolicy,
                                        this.Options.DisableConstraintsOnApplyChanges,
                                        this.Options.UseBulkOperations, this.Options.CleanMetadatas, this.Options.CleanFolder,
                                        serverBatchInfo),
                                    connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // check if we need to delete metadatas
                        if (this.Options.CleanMetadatas && clientChangesApplied.TotalAppliedChanges > 0)
                            await this.Provider.DeleteMetadatasAsync(ctx, schema, lastSyncTS, connection, transaction, cancellationToken, progress);

                        // now the sync is complete, remember the time
                        ctx.CompleteTime = DateTime.UtcNow;

                        // generate the new scope item
                        scope.IsNewScope = false;
                        scope.LastSync = ctx.CompleteTime;
                        scope.LastSyncTimestamp = clientTimestamp;
                        scope.LastServerSyncTimestamp = remoteClientTimestamp;
                        scope.LastSyncDuration = ctx.CompleteTime.Subtract(ctx.StartTime).Ticks;

                        // Write scopes locally
                        ctx = await this.Provider.WriteClientScopeAsync(ctx, this.Options.ScopeInfoTableName, scope, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Locally, almost no probability to change the configuration on the Begin Session
                        // Anyway, to stay consistent, raise a Begin Session anyway
                        await this.Provider.EndSessionAsync(ctx, cancellationToken, progress).ConfigureAwait(false);

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();

                        return clientChangesApplied;
                    }
                }
                catch (Exception ex)
                {
                    // try to let the provider enrich the exception
                    var syncException = new SyncException(ex, SyncStage.DatabaseChangesApplied);

                    this.Provider.EnsureSyncException(syncException);
                    syncException.Side = SyncExceptionSide.ClientSide;
                    throw syncException;
                }
                finally
                {
                    if (transaction != null)
                        transaction.Dispose();

                    if (connection != null && connection.State != ConnectionState.Closed)
                        connection.Close();

                    await this.Provider.InterceptAsync(new ConnectionCloseArgs(ctx, connection, transaction)).ConfigureAwait(false);

                    // Let provider knows a connection is closed
                    this.Provider.OnConnectionClosed(connection);
                }
            }
        }

        /// <summary>
        /// Delete metadatas items from tracking tables
        /// </summary>
        public async Task DeleteMetadatasAsync(long timeStampStart, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (this.Provider == null)
                throw new ArgumentNullException(nameof(this.Provider));

            if (this.Options == null)
                throw new ArgumentNullException(nameof(this.Options));

            if (this.Setup == null)
                throw new ArgumentNullException(nameof(this.Setup));

            // Get context or create a new one
            var ctx = this.GetContext();

            DbTransaction transaction = null;

            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.Provider.OnConnectionOpened(connection);

                    await this.Provider.InterceptAsync(new ConnectionOpenArgs(ctx, connection)).ConfigureAwait(false);

                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {
                        await this.Provider.InterceptAsync(new TransactionOpenArgs(ctx, connection, transaction)).ConfigureAwait(false);

                        SyncSet schema;
                        // Get Schema from remote provider
                        (ctx, schema) = await this.Provider.EnsureSchemaAsync(ctx, this.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        await this.Provider.DeleteMetadatasAsync(ctx, schema, timeStampStart, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();
                    }
                }
                catch (Exception ex)
                {
                    var syncException = new SyncException(ex, SyncStage.CleanupMetadata);
                    // try to let the provider enrich the exception
                    this.Provider.EnsureSyncException(syncException);
                    syncException.Side = SyncExceptionSide.ServerSide;
                    throw syncException;
                }
                finally
                {
                    if (transaction != null)
                        transaction.Dispose();

                    if (connection != null && connection.State != ConnectionState.Closed)
                        connection.Close();

                    await this.Provider.InterceptAsync(new ConnectionCloseArgs(ctx, connection, transaction)).ConfigureAwait(false);

                    // Let provider knows a connection is closed
                    this.Provider.OnConnectionClosed(connection);
                }
            }

        }



        public async Task ApplySnapshotAndGetChangesAsync(SyncSet schema, BatchInfo serverBatchInfo,
                                                          long clientTimestamp, long remoteClientTimestamp,
                                                          CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            if (this.Provider == null)
                throw new ArgumentNullException(nameof(this.Provider));

            if (this.Options == null)
                throw new ArgumentNullException(nameof(this.Options));

            if (serverBatchInfo == null)
                return;


            // Get context or create a new one
            var ctx = this.GetContext();


            ScopeInfo scope;

            ctx.SyncStage = SyncStage.SnapshotApplying;
            await this.Provider.InterceptAsync(new SnapshotApplyingArgs(ctx)).ConfigureAwait(false);


            // Starts sync by :
            // - Getting local config we have set by code
            // - Ensure local scope is created (table and values)
            scope = await this.EnsureScopeAsync(cancellationToken, progress);

            var localSnapshotChanges = await this.ApplyChangesAsync(
                    scope, schema, serverBatchInfo, clientTimestamp, remoteClientTimestamp, cancellationToken, progress);

            ctx.TotalChangesDownloaded += localSnapshotChanges.TotalAppliedChanges;
            ctx.TotalSyncErrors += localSnapshotChanges.TotalAppliedChangesFailed;

            // Get scope again to ensure we have correct timestamp
            scope = await this.EnsureScopeAsync(cancellationToken, progress);

            if (cancellationToken.IsCancellationRequested)
                cancellationToken.ThrowIfCancellationRequested();

            // on local orchestrator, get local changes again just in case we have insert, and get correct timestamp
            var clientChanges = await this.GetChangesAsync(schema, cancellationToken, progress);

            if (cancellationToken.IsCancellationRequested)
                cancellationToken.ThrowIfCancellationRequested();

            // set context
            // ctx = clientChanges.context;

            // Progress & Interceptor
            ctx.SyncStage = SyncStage.SnapshotApplied;
            var snapshotAppliedArgs = new SnapshotAppliedArgs(ctx);
            this.Provider.ReportProgress(ctx, progress, snapshotAppliedArgs);
            await this.Provider.InterceptAsync(snapshotAppliedArgs).ConfigureAwait(false);

            return;

        }

    }
}
