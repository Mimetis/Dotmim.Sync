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
        /// Gets or Sets the start time for this orchestrator
        /// </summary>
        public DateTime? StartTime { get; set; }

        /// <summary>
        /// Gets or Sets the end time for this orchestrator
        /// </summary>
        public DateTime? CompleteTime { get; set; }


        /// <summary>
        /// Local orchestrator used as a client
        /// </summary>
        public LocalOrchestrator(CoreProvider provider, SyncOptions options, SyncSetup setup, string scopeName = SyncOptions.DefaultScopeName)
        {
            this.ScopeName = scopeName ?? throw new ArgumentNullException(nameof(scopeName));
            this.Provider = provider ?? throw new ArgumentNullException(nameof(provider));
            this.Options = options ?? throw new ArgumentNullException(nameof(options));
            this.Setup = setup ?? throw new ArgumentNullException(nameof(setup));
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
            var context = new SyncContext(Guid.NewGuid(), this.ScopeName);

            this.SetContext(context);

            return this.syncContext;
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
            this.Provider.ReportProgress(ctx, progress, sessionArgs);
            await this.Provider.InterceptAsync(sessionArgs).ConfigureAwait(false);
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
            this.Provider.ReportProgress(ctx, progress, sessionArgs);
            await this.Provider.InterceptAsync(sessionArgs).ConfigureAwait(false);
        }

        public async Task<SyncSet> ProvisionAsync(SyncSet schema, SyncProvision provision, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();

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

                        await this.Provider.ProvisionAsync(ctx, schema, provision, this.Options.ScopeInfoTableName, connection, transaction, cancellationToken, progress);

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();
                    }
                }
                catch (Exception ex)
                {
                    var syncException = new SyncException(ex, ctx.SyncStage);

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

            return schema;
        }

        public async Task<SyncSet> DeprovisionAsync(SyncSet schema, SyncProvision provision, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();

            DbTransaction transaction = null;

            using (var connection = this.Provider.CreateConnection())
            {
                // Encapsulate in a try catch for a better exception handling
                // Especially when called from web proxy
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

                        await this.Provider.DeprovisionAsync(ctx, schema, provision, this.Options.ScopeInfoTableName,
                            this.Options.DisableConstraintsOnApplyChanges, connection, transaction, cancellationToken, progress);

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();
                    }
                }
                catch (Exception ex)
                {
                    var syncException = new SyncException(ex, ctx.SyncStage);

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

            return schema;
        }

        public async Task<SyncSet> ReadSchemaAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();

            SyncSet schema = null;

            DbTransaction transaction = null;

            using (var connection = this.Provider.CreateConnection())
            {
                // Encapsulate in a try catch for a better exception handling
                // Especially whew called from web proxy
                try
                {
                    if (this.Setup.Tables.Count <= 0)
                        throw new MissingTablesException();

                    // Set context to schema reading
                    ctx.SyncStage = SyncStage.SchemaReading;

                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.Provider.OnConnectionOpened(connection);

                    await this.Provider.InterceptAsync(new ConnectionOpenArgs(ctx, connection)).ConfigureAwait(false);

                    if (cancellationToken.IsCancellationRequested)
                        cancellationToken.ThrowIfCancellationRequested();

                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {
                        await this.Provider.InterceptAsync(new TransactionOpenArgs(ctx, connection, transaction)).ConfigureAwait(false);

                        (ctx, schema) = await this.Provider.ReadSchemaAsync(ctx, this.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        ctx.SyncStage = SyncStage.SchemaRead;

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();
                    }
                }
                catch (Exception ex)
                {
                    var syncException = new SyncException(ex, ctx.SyncStage);

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

            return schema;
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

            DbTransaction transaction = null;

            using (var connection = this.Provider.CreateConnection())
            {
                // Encapsulate in a try catch for a better exception handling
                // Especially whe called from web proxy
                try
                {
                    // Set context to schema reading
                    ctx.SyncStage = SyncStage.ScopeLoading;

                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.Provider.OnConnectionOpened(connection);

                    await this.Provider.InterceptAsync(new ConnectionOpenArgs(ctx, connection)).ConfigureAwait(false);

                    if (cancellationToken.IsCancellationRequested)
                        cancellationToken.ThrowIfCancellationRequested();

                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {
                        await this.Provider.InterceptAsync(new TransactionOpenArgs(ctx, connection, transaction)).ConfigureAwait(false);

                        // Create scope info table
                        ctx = await this.Provider.EnsureClientScopeAsync(
                                            ctx, this.Options.ScopeInfoTableName,
                                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Get scope
                        ScopeInfo localScope;
                        (ctx, localScope) = await this.Provider.GetClientScopeAsync(
                                            ctx, this.Options.ScopeInfoTableName, this.ScopeName,
                                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Set context to scope loaded
                        ctx.SyncStage = SyncStage.ScopeLoaded;

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();

                        return localScope;
                    }
                }
                catch (Exception ex)
                {

                    var syncException = new SyncException(ex, ctx.SyncStage);

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
        public async Task<(long clientTimestamp, BatchInfo clientBatchInfo, DatabaseChangesSelected clientChangesSelected)>
            GetChangesAsync(ScopeInfo localScopeInfo, CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();

            DbTransaction transaction = null;
            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    // Check if we have a schema in local scope info
                    if (localScopeInfo == null || string.IsNullOrEmpty(localScopeInfo.Schema))
                        throw new ArgumentNullException("can't get changes if we don't have a scope info fill with a schema stored in it");

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
                        clientTimestamp = this.Provider.GetLocalTimestampAsync(ctx, connection, transaction, cancellationToken, progress);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        // Creating the message
                        var message = new MessageGetChangesBatch(remoteScopeId, localScopeInfo.Id, isNew, lastSyncTS, schema, this.Options.BatchSize, this.Options.BatchDirectory);

                        // Locally, if we are new, no need to get changes
                        if (isNew)
                            (clientBatchInfo, clientChangesSelected) = await this.Provider.GetEmptyChangesAsync(message).ConfigureAwait(false);
                        else
                            (ctx, clientBatchInfo, clientChangesSelected) = await this.Provider.GetChangeBatchAsync(ctx, message, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();

                        return (clientTimestamp, clientBatchInfo, clientChangesSelected);

                    }
                }
                catch (Exception ex)
                {
                    // try to let the provider enrich the exception
                    var syncException = new SyncException(ex, ctx.SyncStage);

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
        public async Task<DatabaseChangesApplied>
            ApplyChangesAsync(ScopeInfo scope, SyncSet schema, BatchInfo serverBatchInfo,
                              long clientTimestamp, long remoteClientTimestamp, ConflictResolutionPolicy policy,
                              CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

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


                        (ctx, clientChangesApplied) =
                            await this.Provider.ApplyChangesAsync(ctx,
                                new MessageApplyChanges(scope.Id, Guid.Empty, isNew, lastSyncTS, schema, policy,
                                        this.Options.DisableConstraintsOnApplyChanges,
                                        this.Options.UseBulkOperations, this.Options.CleanMetadatas, this.Options.CleanFolder,
                                        serverBatchInfo),
                                    connection, transaction, cancellationToken, progress).ConfigureAwait(false);

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

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();

                        return clientChangesApplied;
                    }
                }
                catch (Exception ex)
                {
                    // try to let the provider enrich the exception
                    var syncException = new SyncException(ex, ctx.SyncStage);

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
        /// <param name="timeStampStart">Timestamp start. Used to limit the delete metadatas rows from now to this timestamp</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <param name="progress">Progress args</param>
        public async Task DeleteMetadatasAsync(long timeStampStart, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

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
                        (ctx, schema) = await this.Provider.ReadSchemaAsync(ctx, this.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        ctx = await this.Provider.DeleteMetadatasAsync(ctx, schema, timeStampStart, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();
                    }
                }
                catch (Exception ex)
                {
                    var syncException = new SyncException(ex, ctx.SyncStage);
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



        public async Task<(long clientTimestamp, BatchInfo clientBatchInfo, DatabaseChangesSelected clientChangesSelected)>
            ApplySnapshotAndGetChangesAsync(SyncSet schema, BatchInfo serverBatchInfo,
                                            long clientTimestamp, long remoteClientTimestamp,
                                            CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            if (serverBatchInfo == null)
                return (0, null, null);

            // Get context or create a new one
            var ctx = this.GetContext();

            ScopeInfo scope;

            ctx.SyncStage = SyncStage.SnapshotApplying;
            await this.Provider.InterceptAsync(new SnapshotApplyingArgs(ctx)).ConfigureAwait(false);


            // Starts sync by :
            // - Getting local config we have set by code
            // - Ensure local scope is created (table and values)
            scope = await this.EnsureScopeAsync(cancellationToken, progress);

            var localSnapshotChanges = await this.ApplyChangesAsync(scope, schema, serverBatchInfo,
                    clientTimestamp, remoteClientTimestamp, ConflictResolutionPolicy.ServerWins, cancellationToken, progress);

            // Get scope again to ensure we have correct timestamp
            scope = await this.EnsureScopeAsync(cancellationToken, progress);

            if (cancellationToken.IsCancellationRequested)
                cancellationToken.ThrowIfCancellationRequested();

            // on local orchestrator, get local changes again just in case we have insert, and get correct timestamp
            var clientChanges = await this.GetChangesAsync(scope, cancellationToken, progress);

            if (cancellationToken.IsCancellationRequested)
                cancellationToken.ThrowIfCancellationRequested();

            // set context
            // ctx = clientChanges.context;

            // Progress & Interceptor
            ctx.SyncStage = SyncStage.SnapshotApplied;
            var snapshotAppliedArgs = new SnapshotAppliedArgs(ctx);
            this.Provider.ReportProgress(ctx, progress, snapshotAppliedArgs);
            await this.Provider.InterceptAsync(snapshotAppliedArgs).ConfigureAwait(false);

            return clientChanges;

        }

    }
}
