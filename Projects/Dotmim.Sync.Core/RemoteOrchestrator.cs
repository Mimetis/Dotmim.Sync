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
    public class RemoteOrchestrator : IRemoteOrchestrator
    {
        private SyncContext syncContext;

        /// <summary>
        /// Gets or Sets the provider used by this orchestrator
        /// </summary>
        public CoreProvider Provider { get; set; }

        /// <summary>
        /// Gets or Sets the options used by this orchestrator
        /// </summary>
        public SyncOptions Options { get; set; }

        /// <summary>
        /// Gets or Sets the Setup used by this orchestrator
        /// </summary>
        public SyncSetup Setup { get; set; }

        /// <summary>
        /// Gets or Sets the scope name used by this orchestrator
        /// </summary>
        public string ScopeName { get;  set; }


        /// <summary>
        /// Default ctor
        /// </summary>
        public RemoteOrchestrator() => this.ScopeName = SyncOptions.DefaultScopeName;

        /// <summary>
        /// Local orchestrator used as a client
        /// </summary>
        public RemoteOrchestrator(string scopeName, CoreProvider provider, SyncOptions options = null, SyncSetup setup = null)
        {
            this.ScopeName = scopeName ?? throw new ArgumentNullException(nameof(scopeName));
            this.Provider = provider ?? throw new ArgumentNullException(nameof(provider));
            this.Options = options;
            this.Setup = setup;
        }

        /// <summary>
        /// Local orchestrator used as a client
        /// </summary>
        public RemoteOrchestrator(CoreProvider provider, SyncOptions options = null, SyncSetup setup = null)
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
            var context = new SyncContext(Guid.NewGuid()) { StartTime = DateTime.UtcNow };

            this.SetContext(context);

            return this.syncContext;
        }

        /// <summary>
        /// Get the scope infos from remote
        /// </summary>
        public async Task<SyncSet> EnsureSchemaAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
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
            SyncSet schema;
            ServerScopeInfo serverScopeInfo;

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
                        // Open the connection
                        // Interceptors
                        await this.Provider.InterceptAsync(new TransactionOpenArgs(ctx, connection, transaction)).ConfigureAwait(false);

                        // Begin Session 
                        await this.Provider.BeginSessionAsync(ctx, cancellationToken, progress).ConfigureAwait(false);

                        // Get Schema from remote provider
                        (ctx, schema) = await this.Provider.EnsureSchemaAsync(ctx, this.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Create scope server
                        ctx = await this.Provider.EnsureServerScopeAsync(
                                            ctx, this.Options.ScopeInfoTableName, this.ScopeName,
                                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Create scope history
                        ctx = await this.Provider.EnsureServerHistoryScopeAsync(
                                            ctx, this.Options.ScopeInfoTableName, this.ScopeName,
                                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Get scope if exists
                        (ctx, serverScopeInfo) = await this.Provider.GetServerScopeAsync(ctx, this.Options.ScopeInfoTableName, this.ScopeName,
                                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // If we have a stored schema, make a comparison with the one extract from setup
                        if (!string.IsNullOrEmpty(serverScopeInfo.Schema))
                        {
                            var currentSchema = JsonConvert.DeserializeObject<SyncSet>(serverScopeInfo.Schema);

                            var migrationTools = new DbMigrationTools();

                            // Check if current schema has exact same properties, relations, tables and columns..
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

                
                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        serverScopeInfo.Schema = JsonConvert.SerializeObject(schema);

                        ctx = await this.Provider.WriteServerScopeAsync(
                                            ctx, this.Options.ScopeInfoTableName, serverScopeInfo,
                                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();

                        return schema;

                    }
                }
                catch (Exception ex)
                {
                    var syncException = new SyncException(ex, SyncStage.SchemaApplying);
                    // try to let the provider enrich the exception
                    this.Provider.EnsureSyncException(syncException);
                    syncException.Side = SyncExceptionSide.ServerSide;
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



        public async Task<(long, BatchInfo, DatabaseChangesSelected)>
            ApplyThenGetChangesAsync(ScopeInfo scope, SyncSet schema, BatchInfo clientBatchInfo,
                                     CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            if (this.Provider == null)
                throw new ArgumentNullException(nameof(this.Provider));

            if (this.Options == null)
                throw new ArgumentNullException(nameof(this.Options));

            if (this.Setup == null)
                throw new ArgumentNullException(nameof(this.Setup));

            // Get context or create a new one
            var ctx = this.GetContext();


            long remoteClientTimestamp;
            DatabaseChangesSelected serverChangesSelected;
            BatchInfo serverBatchInfo;
            DbTransaction transaction = null;

            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.Provider.OnConnectionOpened(connection);

                    await this.Provider.InterceptAsync(new ConnectionOpenArgs(ctx, connection)).ConfigureAwait(false);

                    // Create two transactions
                    // First one to commit changes
                    // Second one to get changes now that everything is commited
                    using (transaction = connection.BeginTransaction())
                    {
                        await this.Provider.InterceptAsync(new TransactionOpenArgs(ctx, connection, transaction)).ConfigureAwait(false);

                        DatabaseChangesApplied changesApplied;

                        (ctx, changesApplied) =
                            await this.Provider.ApplyChangesAsync(ctx,
                             new MessageApplyChanges(Guid.Empty, scope.Id, false, scope.LastServerSyncTimestamp, schema, this.Options.ConflictResolutionPolicy,
                                        this.Options.DisableConstraintsOnApplyChanges, this.Options.UseBulkOperations, this.Options.CleanMetadatas, this.Options.CleanFolder, clientBatchInfo),
                             connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();
                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);

                        // commit first transaction
                        transaction.Commit();
                    }

                    using (transaction = connection.BeginTransaction())
                    {
                        //Direction set to Download
                        ctx.SyncWay = SyncWay.Download;

                        // JUST Before get changes, get the timestamp, to be sure to 
                        // get rows inserted / updated elsewhere since the sync is not over
                        remoteClientTimestamp = this.Provider.GetLocalTimestampAsync(ctx, connection, transaction, cancellationToken, progress);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        // Get if we need to get all rows from the datasource
                        var fromScratch = scope.IsNewScope || ctx.SyncType == SyncType.Reinitialize || ctx.SyncType == SyncType.ReinitializeWithUpload;

                        // When we get the chnages from server, we create the batches if it's requested by the client
                        // the batch decision comes from batchsize from client
                        (ctx, serverBatchInfo, serverChangesSelected) =
                            await this.Provider.GetChangeBatchAsync(ctx,
                                new MessageGetChangesBatch(scope.Id, Guid.Empty, fromScratch, scope.LastServerSyncTimestamp,
                                    schema, this.Options.BatchSize, this.Options.BatchDirectory),
                                    connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        // generate the new scope item
                        var lastSync = DateTime.UtcNow;
                        var scopeHistory = new ServerHistoryScopeInfo
                        {
                            Id = scope.Id,
                            Name = scope.Name,
                            LastSyncTimestamp = remoteClientTimestamp,
                            LastSync = lastSync,
                            LastSyncDuration = lastSync.Subtract(ctx.StartTime).Ticks,
                        };

                        // Write scopes locally
                        ctx = await this.Provider.WriteServerHistoryScopeAsync(ctx, this.Options.ScopeInfoTableName, scopeHistory, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Commit second transaction for getting changes
                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();
                    }
                }
                catch (Exception ex)
                {
                    var syncException = new SyncException(ex, SyncStage.DatabaseChangesApplying);
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

            return (remoteClientTimestamp, serverBatchInfo, serverChangesSelected);
        }


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


        public async Task CreateSnapshotAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
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
            long remoteClientTimestamp;

            SyncSet schema;
            // Get Schema from remote provider
            schema = await this.EnsureSchemaAsync(cancellationToken, progress).ConfigureAwait(false);


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

                        // JUST Before get changes, get the timestamp, to be sure to 
                        // get rows inserted / updated elsewhere since the sync is not over
                        remoteClientTimestamp = this.Provider.GetLocalTimestampAsync(ctx, connection, transaction, cancellationToken, progress);

                        // Create snapshot
                        await this.Provider.CreateSnapshotAsync(ctx, schema, connection, transaction, this.Options.BatchDirectory, 
                                this.Options.BatchSize, remoteClientTimestamp, cancellationToken, progress).ConfigureAwait(false);

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();
                    }
                }
                catch (Exception ex)
                {
                    var syncException = new SyncException(ex, SyncStage.SnapshotApplying);
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


        public async Task<(long remoteClientTimestamp, BatchInfo serverBatchInfo)>
                GetSnapshotAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (this.Provider == null)
                throw new ArgumentNullException(nameof(this.Provider));

            if (this.Options == null)
                throw new ArgumentNullException(nameof(this.Options));

            if (this.Setup == null)
                throw new ArgumentNullException(nameof(this.Setup));

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

                SyncSet schema;
                // Get Schema from remote provider
                schema = await this.EnsureSchemaAsync(cancellationToken, progress).ConfigureAwait(false);


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
                syncException.Side = SyncExceptionSide.ServerSide;
                throw syncException;
            }

            if (serverBatchInfo == null)
                return (0, null);

            return (serverBatchInfo.Timestamp, serverBatchInfo);
        }
    }
}