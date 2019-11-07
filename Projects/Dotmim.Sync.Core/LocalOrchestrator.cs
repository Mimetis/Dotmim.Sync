using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Messages;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public class LocalOrchestrator : ILocalOrchestrator
    {
        public CoreProvider Provider { get; set; }

        private SyncOptions options;
        private SyncSchema schema;

        private bool syncInProgress;

        public LocalOrchestrator()
        {

        }

        /// <summary>
        /// Local orchestrator used as a client
        /// </summary>
        public LocalOrchestrator(CoreProvider provider) => this.Provider = provider;

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
        /// Get the local configuration, ensures the local scope is created
        /// </summary>
        /// <returns>current context, the local scope info created or get from the database and the configuration from the client if changed </returns>
        public async Task<(SyncContext, ScopeInfo)>
            EnsureScopeAsync(SyncContext context, SyncSchema schema, SyncOptions options,
                                  CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            // Lock sync to prevent multi call to sync at the same time
            LockSync();

            this.options = options;
            // ----------------------------------------
            // 0) Begin Session 
            // ----------------------------------------

            // Locally, almost no probability to change the configuration on the Begin Session
            // Anyway, to stay consistent, raise a Begin Session anyway
            context = await this.Provider.BeginSessionAsync(context, cancellationToken, progress).ConfigureAwait(false);

            // ----------------------------------------
            // 1) Read scope info
            // ----------------------------------------

            using (var connection = this.Provider.CreateConnection())
            {
                await connection.OpenAsync().ConfigureAwait(false);
                await this.Provider.InterceptAsync(new ConnectionOpenArgs(context, connection)).ConfigureAwait(false);

                // Create a transaction
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        await this.Provider.InterceptAsync(new TransactionOpenArgs(context, connection, transaction)).ConfigureAwait(false);

                        // get the scope from local provider 
                        List<ScopeInfo> localScopes;
                        (context, localScopes) = await this.Provider.EnsureScopesAsync(context,
                                            new MessageEnsureScopes(options.ScopeInfoTableName, schema.ScopeName),
                                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (localScopes.Count != 1)
                            throw new Exception("On Local provider, we should have only one scope info");


                        await this.Provider.InterceptAsync(new TransactionCommitArgs(context, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();

                        return (context, localScopes[0]);

                    }
                    catch (Exception)
                    {
                        throw;
                    }
                    finally
                    {
                        if (transaction != null)
                            transaction.Dispose();

                        if (connection != null && connection.State != ConnectionState.Closed)
                            connection.Close();

                        await this.Provider.InterceptAsync(new ConnectionCloseArgs(context, connection, transaction)).ConfigureAwait(false);

                    }
                }
            }
        }

        /// <summary>
        /// Lock sync to prevent multi call to sync at the same time
        /// </summary>
        private void LockSync()
        {
            lock (this)
            {
                if (this.syncInProgress)
                    throw new InProgressException("Synchronization already in progress");

                this.syncInProgress = true;
            }
        }

        /// <summary>
        /// Unlock sync to be able to launch a new sync
        /// </summary>
        private void UnlockSync()
        {
            // Enf sync from local provider
            lock (this)
            {
                this.syncInProgress = false;
            }
        }

        /// <summary>
        /// Save locally configuration and options to be reused later
        /// The schema has been modified probably by server, so reaffect it again.
        /// </summary>
        public Task SetSchemaAsync(SyncContext context, SyncSchema configuration, SyncOptions options, CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            this.schema = configuration;
            this.options = options;
            return Task.CompletedTask;
        }

        /// <summary>
        /// Input : localScopeInfo
        /// </summary>
        /// <returns></returns>
        public async Task<(SyncContext, long, BatchInfo, DatabaseChangesSelected)>
            GetChangesAsync(SyncContext context, SyncSchema schema,
                            ScopeInfo localScopeInfo, ScopeInfo serverScopeInfo,
                            CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            using (var connection = this.Provider.CreateConnection())
            {
                await connection.OpenAsync().ConfigureAwait(false);
                await this.Provider.InterceptAsync(new ConnectionOpenArgs(context, connection)).ConfigureAwait(false);

                // Create a transaction
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        await this.Provider.InterceptAsync(new TransactionOpenArgs(context, connection, transaction)).ConfigureAwait(false);

                        // Output
                        long clientTimestamp;
                        BatchInfo clientBatchInfo;
                        DatabaseChangesSelected clientChangesSelected;

                        // ----------------------------------------
                        // 0) Ensure schema
                        // ----------------------------------------
                        // Apply on local Provider
                        (context, schema) = await this.Provider.EnsureSchemaAsync(context, schema,
                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();


                        // ----------------------------------------
                        // 0) Ensure Database
                        // ----------------------------------------

                        // Client could have, or not, the tables
                        context = await this.Provider.EnsureDatabaseAsync(context,
                            new MessageEnsureDatabase(localScopeInfo, schema.Set, schema.Filters, schema.SerializationFormat),
                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();


                        // ----------------------------------------
                        // 5) Get local changes
                        // ----------------------------------------
                        var clientPolicy = schema.ConflictResolutionPolicy == ConflictResolutionPolicy.ServerWins ? ConflictResolutionPolicy.ClientWins : ConflictResolutionPolicy.ServerWins;

                        // We get from local provider all rows not last updated from the server
                        var fromId = serverScopeInfo.Id;
                        // lastSyncTS : get lines inserted / updated / deteleted after the last sync commited
                        var lastSyncTS = localScopeInfo.LastSyncTimestamp;
                        // isNew : If isNew, lasttimestamp is not correct, so grab all
                        var isNew = localScopeInfo.IsNewScope;
                        //Direction set to Upload
                        context.SyncWay = SyncWay.Upload;

                        // JUST before the whole process, get the timestamp, to be sure to 
                        // get rows inserted / updated elsewhere since the sync is not over
                        (context, clientTimestamp) = this.Provider.GetLocalTimestampAsync(context,
                            new MessageTimestamp(options.ScopeInfoTableName, schema.SerializationFormat),
                            connection, transaction, cancellationToken, progress);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        var scope = new ScopeInfo { Id = fromId, IsNewScope = isNew, Timestamp = lastSyncTS };

                        (context, clientBatchInfo, clientChangesSelected) =
                            await this.Provider.GetChangeBatchAsync(context,
                                    new MessageGetChangesBatch(scope,
                                        schema.Set, this.options.BatchSize, this.options.BatchDirectory, clientPolicy,
                                        schema.Filters, schema.SerializationFormat),
                                    connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        // Set the local schema to the new one coming from remote
                        this.schema = schema;

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(context, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();

                        return (context, clientTimestamp, clientBatchInfo, clientChangesSelected);

                    }
                    catch (Exception)
                    {
                        throw;
                    }
                    finally
                    {
                        if (transaction != null)
                            transaction.Dispose();

                        if (connection != null && connection.State != ConnectionState.Closed)
                            connection.Close();

                        await this.Provider.InterceptAsync(new ConnectionCloseArgs(context, connection, transaction)).ConfigureAwait(false);

                    }
                }
            }


        }


        public async Task<(SyncContext, DatabaseChangesApplied)>
            ApplyChangesAsync(SyncContext context,
                              long clientTimestamp,
                              Guid serverScopeId, ScopeInfo localScopeInfo,
                              BatchInfo serverBatchInfo,
                              CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            using (var connection = this.Provider.CreateConnection())
            {
                await connection.OpenAsync().ConfigureAwait(false);
                await this.Provider.InterceptAsync(new ConnectionOpenArgs(context, connection)).ConfigureAwait(false);

                // Create a transaction
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        await this.Provider.InterceptAsync(new TransactionOpenArgs(context, connection, transaction)).ConfigureAwait(false);
                        DatabaseChangesApplied clientChangesApplied;

                        // Check if we need to apply changes somewhere
                        if (serverBatchInfo.HasData())
                        {
                            // fromId : When applying rows, make sure it's identified as applied by this server scope
                            var fromId = serverScopeId;
                            // lastSyncTS : apply lines only if they are not modified since last client sync
                            var lastSyncTS = localScopeInfo.LastSyncTimestamp;
                            // isNew : if IsNew, don't apply deleted rows from server
                            var isNew = localScopeInfo.IsNewScope;
                            var scope = new ScopeInfo { Id = fromId, IsNewScope = isNew, Timestamp = lastSyncTS };

                            var clientPolicy = schema.ConflictResolutionPolicy == ConflictResolutionPolicy.ServerWins ? ConflictResolutionPolicy.ClientWins : ConflictResolutionPolicy.ServerWins;


                            (context, clientChangesApplied) =
                                await this.Provider.ApplyChangesAsync(context,
                                    new MessageApplyChanges(
                                            scope, schema, clientPolicy, this.options.DisableConstraintsOnApplyChanges,
                                            this.options.UseBulkOperations, this.options.CleanMetadatas, options.ScopeInfoTableName,
                                            serverBatchInfo),
                                        connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                        }
                        else
                        {
                            // no changes to report, just return an empty object (with 0 changes applied)
                            clientChangesApplied = new DatabaseChangesApplied();
                        }

                        // now the sync is complete, remember the time
                        context.CompleteTime = DateTime.Now;

                        localScopeInfo.IsLocal = true;
                        localScopeInfo.IsNewScope = false;
                        localScopeInfo.LastSync = context.CompleteTime;
                        localScopeInfo.LastSyncTimestamp = clientTimestamp;

                        // calculate duration
                        var duration = context.CompleteTime.Subtract(context.StartTime);
                        localScopeInfo.LastSyncDuration = duration.Ticks;

                        // Write scopes locally
                        context = await this.Provider.WriteScopesAsync(context,
                                        new MessageWriteScopes(options.ScopeInfoTableName, new List<ScopeInfo> { localScopeInfo }, schema.SerializationFormat),
                                        connection, transaction, cancellationToken, progress
                                        ).ConfigureAwait(false);

                        // Locally, almost no probability to change the configuration on the Begin Session
                        // Anyway, to stay consistent, raise a Begin Session anyway
                        context = await this.Provider.EndSessionAsync(context, cancellationToken, progress).ConfigureAwait(false);


                        await this.Provider.InterceptAsync(new TransactionCommitArgs(context, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();

                        // set sync ready again
                        UnlockSync();

                        return (context, clientChangesApplied);

                    }
                    catch (Exception)
                    {
                        throw;
                    }
                    finally
                    {
                        if (transaction != null)
                            transaction.Dispose();

                        if (connection != null && connection.State != ConnectionState.Closed)
                            connection.Close();

                        await this.Provider.InterceptAsync(new ConnectionCloseArgs(context, connection, transaction)).ConfigureAwait(false);
                    }

                }
            }
        }

    }
}
