using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Messages;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public class LocalOrchestrator : ILocalOrchestrator
    {
        public CoreProvider Provider { get; set; }

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
            EnsureScopeAsync(SyncContext context, SyncSet schema, SyncOptions options,
                                  CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            // Lock sync to prevent multi call to sync at the same time
            LockSync();

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
                        ScopeInfo localScope;
                        (context, localScope) = await this.Provider.EnsureScopesAsync(
                                            context, options.ScopeInfoTableName, schema.ScopeName,
                                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(context, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();

                        return (context, localScope);

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

        ///// <summary>
        ///// Save locally configuration and options to be reused later
        ///// The schema has been modified probably by server, so reaffect it again.
        ///// </summary>
        //public Task SetSchemaAsync(SyncContext context, SyncSchema configuration, SyncOptions options, CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        //{
        //    this.schema = configuration;
        //    this.options = options;
        //    return Task.CompletedTask;
        //}

        /// <summary>
        /// Input : localScopeInfo
        /// </summary>
        /// <returns></returns>
        public async Task<(SyncContext, long, BatchInfo, DatabaseChangesSelected)>
            GetChangesAsync(SyncContext context, SyncSet schema, ScopeInfo scope, int batchSize, string batchDirectory,
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


                        // If we have already done a sync, we have a lastsync value, so don't need to check schema and database
                        bool checkIfSchemaExists = !scope.LastSync.HasValue;

                        if (checkIfSchemaExists)
                        {
                            // Ensure schema
                            (context, schema) = await this.Provider.EnsureSchemaAsync(context, schema,
                                connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                            if (cancellationToken.IsCancellationRequested)
                                cancellationToken.ThrowIfCancellationRequested();


                            // Ensure Database
                            context = await this.Provider.EnsureDatabaseAsync(context, schema, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                            if (cancellationToken.IsCancellationRequested)
                                cancellationToken.ThrowIfCancellationRequested();
                        }

                        // Fom local provider all rows not "last updated" by the Server
                        // Server scope is identified by Guid.Empty
                        var remoteScopeId = Guid.Empty;
                        // lastSyncTS : get lines inserted / updated / deteleted after the last sync commited
                        var lastSyncTS = scope.LastSyncTimestamp;
                        // isNew : If isNew, lasttimestamp is not correct, so grab all
                        var isNew = scope.IsNewScope;
                        //Direction set to Upload
                        context.SyncWay = SyncWay.Upload;

                        // JUST before the whole process, get the timestamp, to be sure to 
                        // get rows inserted / updated elsewhere since the sync is not over
                        (context, clientTimestamp) = this.Provider.GetLocalTimestampAsync(context, connection, transaction, cancellationToken, progress);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        (context, clientBatchInfo, clientChangesSelected) =
                            await this.Provider.GetChangeBatchAsync(context,
                                    new MessageGetChangesBatch(remoteScopeId, isNew, lastSyncTS,
                                        schema, batchSize, batchDirectory),
                                    connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

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
            ApplyChangesAsync(SyncContext context, ScopeInfo scope, SyncSet schema, BatchInfo serverBatchInfo,
                              ConflictResolutionPolicy clientPolicy, long clientTimestamp, long remoteClientTimestamp,
                              bool disableConstraintsOnApplyChanges, bool useBulkOperations, bool cleanMetadatas, string scopeInfoTableName,
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

                        // fromId : When applying rows, make sure it's identified as applied by this server scope
                        // server scope is identified by Guid.Empty
                        var fromId = Guid.Empty;
                        // lastSyncTS : apply lines only if they are not modified since last client sync
                        var lastSyncTS = scope.LastSyncTimestamp;
                        // isNew : if IsNew, don't apply deleted rows from server
                        var isNew = scope.IsNewScope;

                        (context, clientChangesApplied) =
                            await this.Provider.ApplyChangesAsync(context,
                                new MessageApplyChanges(fromId, isNew, lastSyncTS, schema, clientPolicy, disableConstraintsOnApplyChanges,
                                        useBulkOperations, cleanMetadatas, serverBatchInfo),
                                    connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // now the sync is complete, remember the time
                        context.CompleteTime = DateTime.Now;

                        // generate the new scope item
                        scope.IsNewScope = false;
                        scope.LastSync = context.CompleteTime;
                        scope.LastSyncTimestamp = clientTimestamp;
                        scope.LastServerSyncTimestamp = remoteClientTimestamp;
                        scope.LastSyncDuration = context.CompleteTime.Subtract(context.StartTime).Ticks;

                        // Write scopes locally
                        context = await this.Provider.WriteScopesAsync(context, scopeInfoTableName, scope, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

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
