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
    public class LocalOrchestrator : ILocalOrchestrator
    {
        public CoreProvider Provider { get; set; }

        public LocalOrchestrator() { }

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
            EnsureScopeAsync(SyncContext context, string scopeName, string scopeInfoTableName,
                                  CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            // ----------------------------------------
            // 0) Begin Session 
            // ----------------------------------------

            // Locally, almost no probability to change the configuration on the Begin Session
            // Anyway, to stay consistent, raise a Begin Session anyway
            context = await this.Provider.BeginSessionAsync(context, cancellationToken, progress).ConfigureAwait(false);

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

                    await this.Provider.InterceptAsync(new ConnectionOpenArgs(context, connection)).ConfigureAwait(false);

                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {

                        await this.Provider.InterceptAsync(new TransactionOpenArgs(context, connection, transaction)).ConfigureAwait(false);

                        // Create scope info table
                        context = await this.Provider.EnsureClientScopeAsync(
                                            context, scopeInfoTableName, scopeName,
                                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);


                        // Get scope
                        ScopeInfo localScope;
                        (context, localScope) = await this.Provider.GetClientScopeAsync(
                                            context, scopeInfoTableName, scopeName,
                                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(context, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();

                        return (context, localScope);
                    }
                }
                catch (Exception ex)
                {

                    var syncException = new SyncException(ex, context.SyncStage);

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

                    await this.Provider.InterceptAsync(new ConnectionCloseArgs(context, connection, transaction)).ConfigureAwait(false);

                    // Let provider knows a connection is closed
                    this.Provider.OnConnectionClosed(connection);
                }
            }
        }


        /// <summary>
        /// Input : localScopeInfo
        /// </summary>
        /// <returns></returns>
        public async Task<(SyncContext context, ScopeInfo localScopeInfo, long clientTimestamp, BatchInfo clientBatchInfo, DatabaseChangesSelected clientChangesSelected)>
            GetChangesAsync(SyncContext context, SyncSet schema, string scopeInfoTableName, int batchSize, string batchDirectory,
                            CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            DbTransaction transaction = null;
            ScopeInfo clientScopeInfo;

            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.Provider.OnConnectionOpened(connection);

                    await this.Provider.InterceptAsync(new ConnectionOpenArgs(context, connection)).ConfigureAwait(false);

                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {
                        await this.Provider.InterceptAsync(new TransactionOpenArgs(context, connection, transaction)).ConfigureAwait(false);

                        // Output
                        long clientTimestamp;
                        BatchInfo clientBatchInfo;
                        DatabaseChangesSelected clientChangesSelected;


                        // Starts sync by :
                        // - Getting local config we have set by code
                        // - Ensure local scope is created (table and values)
                        (context, clientScopeInfo) = await this.EnsureScopeAsync(context, schema.ScopeName, scopeInfoTableName, cancellationToken, progress);

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
                                var migrationArgs = new MigrationArgs(context, migrationTools, currentSchema, schema, connection, transaction);

                                // Intercept and let user decides and apply migration
                                await this.Provider.InterceptAsync(migrationArgs).ConfigureAwait(false);
                            }
                        }
                        else
                        {
                            // Ensure databases are ready
                            context = await this.Provider.EnsureDatabaseAsync(context, schema, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                        }


                        // On local, we don't want to chase rows from "others" 
                        // We just want our local rows, so we dont exclude any remote scope id, by setting scope id to NULL
                        Guid? remoteScopeId = null;
                        // lastSyncTS : get lines inserted / updated / deteleted after the last sync commited
                        var lastSyncTS = clientScopeInfo.LastSyncTimestamp;
                        // isNew : If isNew, lasttimestamp is not correct, so grab all
                        var isNew = clientScopeInfo.IsNewScope;
                        //Direction set to Upload
                        context.SyncWay = SyncWay.Upload;

                        // JUST before the whole process, get the timestamp, to be sure to 
                        // get rows inserted / updated elsewhere since the sync is not over
                        (context, clientTimestamp) = this.Provider.GetLocalTimestampAsync(context, connection, transaction, cancellationToken, progress);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        // Creating the message
                        var message = new MessageGetChangesBatch(remoteScopeId, clientScopeInfo.Id, isNew, lastSyncTS, schema, batchSize, batchDirectory);

                        // Locally, if we are new, no need to get changes
                        if (isNew)
                            (clientBatchInfo, clientChangesSelected) = await this.Provider.GetEmptyChangesAsync(message).ConfigureAwait(false);
                        else
                            (context, clientBatchInfo, clientChangesSelected) =
                                await this.Provider.GetChangeBatchAsync(context, message,
                                        connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(context, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();

                        return (context, clientScopeInfo, clientTimestamp, clientBatchInfo, clientChangesSelected);

                    }
                }
                catch (Exception ex)
                {
                    // try to let the provider enrich the exception
                    var syncException = new SyncException(ex, context.SyncStage);

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

                    await this.Provider.InterceptAsync(new ConnectionCloseArgs(context, connection, transaction)).ConfigureAwait(false);

                    // Let provider knows a connection is closed
                    this.Provider.OnConnectionClosed(connection);
                }
            }


        }


        public async Task<(SyncContext context, DatabaseChangesApplied clientChangesApplied)>
            ApplyChangesAsync(SyncContext context, ScopeInfo scope, SyncSet schema, BatchInfo serverBatchInfo,
                              ConflictResolutionPolicy clientPolicy, long clientTimestamp, long remoteClientTimestamp,
                              bool disableConstraintsOnApplyChanges, bool useBulkOperations, bool cleanMetadatas, string scopeInfoTableName, bool cleanFolder,
                              CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            DbTransaction transaction = null;

            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.Provider.OnConnectionOpened(connection);

                    await this.Provider.InterceptAsync(new ConnectionOpenArgs(context, connection)).ConfigureAwait(false);

                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {

                        await this.Provider.InterceptAsync(new TransactionOpenArgs(context, connection, transaction)).ConfigureAwait(false);
                        DatabaseChangesApplied clientChangesApplied;

                        // lastSyncTS : apply lines only if they are not modified since last client sync
                        var lastSyncTS = scope.LastSyncTimestamp;
                        // isNew : if IsNew, don't apply deleted rows from server
                        var isNew = scope.IsNewScope;

                        (context, clientChangesApplied) =
                            await this.Provider.ApplyChangesAsync(context,
                                new MessageApplyChanges(scope.Id, Guid.Empty, isNew, lastSyncTS, schema, clientPolicy, disableConstraintsOnApplyChanges,
                                        useBulkOperations, cleanMetadatas, cleanFolder, serverBatchInfo),
                                    connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // check if we need to delete metadatas
                        if (cleanMetadatas && clientChangesApplied.TotalAppliedChanges > 0)
                            await this.Provider.DeleteMetadatasAsync(context, schema, lastSyncTS, connection, transaction, cancellationToken, progress);

                        // now the sync is complete, remember the time
                        context.CompleteTime = DateTime.UtcNow;

                        // generate the new scope item
                        scope.IsNewScope = false;
                        scope.LastSync = context.CompleteTime;
                        scope.LastSyncTimestamp = clientTimestamp;
                        scope.LastServerSyncTimestamp = remoteClientTimestamp;
                        scope.LastSyncDuration = context.CompleteTime.Subtract(context.StartTime).Ticks;

                        // Write scopes locally
                        context = await this.Provider.WriteClientScopeAsync(context, scopeInfoTableName, scope, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Locally, almost no probability to change the configuration on the Begin Session
                        // Anyway, to stay consistent, raise a Begin Session anyway
                        context = await this.Provider.EndSessionAsync(context, cancellationToken, progress).ConfigureAwait(false);

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(context, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();

                        return (context, clientChangesApplied);
                    }
                }
                catch (Exception ex)
                {
                    // try to let the provider enrich the exception
                    var syncException = new SyncException(ex, context.SyncStage);

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

                    await this.Provider.InterceptAsync(new ConnectionCloseArgs(context, connection, transaction)).ConfigureAwait(false);

                    // Let provider knows a connection is closed
                    this.Provider.OnConnectionClosed(connection);
                }
            }
        }

        /// <summary>
        /// Delete metadatas items from tracking tables
        /// </summary>
        public async Task DeleteMetadatasAsync(SyncContext context, SyncSetup setup, long timeStampStart,
                                 CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            DbTransaction transaction = null;

            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.Provider.OnConnectionOpened(connection);

                    await this.Provider.InterceptAsync(new ConnectionOpenArgs(context, connection)).ConfigureAwait(false);

                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {
                        await this.Provider.InterceptAsync(new TransactionOpenArgs(context, connection, transaction)).ConfigureAwait(false);

                        SyncSet schema;
                        // Get Schema from remote provider
                        (context, schema) = await this.Provider.EnsureSchemaAsync(context, setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        await this.Provider.DeleteMetadatasAsync(context, schema, timeStampStart, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(context, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();
                    }
                }
                catch (Exception ex)
                {
                    var syncException = new SyncException(ex, context.SyncStage);
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

                    await this.Provider.InterceptAsync(new ConnectionCloseArgs(context, connection, transaction)).ConfigureAwait(false);

                    // Let provider knows a connection is closed
                    this.Provider.OnConnectionClosed(connection);
                }
            }

        }



        public async Task<SyncContext> ApplySnapshotAndGetChangesAsync(SyncContext context, SyncSet schema, BatchInfo serverBatchInfo,
                                                          long clientTimestamp, long remoteClientTimestamp, bool disableConstraintsOnApplyChanges,
                                                          int batchSize, string batchDirectory,
                                                          bool useBulkOperations, bool cleanMetadatas, string scopeInfoTableName,
                                                          CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            if (serverBatchInfo == null)
                return context;

            ScopeInfo scope;

            context.SyncStage = SyncStage.SnapshotApplying;
            await this.Provider.InterceptAsync(new SnapshotApplyingArgs(context)).ConfigureAwait(false);


            // Starts sync by :
            // - Getting local config we have set by code
            // - Ensure local scope is created (table and values)
            (context, scope) = await this.EnsureScopeAsync(context, scopeInfoTableName, schema.ScopeName, cancellationToken, progress);


            var localSnapshotChanges = await this.ApplyChangesAsync(
                    context, scope, schema, serverBatchInfo,
                    ConflictResolutionPolicy.ServerWins, clientTimestamp, remoteClientTimestamp,
                    disableConstraintsOnApplyChanges, useBulkOperations,
                    cleanMetadatas, scopeInfoTableName, false,
                    cancellationToken, progress);

            context.TotalChangesDownloaded += localSnapshotChanges.clientChangesApplied.TotalAppliedChanges;
            context.TotalSyncErrors += localSnapshotChanges.clientChangesApplied.TotalAppliedChangesFailed;

            // Get scope again to ensure we have correct timestamp
            (context, scope) = await this.EnsureScopeAsync(context, scope.Name, scopeInfoTableName, cancellationToken, progress);

            if (cancellationToken.IsCancellationRequested)
                cancellationToken.ThrowIfCancellationRequested();

            // on local orchestrator, get local changes again just in case we have insert, and get correct timestamp
            var clientChanges = await this.GetChangesAsync(
                context, schema, scopeInfoTableName, batchSize, batchDirectory, cancellationToken, progress);

            if (cancellationToken.IsCancellationRequested)
                cancellationToken.ThrowIfCancellationRequested();

            // set context
            context = clientChanges.context;

            // Progress & Interceptor
            context.SyncStage = SyncStage.SnapshotApplied;
            var snapshotAppliedArgs = new SnapshotAppliedArgs(context);
            this.Provider.ReportProgress(context, progress, snapshotAppliedArgs);
            await this.Provider.InterceptAsync(snapshotAppliedArgs).ConfigureAwait(false);

            return context;

        }

    }
}
