using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Messages;
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
        public CoreProvider Provider { get; set; }

        public RemoteOrchestrator() { }

        public RemoteOrchestrator(CoreProvider serverProvider) => this.Provider = serverProvider;

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
        /// Get the scope infos from remote
        /// </summary>
        public async Task<(SyncContext, SyncSet)>
            EnsureSchemaAsync(SyncContext context, SyncSetup setup,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
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

                        // Open the connection
                        // Interceptors
                        await this.Provider.InterceptAsync(new TransactionOpenArgs(context, connection, transaction)).ConfigureAwait(false);

                        // Begin Session 
                        context = await this.Provider.BeginSessionAsync(context, cancellationToken, progress).ConfigureAwait(false);

                        SyncSet schema;
                        // Get Schema from remote provider
                        (context, schema) = await this.Provider.EnsureSchemaAsync(context, setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        // Ensure databases are ready
                        context = await this.Provider.EnsureDatabaseAsync(context, schema, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(context, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();

                        return (context, schema);

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

                    if (connection != null && connection.State == ConnectionState.Open)
                        connection.Close();

                    await this.Provider.InterceptAsync(new ConnectionCloseArgs(context, connection, transaction)).ConfigureAwait(false);

                    // Let provider knows a connection is closed
                    this.Provider.OnConnectionClosed(connection);
                }
            }
        }



        public async Task<(SyncContext, long, BatchInfo, ConflictResolutionPolicy, DatabaseChangesSelected)>
            ApplyThenGetChangesAsync(SyncContext context, ScopeInfo scope, SyncSet schema, BatchInfo clientBatchInfo,
                                     bool disableConstraintsOnApplyChanges, bool useBulkOperations, bool cleanMetadatas,
                                     int clientBatchSize, string batchDirectory, ConflictResolutionPolicy policy,
                                     CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

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

                    await this.Provider.InterceptAsync(new ConnectionOpenArgs(context, connection)).ConfigureAwait(false);

                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {
                        await this.Provider.InterceptAsync(new TransactionOpenArgs(context, connection, transaction)).ConfigureAwait(false);

                        DatabaseChangesApplied changesApplied;

                        (context, changesApplied) =
                            await this.Provider.ApplyChangesAsync(context,
                             new MessageApplyChanges(Guid.Empty, scope.Id, false, scope.LastServerSyncTimestamp, schema, policy,
                                        disableConstraintsOnApplyChanges, useBulkOperations, cleanMetadatas, clientBatchInfo),
                             connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        //Direction set to Download
                        context.SyncWay = SyncWay.Download;

                        // JUST Before get changes, get the timestamp, to be sure to 
                        // get rows inserted / updated elsewhere since the sync is not over
                        (context, remoteClientTimestamp) = this.Provider.GetLocalTimestampAsync(context, connection, transaction, cancellationToken, progress);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        // Get if we need to get all rows from the datasource
                        var fromScratch = scope.IsNewScope || context.SyncType == SyncType.Reinitialize || context.SyncType == SyncType.ReinitializeWithUpload;

                        // When we get the chnages from server, we create the batches if it's requested by the client
                        // the batch decision comes from batchsize from client
                        (context, serverBatchInfo, serverChangesSelected) =
                            await this.Provider.GetChangeBatchAsync(context,
                                new MessageGetChangesBatch(scope.Id, Guid.Empty, fromScratch, scope.LastServerSyncTimestamp,
                                    schema, clientBatchSize, batchDirectory),
                                    connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();


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

            return (context, remoteClientTimestamp, serverBatchInfo, policy, serverChangesSelected);
        }


        public async Task DeleteMetadatasAsync(SyncContext context, SyncSetup setup, long timeStampStart,
                                     CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            DbTransaction transaction = null;
            long remoteClientTimestamp;

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

                        (context, remoteClientTimestamp) = this.Provider.GetLocalTimestampAsync(context, connection, transaction, cancellationToken, progress);

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
    }

}