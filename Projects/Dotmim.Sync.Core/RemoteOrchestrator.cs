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
            EnsureSchemaAsync(SyncContext context, SyncSet schema,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            // Is it the first time we come to ensure schema ?
            var checkSchema = schema.HasTables && !schema.HasColumns;

            if (!checkSchema)
                return (context, schema);

            using (var connection = this.Provider.CreateConnection())
            {
                await connection.OpenAsync().ConfigureAwait(false);
                await this.Provider.InterceptAsync(new ConnectionOpenArgs(context, connection)).ConfigureAwait(false);

                // Create a transaction
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        // Open the connection
                        // Interceptors
                        await this.Provider.InterceptAsync(new TransactionOpenArgs(context, connection, transaction)).ConfigureAwait(false);

                        // Begin Session 
                        context = await this.Provider.BeginSessionAsync(context, cancellationToken, progress).ConfigureAwait(false);


                        // Get Schema from remote provider
                        (context, schema) = await this.Provider.EnsureSchemaAsync(context, schema, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

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
                    catch (Exception)
                    {

                        throw;
                    }
                    finally
                    {
                        if (transaction != null)
                            transaction.Dispose();

                        if (connection != null && connection.State == ConnectionState.Open)
                            connection.Close();

                        await this.Provider.InterceptAsync(new ConnectionCloseArgs(context, connection, transaction)).ConfigureAwait(false);

                    }

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

                        DatabaseChangesApplied changesApplied;

                        (context, changesApplied) =
                            await this.Provider.ApplyChangesAsync(context,
                             new MessageApplyChanges(scope.Id, false, scope.LastServerSyncTimestamp, schema, policy,
                                        disableConstraintsOnApplyChanges, useBulkOperations, cleanMetadatas, clientBatchInfo),
                             connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // if ConflictResolutionPolicy.ClientWins or Handler set to Client wins
                        // Conflict occurs here and server loose. 
                        // Conflicts count should be temp saved because applychanges on client side won't raise any conflicts (and so property Context.TotalSyncConflicts will be reset to 0)
                        // var conflictsOnRemoteCount = context.TotalSyncConflicts;

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        //Direction set to Download
                        context.SyncWay = SyncWay.Download;

                        // JUST Before get changes, get the timestamp, to be sure to 
                        // get rows inserted / updated elsewhere since the sync is not over
                        (context, remoteClientTimestamp) = this.Provider.GetLocalTimestampAsync(context, connection, transaction, cancellationToken, progress);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        // When we get the chnages from server, we create the batches if it's requested by the client
                        // the batch decision comes from batchsize from client
                        (context, serverBatchInfo, serverChangesSelected) =
                            await this.Provider.GetChangeBatchAsync(context,
                                new MessageGetChangesBatch(scope.Id, scope.IsNewScope, scope.LastServerSyncTimestamp,
                                    schema, clientBatchSize, batchDirectory),
                                    connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();


                        await this.Provider.InterceptAsync(new TransactionCommitArgs(context, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();

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

            return (context, remoteClientTimestamp, serverBatchInfo, policy, serverChangesSelected);
        }

    }
}
