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
        public SyncSchema Schema { get; set; }
        public SyncOptions Options { get; set; }

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
        public async Task<(SyncContext, ScopeInfo, ScopeInfo, SyncSchema)>
            EnsureScopeAsync(SyncContext context, SyncSchema schema, SyncOptions options, Guid clientScopeId,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            // output
            List<ScopeInfo> serverScopes;


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

                        // ----------------------------------------
                        // 0) Begin Session 
                        // ----------------------------------------

                        // Send the configuration to remote provider and get back a new one if needed
                        context = await this.Provider.BeginSessionAsync(context, cancellationToken, progress).ConfigureAwait(false);

                        // ----------------------------------------
                        // 1) Check remote scopes
                        // ----------------------------------------
                        (context, serverScopes) = await this.Provider.EnsureScopesAsync(context,
                            new MessageEnsureScopes(options.ScopeInfoTableName, schema.ScopeName, clientScopeId), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (serverScopes.Count != 2)
                            throw new Exception("On Remote provider, we should have two scopes (one for server and one for client side)");

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        var serverScopeInfo = serverScopes.First(s => s.Id != clientScopeId);
                        var localScopeReferenceInfo = serverScopes.First(s => s.Id == clientScopeId);

                        // ----------------------------------------
                        // 2) Get Schema from remote provider
                        // ----------------------------------------
                        (context, schema) = await this.Provider.EnsureSchemaAsync(context, schema, 
                                                    connection, transaction, cancellationToken, progress
                                                ).ConfigureAwait(false);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        // ----------------------------------------
                        // 3) Ensure databases are ready
                        // ----------------------------------------

                        // Server should have already the schema
                        context = await this.Provider.EnsureDatabaseAsync(context,
                            new MessageEnsureDatabase(serverScopeInfo, schema.Set, schema.Filters, schema.SerializationFormat),
                            connection, transaction,
                            cancellationToken, progress).ConfigureAwait(false);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        // Save locally the configuration
                        this.Schema = schema;
                        this.Options = options;

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(context, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();

                        return (context, serverScopeInfo, localScopeReferenceInfo, schema);
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

        public async Task<(SyncContext, BatchInfo, DatabaseChangesSelected)>
            ApplyThenGetChangesAsync(SyncContext context,
                                     Guid clientScopeId, ScopeInfo localScopeReferenceInfo,
                                     ScopeInfo serverScopeInfo, BatchInfo clientBatchInfo,
                                     CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            long serverTimestamp;
            DatabaseChangesSelected serverChangesSelected;
            BatchInfo serverBatchInfo;

            // fromId : When applying rows, make sure it's identified as applied by this client scope
            var fromId = clientScopeId;
            // lastSyncTS : apply lines only if thye are not modified since last client sync
            var lastSyncTS = localScopeReferenceInfo.LastSyncTimestamp;
            // isNew : not needed
            var isNew = false;
            var scope = new ScopeInfo { Id = fromId, IsNewScope = isNew, Timestamp = lastSyncTS };

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
                             new MessageApplyChanges(scope, Schema, this.Schema.ConflictResolutionPolicy, this.Options.DisableConstraintsOnApplyChanges,
                                        this.Options.UseBulkOperations, this.Options.CleanMetadatas, Options.ScopeInfoTableName,
                                        clientBatchInfo),
                             connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // if ConflictResolutionPolicy.ClientWins or Handler set to Client wins
                        // Conflict occurs here and server loose. 
                        // Conflicts count should be temp saved because applychanges on client side won't raise any conflicts (and so property Context.TotalSyncConflicts will be reset to 0)
                        // var conflictsOnRemoteCount = context.TotalSyncConflicts;

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        // Get changes from server
                        // fromId : Make sure we don't select lines on server that has been already updated by the client
                        fromId = clientScopeId;
                        // lastSyncTS : apply lines only if thye are not modified since last client sync
                        lastSyncTS = localScopeReferenceInfo.LastSyncTimestamp;
                        // isNew : make sure we take all lines if it's the first time we get 
                        isNew = localScopeReferenceInfo.IsNewScope;
                        scope = new ScopeInfo { Id = fromId, IsNewScope = isNew, Timestamp = lastSyncTS };
                        //Direction set to Download
                        context.SyncWay = SyncWay.Download;

                        // JUST Before get changes, get the timestamp, to be sure to 
                        // get rows inserted / updated elsewhere since the sync is not over
                        (context, serverTimestamp) = this.Provider.GetLocalTimestampAsync(context,
                            new MessageTimestamp(Options.ScopeInfoTableName, Schema.SerializationFormat), connection, transaction, cancellationToken, progress);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        // don't care about progress ?
                        (context, serverBatchInfo, serverChangesSelected) =
                            await this.Provider.GetChangeBatchAsync(context,
                                new MessageGetChangesBatch(scope,
                                    this.Schema.Set, this.Options.BatchSize, this.Options.BatchDirectory, this.Schema.ConflictResolutionPolicy,
                                    this.Schema.Filters, this.Schema.SerializationFormat),
                                    connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        // now the sync is complete, remember the time
                        context.CompleteTime = DateTime.Now;

                        // Set the correct scope local / remote
                        serverScopeInfo.IsLocal = true;
                        localScopeReferenceInfo.IsLocal = false;

                        // Eventually we know now, it's not a new sync anymore
                        serverScopeInfo.IsNewScope = false;
                        localScopeReferenceInfo.IsNewScope = false;

                        serverScopeInfo.LastSync = context.CompleteTime;
                        localScopeReferenceInfo.LastSync = context.CompleteTime;

                        // Set the right timestamp
                        serverScopeInfo.LastSyncTimestamp = serverTimestamp;
                        localScopeReferenceInfo.LastSyncTimestamp = serverTimestamp;

                        // Calculate server duration
                        var duration = context.CompleteTime.Subtract(context.StartTime);
                        serverScopeInfo.LastSyncDuration = duration.Ticks;
                        localScopeReferenceInfo.LastSyncDuration = duration.Ticks;

                        // Write server scopes
                        context = await this.Provider.WriteScopesAsync(context,
                                        new MessageWriteScopes(this.Options.ScopeInfoTableName, new List<ScopeInfo> { serverScopeInfo, localScopeReferenceInfo }, this.Schema.SerializationFormat),
                                        connection, transaction, cancellationToken, progress
                                        ).ConfigureAwait(false);

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

            return (context, serverBatchInfo, serverChangesSelected);
        }

    }
}
