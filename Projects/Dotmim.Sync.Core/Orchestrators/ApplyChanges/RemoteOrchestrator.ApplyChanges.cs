using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Extensions;
using Dotmim.Sync.Serialization;
using System;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains internal methods to apply changes on remote provider.
    /// </summary>
    public partial class RemoteOrchestrator : BaseOrchestrator
    {
        /// <summary>
        /// Apply changes on remote provider.
        /// </summary>
        internal virtual async Task<(SyncContext Context, ServerSyncChanges ServerSyncChanges, ConflictResolutionPolicy ServerResolutionPolicy)>
            InternalApplyThenGetChangesAsync(ScopeInfoClient cScopeInfoClient, ScopeInfo cScopeInfo, SyncContext context, ClientSyncChanges clientChanges,
            DbConnection connection = default, DbTransaction transaction = default, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            try
            {
                if (this.Provider == null)
                    throw new MissingProviderException(nameof(this.InternalApplyThenGetChangesAsync));

                var serializer = SerializersFactory.JsonSerializerFactory.GetSerializer();

                var remoteClientTimestamp = 0L;
                DatabaseChangesSelected serverChangesSelected = null;

                var serverChangesApplied = new DatabaseChangesApplied();

                // Direction set to Upload
                context.SyncWay = SyncWay.Upload;

                // Connection & Transaction runner
                DbConnectionRunner runner = null;

                // Errors batch info
                BatchInfo errorsBatchInfo = null;

                // Storeing all failed rows in a Set
                var failedRows = cScopeInfo.Schema.Clone();

                // if not null, rollback
                Exception failureException = null;

                // Previous sync errors
                BatchInfo lastSyncErrorsBatchInfo = null;

                // If we have a transient error happening, and we are rerunning the tranaction,
                // raising an interceptor
                var onRetry = new Func<Exception, int, TimeSpan, object, Task>((ex, cpt, ts, arg) =>
                    this.InterceptAsync(new TransientErrorOccuredArgs(context, connection, ex, cpt, ts), progress, cancellationToken).AsTask());

                // Defining my retry policy
                SyncPolicy retryPolicy = this.Options.TransactionMode == TransactionMode.AllOrNothing
                 ? retryPolicy = SyncPolicy.WaitAndRetryForever(retryAttempt => TimeSpan.FromMilliseconds(500 * retryAttempt), (ex, arg) => this.Provider.ShouldRetryOn(ex), onRetry)
                 : retryPolicy = SyncPolicy.WaitAndRetry(0, TimeSpan.Zero);

                await retryPolicy.ExecuteAsync(
                    async () =>
                    {
                        try
                        {
                            // Should we ?
                            if (cScopeInfo == null || cScopeInfo.Schema == null)
                                throw new MissingRemoteOrchestratorSchemaException();

                            // Deserialiaze schema
                            var schema = cScopeInfo.Schema;

                            // Transaction mode
                            if (this.Options.TransactionMode == TransactionMode.AllOrNothing)
                            {
                                runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.ChangesApplying,
                                    connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                                connection = runner.Connection;
                                transaction = runner.Transaction;
                                cancellationToken = runner.CancellationToken;
                                progress = runner.Progress;
                            }

                            // Create message containing everything we need to apply on server side
                            var applyChanges = new MessageApplyChanges(Guid.Empty, cScopeInfoClient.Id, false, cScopeInfoClient.LastServerSyncTimestamp, schema,
                                this.Options.ConflictResolutionPolicy, false, this.Options.BatchDirectory, clientChanges.ClientBatchInfo, failedRows, serverChangesApplied);

                            // call interceptor
                            var databaseChangesApplyingArgs = new DatabaseChangesApplyingArgs(context, applyChanges, connection, transaction);
                            await this.InterceptAsync(databaseChangesApplyingArgs, progress, cancellationToken).ConfigureAwait(false);

                            ScopeInfoClient sScopeInfoClient = null;

                            // Get scope info client from server, to get errors if any
                            using var runnerScopeInfo = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ScopeLoading, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                            await using (runnerScopeInfo.ConfigureAwait(false))
                            {
                                (context, sScopeInfoClient) = await this.InternalLoadScopeInfoClientAsync(
                                    context,
                                    runnerScopeInfo.Connection, runnerScopeInfo.Transaction, runnerScopeInfo.Progress, runnerScopeInfo.CancellationToken).ConfigureAwait(false);
                            }

                            // Getting errors batch info path, saved in scope_info_client table
                            if (sScopeInfoClient != null && !string.IsNullOrEmpty(sScopeInfoClient.Errors))
                            {
                                try
                                {
                                    lastSyncErrorsBatchInfo = !string.IsNullOrEmpty(sScopeInfoClient.Errors) ? serializer.Deserialize<BatchInfo>(sScopeInfoClient.Errors) : null;
                                }
                                catch (Exception)
                                {
                                }
                            }

                            //------------------------------------------------------------
                            // STEP 1: Remove errors that are part of batch info, then Try to reapply previous errors from last sync, if any
                            //------------------------------------------------------------

                            // If we have existing errors happened last sync, we should try to apply them now
                            if (lastSyncErrorsBatchInfo != null && lastSyncErrorsBatchInfo.HasData())
                            {
                                // Try to clean errors
                                applyChanges.Changes = clientChanges.ClientBatchInfo;
                                await this.InternalApplyCleanErrorsAsync(cScopeInfo, context, lastSyncErrorsBatchInfo, applyChanges, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                                // Call apply errors on provider
                                applyChanges.Changes = lastSyncErrorsBatchInfo;
                                failureException = await this.InternalApplyChangesAsync(cScopeInfo, context, applyChanges, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                            }

                            if (failureException != null)
                                throw failureException;

                            //------------------------------------------------------------
                            // STEP 2: Try to apply changes coming from client, if any
                            //------------------------------------------------------------
                            applyChanges.Changes = clientChanges.ClientBatchInfo;

                            if (clientChanges.ClientBatchInfo != null && clientChanges.ClientBatchInfo.HasData())
                            {
                                // Call provider to apply changes
                                failureException = await this.InternalApplyChangesAsync(cScopeInfo, context, applyChanges, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                            }

                            if (failureException != null)
                                throw failureException;

                            // Write failed rows to disk
                            if (failedRows.Tables.Any(st => st.HasRows))
                            {
                                var info = runner?.Connection != null && !string.IsNullOrEmpty(runner?.Connection.Database) ? $"{runner?.Connection.Database}_ERRORS" : "ERRORS";
                                errorsBatchInfo = new BatchInfo(this.Options.BatchDirectory, info: info);

                                var batchIndex = 0;
                                foreach (var table in failedRows.Tables)
                                {
                                    if (!table.HasRows)
                                        continue;

                                    using var localSerializer = new LocalJsonSerializer(this, context);

                                    var (filePath, fileName) = errorsBatchInfo.GetNewBatchPartInfoPath(table, batchIndex, "json", info);
                                    var batchPartInfo = new BatchPartInfo(fileName, table.TableName, table.SchemaName, SyncRowState.None, table.Rows.Count, batchIndex);
                                    errorsBatchInfo.BatchPartsInfo.Add(batchPartInfo);

                                    await localSerializer.OpenFileAsync(filePath, table, SyncRowState.None).ConfigureAwait(false);

                                    foreach (var row in table.Rows)
                                        await localSerializer.WriteRowToFileAsync(row, table).ConfigureAwait(false);

                                    await localSerializer.CloseFileAsync().ConfigureAwait(false);

                                    batchIndex++;
                                }
                            }

                            var databaseChangesAppliedArgs = new DatabaseChangesAppliedArgs(context, serverChangesApplied, connection, transaction);
                            await this.InterceptAsync(databaseChangesAppliedArgs, progress, cancellationToken).ConfigureAwait(false);

                            if (this.Options.TransactionMode == TransactionMode.AllOrNothing && runner != null)
                                await runner.CommitAsync().ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            if (runner != null)
                                await runner.RollbackAsync($"InternalApplyThenGetChangesAsync during apply changes Rollback. Error:{ex.Message}").ConfigureAwait(false);

                            throw this.GetSyncError(context, ex);
                        }
                        finally
                        {
                            if (runner != null)
                                await runner.DisposeAsync().ConfigureAwait(false);
                        }
                    }, cancellationToken).ConfigureAwait(false);

                failedRows.Clear();

                try
                {
                    //------------------------------------------------------------
                    // STEP 3: Get Changes from Server
                    //------------------------------------------------------------

                    // Get a no transaction runner for getting changes
                    // Create a new connection, since last one is disposed (at least on mysql)
                    runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesSelecting, default, default, progress, cancellationToken).ConfigureAwait(false);

                    context.ProgressPercentage = 0.55;

                    // Direction set to Download
                    context.SyncWay = SyncWay.Download;

                    // JUST Before get changes, get the timestamp, to be sure to
                    // get rows inserted / updated elsewhere since the sync is not over
                    (context, remoteClientTimestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    // Get if we need to get all rows from the datasource
                    var fromScratch = cScopeInfoClient.IsNewScope || context.SyncType == SyncType.Reinitialize || context.SyncType == SyncType.ReinitializeWithUpload;

                    // Create a batch info
                    var info = runner.Connection != null && !string.IsNullOrEmpty(runner.Connection.Database) ? $"{runner.Connection.Database}_REMOTE_GETCHANGES" : "REMOTE_GETCHANGES";
                    var serverBatchInfo = new BatchInfo(this.Options.BatchDirectory, info: info);

                    // Call interceptor
                    var databaseChangesSelectingArgs = new DatabaseChangesSelectingArgs(
                        context, serverBatchInfo.GetDirectoryFullPath(), this.Options.BatchSize, fromScratch,
                        cScopeInfoClient.LastServerSyncTimestamp, runner.Connection, runner.Transaction);

                    await this.InterceptAsync(databaseChangesSelectingArgs, progress, cancellationToken).ConfigureAwait(false);

                    if (runner.CancellationToken.IsCancellationRequested)
                        runner.CancellationToken.ThrowIfCancellationRequested();

                    // When we get the chnages from server, we create the batches if it's requested by the client
                    // the batch decision comes from batchsize from client
                    serverChangesSelected = await this.InternalGetChangesAsync(cScopeInfo, context, fromScratch, cScopeInfoClient.LastServerSyncTimestamp, cScopeInfoClient.Id,
                        this.Provider.SupportsMultipleActiveResultSets, serverBatchInfo,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (runner.CancellationToken.IsCancellationRequested)
                        runner.CancellationToken.ThrowIfCancellationRequested();

                    // generate the new scope item
                    this.CompleteTime = DateTime.UtcNow;

                    // generate the new scope item
                    var sScopeInfoClient = new ScopeInfoClient
                    {
                        Name = cScopeInfo.Name,
                        Hash = cScopeInfoClient.Hash,
                        Parameters = cScopeInfoClient.Parameters,
                        Id = cScopeInfoClient.Id,
                        IsNewScope = cScopeInfoClient.IsNewScope,
                        LastSyncTimestamp = clientChanges.ClientTimestamp,
                        LastSync = this.CompleteTime,
                        LastServerSyncTimestamp = remoteClientTimestamp,
                        LastSyncDuration = this.CompleteTime.Value.Subtract(context.StartTime).Ticks,
                        Properties = cScopeInfoClient.Properties,
                        Errors = errorsBatchInfo != null && errorsBatchInfo.BatchPartsInfo != null && errorsBatchInfo.BatchPartsInfo.Count > 0 ? serializer.Serialize(errorsBatchInfo).ToUtf8String() : null,
                    };

                    // Save scope info client coming from client
                    // to scope info client table on server
                    await this.InternalSaveScopeInfoClientAsync(sScopeInfoClient, context, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    var serverSyncChanges = new ServerSyncChanges(remoteClientTimestamp, serverBatchInfo, serverChangesSelected, serverChangesApplied);

                    var databaseChangesSelectedArgs = new DatabaseChangesSelectedArgs(context, cScopeInfoClient.LastServerSyncTimestamp, serverBatchInfo, serverChangesSelected, runner.Connection, runner.Transaction);
                    await this.InterceptAsync(databaseChangesSelectedArgs, progress, cancellationToken).ConfigureAwait(false);

                    if (runner.CancellationToken.IsCancellationRequested)
                        runner.CancellationToken.ThrowIfCancellationRequested();

                    return (context, serverSyncChanges, this.Options.ConflictResolutionPolicy);
                }
                catch (Exception ex)
                {
                    if (runner != null)
                        await runner.RollbackAsync($"InternalApplyChangesAsync Rollback during getchanges. Error:{ex.Message}").ConfigureAwait(false);

                    throw this.GetSyncError(context, ex);
                }
                finally
                {
                    if (runner != null)
                        await runner.DisposeAsync().ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }
    }
}