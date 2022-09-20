using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public partial class RemoteOrchestrator : BaseOrchestrator
    {

        /// <summary>
        /// Apply changes on remote provider
        /// </summary>
        internal virtual async Task<(SyncContext context, ServerSyncChanges serverSyncChanges, ConflictResolutionPolicy serverResolutionPolicy)>
            InternalApplyThenGetChangesAsync(ScopeInfoClient cScopeInfoClient, ScopeInfo cScopeInfo, SyncContext context, ClientSyncChanges clientChanges,
            DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                if (Provider == null)
                    throw new MissingProviderException(nameof(InternalApplyThenGetChangesAsync));

                long remoteClientTimestamp = 0L;
                DatabaseChangesSelected serverChangesSelected = null;

                DatabaseChangesApplied serverChangesApplied = new DatabaseChangesApplied();

                //Direction set to Upload
                context.SyncWay = SyncWay.Upload;

                // Connection & Transaction runner
                DbConnectionRunner runner = null;

                // Errors batch info
                BatchInfo errorsBatchInfo = null;

                // Storeing all failed rows in a Set
                SyncSet failedRows = cScopeInfo.Schema.Clone();

                // if not null, rollback
                Exception failureException = null;

                // Previous sync errors
                BatchInfo lastSyncErrorsBatchInfo = null;

                try
                {
                    // Should we ?
                    if (cScopeInfo == null || cScopeInfo.Schema == null)
                        throw new MissingRemoteOrchestratorSchemaException();

                    // Deserialiaze schema
                    var schema = cScopeInfo.Schema;

                    // Transaction mode
                    if (Options.TransactionMode == TransactionMode.AllOrNothing)
                    {
                        runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.ChangesApplying,
                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);

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
                    await using (var runnerScopeInfo = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ScopeLoading,
                        connection, transaction, cancellationToken , progress).ConfigureAwait(false))
                    {
                        (context, sScopeInfoClient) = await this.InternalLoadScopeInfoClientAsync(context,
                            runnerScopeInfo.Connection, runnerScopeInfo.Transaction, runnerScopeInfo.CancellationToken, runnerScopeInfo.Progress).ConfigureAwait(false);
                    }

                    // Getting errors batch info path, saved in scope_info_client table
                    if (sScopeInfoClient != null && !string.IsNullOrEmpty(sScopeInfoClient.Errors))
                    {
                        try
                        {
                            lastSyncErrorsBatchInfo = !string.IsNullOrEmpty(sScopeInfoClient.Errors) ? JsonConvert.DeserializeObject<BatchInfo>(sScopeInfoClient.Errors) : null;
                        }
                        catch (Exception) { }
                    }

                    //------------------------------------------------------------
                    // STEP 1: Remove errors that are part of batch info, then Try to reapply previous errors from last sync, if any
                    //------------------------------------------------------------


                    // If we have existing errors happened last sync, we should try to apply them now
                    if (lastSyncErrorsBatchInfo != null && lastSyncErrorsBatchInfo.HasData())
                    {
                        // Try to clean errors
                        applyChanges.Changes = clientChanges.ClientBatchInfo;
                        await this.InternalApplyCleanErrorsAsync(cScopeInfo, context, lastSyncErrorsBatchInfo, applyChanges, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Call apply errors on provider
                        applyChanges.Changes = lastSyncErrorsBatchInfo;
                        failureException = await this.InternalApplyChangesAsync(cScopeInfo, context, applyChanges, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
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
                        failureException = await this.InternalApplyChangesAsync(cScopeInfo, context, applyChanges,
                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    }

                    if (failureException != null)
                        throw failureException;

                    // Write failed rows to disk
                    if (failedRows.Tables.Any(st => st.HasRows))
                    {
                        string info = runner?.Connection != null && !string.IsNullOrEmpty(runner?.Connection.Database) ? $"{runner?.Connection.Database}_ERRORS" : "ERRORS";
                        errorsBatchInfo = new BatchInfo(this.Options.BatchDirectory, info: info);

                        int batchIndex = 0;
                        foreach (var table in failedRows.Tables)
                        {
                            if (!table.HasRows)
                                continue;

                            var localSerializer = new LocalJsonSerializer();

                            var (filePath, fileName) = errorsBatchInfo.GetNewBatchPartInfoPath(table, batchIndex, "json", info);
                            var batchPartInfo = new BatchPartInfo(fileName, table.TableName, table.SchemaName, table.Rows.Count, batchIndex);
                            errorsBatchInfo.BatchPartsInfo.Add(batchPartInfo);

                            await localSerializer.OpenFileAsync(filePath, table).ConfigureAwait(false);

                            foreach (var row in table.Rows)
                                await localSerializer.WriteRowToFileAsync(row, table).ConfigureAwait(false);

                            await localSerializer.CloseFileAsync();
                            batchIndex++;
                        }

                        failedRows.Dispose();
                    }


                    var databaseChangesAppliedArgs = new DatabaseChangesAppliedArgs(context, serverChangesApplied, connection, transaction);
                    await this.InterceptAsync(databaseChangesAppliedArgs, progress, cancellationToken).ConfigureAwait(false);

                    if (Options.TransactionMode == TransactionMode.AllOrNothing && runner != null)
                        await runner.CommitAsync(false).ConfigureAwait(false);

                }
                catch (Exception)
                {
                    if (runner != null)
                    {
                        await runner.RollbackAsync().ConfigureAwait(false);
                        await runner.DisposeAsync().ConfigureAwait(false);
                    }
                    throw;
                }

                try
                {

                    //------------------------------------------------------------
                    // STEP 3: Get Changes from Server
                    //------------------------------------------------------------

                    // Get a no transaction runner for getting changes
                    runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesSelecting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    context.ProgressPercentage = 0.55;

                    //Direction set to Download
                    context.SyncWay = SyncWay.Download;

                    // JUST Before get changes, get the timestamp, to be sure to 
                    // get rows inserted / updated elsewhere since the sync is not over
                    (context, remoteClientTimestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress);

                    // Get if we need to get all rows from the datasource
                    var fromScratch = cScopeInfoClient.IsNewScope || context.SyncType == SyncType.Reinitialize || context.SyncType == SyncType.ReinitializeWithUpload;

                    // Create a batch info
                    string info = connection != null && !string.IsNullOrEmpty(connection.Database) ? $"{connection.Database}_REMOTE_GETCHANGES" : "REMOTE_GETCHANGES";
                    var serverBatchInfo = new BatchInfo(this.Options.BatchDirectory, info: info);

                    // Call interceptor
                    var databaseChangesSelectingArgs = new DatabaseChangesSelectingArgs(context, serverBatchInfo.GetDirectoryFullPath(), this.Options.BatchSize, fromScratch,
                        cScopeInfoClient.LastServerSyncTimestamp, remoteClientTimestamp,
                        runner.Connection, runner.Transaction);

                    await this.InterceptAsync(databaseChangesSelectingArgs, progress, cancellationToken).ConfigureAwait(false);

                    if (runner.CancellationToken.IsCancellationRequested)
                        runner.CancellationToken.ThrowIfCancellationRequested();

                    // When we get the chnages from server, we create the batches if it's requested by the client
                    // the batch decision comes from batchsize from client
                    serverChangesSelected = await this.InternalGetChangesAsync(cScopeInfo, context, fromScratch, cScopeInfoClient.LastServerSyncTimestamp, remoteClientTimestamp, cScopeInfoClient.Id,
                        this.Provider.SupportsMultipleActiveResultSets, serverBatchInfo,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

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
                        Errors = errorsBatchInfo != null && errorsBatchInfo.BatchPartsInfo != null && errorsBatchInfo.BatchPartsInfo.Count > 0 ? JsonConvert.SerializeObject(errorsBatchInfo) : null,
                    };

                    // Save scope info client coming from client
                    // to scope info client table on server
                    await this.InternalSaveScopeInfoClientAsync(sScopeInfoClient, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    var serverSyncChanges = new ServerSyncChanges(remoteClientTimestamp, serverBatchInfo, serverChangesSelected, serverChangesApplied);

                    var databaseChangesSelectedArgs = new DatabaseChangesSelectedArgs(context, cScopeInfoClient.LastServerSyncTimestamp, remoteClientTimestamp, 
                        serverBatchInfo, serverChangesSelected, runner.Connection, runner.Transaction);

                    await this.InterceptAsync(databaseChangesSelectedArgs, progress, cancellationToken).ConfigureAwait(false);

                    if (runner.CancellationToken.IsCancellationRequested)
                        runner.CancellationToken.ThrowIfCancellationRequested();

                    return (context, serverSyncChanges, this.Options.ConflictResolutionPolicy);
                }
                catch (Exception)
                {
                    if (runner != null)
                        await runner.RollbackAsync().ConfigureAwait(false);
                    throw;
                }
                finally
                {
                    if (runner != null)
                        await runner.DisposeAsync().ConfigureAwait(false);
                }

            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }



    }
}
