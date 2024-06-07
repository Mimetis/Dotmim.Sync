
using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public partial class LocalOrchestrator : BaseOrchestrator
    {

        /// <summary>
        /// Apply changes locally
        /// </summary>
        internal virtual async Task<(SyncContext context, ClientSyncChanges clientSyncChange, ScopeInfoClient CScopeInfoClient)>
            InternalApplyChangesAsync(ScopeInfo cScopeInfo, ScopeInfoClient cScopeInfoClient, SyncContext context, ServerSyncChanges serverSyncChanges,
                              ClientSyncChanges clientSyncChanges, ConflictResolutionPolicy policy, bool snapshotApplied,
                              DbConnection connection = default, DbTransaction transaction = default,
                              CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            // If we have a transient error happening, and we are rerunning the tranaction,
            // raising an interceptor
            var onRetry = new Func<Exception, int, TimeSpan, object, Task>((ex, cpt, ts, arg) =>
                this.InterceptAsync(new TransientErrorOccuredArgs(context, connection, ex, cpt, ts), progress, cancellationToken));

            // Defining my retry policy
            SyncPolicy retryPolicy = Options.TransactionMode == TransactionMode.AllOrNothing
             ? retryPolicy = SyncPolicy.WaitAndRetryForever(retryAttempt => TimeSpan.FromMilliseconds(500 * retryAttempt), (ex, arg) => this.Provider.ShouldRetryOn(ex), onRetry)
             : retryPolicy = SyncPolicy.WaitAndRetry(0, TimeSpan.Zero);

            // Execute my OpenAsync in my policy context
            var applyChangesResult = await retryPolicy.ExecuteAsync<(SyncContext context, ClientSyncChanges clientSyncChange, ScopeInfoClient CScopeInfoClient)>(async ct =>
            {
                // Connection & Transaction runner
                DbConnectionRunner runner = null;
                try
                {
                    var serverBatchInfo = serverSyncChanges.ServerBatchInfo;
                    var remoteClientTimestamp = serverSyncChanges.RemoteClientTimestamp;

                    // applied changes to clients
                    DatabaseChangesApplied clientChangesApplied = new DatabaseChangesApplied();

                    // Create a message containing everything needed to apply errors rows
                    BatchInfo lastSyncErrorsBatchInfo = null;

                    // Storeing all failed rows in a Set
                    SyncSet failedRows = cScopeInfo.Schema.Clone();

                    // if not null, rollback
                    Exception failureException = null;

                    // BatchInfo containing errors
                    BatchInfo errorsBatchInfo = null;

                    // Gets the existing errors from past sync
                    if (cScopeInfoClient != null && !string.IsNullOrEmpty(cScopeInfoClient.Errors))
                    {
                        try
                        {
                            lastSyncErrorsBatchInfo = !string.IsNullOrEmpty(cScopeInfoClient.Errors) ? JsonConvert.DeserializeObject<BatchInfo>(cScopeInfoClient.Errors) : null;
                        }
                        catch (Exception) { }
                    }

                    context.SyncWay = SyncWay.Download;

                    // Transaction mode
                    if (Options.TransactionMode == TransactionMode.AllOrNothing)
                    {
                        runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // affect connection and transaction to reaffect later on save scope
                        connection = runner.Connection;
                        transaction = runner.Transaction;
                        cancellationToken = runner.CancellationToken;
                        progress = runner.Progress;
                    }

                    // Create the message containing everything needed to apply changes
                    var applyChanges = new MessageApplyChanges(cScopeInfoClient.Id, Guid.Empty, cScopeInfoClient.IsNewScope, cScopeInfoClient.LastSyncTimestamp,
                        cScopeInfo.Schema, policy, snapshotApplied, this.Options.BatchDirectory, serverBatchInfo, failedRows, clientChangesApplied);

                    // call interceptor
                    var databaseChangesApplyingArgs = new DatabaseChangesApplyingArgs(context, applyChanges, connection, transaction);
                    await this.InterceptAsync(databaseChangesApplyingArgs, progress, cancellationToken).ConfigureAwait(false);

                    // If we have existing errors happened last sync, we should try to apply them now
                    if (lastSyncErrorsBatchInfo != null && lastSyncErrorsBatchInfo.HasData())
                    {
                        // Try to clean errors before trying
                        applyChanges.Changes = serverBatchInfo;
                        await this.InternalApplyCleanErrorsAsync(cScopeInfo, context, lastSyncErrorsBatchInfo, applyChanges, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Call apply errors on provider
                        applyChanges.Changes = lastSyncErrorsBatchInfo;
                        failureException = await this.InternalApplyChangesAsync(cScopeInfo, context, applyChanges, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    }

                    if (failureException != null)
                        throw failureException;

                    if (serverBatchInfo != null && serverBatchInfo.HasData())
                    {
                        // Call apply changes on provider
                        applyChanges.Changes = serverBatchInfo;
                        failureException = await this.InternalApplyChangesAsync(cScopeInfo, context, applyChanges, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    }

                    if (failureException != null)
                        throw failureException;

                    if (cancellationToken.IsCancellationRequested)
                        cancellationToken.ThrowIfCancellationRequested();

                    // check if we need to delete metadatas
                    if (this.Options.CleanMetadatas && cScopeInfoClient.LastSyncTimestamp.HasValue)
                    {
                        using (var runnerMetadata = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.MetadataCleaning, connection, transaction, cancellationToken, progress).ConfigureAwait(false))
                        {
                            var allScopeHistories = await this.InternalLoadAllScopeInfoClientsAsync(context, runnerMetadata.Connection, runnerMetadata.Transaction, runnerMetadata.CancellationToken, runnerMetadata.Progress).ConfigureAwait(false);

                            List<ScopeInfo> allClientScopes;
                            (context, allClientScopes) = await this.InternalLoadAllScopeInfosAsync(context, runnerMetadata.Connection, runnerMetadata.Transaction, runnerMetadata.CancellationToken, runnerMetadata.Progress).ConfigureAwait(false);

                            if (allScopeHistories.Count > 0 && allClientScopes.Count > 0)
                            {
                                // Get the min value from LastSyncTimestamp from all scopes
                                var minLastTimeStamp = allScopeHistories.Min(scope => scope.LastSyncTimestamp.HasValue ? scope.LastSyncTimestamp.Value : Int64.MaxValue);
                                minLastTimeStamp = minLastTimeStamp > cScopeInfoClient.LastSyncTimestamp.Value ? cScopeInfoClient.LastSyncTimestamp.Value : minLastTimeStamp;

                                DatabaseMetadatasCleaned databaseMetadatasCleaned;
                                (context, databaseMetadatasCleaned) = await this.InternalDeleteMetadatasAsync(allClientScopes, context, minLastTimeStamp, runnerMetadata.Connection, runnerMetadata.Transaction, runnerMetadata.CancellationToken, runnerMetadata.Progress).ConfigureAwait(false);

                                // save last cleanup timestamp
                                if (databaseMetadatasCleaned?.RowsCleanedCount > 0)
                                {
                                    foreach (var clientScopeInfo in allClientScopes)
                                    {
                                        clientScopeInfo.LastCleanupTimestamp = databaseMetadatasCleaned.TimestampLimit;

                                        await this.InternalSaveScopeInfoAsync(clientScopeInfo, context,
                                            runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
                                    }
                                }
                            }
                        };
                    }

                    // now the sync is complete, remember the time
                    this.CompleteTime = DateTime.UtcNow;

                    // Save all failed rows to disk
                    if (failedRows.Tables.Any(st => st.HasRows))
                    {
                        // Create a batch info for error rows
                        string info = connection != null && !string.IsNullOrEmpty(connection.Database) ? $"{connection.Database}_ERRORS" : "ERRORS";
                        errorsBatchInfo = new BatchInfo(this.Options.BatchDirectory, info: info);

                        int batchIndex = 0;
                        foreach (var table in failedRows.Tables)
                        {
                            if (!table.HasRows)
                                continue;

                            var localSerializer = new LocalJsonSerializer(this, context);

                            var (filePath, fileName) = errorsBatchInfo.GetNewBatchPartInfoPath(table, batchIndex, "json", info);
                            var batchPartInfo = new BatchPartInfo(fileName, table.TableName, table.SchemaName, SyncRowState.None, table.Rows.Count, batchIndex);
                            errorsBatchInfo.BatchPartsInfo.Add(batchPartInfo);

                            localSerializer.OpenFile(filePath, table, SyncRowState.None);

                            foreach (var row in table.Rows)
                                await localSerializer.WriteRowToFileAsync(row, table).ConfigureAwait(false);

                            localSerializer.CloseFile();
                            batchIndex++;
                        }
                        failedRows.Dispose();
                    }

                    // generate the new scope item
                    var newCScopeInfoClient = new ScopeInfoClient
                    {
                        Hash = cScopeInfoClient.Hash,
                        Name = cScopeInfoClient.Name,
                        Parameters = cScopeInfoClient.Parameters,
                        Id = cScopeInfoClient.Id,
                        IsNewScope = cScopeInfoClient.IsNewScope,
                        LastSyncTimestamp = clientSyncChanges.ClientTimestamp,
                        LastSync = this.CompleteTime,
                        LastServerSyncTimestamp = remoteClientTimestamp,
                        LastSyncDuration = this.CompleteTime.Value.Subtract(context.StartTime).Ticks,
                        Properties = cScopeInfoClient.Properties,
                        Errors = errorsBatchInfo != null && errorsBatchInfo.BatchPartsInfo != null && errorsBatchInfo.BatchPartsInfo.Count > 0 ? JsonConvert.SerializeObject(errorsBatchInfo) : null,
                    };

                    // Write scopes locally
                    using (var runnerScopeInfo = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ScopeWriting, connection, transaction, cancellationToken, progress).ConfigureAwait(false))
                    {
                        (context, cScopeInfoClient) = await this.InternalSaveScopeInfoClientAsync(newCScopeInfoClient, context,
                            runnerScopeInfo.Connection, runnerScopeInfo.Transaction, runnerScopeInfo.CancellationToken, runnerScopeInfo.Progress).ConfigureAwait(false);
                    };

                    var databaseChangesAppliedArgs = new DatabaseChangesAppliedArgs(context, clientChangesApplied, connection ??= this.Provider.CreateConnection(), transaction);
                    await this.InterceptAsync(databaseChangesAppliedArgs, progress, cancellationToken).ConfigureAwait(false);

                    if (Options.TransactionMode == TransactionMode.AllOrNothing && runner != null)
                        await runner.CommitAsync().ConfigureAwait(false);

                    clientSyncChanges.ClientChangesApplied = clientChangesApplied;

                    return (context, clientSyncChanges, cScopeInfoClient);

                }
                catch (Exception ex)
                {
                    if (runner != null)
                        await runner.RollbackAsync($"InternalApplyChangesAsync Rollback. Error:{ex.Message}").ConfigureAwait(false);

                    throw GetSyncError(context, ex);
                }
                finally
                {
                    if (runner != null)
                        await runner.DisposeAsync().ConfigureAwait(false);
                }
            });

            return applyChangesResult;
        }

        /// <summary>
        /// Apply a snapshot locally
        /// </summary>
        internal virtual async Task<(SyncContext context, ClientSyncChanges clientSyncChanges, ScopeInfoClient cScopeInfoClient)>
            InternalApplySnapshotAsync(ScopeInfo clientScopeInfo,
            ScopeInfoClient cScopeInfoClient,
            SyncContext context, ServerSyncChanges serverSyncChanges, ClientSyncChanges clientSyncChanges,
            DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                if (serverSyncChanges?.ServerBatchInfo == null)
                    return (context, clientSyncChanges, cScopeInfoClient);

                // Get context or create a new one
                context.SyncStage = SyncStage.SnapshotApplying;
                await this.InterceptAsync(new SnapshotApplyingArgs(context, this.Provider.CreateConnection()), progress, cancellationToken).ConfigureAwait(false);

                if (clientScopeInfo.Schema == null)
                    throw new ArgumentNullException(nameof(clientScopeInfo.Schema));

                // Applying changes and getting the new client scope info
                (context, clientSyncChanges, cScopeInfoClient) = await this.InternalApplyChangesAsync(clientScopeInfo, cScopeInfoClient, context, serverSyncChanges,
                        clientSyncChanges, ConflictResolutionPolicy.ServerWins, false, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                await this.InterceptAsync(new SnapshotAppliedArgs(context, clientSyncChanges.ClientChangesApplied), progress, cancellationToken).ConfigureAwait(false);

                // re-apply scope is new flag
                // to be sure we are calling the Initialize method, even for the delta
                // in that particular case, we want the delta rows coming from the current scope
                // cScopeInfoClient.IsNewScope = true;

                return (context, clientSyncChanges, cScopeInfoClient);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }


    }
}
