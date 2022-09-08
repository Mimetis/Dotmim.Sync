using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
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
                BatchInfo serverBatchInfo = null;

                //Direction set to Upload
                context.SyncWay = SyncWay.Upload;

                // Connection & Transaction runner
                DbConnectionRunner runner = null;

                // Errors batch info
                BatchInfo errorsBatchInfo = null;

                try
                {
                    // Should we ?
                    if (cScopeInfo == null || cScopeInfo.Schema == null)
                        throw new MissingRemoteOrchestratorSchemaException();

                    // Deserialiaze schema
                    var schema = cScopeInfo.Schema;

                    // Transaction mode
                    if (Options.TransactionMode == TransactionMode.AllOrNothing)
                        runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.ChangesApplying,
                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);


                    //------------------------------------------------------------
                    // STEP 1: Try to reapply previous errors from last sync, if any
                    //------------------------------------------------------------

                    ScopeInfoClient sScopeInfoClient = null;
                    // Get scope info client from server, to get errors if any
                    await using (var runnerScopeInfo = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ScopeLoading, 
                        runner?.Connection, runner?.Transaction, runner != null ? runner.CancellationToken : default, runner?.Progress).ConfigureAwait(false))
                    {
                        (context, sScopeInfoClient) = await this.InternalLoadScopeInfoClientAsync(context,
                            runnerScopeInfo.Connection, runnerScopeInfo.Transaction, runnerScopeInfo.CancellationToken, runnerScopeInfo.Progress).ConfigureAwait(false);
                    }

                    
                    if (sScopeInfoClient != null && !string.IsNullOrEmpty(sScopeInfoClient.Errors))
                    {
                        // Create a message containing everything needed to apply errors rows
                        BatchInfo retryErrorsBatchInfo = null;

                        // If we can't parse correctly the errors field, just silentely fail and go next.
                        try
                        {
                            retryErrorsBatchInfo = !string.IsNullOrEmpty(sScopeInfoClient.Errors) ? JsonConvert.DeserializeObject<BatchInfo>(sScopeInfoClient.Errors) : null;
                        }
                        catch (Exception) { }

                        if (retryErrorsBatchInfo != null && retryErrorsBatchInfo.HasData())
                        {
                            // Create a batch info for error rows on "retry apply errros" or "apply changes":
                            string info = runner?.Connection != null && !string.IsNullOrEmpty(runner?.Connection.Database) ? $"{runner?.Connection.Database}_ERRORS" : "ERRORS";
                            errorsBatchInfo = new BatchInfo(this.Options.BatchDirectory, info: info);

                            var retryErrorsApplyChanges = new MessageApplyChanges(Guid.Empty, sScopeInfoClient.Id, false, sScopeInfoClient.LastServerSyncTimestamp, schema,
                                    this.Options.ConflictResolutionPolicy, false, this.Options.BatchDirectory, retryErrorsBatchInfo, errorsBatchInfo, serverChangesApplied);

                            // Call apply errors on provider
                            context = await this.InternalApplyChangesAsync(cScopeInfo, context, retryErrorsApplyChanges, 
                                runner?.Connection, runner?.Transaction, runner!= null ? runner.CancellationToken : default, runner?.Progress).ConfigureAwait(false);
                        }
                    }

                    //------------------------------------------------------------
                    // STEP 2: Try to apply changes coming from client, if any
                    //------------------------------------------------------------

                    if (clientChanges.ClientBatchInfo != null && clientChanges.ClientBatchInfo.HasData())
                    {
                        // Create a batch info for error rows on "retry apply errros" or "apply changes":
                        string info = runner?.Connection != null && !string.IsNullOrEmpty(runner?.Connection.Database) ? $"{runner?.Connection.Database}_ERRORS" : "ERRORS";
                        errorsBatchInfo = new BatchInfo(this.Options.BatchDirectory, info: info);

                        // Create message containing everything we need to apply on server side
                        var applyChanges = new MessageApplyChanges(Guid.Empty, cScopeInfoClient.Id, false, cScopeInfoClient.LastServerSyncTimestamp, schema,
                            this.Options.ConflictResolutionPolicy, false, this.Options.BatchDirectory, clientChanges.ClientBatchInfo, errorsBatchInfo, serverChangesApplied);

                        // Call provider to apply changes
                        context = await this.InternalApplyChangesAsync(cScopeInfo, context, applyChanges,
                            runner?.Connection, runner?.Transaction, cancellationToken, progress).ConfigureAwait(false);
                    }

                    if (Options.TransactionMode == TransactionMode.AllOrNothing && runner != null)
                        await runner.CommitAsync().ConfigureAwait(false);

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

                try
                {

                    //------------------------------------------------------------
                    // STEP 3: Get Changes from Server
                    //------------------------------------------------------------

                    // Get a no transaction runner for getting changes
                    runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesSelecting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    Debug.WriteLine($"--- Runner Connection {runner.Connection.Database}. {this.Provider.GetProviderTypeName()}");
                    Stopwatch stopw = new Stopwatch();
                    stopw.Start();


                    context.ProgressPercentage = 0.55;

                    //Direction set to Download
                    context.SyncWay = SyncWay.Download;

                    // JUST Before get changes, get the timestamp, to be sure to 
                    // get rows inserted / updated elsewhere since the sync is not over
                    (context, remoteClientTimestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress);

                    // Get if we need to get all rows from the datasource
                    var fromScratch = cScopeInfoClient.IsNewScope || context.SyncType == SyncType.Reinitialize || context.SyncType == SyncType.ReinitializeWithUpload;

                    // When we get the chnages from server, we create the batches if it's requested by the client
                    // the batch decision comes from batchsize from client
                    (context, serverBatchInfo, serverChangesSelected) =
                        await this.InternalGetChangesAsync(cScopeInfo, context, fromScratch, cScopeInfoClient.LastServerSyncTimestamp, remoteClientTimestamp, cScopeInfoClient.Id,
                        this.Provider.SupportsMultipleActiveResultSets,
                        this.Options.BatchDirectory, null, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

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

                    stopw.Stop();
                    Debug.WriteLine($"--- Total duration :{stopw.Elapsed:hh\\.mm\\:ss\\.fff} serverSyncChanges : {serverSyncChanges.ServerChangesSelected.TotalChangesSelected}");

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
