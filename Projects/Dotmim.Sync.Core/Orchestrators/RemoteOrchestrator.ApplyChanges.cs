using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public partial class RemoteOrchestrator : BaseOrchestrator
    {

        /// <summary>
        /// Apply changes on remote provider
        /// </summary>
        internal virtual async Task<(SyncContext context, ServerSyncChanges serverSyncChanges, DatabaseChangesApplied serverChangesApplied, ConflictResolutionPolicy serverResolutionPolicy)>
            InternalApplyThenGetChangesAsync(ScopeInfoClient cScopeInfoClient, ScopeInfo cScopeInfo, SyncContext context, BatchInfo clientBatchInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                if (Provider == null)
                    throw new MissingProviderException(nameof(InternalApplyThenGetChangesAsync));

                long remoteClientTimestamp = 0L;
                DatabaseChangesSelected serverChangesSelected = null;
                DatabaseChangesApplied clientChangesApplied = null;
                BatchInfo serverBatchInfo = null;

                //Direction set to Upload
                context.SyncWay = SyncWay.Upload;

                // Connection & Transaction runner
                DbConnectionRunner runner = null;

                try
                {
                    // Should we ?
                    if (cScopeInfo == null || cScopeInfo.Schema == null)
                        throw new MissingRemoteOrchestratorSchemaException();

                    // Deserialiaze schema
                    var schema = cScopeInfo.Schema;

                    if (clientBatchInfo.HasData())
                    {
                        // Create message containing everything we need to apply on server side
                        var applyChanges = new MessageApplyChanges(Guid.Empty, cScopeInfoClient.Id, false, cScopeInfoClient.LastServerSyncTimestamp, schema, this.Options.ConflictResolutionPolicy,
                                        this.Options.DisableConstraintsOnApplyChanges, this.Options.CleanMetadatas, this.Options.CleanFolder, false, clientBatchInfo);

                        // Transaction mode
                        if (Options.TransactionMode == TransactionMode.AllOrNothing)
                            runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Call provider to apply changes
                        (context, clientChangesApplied) = await this.InternalApplyChangesAsync(cScopeInfo, context, applyChanges,
                            runner?.Connection, runner?.Transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (Options.TransactionMode == TransactionMode.AllOrNothing && runner != null)
                            await runner.CommitAsync().ConfigureAwait(false);
                    }

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

                    cScopeInfoClient.LastSyncTimestamp = remoteClientTimestamp;
                    cScopeInfoClient.LastSync = this.CompleteTime;
                    cScopeInfoClient.LastSyncDuration = this.CompleteTime.Value.Subtract(context.StartTime).Ticks;

                    // Save scope info client coming from client
                    // to scope info client table on server
                    await this.InternalSaveScopeInfoClientAsync(cScopeInfoClient, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    var serverSyncChanges = new ServerSyncChanges(remoteClientTimestamp, serverBatchInfo, serverChangesSelected);
                    return (context, serverSyncChanges, clientChangesApplied, this.Options.ConflictResolutionPolicy);
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
