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
            InternalApplyThenGetChangesAsync(ClientScopeInfo clientScope, SyncContext context, BatchInfo clientBatchInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                if (Provider == null)
                    throw new MissingProviderException(nameof(InternalApplyThenGetChangesAsync));

                long remoteClientTimestamp = 0L;
                DatabaseChangesSelected serverChangesSelected = null;
                DatabaseChangesApplied clientChangesApplied = null;
                BatchInfo serverBatchInfo = null;
                IScopeInfo serverClientScopeInfo = null;

                //Direction set to Upload
                context.SyncWay = SyncWay.Upload;


                // Connection & Transaction runner
                DbConnectionRunner runner = null;

                // Getting server scope assumes we have already created the schema on server
                // Scope name is the scope name coming from client
                // Since server can have multiples scopes
                await using (runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false))
                    (context, serverClientScopeInfo) = await this.InternalLoadServerScopeInfoAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // Should we ?
                if (serverClientScopeInfo == null || serverClientScopeInfo.Schema == null)
                    throw new MissingRemoteOrchestratorSchemaException();

                // Deserialiaze schema
                var schema = serverClientScopeInfo.Schema;

                if (clientBatchInfo.HasData())
                {
                    // Create message containing everything we need to apply on server side
                    var applyChanges = new MessageApplyChanges(Guid.Empty, clientScope.Id, false, clientScope.LastServerSyncTimestamp, schema, this.Options.ConflictResolutionPolicy,
                                    this.Options.DisableConstraintsOnApplyChanges, this.Options.CleanMetadatas, this.Options.CleanFolder, false, clientBatchInfo);

                    // Transaction mode
                    if (Options.TransactionMode == TransactionMode.AllOrNothing)
                        runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    // Call provider to apply changes
                    (context, clientChangesApplied) = await this.InternalApplyChangesAsync(serverClientScopeInfo, context, applyChanges,
                        runner?.Connection, runner?.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (Options.TransactionMode == TransactionMode.AllOrNothing && runner != null)
                        await runner.CommitAsync().ConfigureAwait(false);
                }

                // Get a no transaction runner for getting changes
                runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesSelecting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                context.ProgressPercentage = 0.55;

                //Direction set to Download
                context.SyncWay = SyncWay.Download;

                // JUST Before get changes, get the timestamp, to be sure to 
                // get rows inserted / updated elsewhere since the sync is not over
                (context, remoteClientTimestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress);

                // Get if we need to get all rows from the datasource
                var fromScratch = clientScope.IsNewScope || context.SyncType == SyncType.Reinitialize || context.SyncType == SyncType.ReinitializeWithUpload;

                // When we get the chnages from server, we create the batches if it's requested by the client
                // the batch decision comes from batchsize from client
                (context, serverBatchInfo, serverChangesSelected) =
                    await this.InternalGetChangesAsync(serverClientScopeInfo, context, fromScratch, clientScope.LastServerSyncTimestamp, remoteClientTimestamp, clientScope.Id,
                    this.Provider.SupportsMultipleActiveResultSets,
                    this.Options.BatchDirectory, null, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (runner.CancellationToken.IsCancellationRequested)
                    runner.CancellationToken.ThrowIfCancellationRequested();

                // generate the new scope item
                this.CompleteTime = DateTime.UtcNow;

                var scopeHistory = new ServerHistoryScopeInfo
                {
                    Id = clientScope.Id,
                    Name = clientScope.Name,
                    LastSyncTimestamp = remoteClientTimestamp,
                    LastSync = this.CompleteTime,
                    LastSyncDuration = this.CompleteTime.Value.Subtract(context.StartTime).Ticks,
                };

                // Write scopes locally
                await this.InternalSaveServerHistoryScopeAsync(scopeHistory, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);
                var serverSyncChanges = new ServerSyncChanges(remoteClientTimestamp, serverBatchInfo, serverChangesSelected);

                return (context, serverSyncChanges, clientChangesApplied, this.Options.ConflictResolutionPolicy);

            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }
    }
}
