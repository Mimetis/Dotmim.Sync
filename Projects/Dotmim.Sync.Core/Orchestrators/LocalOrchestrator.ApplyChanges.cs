using Dotmim.Sync.Args;
using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
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
        internal async Task<(SyncContext context, DatabaseChangesApplied ChangesApplied, ScopeInfoClient CScopeInfoClient)>
            InternalApplyChangesAsync(ScopeInfo cScopeInfo, ScopeInfoClient cScopeInfoClient, SyncContext context, BatchInfo serverBatchInfo,
                              long clientTimestamp, long remoteClientTimestamp, ConflictResolutionPolicy policy, bool snapshotApplied, DatabaseChangesSelected allChangesSelected,
                              DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            // Connection & Transaction runner
            DbConnectionRunner runner = null;

            try
            {
                // lastSyncTS : apply lines only if they are not modified since last client sync
                var lastTimestamp = cScopeInfoClient.LastSyncTimestamp;
                // isNew : if IsNew, don't apply deleted rows from server
                var isNew = cScopeInfoClient.IsNewScope;
                // We are in downloading mode

                // Create the message containing everything needed to apply changes
                var applyChanges = new MessageApplyChanges(cScopeInfoClient.Id, Guid.Empty, isNew, lastTimestamp, cScopeInfo.Schema, policy,
                                this.Options.DisableConstraintsOnApplyChanges, this.Options.CleanMetadatas, this.Options.CleanFolder, snapshotApplied,
                                serverBatchInfo);

                DatabaseChangesApplied clientChangesApplied;


                context.SyncWay = SyncWay.Download;


                // Transaction mode
                if (Options.TransactionMode == TransactionMode.AllOrNothing)
                {
                    runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    // affect connection and transaction to reaffect later on save scope
                    connection = runner.Connection;
                    transaction = runner.Transaction;
                }

                // Call apply changes on provider
                (context, clientChangesApplied) = await this.InternalApplyChangesAsync(cScopeInfo, context, applyChanges, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // check if we need to delete metadatas
                if (this.Options.CleanMetadatas && clientChangesApplied.TotalAppliedChanges > 0 && lastTimestamp.HasValue)
                {
                    using (var runnerMetadata = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.MetadataCleaning, connection, transaction, cancellationToken, progress).ConfigureAwait(false))
                    {
                        List<ScopeInfoClient> allScopeHistories;
                        (context, allScopeHistories) = await this.InternalLoadAllScopeInfoClientsAsync(context, runnerMetadata.Connection, runnerMetadata.Transaction, runnerMetadata.CancellationToken, runnerMetadata.Progress).ConfigureAwait(false);

                        List<ScopeInfo> allClientScopes;
                        (context, allClientScopes) = await this.InternalLoadAllScopeInfosAsync(context, runnerMetadata.Connection, runnerMetadata.Transaction, runnerMetadata.CancellationToken, runnerMetadata.Progress).ConfigureAwait(false);

                        if (allScopeHistories.Count > 0 && allClientScopes.Count > 0)
                        {
                            // Get the min value from LastSyncTimestamp from all scopes
                            var minLastTimeStamp = allScopeHistories.Min(scope => scope.LastSyncTimestamp.HasValue ? scope.LastSyncTimestamp.Value : Int64.MaxValue);
                            minLastTimeStamp = minLastTimeStamp > lastTimestamp.Value ? lastTimestamp.Value : minLastTimeStamp;

                            (context, _) = await this.InternalDeleteMetadatasAsync(allClientScopes, context, minLastTimeStamp, runnerMetadata.Connection, runnerMetadata.Transaction, runnerMetadata.CancellationToken, runnerMetadata.Progress).ConfigureAwait(false);
                        }
                    };
                }

                // now the sync is complete, remember the time
                this.CompleteTime = DateTime.UtcNow;

                // generate the new scope item
                cScopeInfoClient.IsNewScope = false;
                cScopeInfoClient.LastSync = this.CompleteTime;
                cScopeInfoClient.LastSyncTimestamp = clientTimestamp;
                cScopeInfoClient.LastServerSyncTimestamp = remoteClientTimestamp;
                cScopeInfoClient.LastSyncDuration = this.CompleteTime.Value.Subtract(context.StartTime).Ticks;

                // Write scopes locally

                using (var runnerScopeInfo = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.MetadataCleaning, connection, transaction, cancellationToken, progress).ConfigureAwait(false))
                {
                    (context, cScopeInfoClient) = await this.InternalSaveScopeInfoClientAsync(cScopeInfoClient, context,
                        runnerScopeInfo.Connection, runnerScopeInfo.Transaction, runnerScopeInfo.CancellationToken, runnerScopeInfo.Progress).ConfigureAwait(false);
                };

                if (Options.TransactionMode == TransactionMode.AllOrNothing && runner != null)
                    await runner.CommitAsync().ConfigureAwait(false);


                return (context, clientChangesApplied, cScopeInfoClient);
            }
            catch (Exception ex)
            {
                if (runner != null)
                    await runner.RollbackAsync().ConfigureAwait(false);

                throw GetSyncError(context, ex);
            }
            finally
            {
                if (runner != null)
                    await runner.DisposeAsync().ConfigureAwait(false);
            }

        }


        /// <summary>
        /// Apply a snapshot locally
        /// </summary>
        internal async Task<(SyncContext context, DatabaseChangesApplied snapshotChangesApplied, ScopeInfoClient cScopeInfoClient)>
            InternalApplySnapshotAsync(ScopeInfo clientScopeInfo,
            ScopeInfoClient cScopeInfoClient,
            SyncContext context, BatchInfo serverBatchInfo, long clientTimestamp, long remoteClientTimestamp, DatabaseChangesSelected databaseChangesSelected,
            DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                if (serverBatchInfo == null)
                    return (context, new DatabaseChangesApplied(), cScopeInfoClient);

                // Get context or create a new one
                context.SyncStage = SyncStage.SnapshotApplying;
                await this.InterceptAsync(new SnapshotApplyingArgs(context, this.Provider.CreateConnection()), progress, cancellationToken).ConfigureAwait(false);

                if (clientScopeInfo.Schema == null)
                    throw new ArgumentNullException(nameof(clientScopeInfo.Schema));

                // Applying changes and getting the new client scope info
                var (syncContext, changesApplied, newClientScopeInfo) = await this.InternalApplyChangesAsync(clientScopeInfo, cScopeInfoClient, context, serverBatchInfo,
                        clientTimestamp, remoteClientTimestamp, ConflictResolutionPolicy.ServerWins, false, databaseChangesSelected, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                await this.InterceptAsync(new SnapshotAppliedArgs(context, changesApplied), progress, cancellationToken).ConfigureAwait(false);

                // re-apply scope is new flag
                // to be sure we are calling the Initialize method, even for the delta
                // in that particular case, we want the delta rows coming from the current scope
                newClientScopeInfo.IsNewScope = true;

                return (context, changesApplied, newClientScopeInfo);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }


    }
}
