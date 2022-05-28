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
        internal async Task<(SyncContext context, DatabaseChangesApplied ChangesApplied, ClientScopeInfo ClientScopeInfo)>
            InternalApplyChangesAsync(ClientScopeInfo clientScopeInfo, SyncContext context, BatchInfo serverBatchInfo,
                              long clientTimestamp, long remoteClientTimestamp, ConflictResolutionPolicy policy, bool snapshotApplied, DatabaseChangesSelected allChangesSelected,
                              DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            try
            {
                // lastSyncTS : apply lines only if they are not modified since last client sync
                var lastTimestamp = clientScopeInfo.LastSyncTimestamp;
                // isNew : if IsNew, don't apply deleted rows from server
                var isNew = clientScopeInfo.IsNewScope;
                // We are in downloading mode

                // Create the message containing everything needed to apply changes
                var applyChanges = new MessageApplyChanges(clientScopeInfo.Id, Guid.Empty, isNew, lastTimestamp, clientScopeInfo.Schema, policy,
                                this.Options.DisableConstraintsOnApplyChanges, this.Options.CleanMetadatas, this.Options.CleanFolder, snapshotApplied,
                                serverBatchInfo);

                DatabaseChangesApplied clientChangesApplied;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                context.SyncWay = SyncWay.Download;

                // Call apply changes on provider
                (context, clientChangesApplied) = await this.InternalApplyChangesAsync(clientScopeInfo, context, applyChanges, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // check if we need to delete metadatas
                if (this.Options.CleanMetadatas && clientChangesApplied.TotalAppliedChanges > 0 && lastTimestamp.HasValue)
                {
                    List<ClientScopeInfo> allScopes;
                    (context, allScopes) = await this.InternalLoadAllClientScopesInfoAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    if (allScopes.Count > 0)
                    {
                        // Get the min value from LastSyncTimestamp from all scopes
                        var minLastTimeStamp = allScopes.Min(scope => scope.LastSyncTimestamp.HasValue ? scope.LastSyncTimestamp.Value : Int64.MaxValue);
                        minLastTimeStamp = minLastTimeStamp > lastTimestamp.Value ? lastTimestamp.Value : minLastTimeStamp;

                        (context, _) = await this.InternalDeleteMetadatasAsync(allScopes, context, minLastTimeStamp, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                    }
                }

                // now the sync is complete, remember the time
                this.CompleteTime = DateTime.UtcNow;

                // generate the new scope item
                clientScopeInfo.IsNewScope = false;
                clientScopeInfo.LastSync = this.CompleteTime;
                clientScopeInfo.LastSyncTimestamp = clientTimestamp;
                clientScopeInfo.LastServerSyncTimestamp = remoteClientTimestamp;
                clientScopeInfo.LastSyncDuration = this.CompleteTime.Value.Subtract(context.StartTime).Ticks;

                // Write scopes locally
                (context, clientScopeInfo) = await this.InternalSaveClientScopeInfoAsync(clientScopeInfo, context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return (context, clientChangesApplied, clientScopeInfo);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }


        /// <summary>
        /// Apply a snapshot locally
        /// </summary>
        internal async Task<(SyncContext context, DatabaseChangesApplied snapshotChangesApplied, ClientScopeInfo clientScopeInfo)>
            InternalApplySnapshotAsync(ClientScopeInfo clientScopeInfo, SyncContext context, BatchInfo serverBatchInfo, long clientTimestamp, long remoteClientTimestamp, DatabaseChangesSelected databaseChangesSelected,
            DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                if (serverBatchInfo == null)
                    return (context, new DatabaseChangesApplied(), clientScopeInfo);

                // Get context or create a new one
                context.SyncStage = SyncStage.SnapshotApplying;
                await this.InterceptAsync(new SnapshotApplyingArgs(context, this.Provider.CreateConnection()), progress, cancellationToken).ConfigureAwait(false);

                if (clientScopeInfo.Schema == null)
                    throw new ArgumentNullException(nameof(clientScopeInfo.Schema));

                // Applying changes and getting the new client scope info
                var (syncContext, changesApplied, newClientScopeInfo) = await this.InternalApplyChangesAsync(clientScopeInfo, context, serverBatchInfo,
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
