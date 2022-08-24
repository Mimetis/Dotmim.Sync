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
    public partial class RemoteOrchestrator : BaseOrchestrator
    {

        /// <summary>
        /// Get changes from remote database
        /// </summary>
        public virtual async Task<ServerSyncChanges>
            GetChangesAsync(ScopeInfo scopeInfo, Guid clientId, SyncParameters parameters = default, long? lastServerSyncTimestamp = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name)
            {
                Parameters = parameters,
                ClientId = clientId,
            };

            try
            {

                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesSelecting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Before getting changes, be sure we have a remote schema available
                ScopeInfo sScopeInfo;
                (context, sScopeInfo) = await this.InternalEnsureScopeInfoAsync(context, scopeInfo.Setup, false, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // Should we ?
                if (sScopeInfo.Schema == null)
                    throw new MissingRemoteOrchestratorSchemaException();

                ScopeInfoClient sScopeInfoClient;
                (context, sScopeInfoClient) = await this.InternalLoadScopeInfoClientAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                bool isNew = sScopeInfoClient == null || sScopeInfoClient.LastSyncTimestamp <= 0;
                
                if (!lastServerSyncTimestamp.HasValue && sScopeInfoClient != null && sScopeInfoClient.LastSyncTimestamp > 0)
                    lastServerSyncTimestamp = sScopeInfoClient.LastSyncTimestamp;

                //Direction set to Download
                context.SyncWay = SyncWay.Download;

                // Output
                // JUST Before get changes, get the timestamp, to be sure to 
                // get rows inserted / updated elsewhere since the sync is not over
                long remoteClientTimestamp;
                (context, remoteClientTimestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress);


                BatchInfo serverBatchInfo;
                DatabaseChangesSelected serverChangesSelected;
                // When we get the chnages from server, we create the batches if it's requested by the client
                // the batch decision comes from batchsize from client
                (context, serverBatchInfo, serverChangesSelected) =
                    await this.InternalGetChangesAsync(scopeInfo, context, isNew, lastServerSyncTimestamp, remoteClientTimestamp,
                    clientId, this.Provider.SupportsMultipleActiveResultSets,
                    this.Options.BatchDirectory, null, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return new ServerSyncChanges(remoteClientTimestamp, serverBatchInfo, serverChangesSelected);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }

        /// <summary>
        /// Get estimated changes from remote database to be applied on client
        /// </summary>
        public virtual async Task<ServerSyncChanges>
            GetEstimatedChangesCountAsync(string scopeName, Guid clientId, SyncParameters parameters = default, long? lastServerSyncTimestamp = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName)
            {
                Parameters = parameters,
                ClientId = clientId,
            };

            if (parameters != null)
                context.Parameters = parameters;

            try
            {

                await using var runner0 = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.ChangesSelecting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                ScopeInfo sScopeInfo;
                (context, sScopeInfo) = await this.InternalLoadScopeInfoAsync(context, runner0.Connection, runner0.Transaction, runner0.CancellationToken, runner0.Progress).ConfigureAwait(false);

                await runner0.CommitAsync().ConfigureAwait(false);

                // Should we ?
                if (sScopeInfo.Schema == null)
                    throw new MissingRemoteOrchestratorSchemaException();

                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesSelecting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                //Direction set to Download
                context.SyncWay = SyncWay.Download;

                // Output
                // JUST Before get changes, get the timestamp, to be sure to 
                // get rows inserted / updated elsewhere since the sync is not over
                long remoteClientTimestamp;
                (context, remoteClientTimestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress);

                ScopeInfoClient sScopeInfoClient;
                (context, sScopeInfoClient) = await this.InternalLoadScopeInfoClientAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                bool isNew = sScopeInfoClient == null || sScopeInfoClient.LastSyncTimestamp <= 0;

                if (!lastServerSyncTimestamp.HasValue && sScopeInfoClient != null && sScopeInfoClient.LastSyncTimestamp > 0)
                    lastServerSyncTimestamp = sScopeInfoClient.LastSyncTimestamp;

                DatabaseChangesSelected serverChangesSelected;
                // When we get the chnages from server, we create the batches if it's requested by the client
                // the batch decision comes from batchsize from client
                (context, serverChangesSelected) =
                    await this.InternalGetEstimatedChangesCountAsync(sScopeInfo, context, isNew, lastServerSyncTimestamp, 
                    remoteClientTimestamp, clientId, this.Provider.SupportsMultipleActiveResultSets, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                var serverSyncChanges = new ServerSyncChanges(remoteClientTimestamp, null, serverChangesSelected);

                return serverSyncChanges;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }
    }
}
