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
            GetChangesAsync(ScopeInfoClient cScopeInfoClient, SyncParameters parameters = default, long? lastServerSyncTimestamp = default)
        {
            if (cScopeInfoClient.Hash != parameters.GetHash())
                throw new Exception("Parameters are not the same from the scopeinfo client instance and the parameters argument");

            var context = new SyncContext(Guid.NewGuid(), cScopeInfoClient.Name)
            {
                Parameters = parameters,
                ClientId = cScopeInfoClient.Id,
            };

            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesSelecting).ConfigureAwait(false);

                // Before getting changes, be sure we have a remote schema available
                ScopeInfo sScopeInfo;
                (context, sScopeInfo) = await this.InternalGetScopeInfoAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

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
                (context, remoteClientTimestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress);


                BatchInfo serverBatchInfo;
                DatabaseChangesSelected serverChangesSelected;
                // When we get the chnages from server, we create the batches if it's requested by the client
                // the batch decision comes from batchsize from client
                (context, serverBatchInfo, serverChangesSelected) =
                    await this.InternalGetChangesAsync(sScopeInfo, context, isNew, lastServerSyncTimestamp, remoteClientTimestamp,
                    cScopeInfoClient.Id, this.Provider.SupportsMultipleActiveResultSets,
                    this.Options.BatchDirectory, null, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

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
            GetEstimatedChangesCountAsync(ScopeInfoClient cScopeInfoClient, SyncParameters parameters = default, long? lastServerSyncTimestamp = default)
        {
            if (cScopeInfoClient.Hash != parameters.GetHash())
                throw new Exception("Parameters are not the same from the scopeinfo client instance and the parameters argument");

            var context = new SyncContext(Guid.NewGuid(), cScopeInfoClient.Name)
            {
                Parameters = parameters,
                ClientId = cScopeInfoClient.Id,
            };


            try
            {

                ScopeInfo sScopeInfo;
                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesSelecting).ConfigureAwait(false);
               
                (context, sScopeInfo) = await this.InternalGetScopeInfoAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
               
                // Should we ?
                if (sScopeInfo.Schema == null)
                    throw new MissingRemoteOrchestratorSchemaException();

                //Direction set to Download
                context.SyncWay = SyncWay.Download;

                // Output
                // JUST Before get changes, get the timestamp, to be sure to 
                // get rows inserted / updated elsewhere since the sync is not over
                long remoteClientTimestamp;
                (context, remoteClientTimestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress);

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
                    remoteClientTimestamp, cScopeInfoClient.Id, this.Provider.SupportsMultipleActiveResultSets, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

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
