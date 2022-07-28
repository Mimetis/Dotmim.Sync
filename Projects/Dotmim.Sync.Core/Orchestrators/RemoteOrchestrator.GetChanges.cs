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
            GetChangesAsync(ClientScopeInfo clientScope, SyncParameters parameters = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            var context = new SyncContext(Guid.NewGuid(), clientScope.Name);

            if (parameters != null)
                context.Parameters = parameters;

            try
            {

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Reading, SyncStage.ChangesSelecting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Before getting changes, be sure we have a remote schema available
                ServerScopeInfo serverScopeInfo;
                (context, serverScopeInfo) = await this.InternalGetServerScopeInfoAsync(context, clientScope.Setup, runner.Connection, runner.Transaction, cancellationToken, progress);
                // TODO : if serverScope.Schema is null, should we Provision here ?

                // Should we ?
                if (serverScopeInfo.Schema == null)
                    throw new MissingRemoteOrchestratorSchemaException();

                //Direction set to Download
                context.SyncWay = SyncWay.Download;

                // Output
                // JUST Before get changes, get the timestamp, to be sure to 
                // get rows inserted / updated elsewhere since the sync is not over
                long remoteClientTimestamp;
                (context, remoteClientTimestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress);

                // Get if we need to get all rows from the datasource
                var fromScratch = clientScope.IsNewScope || context.SyncType == SyncType.Reinitialize || context.SyncType == SyncType.ReinitializeWithUpload;

                BatchInfo serverBatchInfo;
                DatabaseChangesSelected serverChangesSelected;
                // When we get the chnages from server, we create the batches if it's requested by the client
                // the batch decision comes from batchsize from client
                (context, serverBatchInfo, serverChangesSelected) =
                    await this.InternalGetChangesAsync(clientScope, context, fromScratch, clientScope.LastServerSyncTimestamp, remoteClientTimestamp,
                    clientScope.Id, this.Provider.SupportsMultipleActiveResultSets,
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
            GetEstimatedChangesCountAsync(ClientScopeInfo clientScope, SyncParameters parameters = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), clientScope.Name);

            if (parameters != null)
                context.Parameters = parameters;

            try
            {

                await using var runner0 = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.ChangesSelecting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                ServerScopeInfo serverScopeInfo;
                (context, serverScopeInfo) = await this.InternalGetServerScopeInfoAsync(context, clientScope.Setup, runner0.Connection, runner0.Transaction, runner0.CancellationToken, runner0.Progress).ConfigureAwait(false);

                await runner0.CommitAsync().ConfigureAwait(false);

                // Should we ?
                if (serverScopeInfo.Schema == null)
                    throw new MissingRemoteOrchestratorSchemaException();

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Reading, SyncStage.ChangesSelecting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                //Direction set to Download
                context.SyncWay = SyncWay.Download;

                // Output
                // JUST Before get changes, get the timestamp, to be sure to 
                // get rows inserted / updated elsewhere since the sync is not over
                long remoteClientTimestamp;
                (context, remoteClientTimestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress);

                // Get if we need to get all rows from the datasource
                var fromScratch = clientScope.IsNewScope || context.SyncType == SyncType.Reinitialize || context.SyncType == SyncType.ReinitializeWithUpload;

                DatabaseChangesSelected serverChangesSelected;
                // When we get the chnages from server, we create the batches if it's requested by the client
                // the batch decision comes from batchsize from client
                (context, serverChangesSelected) =
                    await this.InternalGetEstimatedChangesCountAsync(serverScopeInfo, context, fromScratch, clientScope.LastServerSyncTimestamp, remoteClientTimestamp, clientScope.Id, this.Provider.SupportsMultipleActiveResultSets, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

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
