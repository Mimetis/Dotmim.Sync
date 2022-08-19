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
        /// Get changes from local database from a specific scope name
        /// </summary>
        public async Task<ClientSyncChanges>
            GetChangesAsync(string scopeName = SyncOptions.DefaultScopeName, SyncParameters parameters = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);

            if (parameters != null)
                context.Parameters = parameters;

            try
            {

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Reading, SyncStage.ChangesSelecting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.Client, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    return default;

                ClientScopeInfo localScopeInfo;
                (context, localScopeInfo) = await this.InternalLoadClientScopeInfoAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (localScopeInfo == null)
                    return default;

                ClientSyncChanges clientChanges = null;
                (context, clientChanges) = await this.InternalGetChangesAsync(localScopeInfo, context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return clientChanges;

            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }

        /// <summary>
        /// Get estimated changes from local database to be sent to the server
        /// </summary>
        public async Task<ClientSyncChanges>
            GetEstimatedChangesCountAsync(string scopeName = SyncOptions.DefaultScopeName, SyncParameters parameters = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            
            if (parameters != null)
                context.Parameters = parameters;

            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.Reading, SyncStage.ChangesSelecting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.Client, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    return default;

                ClientScopeInfo localScopeInfo;
                (context, localScopeInfo) = await this.InternalLoadClientScopeInfoAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (localScopeInfo == null)
                    return default;

                // If no schema in the client scope. Maybe the client scope table does not exists, or we never get the schema from server
                if (localScopeInfo.Schema == null)
                    throw new MissingLocalOrchestratorSchemaException();

                // On local, we don't want to chase rows from "others" 
                // We just want our local rows, so we dont exclude any remote scope id, by setting scope id to NULL
                Guid? remoteScopeId = null;
                var lastTimestamp = localScopeInfo.LastSyncTimestamp;
                var isNew = localScopeInfo.IsNewScope;

                //Direction set to Upload
                context.SyncWay = SyncWay.Upload;

                // Output
                // JUST before the whole process, get the timestamp, to be sure to 
                // get rows inserted / updated elsewhere since the sync is not over
                long clientTimestamp;
                (context, clientTimestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                DatabaseChangesSelected clientChangesSelected;

                // Locally, if we are new, no need to get changes
                if (isNew)
                    clientChangesSelected = new DatabaseChangesSelected();
                else
                    (context, clientChangesSelected) = await this.InternalGetEstimatedChangesCountAsync(
                        localScopeInfo, context,
                        isNew, lastTimestamp, clientTimestamp, remoteScopeId, this.Provider.SupportsMultipleActiveResultSets,
                        runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                var changes = new ClientSyncChanges(clientTimestamp, null, clientChangesSelected);

                return changes;

            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Get changes from local database from a specific scope you already fetched from local database
        /// </summary>
        internal virtual async Task<(SyncContext context, ClientSyncChanges syncChanges)>
            InternalGetChangesAsync(ClientScopeInfo clientScopeInfo, SyncContext context, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.Reading, SyncStage.ChangesSelecting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                // Output
                long clientTimestamp = 0L;
                BatchInfo clientBatchInfo = null;
                DatabaseChangesSelected clientChangesSelected = null;

                // If no schema in the client scope. Maybe the client scope table does not exists, or we never get the schema from server
                if (clientScopeInfo.Schema == null)
                    throw new MissingLocalOrchestratorSchemaException();

                // On local, we don't want to chase rows from "others" 
                // We just want our local rows, so we dont exclude any remote scope id, by setting scope id to NULL
                Guid? remoteScopeId = null;
                // lastSyncTS : get lines inserted / updated / deteleted after the last sync commited
                var lastTimestamp = clientScopeInfo.LastSyncTimestamp;
                // isNew : If isNew, lasttimestamp is not correct, so grab all
                var isNew = clientScopeInfo.IsNewScope;

                //Direction set to Upload
                context.SyncWay = SyncWay.Upload;

                // JUST before the whole process, get the timestamp, to be sure to 
                // get rows inserted / updated elsewhere since the sync is not over
                (context, clientTimestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // Locally, if we are new, no need to get changes
                if (isNew)
                    (clientBatchInfo, clientChangesSelected) = await this.InternalGetEmptyChangesAsync(clientScopeInfo, this.Options.BatchDirectory).ConfigureAwait(false);
                else
                    (context, clientBatchInfo, clientChangesSelected) = await this.InternalGetChangesAsync(clientScopeInfo, 
                        context, isNew, lastTimestamp, clientTimestamp, remoteScopeId, this.Provider.SupportsMultipleActiveResultSets,
                        this.Options.BatchDirectory, null, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                var changes = new ClientSyncChanges(clientTimestamp, clientBatchInfo, clientChangesSelected);

                await runner.CommitAsync().ConfigureAwait(false);

                return (context, changes);

            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }
    }
}
