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
        /// Delete all metadatas from tracking tables, based on min timestamp from scope info table
        /// </summary>
        public async Task<DatabaseMetadatasCleaned> DeleteMetadatasAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);

            await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            bool exists;
            (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
             
            if (!exists)
                (context, _) = await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

            List<ClientScopeInfo> clientScopeInfos;
            (context, clientScopeInfos) = await this.InternalLoadAllClientScopesInfoAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

            if (clientScopeInfos == null || clientScopeInfos.Count == 0)
                return new DatabaseMetadatasCleaned();

            var minTimestamp = clientScopeInfos.Min(scope => scope.LastSyncTimestamp);

            if (!minTimestamp.HasValue || minTimestamp.Value == 0)
                return new DatabaseMetadatasCleaned();

            DatabaseMetadatasCleaned databaseMetadatasCleaned;
            (context, databaseMetadatasCleaned) = await this.InternalDeleteMetadatasAsync(context, minTimestamp, runner.Connection, runner.Transaction,
                runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            await runner.CommitAsync().ConfigureAwait(false);

            return databaseMetadatasCleaned;

        }


        public virtual async Task<DatabaseMetadatasCleaned>
            DeleteMetadatasAsync(long? timeStampStart = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);
            DatabaseMetadatasCleaned databaseMetadatasCleaned;
            (_, databaseMetadatasCleaned) = await InternalDeleteMetadatasAsync(context, timeStampStart, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            return databaseMetadatasCleaned;
        }


        /// <summary>
        /// Delete metadatas items from tracking tables
        /// </summary>
        /// <param name="timeStampStart">Timestamp start. Used to limit the delete metadatas rows from now to this timestamp</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <param name="progress">Progress args</param>
        internal async Task<(SyncContext context, DatabaseMetadatasCleaned databaseMetadatasCleaned)>
            InternalDeleteMetadatasAsync(SyncContext context, long? timeStampStart = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!timeStampStart.HasValue)
                return (context, null);

            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.MetadataCleaning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.Client, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.Client, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                List<ClientScopeInfo> clientScopeInfos;
                (context, clientScopeInfos) = await this.InternalLoadAllClientScopesInfoAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (clientScopeInfos == null || clientScopeInfos.Count == 0)
                    return (context, new DatabaseMetadatasCleaned());

                DatabaseMetadatasCleaned databaseMetadatasCleaned;
                (context, databaseMetadatasCleaned) = await this.InternalDeleteMetadatasAsync(clientScopeInfos, context, timeStampStart.Value, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return (context, databaseMetadatasCleaned);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }



    }
}
