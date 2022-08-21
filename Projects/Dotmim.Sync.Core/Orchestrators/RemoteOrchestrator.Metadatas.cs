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
        /// Delete all metadatas from tracking tables, based on min timestamp from history client table
        /// </summary>
        public virtual async Task<DatabaseMetadatasCleaned>
            DeleteMetadatasAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);

            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.MetadataCleaning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                List<ServerHistoryScopeInfo> histories;

                (context, histories) = await this.InternalLoadAllServerHistoriesScopesAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (histories == null || histories.Count == 0)
                    return new DatabaseMetadatasCleaned();

                var minTimestamp = histories.Min(shsi => shsi.LastSyncTimestamp);

                if (minTimestamp == 0)
                    return new DatabaseMetadatasCleaned();

                DatabaseMetadatasCleaned databaseMetadatasCleaned;
                (context, databaseMetadatasCleaned) = await this.InternalDeleteMetadatasAsync(minTimestamp, context, runner.Connection, runner.Transaction,
                    runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return databaseMetadatasCleaned;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }


        /// <summary>
        /// Delete metadatas items from tracking tables
        /// </summary>
        /// <param name="timeStampStart">Timestamp start. Used to limit the delete metadatas rows from now to this timestamp</param>
        public virtual async Task<DatabaseMetadatasCleaned>
            DeleteMetadatasAsync(long? timeStampStart, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);

            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.MetadataCleaning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                DatabaseMetadatasCleaned databaseMetadatasCleaned;
                (context, databaseMetadatasCleaned) = await this.InternalDeleteMetadatasAsync(timeStampStart, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return databaseMetadatasCleaned;

            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }

        /// <summary>
        /// Delete metadatas items from tracking tables
        /// </summary>
        /// <param name="timeStampStart">Timestamp start. Used to limit the delete metadatas rows from now to this timestamp</param>
        public virtual async Task<(SyncContext context, DatabaseMetadatasCleaned databaseMetadatasCleaned)>
            InternalDeleteMetadatasAsync(long? timeStampStart, SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            if (!timeStampStart.HasValue)
                return (context, new DatabaseMetadatasCleaned());

            bool exists;
            (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.Server, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (!exists)
                await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.Server, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            List<ServerScopeInfo> allScopes;
            (context, allScopes) = await this.InternalLoadAllServerScopesInfosAsync(context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (allScopes == null || allScopes.Count == 0)
                return (context, new DatabaseMetadatasCleaned());

            DatabaseMetadatasCleaned databaseMetadatasCleaned;
            (context, databaseMetadatasCleaned) = await this.InternalDeleteMetadatasAsync(allScopes, context, timeStampStart.Value, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            foreach (var serverScopeInfo in allScopes)
            {
                serverScopeInfo.LastCleanupTimestamp = databaseMetadatasCleaned.TimestampLimit;
                var tmpContext = new SyncContext(Guid.NewGuid(), serverScopeInfo.Name);

                await this.InternalSaveServerScopeInfoAsync(serverScopeInfo, tmpContext, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            }
            return (context, databaseMetadatasCleaned);
        }

    }
}
