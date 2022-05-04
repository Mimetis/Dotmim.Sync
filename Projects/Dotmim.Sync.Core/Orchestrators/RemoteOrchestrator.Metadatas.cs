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
        public virtual async Task<DatabaseMetadatasCleaned> DeleteMetadatasAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            await using var runner = await this.GetConnectionAsync(SyncOptions.DefaultScopeName, SyncMode.Writing, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            var exists = await this.InternalExistsScopeInfoTableAsync(SyncOptions.DefaultScopeName, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

            if (!exists)
                await this.InternalCreateScopeInfoTableAsync(SyncOptions.DefaultScopeName, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

            var histories = await this.InternalLoadAllServerHistoriesScopesAsync(SyncOptions.DefaultScopeName, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

            if (histories == null || histories.Count == 0)
                return new DatabaseMetadatasCleaned();

            var minTimestamp = histories.Min(shsi => shsi.LastSyncTimestamp);

            if (minTimestamp == 0)
                return new DatabaseMetadatasCleaned();

            var result = await this.DeleteMetadatasAsync(minTimestamp, runner.Connection, runner.Transaction, 
                runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            await runner.CommitAsync().ConfigureAwait(false);

            return result;

        }

        /// <summary>
        /// Delete metadatas items from tracking tables
        /// </summary>
        /// <param name="timeStampStart">Timestamp start. Used to limit the delete metadatas rows from now to this timestamp</param>
        public virtual async Task<DatabaseMetadatasCleaned> DeleteMetadatasAsync(long? timeStampStart, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                if (!timeStampStart.HasValue)
                    return new DatabaseMetadatasCleaned();

                await using var runner = await this.GetConnectionAsync(SyncOptions.DefaultScopeName, SyncMode.Writing, SyncStage.MetadataCleaning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var exists = await this.InternalExistsScopeInfoTableAsync(SyncOptions.DefaultScopeName, DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(SyncOptions.DefaultScopeName, DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);


                var allScopes = await this.InternalLoadAllServerScopesInfosAsync(SyncOptions.DefaultScopeName, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (allScopes == null || allScopes.Count == 0)
                    return new DatabaseMetadatasCleaned();

                var databaseMetadatasCleaned = await this.InternalDeleteMetadatasAsync(allScopes, timeStampStart.Value, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                foreach(var serverScopeInfo in allScopes)
                {
                    serverScopeInfo.LastCleanupTimestamp = databaseMetadatasCleaned.TimestampLimit;
                    await this.InternalSaveServerScopeInfoAsync(serverScopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                await runner.CommitAsync().ConfigureAwait(false);

                return databaseMetadatasCleaned;

            }
            catch (Exception ex)
            {
                throw GetSyncError(SyncOptions.DefaultScopeName, ex);
            }
        }

    }
}
