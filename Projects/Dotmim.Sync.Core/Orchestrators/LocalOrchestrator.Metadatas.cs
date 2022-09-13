
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
        /// Delete all metadatas from tracking tables, based on min timestamp from scope info client table
        /// <example>
        /// <code>
        /// var localOrchestrator = new LocalOrchestrator(clientProvider);
        /// await localOrchestrator.DeleteMetadatasAsync();
        /// </code>
        /// </example>
        /// </summary>
        public async Task<DatabaseMetadatasCleaned> DeleteMetadatasAsync(DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);

            await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.MetadataCleaning, connection, transaction).ConfigureAwait(false);

            bool exists;
            (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo, 
                runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
             
            if (!exists)
                (context, _) = await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfo, 
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            List<ScopeInfo> clientScopeInfos;
            (context, clientScopeInfos) = await this.InternalLoadAllScopeInfosAsync(context, 
                runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            if (clientScopeInfos == null || clientScopeInfos.Count == 0)
                return new DatabaseMetadatasCleaned();

            bool existsCScopeInfoClient;
            (context, existsCScopeInfoClient) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient, 
                runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            if (!existsCScopeInfoClient)
                (context, _) = await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient, 
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            List<ScopeInfoClient> clientHistoriesScopeInfos;
            (context, clientHistoriesScopeInfos) = await this.InternalLoadAllScopeInfoClientsAsync(context, 
                runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            if (clientHistoriesScopeInfos == null || clientHistoriesScopeInfos.Count == 0)
                return new DatabaseMetadatasCleaned();

            var minTimestamp = clientHistoriesScopeInfos.Min(scope => scope.LastSyncTimestamp);

            if (!minTimestamp.HasValue || minTimestamp.Value == 0)
                return new DatabaseMetadatasCleaned();

            DatabaseMetadatasCleaned databaseMetadatasCleaned;
            (context, databaseMetadatasCleaned) = await this.InternalDeleteMetadatasAsync(context, minTimestamp, 
                runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            await runner.CommitAsync().ConfigureAwait(false);

            return databaseMetadatasCleaned;

        }


        /// <summary>
        /// Delete all metadatas from tracking tables, based on min timestamp
        /// <example>
        /// <code>
        /// var localOrchestrator = new LocalOrchestrator(clientProvider);
        /// await localOrchestrator.DeleteMetadatasAsync();
        /// </code>
        /// </example>
        /// </summary>
        public async Task<DatabaseMetadatasCleaned> DeleteMetadatasAsync(long timeStampStart, DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);
            DatabaseMetadatasCleaned databaseMetadatasCleaned;
            (_, databaseMetadatasCleaned) = await InternalDeleteMetadatasAsync(context, timeStampStart, connection, transaction).ConfigureAwait(false);
            return databaseMetadatasCleaned;
        }


        /// <summary>
        /// Delete metadatas items from tracking tables
        /// </summary>
        internal virtual async Task<(SyncContext context, DatabaseMetadatasCleaned databaseMetadatasCleaned)>
            InternalDeleteMetadatasAsync(SyncContext context, long? timeStampStart = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!timeStampStart.HasValue)
                return (context, null);

            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.MetadataCleaning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                List<ScopeInfo> clientScopeInfos;
                (context, clientScopeInfos) = await this.InternalLoadAllScopeInfosAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

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
