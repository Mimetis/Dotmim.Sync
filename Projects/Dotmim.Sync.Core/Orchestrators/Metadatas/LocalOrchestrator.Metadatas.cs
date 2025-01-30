﻿using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains methods to clear metadatas on the local provider.
    /// </summary>
    public partial class LocalOrchestrator : BaseOrchestrator
    {
        /// <summary>
        /// Delete all metadatas from tracking tables, based on min timestamp from scope info client table.
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
            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.MetadataCleaning, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {

                    bool exists;
                    (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (!exists)
                    {
                        (context, _) = await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                    }

                    List<ScopeInfo> clientScopeInfos;
                    (context, clientScopeInfos) = await this.InternalLoadAllScopeInfosAsync(
                        context,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (clientScopeInfos == null || clientScopeInfos.Count == 0)
                        return new DatabaseMetadatasCleaned();

                    bool existsCScopeInfoClient;
                    (context, existsCScopeInfoClient) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (!existsCScopeInfoClient)
                    {
                        (context, _) = await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                    }

                    var clientHistoriesScopeInfos = await this.InternalLoadAllScopeInfoClientsAsync(
                        context,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (clientHistoriesScopeInfos == null || clientHistoriesScopeInfos.Count == 0)
                        return new DatabaseMetadatasCleaned();

                    var minTimestamp = clientHistoriesScopeInfos.Min(scope => scope.LastSyncTimestamp);

                    if (!minTimestamp.HasValue || minTimestamp.Value == 0)
                        return new DatabaseMetadatasCleaned();

                    DatabaseMetadatasCleaned databaseMetadatasCleaned;
                    (context, databaseMetadatasCleaned) = await this.InternalDeleteMetadatasAsync(context, minTimestamp,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    await runner.CommitAsync().ConfigureAwait(false);

                    return databaseMetadatasCleaned;
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Delete all metadatas from tracking tables, based on min timestamp.
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
            try
            {
                DatabaseMetadatasCleaned databaseMetadatasCleaned;
                (_, databaseMetadatasCleaned) = await this.InternalDeleteMetadatasAsync(context, timeStampStart, connection, transaction).ConfigureAwait(false);
                return databaseMetadatasCleaned;
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Delete metadatas items from tracking tables.
        /// </summary>
        internal virtual async Task<(SyncContext Context, DatabaseMetadatasCleaned IsDatabaseMetadatasCleaned)>
            InternalDeleteMetadatasAsync(SyncContext context, long? timeStampStart = default, DbConnection connection = default, DbTransaction transaction = default, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            if (!timeStampStart.HasValue)
                return (context, null);
            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.MetadataCleaning, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    bool exists;
                    (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo, runner.Connection, runner.Transaction, progress, cancellationToken).ConfigureAwait(false);

                    if (!exists)
                        await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfo, runner.Connection, runner.Transaction, progress, cancellationToken).ConfigureAwait(false);

                    List<ScopeInfo> clientScopeInfos;
                    (context, clientScopeInfos) = await this.InternalLoadAllScopeInfosAsync(context, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (clientScopeInfos == null || clientScopeInfos.Count == 0)
                        return (context, new DatabaseMetadatasCleaned());

                    DatabaseMetadatasCleaned databaseMetadatasCleaned;
                    (context, databaseMetadatasCleaned) = await this.InternalDeleteMetadatasAsync(clientScopeInfos, context, timeStampStart.Value, runner.Connection, runner.Transaction, progress, cancellationToken).ConfigureAwait(false);

                    // save last cleanup timestamp
                    foreach (var clientScopeInfo in clientScopeInfos)
                    {
                        clientScopeInfo.LastCleanupTimestamp = databaseMetadatasCleaned.TimestampLimit;

                        await this.InternalSaveScopeInfoAsync(clientScopeInfo, context,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                    }

                    await runner.CommitAsync().ConfigureAwait(false);

                    return (context, databaseMetadatasCleaned);
                }
            }
            catch (Exception ex)
            {
                string message = null;

                message += $"TimestampStart:{timeStampStart}.";

                throw this.GetSyncError(context, ex, message);
            }
        }
    }
}