using Dotmim.Sync.Builders;
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
    /// Contains methods to clear metadatas on the remote provider.
    /// </summary>
    public partial class RemoteOrchestrator : BaseOrchestrator
    {

        /// <summary>
        /// Delete all metadatas from tracking tables, based on min timestamp from scope info client table.
        /// </summary>
        public virtual async Task<DatabaseMetadatasCleaned> DeleteMetadatasAsync(DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);

            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.MetadataCleaning, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    bool exists;
                    (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (!exists)
                    {
                        await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                    }

                    var sScopeInfoClients = await this.InternalLoadAllScopeInfoClientsAsync(
                        context,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (sScopeInfoClients == null || sScopeInfoClients.Count == 0)
                        return new DatabaseMetadatasCleaned();

                    var minTimestamp = sScopeInfoClients.Min(shsi => shsi.LastSyncTimestamp);

                    if (minTimestamp == 0)
                        return new DatabaseMetadatasCleaned();

                    DatabaseMetadatasCleaned databaseMetadatasCleaned;
                    (context, databaseMetadatasCleaned) = await this.InternalDeleteMetadatasAsync(minTimestamp, context,
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
        /// Delete metadatas items from tracking tables.
        /// </summary>
        /// <param name="timeStampStart">Timestamp start. Used to limit the delete metadatas rows from now to this timestamp.</param>
        /// <param name="connection">Optional Connection.</param>
        /// <param name="transaction">Optional Transaction.</param>
        public virtual async Task<DatabaseMetadatasCleaned> DeleteMetadatasAsync(long timeStampStart, DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);

            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.MetadataCleaning, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    DatabaseMetadatasCleaned databaseMetadatasCleaned;
                    (context, databaseMetadatasCleaned) = await this.InternalDeleteMetadatasAsync(timeStampStart, context,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    await runner.CommitAsync().ConfigureAwait(false);

                    return databaseMetadatasCleaned;
                }
            }
            catch (Exception ex)
            {
                string message = null;

                message += $"TimestampStart:{timeStampStart}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Delete metadatas items from tracking tables.
        /// </summary>
        internal virtual async Task<(SyncContext Context, DatabaseMetadatasCleaned DatabaseMetadatasCleaned)>
            InternalDeleteMetadatasAsync(long? timeStampStart, SyncContext context, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {

            if (!timeStampStart.HasValue)
                return (context, new DatabaseMetadatasCleaned());
            try
            {

                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.MetadataCleaning, connection, transaction, cancellationToken: cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {

                    bool exists;
                    (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                    if (!exists)
                        await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfo, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                    List<ScopeInfo> sScopeInfos;
                    (context, sScopeInfos) = await this.InternalLoadAllScopeInfosAsync(context, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                    if (sScopeInfos == null || sScopeInfos.Count == 0)
                        return (context, new DatabaseMetadatasCleaned());

                    DatabaseMetadatasCleaned databaseMetadatasCleaned;
                    (context, databaseMetadatasCleaned) = await this.InternalDeleteMetadatasAsync(sScopeInfos, context, timeStampStart.Value, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                    foreach (var serverScopeInfo in sScopeInfos)
                    {
                        serverScopeInfo.LastCleanupTimestamp = databaseMetadatasCleaned.TimestampLimit;
                        var tmpContext = new SyncContext(Guid.NewGuid(), serverScopeInfo.Name);

                        await this.InternalSaveScopeInfoAsync(serverScopeInfo, tmpContext, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
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