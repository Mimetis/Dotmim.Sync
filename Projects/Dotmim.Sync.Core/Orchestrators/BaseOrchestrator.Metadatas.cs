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
    public abstract partial class BaseOrchestrator
    {

        /// <summary>
        /// Delete metadatas items from tracking tables
        /// </summary>
        /// <param name="timeStampStart">Timestamp start. Used to limit the delete metadatas rows from now to this timestamp</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <param name="progress">Progress args</param>
        public virtual async Task<DatabaseMetadatasCleaned> DeleteMetadatasAsync(string scopeName, long? timeStampStart, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!timeStampStart.HasValue)
                return null;

            try
            {
                await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Writing, SyncStage.MetadataCleaning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var scopeInfo = await this.InternalGetScopeAsync(scopeName, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                var databaseMetadatasCleaned = await this.InternalDeleteMetadatasAsync(scopeInfo, timeStampStart.Value, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                await runner.CommitAsync().ConfigureAwait(false);
                return databaseMetadatasCleaned;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeName, ex);
            }

        }


        internal virtual async Task<DatabaseMetadatasCleaned> InternalDeleteMetadatasAsync(IScopeInfo scopeInfo, long timestampLimit,
                    DbConnection connection, DbTransaction transaction,
                    CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var context = this.GetContext(scopeInfo.Name);

            await this.InterceptAsync(new MetadataCleaningArgs(context, scopeInfo.Setup, timestampLimit, connection, transaction),progress, cancellationToken).ConfigureAwait(false);

            DatabaseMetadatasCleaned databaseMetadatasCleaned = new DatabaseMetadatasCleaned { TimestampLimit = timestampLimit };

            foreach (var syncTable in scopeInfo.Schema.Tables)
            {
                // Create sync adapter
                var syncAdapter = this.GetSyncAdapter(syncTable, scopeInfo);

                var (command, _) = await syncAdapter.GetCommandAsync(DbCommandType.DeleteMetadata, connection, transaction);

                if (command != null)
                {

                    // Set the special parameters for delete metadata
                    DbSyncAdapter.SetParameterValue(command, "sync_row_timestamp", timestampLimit);

                    await this.InterceptAsync(new DbCommandArgs(context, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                    var rowsCleanedCount = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                    // Check if we have a return value instead
                    var syncRowCountParam = DbSyncAdapter.GetParameter(command, "sync_row_count");

                    if (syncRowCountParam != null)
                        rowsCleanedCount = (int)syncRowCountParam.Value;

                    // Only add a new table metadata stats object, if we have, at least, purged 1 or more rows
                    if (rowsCleanedCount > 0)
                    {
                        var tableMetadatasCleaned = new TableMetadatasCleaned(syncTable.TableName, syncTable.SchemaName)
                        {
                            RowsCleanedCount = rowsCleanedCount,
                            TimestampLimit = timestampLimit
                        };

                        databaseMetadatasCleaned.Tables.Add(tableMetadatasCleaned);
                    }

                }
            }

            await this.InterceptAsync(new MetadataCleanedArgs(context, databaseMetadatasCleaned, connection), progress, cancellationToken).ConfigureAwait(false);
            return databaseMetadatasCleaned;
        }



        /// <summary>
        /// Update a metadata row
        /// </summary>
        internal async Task<bool> InternalUpdateMetadatasAsync(IScopeInfo scopeInfo, DbSyncAdapter syncAdapter, SyncRow row, Guid? senderScopeId, bool forceWrite, DbConnection connection, DbTransaction transaction)
        {
            var (command, _) = await syncAdapter.GetCommandAsync(DbCommandType.UpdateMetadata, connection, transaction);

            if (command == null) return false;

            // Set the parameters value from row
            syncAdapter.SetColumnParametersValues(command, row);

            // Set the special parameters for update
            syncAdapter.AddScopeParametersValues(command, senderScopeId, 0, row.RowState == DataRowState.Deleted, forceWrite);

            var context = this.GetContext(scopeInfo.Name);

            await this.InterceptAsync(new DbCommandArgs(context, command, connection, transaction)).ConfigureAwait(false);

            var metadataUpdatedRowsCount = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

            // Check if we have a return value instead
            var syncRowCountParam = DbSyncAdapter.GetParameter(command, "sync_row_count");

            if (syncRowCountParam != null)
                metadataUpdatedRowsCount = (int)syncRowCountParam.Value;

            return metadataUpdatedRowsCount > 0;
        }


    }
}
