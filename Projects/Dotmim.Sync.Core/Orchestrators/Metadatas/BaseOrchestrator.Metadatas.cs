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
    /// Contains internals methods to clear metadatas.
    /// </summary>
    public abstract partial class BaseOrchestrator
    {

        /// <summary>
        /// Delete metadatas rows from the tracking table.
        /// </summary>
        internal virtual async Task<(SyncContext Context, DatabaseMetadatasCleaned DatabaseMetadatasCleaned)>
            InternalDeleteMetadatasAsync(
                    IEnumerable<ScopeInfo> scopeInfos, SyncContext context, long timestampLimit,
                    DbConnection connection, DbTransaction transaction,
                    IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            context.SyncStage = SyncStage.MetadataCleaning;

            var databaseMetadatasCleaned = new DatabaseMetadatasCleaned { TimestampLimit = timestampLimit };

            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.MetadataCleaning, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    await this.InterceptAsync(
                        new MetadataCleaningArgs(context, scopeInfos, timestampLimit,
                                            runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    // contains all tables already processed
                    var doneList = new List<SetupTable>();

                    foreach (var scopeInfo in scopeInfos)
                    {
                        if (scopeInfo.Setup?.Tables == null || scopeInfo.Setup.Tables.Count <= 0)
                            continue;

                        foreach (var setupTable in scopeInfo.Setup.Tables)
                        {
                            var isDone = doneList.Any(t => t.EqualsByName(setupTable));

                            if (isDone)
                                continue;

                            var syncTable = scopeInfo.Schema.Tables[setupTable.TableName, setupTable.SchemaName];
                            var syncAdapter = this.GetSyncAdapter(syncTable, scopeInfo);

                            var (command, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, DbCommandType.DeleteMetadata,
                                runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                            if (command != null)
                            {
                                // Set the special parameters for delete metadata
                                command = this.InternalSetCommandParametersValues(context, command, DbCommandType.DeleteMetadata, syncAdapter,
                                    runner.Connection, runner.Transaction,
                                    sync_min_timestamp: timestampLimit, progress: runner.Progress, cancellationToken: runner.CancellationToken);

                                await this.InterceptAsync(new ExecuteCommandArgs(context, command, DbCommandType.DeleteMetadata, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                                var rowsCleanedCount = 0;
                                rowsCleanedCount = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

                                // Check if we have a return value instead
                                var syncRowCountParam = syncAdapter.GetParameter(context, command, "sync_row_count");

                                if (syncRowCountParam != null && syncRowCountParam.Value != null && syncRowCountParam.Value != DBNull.Value)
                                    rowsCleanedCount = (int)syncRowCountParam.Value;

                                // Only add a new table metadata stats object, if we have, at least, purged 1 or more rows
                                if (rowsCleanedCount > 0)
                                {
                                    var tableMetadatasCleaned = new TableMetadatasCleaned(syncTable.TableName, syncTable.SchemaName)
                                    {
                                        RowsCleanedCount = rowsCleanedCount,
                                        TimestampLimit = timestampLimit,
                                    };

                                    databaseMetadatasCleaned.Tables.Add(tableMetadatasCleaned);
                                }

                                command.Dispose();
                            }

                            doneList.Add(setupTable);
                        }
                    }

                    await this.InterceptAsync(new MetadataCleanedArgs(context, databaseMetadatasCleaned, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    await runner.CommitAsync().ConfigureAwait(false);

                    return (context, databaseMetadatasCleaned);
                }
            }
            catch (Exception ex)
            {
                string message = null;

                message += $"TimestampLimit:{timestampLimit}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Update a metadata row.
        /// </summary>
        internal async Task<(SyncContext Context, bool IsMetadataUpdated, Exception Exception)> InternalUpdateMetadatasAsync(ScopeInfo scopeInfo, SyncContext context,
                                SyncRow row, SyncTable schemaTable, Guid? senderScopeId, bool forceWrite,
                                DbConnection connection, DbTransaction transaction,
                                IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            context.SyncStage = SyncStage.ChangesApplying;

            var syncAdapter = this.GetSyncAdapter(schemaTable, scopeInfo);

            using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.MetadataCleaning, connection, transaction, cancellationToken: cancellationToken).ConfigureAwait(false);
            await using (runner.ConfigureAwait(false))
            {
                var (command, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, DbCommandType.UpdateMetadata,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                if (command == null)
                    return (context, false, null);

                // Set the parameters value from row
                this.InternalSetCommandParametersValues(context, command, DbCommandType.UpdateMetadata, syncAdapter, connection, transaction,
                    row, senderScopeId, 0, row.RowState == SyncRowState.Deleted, forceWrite, progress, cancellationToken);

                await this.InterceptAsync(new ExecuteCommandArgs(context, command, DbCommandType.UpdateMetadata, runner.Connection, runner.Transaction), cancellationToken: cancellationToken).ConfigureAwait(false);

                Exception exception = null;
                var metadataUpdatedRowsCount = 0;

                try
                {
                    metadataUpdatedRowsCount = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

                    // Check if we have a return value instead
                    var syncRowCountParam = syncAdapter.GetParameter(context, command, "sync_row_count");

                    if (syncRowCountParam != null && syncRowCountParam.Value != null && syncRowCountParam.Value != DBNull.Value)
                        metadataUpdatedRowsCount = (int)syncRowCountParam.Value;
                }
                catch (Exception ex)
                {
                    exception = ex;
                }
                finally
                {
                    command.Dispose();
                }

                return (context, metadataUpdatedRowsCount > 0, exception);
            }
        }
    }
}