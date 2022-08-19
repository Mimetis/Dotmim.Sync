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

        internal virtual async Task<(SyncContext context, DatabaseMetadatasCleaned databaseMetadatasCleaned)>
            InternalDeleteMetadatasAsync(
                    IEnumerable<IScopeInfo> scopeInfos, SyncContext context, long timestampLimit,
                    DbConnection connection, DbTransaction transaction,
                    CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            context.SyncStage = SyncStage.ChangesApplying;

            var databaseMetadatasCleaned = new DatabaseMetadatasCleaned { TimestampLimit = timestampLimit };

            await this.InterceptAsync(new MetadataCleaningArgs(context, scopeInfos, timestampLimit,
                                        connection, transaction), progress, cancellationToken).ConfigureAwait(false);

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

                    // create a fake syncTable
                    // Don't need anything else than table name to make a delete metadata clean up
                    var syncTable = new SyncTable(setupTable.TableName, setupTable.SchemaName);

                    // Create sync adapter
                    var syncAdapter = this.GetSyncAdapter(syncTable, scopeInfo);

                    var (command, _) = await syncAdapter.GetCommandAsync(DbCommandType.DeleteMetadata, connection, transaction);

                    if (command != null)
                    {
                        // Set the special parameters for delete metadata
                        DbSyncAdapter.SetParameterValue(command, "sync_row_timestamp", timestampLimit);

                        await this.InterceptAsync(new DbCommandArgs(context, command, DbCommandType.DeleteMetadata, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

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

                        command.Dispose();
                    }

                    doneList.Add(setupTable);
                }
            }

            await this.InterceptAsync(new MetadataCleanedArgs(context, databaseMetadatasCleaned, connection), progress, cancellationToken).ConfigureAwait(false);
            return (context, databaseMetadatasCleaned);
        }



        /// <summary>
        /// Update a metadata row
        /// </summary>
        internal async Task<(SyncContext context, bool metadataUpdated, Exception exception)> InternalUpdateMetadatasAsync(IScopeInfo scopeInfo, SyncContext context, DbSyncAdapter syncAdapter, SyncRow row, Guid? senderScopeId, bool forceWrite, DbConnection connection, DbTransaction transaction)
        {
            context.SyncStage = SyncStage.ChangesApplying;

            var (command, _) = await syncAdapter.GetCommandAsync(DbCommandType.UpdateMetadata, connection, transaction);

            if (command == null) return (context, false, null);

            // Set the parameters value from row
            syncAdapter.SetColumnParametersValues(command, row);

            // Set the special parameters for update
            syncAdapter.AddScopeParametersValues(command, senderScopeId, 0, row.RowState == DataRowState.Deleted, forceWrite);

            await this.InterceptAsync(new DbCommandArgs(context, command, DbCommandType.UpdateMetadata, connection, transaction)).ConfigureAwait(false);

            Exception exception = null;
            int metadataUpdatedRowsCount = 0;
            try
            {
                metadataUpdatedRowsCount = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                // Check if we have a return value instead
                var syncRowCountParam = DbSyncAdapter.GetParameter(command, "sync_row_count");

                if (syncRowCountParam != null)
                    metadataUpdatedRowsCount = (int)syncRowCountParam.Value;

                command.Dispose();
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            return (context, metadataUpdatedRowsCount > 0, exception);
        }


    }
}
