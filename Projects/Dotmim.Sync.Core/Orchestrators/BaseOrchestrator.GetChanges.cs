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
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {
        /// <summary>
        /// Gets a batch of changes to synchronize when given batch size, 
        /// destination knowledge, and change data retriever parameters.
        /// </summary>
        /// <returns>A DbSyncContext object that will be used to retrieve the modified data.</returns>
        internal virtual async Task<(SyncContext, BatchInfo, DatabaseChangesSelected)> InternalGetChangeBatchAsync(
                             SyncContext context, MessageGetChangesBatch message,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            // batch info containing changes
            BatchInfo batchInfo;

            this.logger.LogDebug(SyncEventsId.GetChanges, message);

            // Statistics about changes that are selected
            DatabaseChangesSelected changesSelected;

            if (context.SyncWay == SyncWay.Upload && context.SyncType == SyncType.Reinitialize)
            {
                (batchInfo, changesSelected) = await this.InternalGetEmptyChangesAsync(message).ConfigureAwait(false);
                return (context, batchInfo, changesSelected);
            }

            // create local directory
            if (message.BatchSize > 0 && !string.IsNullOrEmpty(message.BatchDirectory) && !Directory.Exists(message.BatchDirectory))
            {
                this.logger.LogDebug(SyncEventsId.CreateDirectory, new { DirectoryPath = message.BatchDirectory });
                Directory.CreateDirectory(message.BatchDirectory);
            }

            // numbers of batch files generated
            var batchIndex = 0;

            // Check if we are in batch mode
            var isBatch = message.BatchSize > 0;

            // Create stats object to store changes count
            var changes = new DatabaseChangesSelected();

            // Create a batch info in memory (if !isBatch) or serialized on disk (if isBatch)
            // batchinfo generate a schema clone with scope columns if needed
            batchInfo = new BatchInfo(!isBatch, message.Schema, message.BatchDirectory);

            // Clean SyncSet, we will add only tables we need in the batch info
            var changesSet = new SyncSet();

            foreach (var syncTable in message.Schema.Tables)
            {
                this.logger.LogDebug(SyncEventsId.GetChanges, syncTable);

                // if we are in upload stage, so check if table is not download only
                if (context.SyncWay == SyncWay.Upload && syncTable.SyncDirection == SyncDirection.DownloadOnly)
                    continue;

                // if we are in download stage, so check if table is not download only
                if (context.SyncWay == SyncWay.Download && syncTable.SyncDirection == SyncDirection.UploadOnly)
                    continue;

                var syncAdapter = this.Provider.GetSyncAdapter(syncTable, message.Setup);

                // launch interceptor if any
                await this.InterceptAsync(new TableChangesSelectingArgs(context, syncTable, connection, transaction), cancellationToken).ConfigureAwait(false);

                // Get Command
                var selectIncrementalChangesCommand = await this.GetSelectChangesCommandAsync(context, syncAdapter, syncTable, message.IsNew, connection, transaction);

                // Set parameters
                this.SetSelectChangesCommonParameters(context, syncTable, message.ExcludingScopeId, message.IsNew, message.LastTimestamp, selectIncrementalChangesCommand);

                // log
                this.logger.LogDebug(SyncEventsId.GetChanges, selectIncrementalChangesCommand);

                // Statistics
                var tableChangesSelected = new TableChangesSelected(syncTable.TableName, syncTable.SchemaName);

                // Create a chnages table with scope columns
                var changesSetTable = SyncAdapter.CreateChangesTable(message.Schema.Tables[syncTable.TableName, syncTable.SchemaName], changesSet);

                // Get the reader
                using (var dataReader = await selectIncrementalChangesCommand.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    // memory size total
                    double rowsMemorySize = 0L;

                    while (dataReader.Read())
                    {
                        // Create a row from dataReader
                        var row = CreateSyncRowFromReader(dataReader, changesSetTable);

                        // Add the row to the changes set
                        changesSetTable.Rows.Add(row);

                        // Log trace row
                        this.logger.LogTrace(SyncEventsId.GetChanges, row);

                        // Set the correct state to be applied
                        if (row.RowState == DataRowState.Deleted)
                            tableChangesSelected.Deletes++;
                        else if (row.RowState == DataRowState.Modified)
                            tableChangesSelected.Upserts++;

                        // calculate row size if in batch mode
                        if (isBatch)
                        {
                            var fieldsSize = ContainerTable.GetRowSizeFromDataRow(row.ToArray());
                            var finalFieldSize = fieldsSize / 1024d;

                            if (finalFieldSize > message.BatchSize)
                                throw new RowOverSizedException(finalFieldSize.ToString());

                            // Calculate the new memory size
                            rowsMemorySize += finalFieldSize;

                            // Next line if we don't reach the batch size yet.
                            if (rowsMemorySize <= message.BatchSize)
                                continue;

                            // Check interceptor
                            var batchTableChangesSelectedArgs = new TableChangesSelectedArgs(context, changesSetTable, tableChangesSelected, connection, transaction);
                            await this.InterceptAsync(batchTableChangesSelectedArgs, cancellationToken).ConfigureAwait(false);

                            // add changes to batchinfo
                            await batchInfo.AddChangesAsync(changesSet, batchIndex, false, this).ConfigureAwait(false);

                            this.logger.LogDebug(SyncEventsId.GetChanges, changesSet);

                            // increment batch index
                            batchIndex++;

                            // we know the datas are serialized here, so we can flush  the set
                            changesSet.Clear();

                            // Recreate an empty ContainerSet and a ContainerTable
                            changesSet = new SyncSet();

                            changesSetTable = SyncAdapter.CreateChangesTable(message.Schema.Tables[syncTable.TableName, syncTable.SchemaName], changesSet);

                            // Init the row memory size
                            rowsMemorySize = 0L;
                        }
                    }
                }

                // be sure it's disposed
                // selectIncrementalChangesCommand.Dispose();

                // We don't report progress if no table changes is empty, to limit verbosity
                if (tableChangesSelected.Deletes > 0 || tableChangesSelected.Upserts > 0)
                {
                    changes.TableChangesSelected.Add(tableChangesSelected);
                    var tableChangesSelectedArgs = new TableChangesSelectedArgs(context, changesSetTable, tableChangesSelected, connection, transaction);

                    await this.InterceptAsync(tableChangesSelectedArgs, cancellationToken).ConfigureAwait(false);

                    if (tableChangesSelectedArgs.TableChangesSelected.TotalChanges > 0)
                        this.ReportProgress(context, progress, tableChangesSelectedArgs);
                }

            }

            // We are in batch mode, and we are at the last batchpart info
            // Even if we don't have rows inside, we return the changesSet, since it contains at least schema
            if (changesSet != null && changesSet.HasTables && changesSet.HasRows)
            {
                await batchInfo.AddChangesAsync(changesSet, batchIndex, true, this).ConfigureAwait(false);
                this.logger.LogDebug(SyncEventsId.GetChanges, changesSet);
            }

            // Check the last index as the last batch
            batchInfo.EnsureLastBatch();

            return (context, batchInfo, changes);

        }


        /// <summary>
        /// Gets changes rows count estimation, 
        /// </summary>
        internal virtual async Task<(SyncContext, DatabaseChangesSelected)> InternalGetEstimatedChangesCountAsync(
                             SyncContext context, MessageGetChangesBatch message,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            this.logger.LogDebug(SyncEventsId.GetChanges, message);

            // Create stats object to store changes count
            var changes = new DatabaseChangesSelected();

            if (context.SyncWay == SyncWay.Upload && context.SyncType == SyncType.Reinitialize)
                return (context, changes);

            foreach (var syncTable in message.Schema.Tables)
            {
                this.logger.LogDebug(SyncEventsId.GetChanges, syncTable);

                // if we are in upload stage, so check if table is not download only
                if (context.SyncWay == SyncWay.Upload && syncTable.SyncDirection == SyncDirection.DownloadOnly)
                    continue;

                // if we are in download stage, so check if table is not download only
                if (context.SyncWay == SyncWay.Download && syncTable.SyncDirection == SyncDirection.UploadOnly)
                    continue;

                var syncAdapter = this.Provider.GetSyncAdapter(syncTable, message.Setup);

                // launch interceptor if any
                await this.InterceptAsync(new TableChangesSelectingArgs(context, syncTable, connection, transaction), cancellationToken).ConfigureAwait(false);

                // Get Command
                var selectIncrementalChangesCommand = await this.GetSelectChangesCommandAsync(context, syncAdapter, syncTable, message.IsNew, connection, transaction);

                // Set parameters
                this.SetSelectChangesCommonParameters(context, syncTable, message.ExcludingScopeId, message.IsNew, message.LastTimestamp, selectIncrementalChangesCommand);

                // log
                this.logger.LogDebug(SyncEventsId.GetChanges, selectIncrementalChangesCommand);

                // Statistics
                var tableChangesSelected = new TableChangesSelected(syncTable.TableName, syncTable.SchemaName);

                // Get the reader
                using (var dataReader = await selectIncrementalChangesCommand.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (dataReader.Read())
                    {
                        bool isTombstone = false;
                        for (var i = 0; i < dataReader.FieldCount; i++)
                        {
                            if (dataReader.GetName(i) == "sync_row_is_tombstone")
                            {
                                isTombstone = Convert.ToInt64(dataReader.GetValue(i)) > 0;
                                break;
                            }
                        }

                        // Set the correct state to be applied
                        if (isTombstone)
                            tableChangesSelected.Deletes++;
                        else
                            tableChangesSelected.Upserts++;
                    }
                }

                if (tableChangesSelected.Deletes > 0 || tableChangesSelected.Upserts > 0)
                    changes.TableChangesSelected.Add(tableChangesSelected);
            }

            return (context, changes);
        }

        /// <summary>
        /// Generate an empty BatchInfo
        /// </summary>
        internal Task<(BatchInfo, DatabaseChangesSelected)> InternalGetEmptyChangesAsync(MessageGetChangesBatch message)
        {
            // Get config
            var isBatched = message.BatchSize > 0;

            // Create the batch info, in memory
            var batchInfo = new BatchInfo(!isBatched, message.Schema, message.BatchDirectory); ;

            this.logger.LogDebug(SyncEventsId.GetChanges, new DatabaseChangesSelected());

            // Create a new empty in-memory batch info
            return Task.FromResult((batchInfo, new DatabaseChangesSelected()));

        }


        /// <summary>
        /// Get the correct Select changes command 
        /// Can be either
        /// - SelectInitializedChanges              : All changes for first sync
        /// - SelectChanges                         : All changes filtered by timestamp
        /// - SelectInitializedChangesWithFilters   : All changes for first sync with filters
        /// - SelectChangesWithFilters              : All changes filtered by timestamp with filters
        /// </summary>
        internal async Task<DbCommand> GetSelectChangesCommandAsync(SyncContext context, SyncAdapter syncAdapter, SyncTable syncTable, bool isNew, DbConnection connection, DbTransaction transaction)
        {
            DbCommand command;
            DbCommandType dbCommandType;

            SyncFilter tableFilter = null;

            // Check if we have parameters specified

            // Sqlite does not have any filter, since he can't be server side
            if (this.Provider.CanBeServerProvider)
                tableFilter = syncTable.GetFilter();

            var hasFilters = tableFilter != null;

            // Determing the correct DbCommandType
            if (isNew && hasFilters)
                dbCommandType = DbCommandType.SelectInitializedChangesWithFilters;
            else if (isNew && !hasFilters)
                dbCommandType = DbCommandType.SelectInitializedChanges;
            else if (!isNew && hasFilters)
                dbCommandType = DbCommandType.SelectChangesWithFilters;
            else
                dbCommandType = DbCommandType.SelectChanges;

            // Get correct Select incremental changes command 
            command = await syncAdapter.PrepareCommandAsync(dbCommandType, connection, transaction, tableFilter);

            return command;
        }

        /// <summary>
        /// Set common parameters to SelectChanges Sql command
        /// </summary>
        internal void SetSelectChangesCommonParameters(SyncContext context, SyncTable syncTable, Guid? excludingScopeId, bool isNew, long lastTimestamp, DbCommand selectIncrementalChangesCommand)
        {
            // Generate the isNewScope Flag.
            var isNewScope = isNew ? 1 : 0;
            var isReinit = context.SyncType == SyncType.Reinitialize ? 1 : 0;

            switch (context.SyncWay)
            {
                case SyncWay.Upload:
                    // Overwrite if we are in Reinitialize mode (not RenitializeWithUpload)
                    isNewScope = context.SyncType == SyncType.Reinitialize ? 1 : isNewScope;
                    lastTimestamp = context.SyncType == SyncType.Reinitialize ? 0 : lastTimestamp;
                    isReinit = context.SyncType == SyncType.Reinitialize ? 1 : 0;
                    break;
                case SyncWay.Download:
                    // Ovewrite on bot Reinitialize and ReinitializeWithUpload
                    isNewScope = context.SyncType != SyncType.Normal ? 1 : isNewScope;
                    lastTimestamp = context.SyncType != SyncType.Normal ? 0 : lastTimestamp;
                    isReinit = context.SyncType != SyncType.Normal ? 1 : 0;
                    break;
                default:
                    break;
            }

            // Set the parameters
            SyncAdapter.SetParameterValue(selectIncrementalChangesCommand, "sync_min_timestamp", lastTimestamp);
            SyncAdapter.SetParameterValue(selectIncrementalChangesCommand, "sync_scope_id", excludingScopeId.HasValue ? (object)excludingScopeId.Value : DBNull.Value);

            // Check filters
            SyncFilter tableFilter = null;

            // Sqlite does not have any filter, since he can't be server side
            if (this.Provider.CanBeServerProvider)
                tableFilter = syncTable.GetFilter();

            var hasFilters = tableFilter != null;

            if (!hasFilters)
                return;

            // context parameters can be null at some point.
            var contexParameters = context.Parameters ?? new SyncParameters();

            foreach (var filterParam in tableFilter.Parameters)
            {
                var parameter = contexParameters.FirstOrDefault(p =>
                    p.Name.Equals(filterParam.Name, SyncGlobalization.DataSourceStringComparison));

                object val = parameter?.Value;

                SyncAdapter.SetParameterValue(selectIncrementalChangesCommand, filterParam.Name, val);
            }

        }

        /// <summary>
        /// Create a new SyncRow from a dataReader.
        /// </summary>
        internal SyncRow CreateSyncRowFromReader(IDataReader dataReader, SyncTable table)
        {
            // Create a new row, based on table structure
            var row = table.NewRow();

            bool isTombstone = false;

            for (var i = 0; i < dataReader.FieldCount; i++)
            {
                var columnName = dataReader.GetName(i);

                // if we have the tombstone value, do not add it to the table
                if (columnName == "sync_row_is_tombstone")
                {
                    isTombstone = Convert.ToInt64(dataReader.GetValue(i)) > 0;
                    continue;
                }
                if (columnName == "update_scope_id")
                {
                    //var readerScopeId = dataReader.GetValue(i);

                    //// if update_scope_id is null, so the row owner is the local database
                    //// if update_scope_id is not null, the row owner is someone else
                    //if (readerScopeId == DBNull.Value || readerScopeId == null)
                    //    row.UpdateScopeId = localScopeId;
                    //else if (SyncTypeConverter.TryConvertTo<Guid>(readerScopeId, out var updateScopeIdObject))
                    //    row.UpdateScopeId = (Guid)updateScopeIdObject;
                    //else
                    //    throw new Exception("Impossible to parse row['update_scope_id']");

                    continue;
                }

                var columnValueObject = dataReader.GetValue(i);
                var columnValue = columnValueObject == DBNull.Value ? null : columnValueObject;

                row[columnName] = columnValue;

            }

            //// during initialization, row["update_scope_id"] is not part of the data reader
            //// so we affect the local scope id owner manually
            //if (!row.UpdateScopeId.HasValue)
            //    row.UpdateScopeId = localScopeId;

            row.RowState = isTombstone ? DataRowState.Deleted : DataRowState.Modified;

            return row;
        }




    }
}
