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
        internal virtual async Task<(SyncContext, BatchInfo, DatabaseChangesSelected)> InternalGetChangesAsync(
                             SyncContext context, MessageGetChangesBatch message,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            // batch info containing changes
            BatchInfo batchInfo;


            // Statistics about changes that are selected
            DatabaseChangesSelected changesSelected;

            if (context.SyncWay == SyncWay.Upload && context.SyncType == SyncType.Reinitialize)
            {
                (batchInfo, changesSelected) = await this.InternalGetEmptyChangesAsync(message).ConfigureAwait(false);
                return (context, batchInfo, changesSelected);
            }

            // Call interceptor
            await this.InterceptAsync(new DatabaseChangesSelectingArgs(context, message, connection, transaction), cancellationToken).ConfigureAwait(false);

            // create local directory
            if (message.BatchSize > 0 && !string.IsNullOrEmpty(message.BatchDirectory) && !Directory.Exists(message.BatchDirectory))
                Directory.CreateDirectory(message.BatchDirectory);

            changesSelected = new DatabaseChangesSelected();

            // numbers of batch files generated
            var batchIndex = 0;

            // Check if we are in batch mode
            var isBatch = message.BatchSize > 0;

            // Create a batch info in memory (if !isBatch) or serialized on disk (if isBatch)
            // batchinfo generate a schema clone with scope columns if needed
            batchInfo = new BatchInfo(!isBatch, message.Schema, message.BatchDirectory);

            // Clean SyncSet, we will add only tables we need in the batch info
            var changesSet = new SyncSet();

            var cptSyncTable = 0;
            var currentProgress = context.ProgressPercentage;
            foreach (var syncTable in message.Schema.Tables)
            {
                // tmp count of table for report progress pct
                cptSyncTable++;

                // Only table schema is replicated, no datas are applied
                if (syncTable.SyncDirection == SyncDirection.None)
                    continue;

                // if we are in upload stage, so check if table is not download only
                if (context.SyncWay == SyncWay.Upload && syncTable.SyncDirection == SyncDirection.DownloadOnly)
                    continue;

                // if we are in download stage, so check if table is not download only
                if (context.SyncWay == SyncWay.Download && syncTable.SyncDirection == SyncDirection.UploadOnly)
                    continue;

                // Get Command
                var selectIncrementalChangesCommand = await this.GetSelectChangesCommandAsync(context, syncTable, message.Setup, message.IsNew, connection, transaction);

                // Set parameters
                this.SetSelectChangesCommonParameters(context, syncTable, message.ExcludingScopeId, message.IsNew, message.LastTimestamp, selectIncrementalChangesCommand);

                // launch interceptor if any
                var args = new TableChangesSelectingArgs(context, syncTable, selectIncrementalChangesCommand, connection, transaction);
                await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);

                if (!args.Cancel && args.Command != null)
                {
                    // Statistics
                    var tableChangesSelected = new TableChangesSelected(syncTable.TableName, syncTable.SchemaName);

                    // Create a chnages table with scope columns
                    var changesSetTable = DbSyncAdapter.CreateChangesTable(message.Schema.Tables[syncTable.TableName, syncTable.SchemaName], changesSet);

                    // Get the reader
                    using var dataReader = await args.Command.ExecuteReaderAsync().ConfigureAwait(false);

                    // memory size total
                    double rowsMemorySize = 0L;

                    while (dataReader.Read())
                    {
                        // Create a row from dataReader
                        var row = CreateSyncRowFromReader(dataReader, changesSetTable);

                        // Add the row to the changes set
                        changesSetTable.Rows.Add(row);

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
                            await batchInfo.AddChangesAsync(changesSet, batchIndex, false, message.SerializerFactory, this).ConfigureAwait(false);

                            // increment batch index
                            batchIndex++;

                            // we know the datas are serialized here, so we can flush  the set
                            changesSet.Clear();

                            // Recreate an empty ContainerSet and a ContainerTable
                            changesSet = new SyncSet();

                            changesSetTable = DbSyncAdapter.CreateChangesTable(message.Schema.Tables[syncTable.TableName, syncTable.SchemaName], changesSet);

                            // Init the row memory size
                            rowsMemorySize = 0L;
                        }
                    }

                    dataReader.Close();

                    // We don't report progress if no table changes is empty, to limit verbosity
                    if (tableChangesSelected.Deletes > 0 || tableChangesSelected.Upserts > 0)
                        changesSelected.TableChangesSelected.Add(tableChangesSelected);

                    // even if no rows raise the interceptor
                    var tableChangesSelectedArgs = new TableChangesSelectedArgs(context, changesSetTable, tableChangesSelected, connection, transaction);
                    await this.InterceptAsync(tableChangesSelectedArgs, cancellationToken).ConfigureAwait(false);

                    context.ProgressPercentage = currentProgress + (cptSyncTable * 0.2d / message.Schema.Tables.Count);

                    // only raise report progress if we have something
                    if (tableChangesSelectedArgs.TableChangesSelected.TotalChanges > 0)
                        this.ReportProgress(context, progress, tableChangesSelectedArgs);

                }
            }

            // We are in batch mode, and we are at the last batchpart info
            // Even if we don't have rows inside, we return the changesSet, since it contains at least schema
            if (changesSet != null && changesSet.HasTables && changesSet.HasRows)
            {
                await batchInfo.AddChangesAsync(changesSet, batchIndex, true, message.SerializerFactory, this).ConfigureAwait(false);
            }

            //Set the total rows count contained in the batch info
            batchInfo.RowsCount = changesSelected.TotalChangesSelected;

            // Check the last index as the last batch
            batchInfo.EnsureLastBatch();

            // Raise database changes selected
            if (changesSelected.TotalChangesSelected > 0 || changesSelected.TotalChangesSelectedDeletes > 0 || changesSelected.TotalChangesSelectedUpdates > 0)
            {
                var databaseChangesSelectedArgs = new DatabaseChangesSelectedArgs(context, message.LastTimestamp, batchInfo, changesSelected, connection);
                this.ReportProgress(context, progress, databaseChangesSelectedArgs);
                await this.InterceptAsync(databaseChangesSelectedArgs, cancellationToken).ConfigureAwait(false);
            }

            return (context, batchInfo, changesSelected);

        }


        /// <summary>
        /// Gets changes rows count estimation, 
        /// </summary>
        internal virtual async Task<(SyncContext, DatabaseChangesSelected)> InternalGetEstimatedChangesCountAsync(
                             SyncContext context, MessageGetChangesBatch message,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            // Call interceptor
            await this.InterceptAsync(new DatabaseChangesSelectingArgs(context, message, connection, transaction), cancellationToken).ConfigureAwait(false);

            // Create stats object to store changes count
            var changes = new DatabaseChangesSelected();

            if (context.SyncWay == SyncWay.Upload && context.SyncType == SyncType.Reinitialize)
                return (context, changes);

            foreach (var syncTable in message.Schema.Tables)
            {
                // Only table schema is replicated, no datas are applied
                if (syncTable.SyncDirection == SyncDirection.None)
                    continue;

                // if we are in upload stage, so check if table is not download only
                if (context.SyncWay == SyncWay.Upload && syncTable.SyncDirection == SyncDirection.DownloadOnly)
                    continue;

                // if we are in download stage, so check if table is not download only
                if (context.SyncWay == SyncWay.Download && syncTable.SyncDirection == SyncDirection.UploadOnly)
                    continue;

                // Get Command
                var command = await this.GetSelectChangesCommandAsync(context, syncTable, message.Setup, message.IsNew, connection, transaction);

                // Set parameters
                this.SetSelectChangesCommonParameters(context, syncTable, message.ExcludingScopeId, message.IsNew, message.LastTimestamp, command);

                // launch interceptor if any
                var args = new TableChangesSelectingArgs(context, syncTable, command, connection, transaction);
                await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);

                if (args.Cancel || args.Command == null)
                    continue;

                // Statistics
                var tableChangesSelected = new TableChangesSelected(syncTable.TableName, syncTable.SchemaName);

                // Get the reader
                using var dataReader = await args.Command.ExecuteReaderAsync().ConfigureAwait(false);

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

                dataReader.Close();

                // Check interceptor
                var changesArgs = new TableChangesSelectedArgs(context, null, tableChangesSelected, connection, transaction);
                await this.InterceptAsync(changesArgs, cancellationToken).ConfigureAwait(false);

                if (tableChangesSelected.Deletes > 0 || tableChangesSelected.Upserts > 0)
                    changes.TableChangesSelected.Add(tableChangesSelected);
            }

            // Raise database changes selected
            var databaseChangesSelectedArgs = new DatabaseChangesSelectedArgs(context, message.LastTimestamp, null, changes, connection);
            this.ReportProgress(context, progress, databaseChangesSelectedArgs);
            await this.InterceptAsync(databaseChangesSelectedArgs, cancellationToken).ConfigureAwait(false);

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
        internal async Task<DbCommand> GetSelectChangesCommandAsync(SyncContext context, SyncTable syncTable, SyncSetup setup, bool isNew, DbConnection connection, DbTransaction transaction)
        {
            DbCommand command;
            DbCommandType dbCommandType;

            SyncFilter tableFilter = null;

            var syncAdapter = this.GetSyncAdapter(syncTable, setup);

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
            command = await syncAdapter.GetCommandAsync(dbCommandType, connection, transaction, tableFilter);

            return command;
        }

        /// <summary>
        /// Set common parameters to SelectChanges Sql command
        /// </summary>
        internal void SetSelectChangesCommonParameters(SyncContext context, SyncTable syncTable, Guid? excludingScopeId, bool isNew, long? lastTimestamp, DbCommand selectIncrementalChangesCommand)
        {
            // Set the parameters
            DbSyncAdapter.SetParameterValue(selectIncrementalChangesCommand, "sync_min_timestamp", lastTimestamp);
            DbSyncAdapter.SetParameterValue(selectIncrementalChangesCommand, "sync_scope_id", excludingScopeId.HasValue ? (object)excludingScopeId.Value : DBNull.Value);

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

                DbSyncAdapter.SetParameterValue(selectIncrementalChangesCommand, filterParam.Name, val);
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
                if (columnName == "sync_update_scope_id")
                    continue;

                var columnValueObject = dataReader.GetValue(i);
                var columnValue = columnValueObject == DBNull.Value ? null : columnValueObject;

                row[columnName] = columnValue;

            }

            row.RowState = isTombstone ? DataRowState.Deleted : DataRowState.Modified;

            return row;
        }




    }
}
