using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
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
            if (!string.IsNullOrEmpty(message.BatchDirectory) && !Directory.Exists(message.BatchDirectory))
                Directory.CreateDirectory(message.BatchDirectory);

            changesSelected = new DatabaseChangesSelected();

            // numbers of batch files generated
            var batchIndex = 0;

            // Create a batch 
            // batchinfo generate a schema clone with scope columns if needed
            batchInfo = new BatchInfo(message.Schema, message.BatchDirectory);

            if (Directory.Exists(batchInfo.GetDirectoryFullPath()))
                Directory.Delete(batchInfo.GetDirectoryFullPath(), true);

            Directory.CreateDirectory(batchInfo.GetDirectoryFullPath());

            var cptSyncTable = 0;
            var currentProgress = context.ProgressPercentage;

            var schemaTables = message.Schema.Tables.SortByDependencies(tab => tab.GetRelations().Select(r => r.GetParentTable()));

            foreach (var syncTable in schemaTables)
            {
                var columnsCount = syncTable.GetMutableColumnsWithPrimaryKeys().Count();

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

                if (selectIncrementalChangesCommand == null) continue;

                var tableName = ParserName.Parse(syncTable).Unquoted().Normalized().ToString();
                var fileName = BatchInfo.GenerateNewFileName2(batchIndex.ToString(), tableName, "json");
                var fullPath = Path.Combine(batchInfo.GetDirectoryFullPath(), fileName);
                var sw = new StreamWriter(fullPath);
                var writer = new JsonTextWriter(sw) { CloseOutput = true };

                // Open file
                writer.WriteStartObject();
                writer.WritePropertyName("t");
                writer.WriteStartArray();
                writer.WriteStartObject();
                writer.WritePropertyName("n");
                writer.WriteValue(syncTable.TableName);
                writer.WritePropertyName("s");
                writer.WriteValue(syncTable.SchemaName);
                writer.WritePropertyName("r");
                writer.WriteStartArray();
                writer.WriteWhitespace(Environment.NewLine);

                // Statistics
                var tableChangesSelected = new TableChangesSelected(syncTable.TableName, syncTable.SchemaName);

                var rowsCountInBatch = 0;

                // Set parameters
                this.SetSelectChangesCommonParameters(context, syncTable, message.ExcludingScopeId, message.IsNew, message.LastTimestamp, selectIncrementalChangesCommand);

                // launch interceptor if any
                var args = new TableChangesSelectingArgs(context, syncTable, selectIncrementalChangesCommand, connection, transaction);
                await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);

                if (!args.Cancel && args.Command != null)
                {
                    // Get the reader
                    using var dataReader = await args.Command.ExecuteReaderAsync().ConfigureAwait(false);

                    while (dataReader.Read())
                    {
                        // Create a row from dataReader
                        var row = CreateSyncRowFromReader2(dataReader, columnsCount);
                        rowsCountInBatch++;

                        // Set the correct state to be applied
                        if ((int)row[0] == (int)DataRowState.Deleted)
                            tableChangesSelected.Deletes++;
                        else if ((int)row[0] == (int)DataRowState.Modified)
                            tableChangesSelected.Upserts++;

                        writer.WriteStartArray();
                        for (var i = 0; i < row.Length; i++)
                            writer.WriteValue(row[i]);
                        writer.WriteEndArray();
                        writer.WriteWhitespace(Environment.NewLine);
                        writer.Flush();

                        var currentBatchSize = sw.BaseStream.Position / 1024;

                        // Next line if we don't reach the batch size yet.
                        if (currentBatchSize <= message.BatchSize)
                            continue;

                        var bpi = new BatchPartInfo { FileName = fileName };

                        // Create the info on the batch part
                        BatchPartTableInfo tableInfo = new BatchPartTableInfo
                        {
                            TableName = tableChangesSelected.TableName,
                            SchemaName = tableChangesSelected.SchemaName,
                            RowsCount = rowsCountInBatch

                        };
                        bpi.Tables = new BatchPartTableInfo[] { tableInfo };
                        bpi.RowsCount = rowsCountInBatch;
                        bpi.IsLastBatch = false;
                        bpi.Index = batchIndex;
                        batchInfo.RowsCount += rowsCountInBatch;
                        batchInfo.BatchPartsInfo.Add(bpi);

                        writer.WriteEndArray();
                        writer.WriteEndObject();
                        writer.WriteEndArray();
                        writer.WriteEndObject();
                        writer.Flush();
                        writer.Close();

                        // increment batch index
                        batchIndex++;

                        fileName = BatchInfo.GenerateNewFileName2(batchIndex.ToString(), tableName, "json");
                        fullPath = Path.Combine(batchInfo.GetDirectoryFullPath(), fileName);

                        sw = new StreamWriter(fullPath);
                        writer = new JsonTextWriter(sw) { CloseOutput = true };
                        rowsCountInBatch = 0;

                        writer.WriteStartObject();
                        writer.WritePropertyName("t");
                        writer.WriteStartArray();
                        writer.WriteStartObject();
                        writer.WritePropertyName("n");
                        writer.WriteValue(tableChangesSelected.TableName);
                        writer.WritePropertyName("s");
                        writer.WriteValue(tableChangesSelected.SchemaName);
                        writer.WritePropertyName("r");
                        writer.WriteStartArray();
                        writer.WriteWhitespace(Environment.NewLine);

                    }

                    dataReader.Close();

                }

                writer.WriteEndArray();
                writer.WriteEndObject();
                writer.WriteEndArray();
                writer.WriteEndObject();
                writer.Flush();
                writer.Close();

                // Check if we have ..something.
                // Delete folder if nothing
                // Add the BPI to BI if something
                if (rowsCountInBatch == 0 && File.Exists(fullPath))
                {
                    File.Delete(fullPath);
                }
                else
                {
                    var bpi2 = new BatchPartInfo { FileName = fileName };

                    // Create the info on the batch part
                    BatchPartTableInfo tableInfo2 = new BatchPartTableInfo
                    {
                        TableName = tableChangesSelected.TableName,
                        SchemaName = tableChangesSelected.SchemaName,
                        RowsCount = rowsCountInBatch
                    };
                    bpi2.Tables = new BatchPartTableInfo[] { tableInfo2 };
                    bpi2.RowsCount = rowsCountInBatch;
                    bpi2.IsLastBatch = true;
                    bpi2.Index = batchIndex;
                    batchInfo.RowsCount += rowsCountInBatch;
                    batchInfo.BatchPartsInfo.Add(bpi2);
                    batchIndex++;

                }

                // We don't report progress if no table changes is empty, to limit verbosity
                if (tableChangesSelected.Deletes > 0 || tableChangesSelected.Upserts > 0)
                    changesSelected.TableChangesSelected.Add(tableChangesSelected);

                // even if no rows raise the interceptor
                var tableChangesSelectedArgs = new TableChangesSelectedArgs(context, null, tableChangesSelected, connection, transaction);
                await this.InterceptAsync(tableChangesSelectedArgs, cancellationToken).ConfigureAwait(false);

                context.ProgressPercentage = currentProgress + (cptSyncTable * 0.2d / message.Schema.Tables.Count);

                // only raise report progress if we have something
                if (tableChangesSelectedArgs.TableChangesSelected.TotalChanges > 0)
                    this.ReportProgress(context, progress, tableChangesSelectedArgs);
            }

            //Set the total rows count contained in the batch info
            batchInfo.EnsureLastBatch();
            batchInfo.RowsCount = changesSelected.TotalChangesSelected;

            if (batchInfo.RowsCount <= 0)
                batchInfo.Clear(true);

            // delete all empty batchparts (empty tables)
            foreach (var bpi in batchInfo.BatchPartsInfo.ToArray())
            {
                if (bpi.RowsCount <= 0)
                {
                    var fullPathToDelete = Path.Combine(batchInfo.GetDirectoryFullPath(), bpi.FileName);
                    File.Delete(fullPathToDelete);
                    batchInfo.BatchPartsInfo.Remove(bpi);
                }
            }

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

                if (command == null) continue;

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
            // Create the batch info, in memory
            var batchInfo = new BatchInfo(message.Schema, message.BatchDirectory); ;

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
            var (command, _) = await syncAdapter.GetCommandAsync(dbCommandType, connection, transaction, tableFilter);

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
            //DbSyncAdapter.SetParameterValue(selectIncrementalChangesCommand, "sync_index", index);
            //DbSyncAdapter.SetParameterValue(selectIncrementalChangesCommand, "sync_batch_size", batchSize == 0 ? -1 : batchSize); // -1 means all

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

                row[i] = columnValue;
            }

            row.RowState = isTombstone ? DataRowState.Deleted : DataRowState.Modified;
            return row;
        }


        /// <summary>
        /// Create a new SyncRow from a dataReader.
        /// </summary>
        internal object[] CreateSyncRowFromReader2(IDataReader dataReader, int bufferLength)
        {
            // Create a new row, based on table structure

            var buffer = new object[bufferLength + 1];

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

                buffer[i + 1] = columnValue;
            }

            buffer[0] = isTombstone ? DataRowState.Deleted : DataRowState.Modified;
            return buffer;
        }



    }
}
