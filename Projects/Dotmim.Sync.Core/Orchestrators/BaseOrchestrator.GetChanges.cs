using Dotmim.Sync.Args;
using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using System;
using System.Collections.Concurrent;
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
                             IScopeInfo scopeInfo, SyncContext context, bool isNew, long? fromLastTimestamp, long? toNewTimestamp, Guid? excludingScopeId,
                             bool supportsMultiActiveResultSets, string batchRootDirectory, string batchDirectoryName,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            // batch info containing changes
            BatchInfo batchInfo;

            // Statistics about changes that are selected
            DatabaseChangesSelected changesSelected;

            context.SyncStage = SyncStage.ChangesSelecting;

            if (context.SyncWay == SyncWay.Upload && context.SyncType == SyncType.Reinitialize)
            {
                (batchInfo, changesSelected) = await this.InternalGetEmptyChangesAsync(scopeInfo, batchRootDirectory).ConfigureAwait(false);
                return (context, batchInfo, changesSelected);
            }

            // create local directory
            if (!string.IsNullOrEmpty(batchRootDirectory) && !Directory.Exists(batchRootDirectory))
                Directory.CreateDirectory(batchRootDirectory);

            changesSelected = new DatabaseChangesSelected();

            // Create a batch 
            // batchinfo generate a schema clone with scope columns if needed
            batchInfo = new BatchInfo(scopeInfo.Schema, batchRootDirectory, batchDirectoryName);
            batchInfo.TryRemoveDirectory();
            batchInfo.CreateDirectory();

            // Call interceptor
            var databaseChangesSelectingArgs = new DatabaseChangesSelectingArgs(context, batchInfo.GetDirectoryFullPath(), this.Options.BatchSize, isNew,
                fromLastTimestamp, toNewTimestamp, connection, transaction);
            await this.InterceptAsync(databaseChangesSelectingArgs, progress, cancellationToken).ConfigureAwait(false);

            var cptSyncTable = 0;
            var currentProgress = context.ProgressPercentage;

            var schemaTables = scopeInfo.Schema.Tables.SortByDependencies(tab => tab.GetRelations().Select(r => r.GetParentTable()));

            var lstAllBatchPartInfos = new ConcurrentBag<BatchPartInfo>();
            var lstTableChangesSelected = new ConcurrentBag<TableChangesSelected>();

            var threadNumberLimits = supportsMultiActiveResultSets ? 16 : 1;

            if (supportsMultiActiveResultSets)
            {
                await schemaTables.ForEachAsync(async syncTable =>
                {
                    if (cancellationToken.IsCancellationRequested)
                        return;

                    // tmp count of table for report progress pct
                    cptSyncTable++;

                    List<BatchPartInfo> syncTableBatchPartInfos;
                    TableChangesSelected tableChangesSelected;
                    (context, syncTableBatchPartInfos, tableChangesSelected) = await InternalReadSyncTableChangesAsync(
                            scopeInfo, context, excludingScopeId, syncTable, batchInfo, isNew, fromLastTimestamp, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (syncTableBatchPartInfos == null)
                        return;

                    // We don't report progress if no table changes is empty, to limit verbosity
                    if (tableChangesSelected != null && (tableChangesSelected.Deletes > 0 || tableChangesSelected.Upserts > 0))
                        lstTableChangesSelected.Add(tableChangesSelected);

                    // Add sync table bpi to all bpi
                    syncTableBatchPartInfos.ForEach(bpi => lstAllBatchPartInfos.Add(bpi));

                    context.ProgressPercentage = currentProgress + (cptSyncTable * 0.2d / scopeInfo.Schema.Tables.Count);

                }, threadNumberLimits);
            }
            else
            {
                foreach (var syncTable in schemaTables)
                {
                    if (cancellationToken.IsCancellationRequested)
                        continue;

                    // tmp count of table for report progress pct
                    cptSyncTable++;

                    List<BatchPartInfo> syncTableBatchPartInfos;
                    TableChangesSelected tableChangesSelected;
                    (context, syncTableBatchPartInfos, tableChangesSelected) = await InternalReadSyncTableChangesAsync(
                            scopeInfo, context, excludingScopeId, syncTable, batchInfo, isNew, fromLastTimestamp, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (syncTableBatchPartInfos == null)
                        continue;

                    // We don't report progress if no table changes is empty, to limit verbosity
                    if (tableChangesSelected != null && (tableChangesSelected.Deletes > 0 || tableChangesSelected.Upserts > 0))
                        lstTableChangesSelected.Add(tableChangesSelected);

                    // Add sync table bpi to all bpi
                    syncTableBatchPartInfos.ForEach(bpi => lstAllBatchPartInfos.Add(bpi));

                    context.ProgressPercentage = currentProgress + (cptSyncTable * 0.2d / scopeInfo.Schema.Tables.Count);

                }
            }

            while (!lstTableChangesSelected.IsEmpty)
                if (lstTableChangesSelected.TryTake(out var tableChangesSelected))
                    changesSelected.TableChangesSelected.Add(tableChangesSelected);


            // delete all empty batchparts (empty tables)
            foreach (var bpi in lstAllBatchPartInfos.Where(bpi => bpi.RowsCount <= 0))
                File.Delete(Path.Combine(batchInfo.GetDirectoryFullPath(), bpi.FileName));

            // Generate a good index order to be compliant with previous versions
            var tmpLstBatchPartInfos = new List<BatchPartInfo>();
            foreach (var table in schemaTables)
            {
                // get all bpi where count > 0 and ordered by index
                foreach (var bpi in lstAllBatchPartInfos.Where(bpi => bpi.RowsCount > 0 && bpi.Tables[0].EqualsByName(new BatchPartTableInfo(table.TableName, table.SchemaName))).OrderBy(bpi => bpi.Index).ToArray())
                {
                    batchInfo.BatchPartsInfo.Add(bpi);
                    batchInfo.RowsCount += bpi.RowsCount;

                    tmpLstBatchPartInfos.Add(bpi);
                }
            }

            var newBatchIndex = 0;
            foreach (var bpi in tmpLstBatchPartInfos)
            {
                bpi.Index = newBatchIndex;
                newBatchIndex++;
                bpi.IsLastBatch = newBatchIndex == tmpLstBatchPartInfos.Count;
            }

            //Set the total rows count contained in the batch info
            batchInfo.EnsureLastBatch();

            if (batchInfo.RowsCount <= 0)
            {
                var cleanFolder = await this.InternalCanCleanFolderAsync(scopeInfo.Name, context.Parameters, batchInfo, cancellationToken).ConfigureAwait(false);
                batchInfo.Clear(cleanFolder);
            }

            var databaseChangesSelectedArgs = new DatabaseChangesSelectedArgs(context, fromLastTimestamp, toNewTimestamp, batchInfo, changesSelected, connection);
            await this.InterceptAsync(databaseChangesSelectedArgs, progress, cancellationToken).ConfigureAwait(false);

            return (context, batchInfo, changesSelected);

        }


        internal virtual async Task<(SyncContext context, List<BatchPartInfo> batchPartInfos, TableChangesSelected tableChangesSelected)>
            InternalReadSyncTableChangesAsync(
            IScopeInfo scopeInfo, SyncContext context, Guid? excludintScopeId, SyncTable syncTable,
            BatchInfo batchInfo, bool isNew, long? lastTimestamp,
            DbConnection connection, DbTransaction transaction,
            CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (cancellationToken.IsCancellationRequested)
                return default;

            var setupTable = scopeInfo.Setup.Tables[syncTable.TableName, syncTable.SchemaName];

            if (setupTable == null)
                return (context, default, default);

            // Only table schema is replicated, no datas are applied
            if (setupTable.SyncDirection == SyncDirection.None)
                return (context, default, default);

            // if we are in upload stage, so check if table is not download only
            if (context.SyncWay == SyncWay.Upload && setupTable.SyncDirection == SyncDirection.DownloadOnly)
                return (context, default, default);

            // if we are in download stage, so check if table is not download only
            if (context.SyncWay == SyncWay.Download && setupTable.SyncDirection == SyncDirection.UploadOnly)
                return (context, default, default);

            var (selectIncrementalChangesCommand, dbCommandType) = await this.InternalGetSelectChangesCommandAsync(scopeInfo, context, syncTable, isNew, connection, transaction);

            if (selectIncrementalChangesCommand == null)
                return (context, default, default);

            this.InternalSetSelectChangesCommonParameters(context, syncTable, excludintScopeId, isNew, lastTimestamp, selectIncrementalChangesCommand);

            var schemaChangesTable = CreateChangesTable(syncTable);

            // numbers of batch files generated
            var batchIndex = 0;

            var localSerializer = new LocalJsonSerializer();

            var interceptorsWriting = this.interceptors.GetInterceptors<SerializingRowArgs>();
            if (interceptorsWriting.Count > 0)
            {
                localSerializer.OnWritingRow(async (syncTable, rowArray) =>
                {
                    var copyArray = new object[rowArray.Length];
                    Array.Copy(rowArray, copyArray, rowArray.Length);

                    var args = new SerializingRowArgs(context, syncTable, copyArray);
                    await this.InterceptAsync(args, progress, cancellationToken).ConfigureAwait(false);
                    return args.Result;
                });
            }

            var (batchPartInfoFullPath, batchPartFileName) = batchInfo.GetNewBatchPartInfoPath(schemaChangesTable, batchIndex, localSerializer.Extension);

            // Statistics
            var tableChangesSelected = new TableChangesSelected(schemaChangesTable.TableName, schemaChangesTable.SchemaName);

            var rowsCountInBatch = 0;
            var syncTableBatchPartInfos = new List<BatchPartInfo>();

            // launch interceptor if any
            var args = await this.InterceptAsync(new TableChangesSelectingArgs(context, schemaChangesTable, selectIncrementalChangesCommand, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            if (!args.Cancel && args.Command != null)
            {
                // open the file and write table header
                await localSerializer.OpenFileAsync(batchPartInfoFullPath, schemaChangesTable).ConfigureAwait(false);

                await this.InterceptAsync(new ExecuteCommandArgs(context, args.Command, dbCommandType, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                // Get the reader
                using var dataReader = await args.Command.ExecuteReaderAsync().ConfigureAwait(false);

                while (dataReader.Read())
                {
                    // Create a row from dataReader
                    var syncRow = CreateSyncRowFromReader2(dataReader, schemaChangesTable);
                    rowsCountInBatch++;

                    var tableChangesSelectedSyncRowArgs = await this.InterceptAsync(new RowsChangesSelectedArgs(context, syncRow, schemaChangesTable, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
                    syncRow = tableChangesSelectedSyncRowArgs.SyncRow;

                    if (syncRow == null)
                        continue;

                    // Set the correct state to be applied
                    if (syncRow.RowState == DataRowState.Deleted)
                        tableChangesSelected.Deletes++;
                    else if (syncRow.RowState == DataRowState.Modified)
                        tableChangesSelected.Upserts++;

                    await localSerializer.WriteRowToFileAsync(syncRow, schemaChangesTable).ConfigureAwait(false);

                    var currentBatchSize = await localSerializer.GetCurrentFileSizeAsync().ConfigureAwait(false);

                    // Next line if we don't reach the batch size yet.
                    if (currentBatchSize <= this.Options.BatchSize)
                        continue;

                    var bpi = BatchPartInfo.NewBatchPartInfo(batchPartFileName, tableChangesSelected.TableName, tableChangesSelected.SchemaName, rowsCountInBatch, batchIndex);

                    syncTableBatchPartInfos.Add(bpi);

                    // Close file
                    await localSerializer.CloseFileAsync(batchPartInfoFullPath, schemaChangesTable).ConfigureAwait(false);

                    // increment batch index
                    batchIndex++;
                    // Reinit rowscount in batch
                    rowsCountInBatch = 0;

                    // generate a new path
                    (batchPartInfoFullPath, batchPartFileName) = batchInfo.GetNewBatchPartInfoPath(schemaChangesTable, batchIndex, localSerializer.Extension);

                    // open a new file and write table header
                    await localSerializer.OpenFileAsync(batchPartInfoFullPath, schemaChangesTable).ConfigureAwait(false);
                }

                dataReader.Close();

                // Close file
                await localSerializer.CloseFileAsync(batchPartInfoFullPath, schemaChangesTable).ConfigureAwait(false);
            }

            // Check if we have ..something.
            // Delete folder if nothing
            // Add the BPI to BI if something
            if (rowsCountInBatch == 0 && File.Exists(batchPartInfoFullPath))
            {
                File.Delete(batchPartInfoFullPath);
            }
            else
            {
                var bpi2 = BatchPartInfo.NewBatchPartInfo(batchPartFileName, tableChangesSelected.TableName, tableChangesSelected.SchemaName, rowsCountInBatch, batchIndex);
                bpi2.IsLastBatch = true;
                syncTableBatchPartInfos.Add(bpi2);
            }


            // even if no rows raise the interceptor
            var tableChangesSelectedArgs = new TableChangesSelectedArgs(context, batchInfo, syncTableBatchPartInfos, syncTable, tableChangesSelected, connection, transaction);
            await this.InterceptAsync(tableChangesSelectedArgs, progress, cancellationToken).ConfigureAwait(false);

            return (context, syncTableBatchPartInfos, tableChangesSelected);
        }

        /// <summary>
        /// Gets changes rows count estimation, 
        /// </summary>
        internal virtual async Task<(SyncContext, DatabaseChangesSelected)> InternalGetEstimatedChangesCountAsync(
                             IScopeInfo scopeInfo, SyncContext context, bool isNew, long? fromLastTimestamp, long? toLastTimestamp, Guid? excludingScopeId,
                             bool supportsMultiActiveResultSets,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            context.SyncStage = SyncStage.ChangesSelecting;

            // Call interceptor
            await this.InterceptAsync(new DatabaseChangesSelectingArgs(context, this.Options.BatchDirectory, this.Options.BatchSize, isNew, fromLastTimestamp, toLastTimestamp, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            // Create stats object to store changes count
            var changes = new DatabaseChangesSelected();

            if (context.SyncWay == SyncWay.Upload && context.SyncType == SyncType.Reinitialize)
                return (context, changes);

            var threadNumberLimits = supportsMultiActiveResultSets ? 8 : 1;

            await scopeInfo.Schema.Tables.ForEachAsync(async syncTable =>
            {
                if (cancellationToken.IsCancellationRequested)
                    return;

                var setupTable = scopeInfo.Setup.Tables[syncTable.TableName, syncTable.SchemaName];

                if (setupTable == null)
                    return;

                // Only table schema is replicated, no datas are applied
                if (setupTable.SyncDirection == SyncDirection.None)
                    return;

                // if we are in upload stage, so check if table is not download only
                if (context.SyncWay == SyncWay.Upload && setupTable.SyncDirection == SyncDirection.DownloadOnly)
                    return;

                // if we are in download stage, so check if table is not download only
                if (context.SyncWay == SyncWay.Download && setupTable.SyncDirection == SyncDirection.UploadOnly)
                    return;

                // Get Command
                var (command, dbCommandType) = await this.InternalGetSelectChangesCommandAsync(scopeInfo, context, syncTable, isNew, connection, transaction);

                if (command == null) return;

                this.InternalSetSelectChangesCommonParameters(context, syncTable, excludingScopeId, isNew, fromLastTimestamp, command);
                
                // launch interceptor if any
                var args = new TableChangesSelectingArgs(context, syncTable, command, connection, transaction);
                await this.InterceptAsync(args, progress, cancellationToken).ConfigureAwait(false);

                if (args.Cancel || args.Command == null)
                    return;

                // Statistics
                var tableChangesSelected = new TableChangesSelected(syncTable.TableName, syncTable.SchemaName);

                await this.InterceptAsync(new ExecuteCommandArgs(context, args.Command, dbCommandType, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
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
                var changesArgs = new TableChangesSelectedArgs(context, null, null, syncTable, tableChangesSelected, connection, transaction);
                await this.InterceptAsync(changesArgs, progress, cancellationToken).ConfigureAwait(false);

                if (tableChangesSelected.Deletes > 0 || tableChangesSelected.Upserts > 0)
                    changes.TableChangesSelected.Add(tableChangesSelected);

            }, threadNumberLimits);

            // Raise database changes selected
            var databaseChangesSelectedArgs = new DatabaseChangesSelectedArgs(context, fromLastTimestamp, toLastTimestamp, null, changes, connection);
            await this.InterceptAsync(databaseChangesSelectedArgs, progress, cancellationToken).ConfigureAwait(false);

            return (context, changes);
        }



        /// <summary>
        /// Generate an empty BatchInfo
        /// </summary>
        internal Task<(BatchInfo, DatabaseChangesSelected)> InternalGetEmptyChangesAsync(IScopeInfo scopeInfo, string dir)
        {
            // Create the batch info, in memory
            var batchInfo = new BatchInfo(scopeInfo.Schema, dir);

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
        internal async Task<(DbCommand, DbCommandType)> InternalGetSelectChangesCommandAsync(IScopeInfo scopeInfo, SyncContext context, 
            SyncTable syncTable,  bool isNew, DbConnection connection, DbTransaction transaction)
        {
            DbCommandType dbCommandType;

            SyncFilter tableFilter = null;


            // Check if we have parameters specified

            // Sqlite does not have any filter, since he can't be server side
            if (this.Provider != null && this.Provider.CanBeServerProvider)
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
            var (command, _) = await this.GetCommandAsync(scopeInfo, context, syncTable, dbCommandType, tableFilter, 
                connection, transaction, default, default).ConfigureAwait(false);

            return (command, dbCommandType);
        }

        /// <summary>
        /// Set common parameters to SelectChanges Sql command
        /// </summary>
        internal void InternalSetSelectChangesCommonParameters(SyncContext context, SyncTable syncTable, Guid? excludingScopeId, bool isNew, long? lastTimestamp, DbCommand selectIncrementalChangesCommand)
        {
            // Set the parameters
            SetParameterValue(selectIncrementalChangesCommand, "sync_min_timestamp", lastTimestamp);
            SetParameterValue(selectIncrementalChangesCommand, "sync_scope_id", excludingScopeId.HasValue ? excludingScopeId.Value : DBNull.Value);

            // Check filters
            SyncFilter tableFilter = null;

            // Sqlite does not have any filter, since he can't be server side
            if (this.Provider != null && this.Provider.CanBeServerProvider)
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

                SetParameterValue(selectIncrementalChangesCommand, filterParam.Name, val);
            }

        }

        /// <summary>
        /// Create a new SyncRow from a dataReader.
        /// </summary>
        internal SyncRow CreateSyncRowFromReader2(IDataReader dataReader, SyncTable schemaTable)
        {
            // Create a new row, based on table structure

            var syncRow = new SyncRow(schemaTable);

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

                syncRow[i] = columnValue;
            }

            syncRow.RowState = isTombstone ? DataRowState.Deleted : DataRowState.Modified;
            return syncRow;
        }



    }
}
