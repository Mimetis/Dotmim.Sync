
using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {

        /// <summary>
        /// Gets a batch of changes to synchronize when given batch size, 
        /// destination knowledge, and change data retriever parameters.
        /// </summary>
        /// <returns>A DbSyncContext object that will be used to retrieve the modified data.</returns>
        internal virtual async Task<DatabaseChangesSelected> InternalGetChangesAsync(
                             ScopeInfo scopeInfo, SyncContext context, bool isNew, long? fromLastTimestamp, long? toNewTimestamp, Guid? excludingScopeId,
                             bool supportsMultiActiveResultSets, BatchInfo batchInfo,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            try
            {
                // Statistics about changes that are selected
                DatabaseChangesSelected changesSelected;

                context.SyncStage = SyncStage.ChangesSelecting;

                // Create a new empty in-memory batch info
                if (context.SyncWay == SyncWay.Upload && context.SyncType == SyncType.Reinitialize)
                    return new DatabaseChangesSelected();

                // create local directory
                if (!string.IsNullOrEmpty(batchInfo.DirectoryRoot) && !Directory.Exists(batchInfo.DirectoryRoot))
                    Directory.CreateDirectory(batchInfo.DirectoryRoot);

                changesSelected = new DatabaseChangesSelected();

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

                // Ensure correct order
                this.EnsureLastBatchInfo(scopeInfo, context, batchInfo, lstAllBatchPartInfos, schemaTables);

                if (batchInfo.RowsCount <= 0)
                {
                    var cleanFolder = await this.InternalCanCleanFolderAsync(scopeInfo.Name, context.Parameters, batchInfo).ConfigureAwait(false);

                    if (cleanFolder)
                        batchInfo.TryRemoveDirectory();
                }
                return changesSelected;
            }
            catch (Exception ex)
            {
                string message = null;

                if (batchInfo != null && batchInfo.DirectoryRoot != null)
                    message += $"Directory:{batchInfo.DirectoryRoot}.";

                message += $"Supports MultiActiveResultSets:{supportsMultiActiveResultSets}.";
                message += $"Is New:{isNew}.";
                message += $"Interval:{fromLastTimestamp}/{toNewTimestamp}.";

                throw GetSyncError(context, ex, message);
            }
        }


        internal virtual async Task<(SyncContext context, List<BatchPartInfo> batchPartInfos, TableChangesSelected tableChangesSelected)>
            InternalReadSyncTableChangesAsync(
            ScopeInfo scopeInfo, SyncContext context, Guid? excludintScopeId, SyncTable syncTable,
            BatchInfo batchInfo, bool isNew, long? lastTimestamp, 
            DbConnection connection, DbTransaction transaction,
            CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (cancellationToken.IsCancellationRequested)
                return default;

            DbCommand selectIncrementalChangesCommand = null;

            var localSerializerModified = new LocalJsonSerializer(this, context);
            var localSerializerDeleted = new LocalJsonSerializer(this, context);
            try
            {
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

                DbCommandType dbCommandType;
                (selectIncrementalChangesCommand, dbCommandType) = await this.InternalGetSelectChangesCommandAsync(scopeInfo, context, syncTable, isNew,
                        connection, transaction);

                if (selectIncrementalChangesCommand == null)
                    return (context, default, default);

                // Get correct adapter
                var syncAdapter = this.GetSyncAdapter(scopeInfo.Name, syncTable, scopeInfo.Setup);

                this.InternalSetCommandParametersValues(context, selectIncrementalChangesCommand, dbCommandType, syncAdapter, connection, transaction, cancellationToken, progress,
                    sync_scope_id:excludintScopeId, sync_min_timestamp:lastTimestamp);

                var schemaChangesTable = CreateChangesTable(syncTable);


                // Statistics
                var tableChangesSelected = new TableChangesSelected(schemaChangesTable.TableName, schemaChangesTable.SchemaName);

                //var rowsCountInBatchModified = 0;
                //var rowsCountInBatchDeleted = 0;

                var batchPartInfos = new List<BatchPartInfo>();

                BatchPartInfo batchPartInfoUpserts = null;
                BatchPartInfo batchPartInfoDeleted = null;

                // launch interceptor if any
                var args = await this.InterceptAsync(new TableChangesSelectingArgs(context, schemaChangesTable, selectIncrementalChangesCommand, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                if (!args.Cancel && args.Command != null)
                {
                    await this.InterceptAsync(new ExecuteCommandArgs(context, args.Command, dbCommandType, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                    // Get the reader
                    using var dataReader = await args.Command.ExecuteReaderAsync().ConfigureAwait(false);

                    while (dataReader.Read())
                    {
                        // Create a row from dataReader
                        var syncRow = CreateSyncRowFromReader(context, dataReader, schemaChangesTable);

                        var tableChangesSelectedSyncRowArgs = await this.InterceptAsync(new RowsChangesSelectedArgs(context, syncRow, schemaChangesTable, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
                        syncRow = tableChangesSelectedSyncRowArgs.SyncRow;

                        if (syncRow == null)
                            continue;

                        if (syncRow.RowState == SyncRowState.Deleted)
                            batchPartInfoDeleted = await this.InternalAddRowToBatchPartInfoAsync(context, localSerializerDeleted, syncRow, batchInfo, batchPartInfoDeleted, batchPartInfos, schemaChangesTable, tableChangesSelected, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                        else
                            batchPartInfoUpserts = await this.InternalAddRowToBatchPartInfoAsync(context, localSerializerModified, syncRow, batchInfo, batchPartInfoUpserts, batchPartInfos, schemaChangesTable, tableChangesSelected, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    }

                    dataReader.Close();

                    // tmp Func
                    var CloseSerializer = new Func<LocalJsonSerializer, BatchChangesCreatedArgs, Task>(async (localJsonSerializer, args) =>
                    {
                        // Close file
                        if (localJsonSerializer != null && localJsonSerializer.IsOpen)
                        {
                            await localJsonSerializer.CloseFileAsync().ConfigureAwait(false);
                            await this.InterceptAsync(args, progress, cancellationToken);
                        }
                    });

                    if (batchPartInfoUpserts != null || batchPartInfoDeleted != null)
                    {
                        if (batchPartInfoUpserts?.Index > batchPartInfoDeleted?.Index)
                        {
                            await CloseSerializer(localSerializerDeleted, new BatchChangesCreatedArgs(context, batchPartInfoDeleted, schemaChangesTable, tableChangesSelected, SyncRowState.Deleted, connection, transaction)).ConfigureAwait(false);
                            await CloseSerializer(localSerializerModified, new BatchChangesCreatedArgs(context, batchPartInfoUpserts, schemaChangesTable, tableChangesSelected, SyncRowState.Modified, connection, transaction)).ConfigureAwait(false);
                        }
                        else
                        {
                            await CloseSerializer(localSerializerModified, new BatchChangesCreatedArgs(context, batchPartInfoUpserts, schemaChangesTable, tableChangesSelected, SyncRowState.Modified, connection, transaction)).ConfigureAwait(false);
                            await CloseSerializer(localSerializerDeleted, new BatchChangesCreatedArgs(context, batchPartInfoDeleted, schemaChangesTable, tableChangesSelected, SyncRowState.Deleted, connection, transaction)).ConfigureAwait(false);
                        }

                    }


                    // Close file
                    if (localSerializerModified.IsOpen)
                    {
                        await localSerializerModified.CloseFileAsync().ConfigureAwait(false);
                        await this.InterceptAsync(new BatchChangesCreatedArgs(context, batchPartInfoUpserts, schemaChangesTable, tableChangesSelected, SyncRowState.Modified, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                    }

                    if (localSerializerDeleted.IsOpen)
                    {
                        await localSerializerDeleted.CloseFileAsync().ConfigureAwait(false);
                        await this.InterceptAsync(new BatchChangesCreatedArgs(context, batchPartInfoDeleted, schemaChangesTable, tableChangesSelected, SyncRowState.Deleted, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
                    }
                }

                foreach (var bpi in batchPartInfos.ToArray())
                {
                    string fullPath = batchInfo.GetBatchPartInfoPath(bpi);

                    if (fullPath != null && bpi.RowsCount == 0 && File.Exists(fullPath))
                    {
                        File.Delete(fullPath);
                        batchPartInfos.Remove(bpi);
                    }
                }

                var tableChangesSelectedArgs = new TableChangesSelectedArgs(context, batchInfo, batchPartInfos, syncTable, tableChangesSelected, connection, transaction);
                await this.InterceptAsync(tableChangesSelectedArgs, progress, cancellationToken).ConfigureAwait(false);

                return (context, batchPartInfos, tableChangesSelected);
            }
            catch (Exception ex)
            {
                string message = null;

                if (selectIncrementalChangesCommand != null)
                    message += $"SelectChangesCommand:{selectIncrementalChangesCommand.CommandText}.";

                if (syncTable != null)
                    message += $"Table:{syncTable.GetFullName()}.";

                message += $"Is New:{isNew}.";

                message += $"LastTimestamp:{lastTimestamp}.";

                throw GetSyncError(context, ex, message);
            }
            finally
            {
                if (localSerializerModified != null)
                await localSerializerModified.DisposeAsync().ConfigureAwait(false);

                if (localSerializerDeleted != null)
                    await localSerializerDeleted.DisposeAsync().ConfigureAwait(false);

            }
        }


        internal async Task<BatchPartInfo> InternalAddRowToBatchPartInfoAsync(SyncContext context, LocalJsonSerializer localJsonSerializer, SyncRow syncRow, BatchInfo batchInfo,
            BatchPartInfo batchPartInfo, List<BatchPartInfo> batchPartInfos, SyncTable schemaChangesTable, TableChangesSelected tableChangesSelected,
            DbConnection connection, DbTransaction transaction,
            CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // open the file and write table header for all deleted rows

            var ext = syncRow.RowState == SyncRowState.Deleted ? "DELETED" : "UPSERTS";
            var index = batchPartInfos != null ? batchPartInfos.Count : 0;

            if (!localJsonSerializer.IsOpen)
            {
                var (batchPartInfoFullPath, batchPartFileName) = batchInfo.GetNewBatchPartInfoPath(schemaChangesTable, index, localJsonSerializer.Extension, ext);
                await localJsonSerializer.OpenFileAsync(batchPartInfoFullPath, schemaChangesTable, syncRow.RowState).ConfigureAwait(false);

                batchPartInfo = new BatchPartInfo(batchPartFileName, schemaChangesTable.TableName, schemaChangesTable.SchemaName, syncRow.RowState, 0, index);
                batchPartInfos.Add(batchPartInfo);
            }

            if (syncRow.RowState == SyncRowState.Deleted)
                tableChangesSelected.Deletes++;
            else
                tableChangesSelected.Upserts++;

            await localJsonSerializer.WriteRowToFileAsync(syncRow, schemaChangesTable).ConfigureAwait(false);
            batchPartInfo.RowsCount++;

            var currentBatchSize = await localJsonSerializer.GetCurrentFileSizeAsync().ConfigureAwait(false);

            if (currentBatchSize > this.Options.BatchSize && localJsonSerializer.IsOpen)
            {
                await localJsonSerializer.CloseFileAsync().ConfigureAwait(false);
                await this.InterceptAsync(new BatchChangesCreatedArgs(context, batchPartInfo, schemaChangesTable, tableChangesSelected, syncRow.RowState, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            }

            return batchPartInfo;
        }

        /// <summary>
        /// Gets changes rows count estimation, 
        /// </summary>
        internal virtual async Task<(SyncContext, DatabaseChangesSelected)> InternalGetEstimatedChangesCountAsync(
                             ScopeInfo scopeInfo, SyncContext context, bool isNew, long? fromLastTimestamp, long? toLastTimestamp, Guid? excludingScopeId,
                             bool supportsMultiActiveResultSets,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            try
            {
                context.SyncStage = SyncStage.ChangesSelecting;

                // Create stats object to store changes count
                var changes = new DatabaseChangesSelected();

                // Call interceptor
                var databaseChangesSelectingArgs = new DatabaseChangesSelectingArgs(context, default, this.Options.BatchSize, true,
                    fromLastTimestamp, toLastTimestamp, connection, transaction);

                await this.InterceptAsync(databaseChangesSelectingArgs, progress, cancellationToken).ConfigureAwait(false);

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

                    // Get correct adapter
                    var syncAdapter = this.GetSyncAdapter(scopeInfo.Name, syncTable, scopeInfo.Setup);

                    // Get Command
                    var (command, dbCommandType) = await this.InternalGetSelectChangesCommandAsync(scopeInfo, context, syncTable, isNew, connection, transaction).ConfigureAwait(false);

                    if (command == null) return;

                    this.InternalSetCommandParametersValues(context, command, dbCommandType, syncAdapter, connection, transaction, cancellationToken, progress,
                        sync_scope_id: excludingScopeId, sync_min_timestamp: fromLastTimestamp);

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
                            var columnName = dataReader.GetName(i);
                            
                            // if we have the tombstone value, do not add it to the table
                            if (columnName == "sync_row_is_tombstone")
                            {
                                var objIsTombstone = dataReader.GetValue(i);
                                isTombstone = objIsTombstone == DBNull.Value ? false : Convert.ToInt64(objIsTombstone) > 0;
                                continue;
                            }
                            if (columnName == "sync_update_scope_id")
                                continue;
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


                var databaseChangesSelectedArgs = new DatabaseChangesSelectedArgs(context, fromLastTimestamp, toLastTimestamp,
                            default, changes, connection, transaction);

                await this.InterceptAsync(databaseChangesSelectedArgs, progress, cancellationToken).ConfigureAwait(false);


                return (context, changes);
            }
            catch (Exception ex)
            {
                string message = null;

                message += $"Supports MultiActiveResultSets:{supportsMultiActiveResultSets}.";
                message += $"Is New:{isNew}.";
                message += $"Interval:{fromLastTimestamp}/{toLastTimestamp}.";

                throw GetSyncError(context, ex, message);
            }
        }


        /// <summary>
        /// Get the correct Select changes command 
        /// Can be either
        /// - SelectInitializedChanges              : All changes for first sync
        /// - SelectChanges                         : All changes filtered by timestamp
        /// - SelectInitializedChangesWithFilters   : All changes for first sync with filters
        /// - SelectChangesWithFilters              : All changes filtered by timestamp with filters
        /// </summary>
        internal async Task<(DbCommand, DbCommandType)> InternalGetSelectChangesCommandAsync(ScopeInfo scopeInfo, SyncContext context,
            SyncTable syncTable, bool isNew, DbConnection connection, DbTransaction transaction)
        {
            DbCommandType dbCommandType = DbCommandType.None;
            SyncFilter tableFilter = null;

            try
            {
                // Sqlite does not have any filter, since he can't be server side
                //if (this.Provider != null && this.Provider.CanBeServerProvider)
                if (this.Provider != null)
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
                var syncAdapter = this.GetSyncAdapter(scopeInfo.Name, syncTable, scopeInfo.Setup);

                var (command, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, dbCommandType, 
                    connection, transaction, default, default).ConfigureAwait(false);

                return (command, dbCommandType);
            }
            catch (Exception ex)
            {
                string message = null;

                if (syncTable != null)
                    message += $"Table:{syncTable.GetFullName()}.";

                message += $"Is New:{isNew}.";

                if (dbCommandType != DbCommandType.None)
                    message += $"dbCommandType:{dbCommandType}.";

                throw GetSyncError(context, ex, message);
            }
        }

        ///// <summary>
        ///// Set common parameters to SelectChanges Sql command
        ///// </summary>
        //internal async Task InternalSetSelectChangesCommonParametersAsync(SyncContext context, SyncTable syncTable, Guid? excludingScopeId, bool isNew, long? lastTimestamp,
        //    DbCommandType commandType, DbCommand selectIncrementalChangesCommand, DbSyncAdapter adapter, DbConnection connection, DbTransaction transaction)
        //{
        //    try
        //    {
        //        // Set the parameters
        //        await adapter.AddCommandParameterValueAsync("sync_min_timestamp", lastTimestamp, commandType, selectIncrementalChangesCommand, connection, transaction).ConfigureAwait(false);
        //        await adapter.AddCommandParameterValueAsync("sync_scope_id", excludingScopeId.HasValue ? excludingScopeId.Value : DBNull.Value, commandType, selectIncrementalChangesCommand, connection, transaction).ConfigureAwait(false);

        //        // Check filters
        //        SyncFilter tableFilter = null;

        //        // Sqlite does not have any filter, since he can't be server side
        //        if (this.Provider != null && this.Provider.CanBeServerProvider)
        //            tableFilter = syncTable.GetFilter();

        //        var hasFilters = tableFilter != null;

        //        if (!hasFilters)
        //            return;

        //        // context parameters can be null at some point.
        //        var contexParameters = context.Parameters ?? new SyncParameters();

        //        foreach (var filterParam in tableFilter.Parameters)
        //        {
        //            var parameter = contexParameters.FirstOrDefault(p =>
        //                p.Name.Equals(filterParam.Name, SyncGlobalization.DataSourceStringComparison));

        //            object val = parameter?.Value;

        //            await adapter.AddCommandParameterValueAsync(filterParam.Name, val, commandType, selectIncrementalChangesCommand, connection, transaction).ConfigureAwait(false);
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        string message = null;

        //        if (syncTable != null)
        //            message += $"Table:{syncTable.GetFullName()}.";

        //        message += $"Is New:{isNew}.";
        //        message += $"lastTimestamp:{lastTimestamp}.";

        //        throw GetSyncError(context, ex, message);
        //    }
        //}

        /// <summary>
        /// Create a new SyncRow from a dataReader.
        /// </summary>
        internal SyncRow CreateSyncRowFromReader(SyncContext context, IDataReader dataReader, SyncTable schemaTable)
        {
            // Create a new row, based on table structure
            SyncRow syncRow = null;
            try
            {
                syncRow = new SyncRow(schemaTable);

                bool isTombstone = false;

                for (var i = 0; i < dataReader.FieldCount; i++)
                {
                    var columnName = dataReader.GetName(i);

                    // if we have the tombstone value, do not add it to the table
                    if (columnName == "sync_row_is_tombstone")
                    {
                        var objIsTombstone = dataReader.GetValue(i);
                        isTombstone = objIsTombstone == DBNull.Value ? false : Convert.ToInt64(objIsTombstone) > 0;
                        continue;
                    }
                    if (columnName == "sync_update_scope_id")
                        continue;

                    var columnValueObject = dataReader.GetValue(i);
                    var columnValue = columnValueObject == DBNull.Value ? null : columnValueObject;

                    syncRow[i] = columnValue;
                }

                syncRow.RowState = isTombstone ? SyncRowState.Deleted : SyncRowState.Modified;
                return syncRow;
            }
            catch (Exception ex)
            {
                string message = null;

                if (schemaTable != null)
                    message += $"Table:{schemaTable.GetFullName()}.";

                if (syncRow != null)
                    message += $"Row:{syncRow}.";

                throw GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Ensure we have a correct order for last batch in batch part infos
        /// </summary>
        /// <returns></returns>
        internal void EnsureLastBatchInfo(ScopeInfo scopeInfo, SyncContext context, BatchInfo batchInfo, IEnumerable<BatchPartInfo> lstAllBatchPartInfos, IEnumerable<SyncTable> schemaTables)
        {
            try
            {
                // delete all empty batchparts (empty tables)
                foreach (var bpi in lstAllBatchPartInfos.Where(bpi => bpi.RowsCount <= 0))
                    File.Delete(Path.Combine(batchInfo.GetDirectoryFullPath(), bpi.FileName));

                // Generate a good index order to be compliant with previous versions
                var tmpLstBatchPartInfos = new List<BatchPartInfo>();
                foreach (var table in schemaTables)
                {
                    // get all bpi where count > 0 and ordered by index
                    foreach (var bpi in lstAllBatchPartInfos.Where(bpi => bpi.RowsCount > 0 && bpi.EqualsByName(new BatchPartInfo { TableName = table.TableName, SchemaName = table.SchemaName })).OrderBy(bpi => bpi.Index).ToArray())
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
            }
            catch (Exception ex)
            {
                string message = null;

                if (batchInfo != null && batchInfo.DirectoryRoot != null)
                    message += $"Directory:{batchInfo.DirectoryRoot}.";

                if (batchInfo != null && batchInfo.DirectoryName != null)
                    message += $"Folder:{batchInfo.DirectoryName}.";

                throw GetSyncError(context, ex, message);
            }
        }
    }
}
