﻿using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains the logic to get changes.
    /// </summary>
    public abstract partial class BaseOrchestrator
    {

        /// <summary>
        /// Gets a batch of changes to synchronize when given batch size,
        /// destination knowledge, and change data retriever parameters.
        /// </summary>
        /// <returns>A DbSyncContext object that will be used to retrieve the modified data.</returns>
        internal virtual async Task<DatabaseChangesSelected> InternalGetChangesAsync(
                             ScopeInfo scopeInfo, SyncContext context, bool isNew, long? fromLastTimestamp, Guid? excludingScopeId,
                             bool supportsMultiActiveResultSets, BatchInfo batchInfo,
                             DbConnection connection, DbTransaction transaction,
                             IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
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
                    await schemaTables.ForEachAsync(
                        async syncTable =>
                    {
                        if (cancellationToken.IsCancellationRequested)
                            return;

                        // tmp count of table for report progress pct
                        cptSyncTable++;

                        List<BatchPartInfo> syncTableBatchPartInfos;
                        TableChangesSelected tableChangesSelected;
                        (context, syncTableBatchPartInfos, tableChangesSelected) = await this.InternalReadSyncTableChangesAsync(
                                scopeInfo, context, excludingScopeId, syncTable, batchInfo, isNew, fromLastTimestamp, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                        if (syncTableBatchPartInfos == null)
                            return;

                        // We don't report progress if no table changes is empty, to limit verbosity
                        if (tableChangesSelected != null && (tableChangesSelected.Deletes > 0 || tableChangesSelected.Upserts > 0))
                            lstTableChangesSelected.Add(tableChangesSelected);

                        // Add sync table bpi to all bpi
                        syncTableBatchPartInfos.ForEach(bpi => lstAllBatchPartInfos.Add(bpi));

                        context.ProgressPercentage = currentProgress + (cptSyncTable * 0.2d / scopeInfo.Schema.Tables.Count);
                    }, threadNumberLimits).ConfigureAwait(false);
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
                        (context, syncTableBatchPartInfos, tableChangesSelected) = await this.InternalReadSyncTableChangesAsync(
                                scopeInfo, context, excludingScopeId, syncTable, batchInfo, isNew, fromLastTimestamp, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

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
                {
                    if (lstTableChangesSelected.TryTake(out var tableChangesSelected))
                        changesSelected.TableChangesSelected.Add(tableChangesSelected);
                }

                // Ensure correct order
                this.EnsureLastBatchInfo(scopeInfo, context, batchInfo, lstAllBatchPartInfos, schemaTables);

                if (batchInfo.RowsCount <= 0)
                {
                    var cleanFolder = await this.InternalCanCleanFolderAsync(scopeInfo.Name, context.Parameters, batchInfo, cancellationToken: cancellationToken).ConfigureAwait(false);

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
                message += $"From:{fromLastTimestamp}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Read changes from a sync table.
        /// </summary>
        internal virtual async Task<(SyncContext Context, List<BatchPartInfo> BatchPartInfos, TableChangesSelected TableChangesSelected)>
            InternalReadSyncTableChangesAsync(
            ScopeInfo scopeInfo, SyncContext context, Guid? excludintScopeId, SyncTable syncTable,
            BatchInfo batchInfo, bool isNew, long? lastTimestamp,
            DbConnection connection, DbTransaction transaction,
            IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
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
                        connection, transaction).ConfigureAwait(false);

                if (selectIncrementalChangesCommand == null)
                    return (context, default, default);

                // Get correct adapter
                var syncAdapter = this.GetSyncAdapter(syncTable, scopeInfo);

                this.InternalSetCommandParametersValues(context, selectIncrementalChangesCommand, dbCommandType, syncAdapter, connection, transaction,
                    sync_scope_id: excludintScopeId, sync_min_timestamp: lastTimestamp, progress: progress, cancellationToken: cancellationToken);

                var schemaChangesTable = CreateChangesTable(syncTable);

                // Statistics
                var tableChangesSelected = new TableChangesSelected(schemaChangesTable.TableName, schemaChangesTable.SchemaName);

                // var rowsCountInBatchModified = 0;
                // var rowsCountInBatchDeleted = 0;
                var batchPartInfos = new List<BatchPartInfo>();

                BatchPartInfo batchPartInfoUpserts = null;
                BatchPartInfo batchPartInfoDeleted = null;

                // launch interceptor if any
                var args = await this.InterceptAsync(new TableChangesSelectingArgs(context, schemaChangesTable, selectIncrementalChangesCommand, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                if (!args.Cancel && args.Command != null)
                {
                    await this.InterceptAsync(new ExecuteCommandArgs(context, args.Command, dbCommandType, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                    // Get the reader
                    using var dataReader = await args.Command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

                    while (await dataReader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        // Create a row from dataReader
                        var syncRow = this.CreateSyncRowFromReader(context, dataReader, schemaChangesTable);

                        var tableChangesSelectedSyncRowArgs = await this.InterceptAsync(new RowsChangesSelectedArgs(context, syncRow, schemaChangesTable, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
                        syncRow = tableChangesSelectedSyncRowArgs.SyncRow;

                        if (syncRow == null)
                            continue;

                        if (syncRow.RowState == SyncRowState.Deleted)
                            batchPartInfoDeleted = await this.InternalAddRowToBatchPartInfoAsync(context, localSerializerDeleted, syncRow, batchInfo, batchPartInfoDeleted, batchPartInfos, schemaChangesTable, tableChangesSelected, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                        else
                            batchPartInfoUpserts = await this.InternalAddRowToBatchPartInfoAsync(context, localSerializerModified, syncRow, batchInfo, batchPartInfoUpserts, batchPartInfos, schemaChangesTable, tableChangesSelected, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                    }

#if NET6_0_OR_GREATER
                    await dataReader.CloseAsync().ConfigureAwait(false);
#else
                    dataReader.Close();
#endif

                    // tmp Func
                    var closeSerializer = new Func<LocalJsonSerializer, BatchChangesCreatedArgs, Task>(async (localJsonSerializer, args) =>
                    {
                        // Close file
                        if (localJsonSerializer != null && localJsonSerializer.IsOpen)
                        {
                            await localJsonSerializer.CloseFileAsync().ConfigureAwait(false);
                            await this.InterceptAsync(args, progress, cancellationToken).ConfigureAwait(false);
                        }
                    });

                    if (batchPartInfoUpserts != null || batchPartInfoDeleted != null)
                    {
                        if (batchPartInfoUpserts?.Index > batchPartInfoDeleted?.Index)
                        {
                            await closeSerializer(localSerializerDeleted, new BatchChangesCreatedArgs(context, batchPartInfoDeleted, schemaChangesTable, tableChangesSelected, SyncRowState.Deleted, connection, transaction)).ConfigureAwait(false);
                            await closeSerializer(localSerializerModified, new BatchChangesCreatedArgs(context, batchPartInfoUpserts, schemaChangesTable, tableChangesSelected, SyncRowState.Modified, connection, transaction)).ConfigureAwait(false);
                        }
                        else
                        {
                            await closeSerializer(localSerializerModified, new BatchChangesCreatedArgs(context, batchPartInfoUpserts, schemaChangesTable, tableChangesSelected, SyncRowState.Modified, connection, transaction)).ConfigureAwait(false);
                            await closeSerializer(localSerializerDeleted, new BatchChangesCreatedArgs(context, batchPartInfoDeleted, schemaChangesTable, tableChangesSelected, SyncRowState.Deleted, connection, transaction)).ConfigureAwait(false);
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
                    var fullPath = batchInfo.GetBatchPartInfoFullPath(bpi);

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

                throw this.GetSyncError(context, ex, message);
            }
            finally
            {
                await localSerializerModified.DisposeAsync().ConfigureAwait(false);
                await localSerializerDeleted.DisposeAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Add a row to a batch part info.
        /// </summary>
        internal async Task<BatchPartInfo> InternalAddRowToBatchPartInfoAsync(SyncContext context, LocalJsonSerializer localJsonSerializer, SyncRow syncRow, BatchInfo batchInfo,
            BatchPartInfo batchPartInfo, List<BatchPartInfo> batchPartInfos, SyncTable schemaChangesTable, TableChangesSelected tableChangesSelected,
            DbConnection connection, DbTransaction transaction,
            IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            // open the file and write table header for all deleted rows
            var ext = syncRow.RowState == SyncRowState.Deleted ? "DELETED" : "UPSERTS";
            var index = batchPartInfos != null ? batchPartInfos.Count : 0;

            if (!localJsonSerializer.IsOpen)
            {
                var (batchPartInfoFullPath, batchPartFileName) = batchInfo.GetNewBatchPartInfoPath(schemaChangesTable, index, LocalJsonSerializer.Extension, ext);
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
        /// Gets changes rows count estimation.
        /// </summary>
        internal virtual async Task<(SyncContext Context, DatabaseChangesSelected DatabaseChangesSelected)> InternalGetEstimatedChangesCountAsync(
                             ScopeInfo scopeInfo, SyncContext context, bool isNew, long? fromLastTimestamp, Guid? excludingScopeId,
                             bool supportsMultiActiveResultSets,
                             DbConnection connection, DbTransaction transaction,
                             IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {

            try
            {
                context.SyncStage = SyncStage.ChangesSelecting;

                // Create stats object to store changes count
                var changes = new DatabaseChangesSelected();

                // Call interceptor
                var databaseChangesSelectingArgs = new DatabaseChangesSelectingArgs(context, default, this.Options.BatchSize, true,
                    fromLastTimestamp, connection, transaction);

                await this.InterceptAsync(databaseChangesSelectingArgs, progress, cancellationToken).ConfigureAwait(false);

                if (context.SyncWay == SyncWay.Upload && context.SyncType == SyncType.Reinitialize)
                    return (context, changes);

                var threadNumberLimits = supportsMultiActiveResultSets ? 8 : 1;

                await scopeInfo.Schema.Tables.ForEachAsync(
                    async syncTable =>
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
                    var syncAdapter = this.GetSyncAdapter(syncTable, scopeInfo);

                    // Get Command
                    var (command, dbCommandType) = await this.InternalGetSelectChangesCommandAsync(scopeInfo, context, syncTable, isNew, connection, transaction).ConfigureAwait(false);

                    if (command == null)
                        return;

                    this.InternalSetCommandParametersValues(context, command, dbCommandType, syncAdapter, connection, transaction,
                        sync_scope_id: excludingScopeId, sync_min_timestamp: fromLastTimestamp, progress: progress, cancellationToken: cancellationToken);

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

                    while (await dataReader.ReadAsync().ConfigureAwait(false))
                    {
                        var isTombstone = false;
                        for (var i = 0; i < dataReader.FieldCount; i++)
                        {
                            var columnName = dataReader.GetName(i);

                            // if we have the tombstone value, do not add it to the table
                            if (columnName == "sync_row_is_tombstone")
                            {
                                var objIsTombstone = dataReader.GetValue(i);
                                isTombstone = objIsTombstone != DBNull.Value && SyncTypeConverter.TryConvertTo<long>(objIsTombstone) > 0;
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

#if NET6_0_OR_GREATER
                    await dataReader.CloseAsync().ConfigureAwait(false);
#else
                    dataReader.Close();
#endif

                    // Check interceptor
                    var changesArgs = new TableChangesSelectedArgs(context, null, null, syncTable, tableChangesSelected, connection, transaction);
                    await this.InterceptAsync(changesArgs, progress, cancellationToken).ConfigureAwait(false);

                    if (tableChangesSelected.Deletes > 0 || tableChangesSelected.Upserts > 0)
                        changes.TableChangesSelected.Add(tableChangesSelected);
                }, threadNumberLimits).ConfigureAwait(false);

                var databaseChangesSelectedArgs = new DatabaseChangesSelectedArgs(context, fromLastTimestamp,
                            default, changes, connection, transaction);

                await this.InterceptAsync(databaseChangesSelectedArgs, progress, cancellationToken).ConfigureAwait(false);

                return (context, changes);
            }
            catch (Exception ex)
            {
                string message = null;

                message += $"Supports MultiActiveResultSets:{supportsMultiActiveResultSets}.";
                message += $"Is New:{isNew}.";
                message += $"From:{fromLastTimestamp}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Get the correct Select changes command
        /// Can be either
        /// - SelectInitializedChanges              : All changes for first sync
        /// - SelectChanges                         : All changes filtered by timestamp
        /// - SelectInitializedChangesWithFilters   : All changes for first sync with filters
        /// - SelectChangesWithFilters              : All changes filtered by timestamp with filters.
        /// </summary>
        internal async Task<(DbCommand Command, DbCommandType CommandType)> InternalGetSelectChangesCommandAsync(ScopeInfo scopeInfo, SyncContext context,
            SyncTable syncTable, bool isNew, DbConnection connection, DbTransaction transaction)
        {
            var dbCommandType = DbCommandType.None;
            SyncFilter tableFilter = null;

            try
            {
                // Sqlite does not have any filter, since he can't be server side
                // if (this.Provider != null && this.Provider.CanBeServerProvider)
                if (this.Provider != null)
                    tableFilter = syncTable.GetFilter();

                var hasFilters = tableFilter != null;

                // Determing the correct DbCommandType
                if (isNew && hasFilters)
                    dbCommandType = DbCommandType.SelectInitializedChangesWithFilters;
                else if (isNew && !hasFilters)
                    dbCommandType = DbCommandType.SelectInitializedChanges;
                else dbCommandType = !isNew && hasFilters ? DbCommandType.SelectChangesWithFilters : DbCommandType.SelectChanges;

                // Get correct Select incremental changes command
                var syncAdapter = this.GetSyncAdapter(syncTable, scopeInfo);

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

                throw this.GetSyncError(context, ex, message);
            }
        }

        ///// <summary>
        ///// Set common parameters to SelectChanges Sql command
        ///// </summary>
        // internal async Task InternalSetSelectChangesCommonParametersAsync(SyncContext context, SyncTable syncTable, Guid? excludingScopeId, bool isNew, long? lastTimestamp,
        //    DbCommandType commandType, DbCommand selectIncrementalChangesCommand, DbSyncAdapter adapter, DbConnection connection, DbTransaction transaction)
        // {
        //    try
        //    {
        //        // Set the parameters
        //        await adapter.AddCommandParameterValueAsync("sync_min_timestamp", lastTimestamp, commandType, selectIncrementalChangesCommand, connection, transaction).ConfigureAwait(false);
        //        await adapter.AddCommandParameterValueAsync("sync_scope_id", excludingScopeId.HasValue ? excludingScopeId.Value : DBNull.Value, commandType, selectIncrementalChangesCommand, connection, transaction).ConfigureAwait(false);

        // // Check filters
        //        SyncFilter tableFilter = null;

        // // Sqlite does not have any filter, since he can't be server side
        //        if (this.Provider != null && this.Provider.CanBeServerProvider)
        //            tableFilter = syncTable.GetFilter();

        // var hasFilters = tableFilter != null;

        // if (!hasFilters)
        //            return;

        // // context parameters can be null at some point.
        //        var contexParameters = context.Parameters ?? new SyncParameters();

        // foreach (var filterParam in tableFilter.Parameters)
        //        {
        //            var parameter = contexParameters.FirstOrDefault(p =>
        //                p.Name.Equals(filterParam.Name, SyncGlobalization.DataSourceStringComparison));

        // object val = parameter?.Value;

        // await adapter.AddCommandParameterValueAsync(filterParam.Name, val, commandType, selectIncrementalChangesCommand, connection, transaction).ConfigureAwait(false);
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        string message = null;

        // if (syncTable != null)
        //            message += $"Table:{syncTable.GetFullName()}.";

        // message += $"Is New:{isNew}.";
        //        message += $"lastTimestamp:{lastTimestamp}.";

        // throw GetSyncError(context, ex, message);
        //    }
        // }

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

                var isTombstone = false;

                for (var i = 0; i < dataReader.FieldCount; i++)
                {
                    var columnName = dataReader.GetName(i);

                    // if we have the tombstone value, do not add it to the table
                    if (columnName == "sync_row_is_tombstone")
                    {
                        var objIsTombstone = dataReader.GetValue(i);
                        isTombstone = objIsTombstone != DBNull.Value && SyncTypeConverter.TryConvertTo<long>(objIsTombstone) > 0;
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

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Ensure we have a correct order for last batch in batch part infos.
        /// </summary>
        internal void EnsureLastBatchInfo(ScopeInfo scopeInfo, SyncContext context, BatchInfo batchInfo, IEnumerable<BatchPartInfo> lstAllBatchPartInfos, IEnumerable<SyncTable> schemaTables)
        {
            try
            {
                if (lstAllBatchPartInfos == null)
                    return;

                var batchPartInfos = lstAllBatchPartInfos.ToList();

                // delete all empty batchparts (empty tables)
                foreach (var bpi in batchPartInfos.Where(bpi => bpi.RowsCount <= 0))
                    File.Delete(Path.Combine(batchInfo.GetDirectoryFullPath(), bpi.FileName));

                // Generate a good index order to be compliant with previous versions
                var tmpLstBatchPartInfos = new List<BatchPartInfo>();
                foreach (var table in schemaTables)
                {
                    // get all bpi where count > 0 and ordered by index
                    foreach (var bpi in batchPartInfos.Where(bpi => bpi.RowsCount > 0 && bpi.EqualsByName(new BatchPartInfo { TableName = table.TableName, SchemaName = table.SchemaName })).OrderBy(bpi => bpi.Index).ToArray())
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

                // Set the total rows count contained in the batch info
                batchInfo.EnsureLastBatch();
            }
            catch (Exception ex)
            {
                string message = null;

                if (batchInfo != null && batchInfo.DirectoryRoot != null)
                    message += $"Directory:{batchInfo.DirectoryRoot}.";

                if (batchInfo != null && batchInfo.DirectoryName != null)
                    message += $"Folder:{batchInfo.DirectoryName}.";

                throw this.GetSyncError(context, ex, message);
            }
        }
    }
}