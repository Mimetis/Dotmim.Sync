using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Filter;
using Dotmim.Sync.Manager;
using Dotmim.Sync.Messages;
using Dotmim.Sync.Serialization;
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
    public abstract partial class CoreProvider
    {
        /// <summary>
        /// Gets a batch of changes to synchronize when given batch size, 
        /// destination knowledge, and change data retriever parameters.
        /// </summary>
        /// <returns>A DbSyncContext object that will be used to retrieve the modified data.</returns>
        public virtual async Task<(SyncContext, BatchInfo, DatabaseChangesSelected)> GetChangeBatchAsync(
                             SyncContext context, MessageGetChangesBatch message,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                // Check if the provider is not outdated
                var isOutdated = this.IsRemoteOutdated();

                // Get a chance to make the sync even if it's outdated
                if (isOutdated)
                {
                    var outdatedArgs = new OutdatedArgs(context, null, null);

                    // Interceptor
                    await this.InterceptAsync(outdatedArgs).ConfigureAwait(false);

                    if (outdatedArgs.Action != OutdatedSyncAction.Rollback)
                        context.SyncType = outdatedArgs.Action == OutdatedSyncAction.Reinitialize ? SyncType.Reinitialize : SyncType.ReinitializeWithUpload;

                    if (outdatedArgs.Action == OutdatedSyncAction.Rollback)
                        throw new OutOfDateException("The provider is out of date ! Try to make a Reinitialize sync");
                }

                // create local directory
                if (message.BatchSize > 0 && !string.IsNullOrEmpty(message.BatchDirectory) && !Directory.Exists(message.BatchDirectory))
                    Directory.CreateDirectory(message.BatchDirectory);

                // batch info containing changes
                BatchInfo batchInfo;

                // Statistics about changes that are selected
                DatabaseChangesSelected changesSelected;

                // if we try a Reinitialize action, don't get any changes from client
                // else get changes from batch or in memory methods
                if (context.SyncWay == SyncWay.Upload && context.SyncType == SyncType.Reinitialize)
                    (batchInfo, changesSelected) = this.GetEmptyChanges(message);
                else if (message.BatchSize == 0)
                    (batchInfo, changesSelected) = await this.EnumerateChangesInternalAsync(context, message.ExcludingScopeId, message.IsNew, message.LastTimestamp, message.Schema, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                else
                    (batchInfo, changesSelected) = await this.EnumerateChangesInBatchesInternalAsync(context, message.ExcludingScopeId, message.IsNew, message.LastTimestamp, message.BatchSize, message.Schema, message.BatchDirectory, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                return (context, batchInfo, changesSelected);
            }
            catch (Exception ex)
            {
                throw new SyncException(ex, SyncStage.TableChangesSelecting);
            }
        }



        /// <summary>
        /// Generate an empty BatchInfo
        /// </summary>
        internal (BatchInfo, DatabaseChangesSelected) GetEmptyChanges(MessageGetChangesBatch message)
        {
            // Get config
            var isBatched = message.BatchSize > 0;

            // Create the batch info, in memory
            var batchInfo = new BatchInfo(!isBatched, message.Schema, message.BatchDirectory); ;

            // add changes to batchInfo
            batchInfo.AddChanges(new SyncSet());

            // Create a new empty in-memory batch info
            return (batchInfo, new DatabaseChangesSelected());

        }

        /// <summary>
        /// Enumerate all internal changes, no batch mode
        /// </summary>
        internal async Task<(BatchInfo, DatabaseChangesSelected)> EnumerateChangesInternalAsync(
            SyncContext context, Guid excludingScopeId, bool isNew, long lastTimestamp, SyncSet schema,
            DbConnection connection, DbTransaction transaction,
            CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            // create the in memory changes set
            var changesSet = new SyncSet
            {
                CaseSensitive = schema.CaseSensitive,
                CultureInfoName = schema.CultureInfoName,
                ScopeName = schema.ScopeName,
                DataSourceName = schema.DataSourceName
            };

            foreach (var table in schema.Tables)
            {
                DbSyncAdapter.CreateChangesTable(schema.Tables[table.TableName, table.SchemaName], changesSet);
            }


            // Create the batch info, in memory
            // No need to geneate a directory name, since we are in memory
            // Batch info clone the schema and adds scopes columns
            var batchInfo = new BatchInfo(true, changesSet);

            try
            {
                // changes that will be returned as selected changes
                var changes = new DatabaseChangesSelected();

                foreach (var syncTable in schema.Tables)
                {
                    // if we are in upload stage, so check if table is not download only
                    if (context.SyncWay == SyncWay.Upload && syncTable.SyncDirection == SyncDirection.DownloadOnly)
                        continue;

                    // if we are in download stage, so check if table is not download only
                    if (context.SyncWay == SyncWay.Download && syncTable.SyncDirection == SyncDirection.UploadOnly)
                        continue;

                    var builder = this.GetDatabaseBuilder(syncTable);
                    var syncAdapter = builder.CreateSyncAdapter(connection, transaction);

                    // raise before event
                    context.SyncStage = SyncStage.TableChangesSelecting;
                    // launch any interceptor
                    await this.InterceptAsync(new TableChangesSelectingArgs(context, syncTable.TableName, connection, transaction)).ConfigureAwait(false);

                    // selected changes for the current table
                    var tableSelectedChanges = new TableChangesSelected(syncTable.TableName);

                    // Get Command
                    DbCommand selectIncrementalChangesCommand;
                    DbCommandType dbCommandType;

                    if (this.CanBeServerProvider && context.Parameters != null
                        && context.Parameters.Count > 0
                        && schema.Filters != null && schema.Filters.Count > 0)
                    {
                        var tableFilters = schema.Filters.Where(syncTable);

                        if (tableFilters != null && tableFilters.Count() > 0)
                        {
                            dbCommandType = DbCommandType.SelectChangesWitFilters;
                            selectIncrementalChangesCommand = syncAdapter.GetCommand(dbCommandType, tableFilters);
                            if (selectIncrementalChangesCommand == null)
                                throw new Exception("Missing command 'SelectIncrementalChangesCommand'");
                            syncAdapter.SetCommandParameters(dbCommandType, selectIncrementalChangesCommand, tableFilters);
                        }
                        else
                        {
                            dbCommandType = DbCommandType.SelectChanges;
                            selectIncrementalChangesCommand = syncAdapter.GetCommand(dbCommandType);
                            if (selectIncrementalChangesCommand == null)
                                throw new Exception("Missing command 'SelectIncrementalChangesCommand'");
                            syncAdapter.SetCommandParameters(dbCommandType, selectIncrementalChangesCommand);
                        }
                    }
                    else
                    {
                        dbCommandType = DbCommandType.SelectChanges;
                        selectIncrementalChangesCommand = syncAdapter.GetCommand(dbCommandType);
                        if (selectIncrementalChangesCommand == null)
                            throw new Exception("Missing command 'SelectIncrementalChangesCommand'");
                        syncAdapter.SetCommandParameters(dbCommandType, selectIncrementalChangesCommand);
                    }

                    SetSelectChangesCommonParameters(context, excludingScopeId, isNew, lastTimestamp, selectIncrementalChangesCommand);

                    // Set filter parameters if any
                    if (this.CanBeServerProvider && context.Parameters != null && context.Parameters.Count > 0
                        && schema.Filters != null && schema.Filters.Count > 0)
                    {
                        var tableFilters = schema.Filters.Where(syncTable);

                        foreach (var filter in tableFilters)
                        {
                            var parameter = context.Parameters.FirstOrDefault(p => p.ColumnName.Equals(filter.ColumnName, StringComparison.InvariantCultureIgnoreCase) && p.TableName.Equals(filter.TableName, StringComparison.InvariantCultureIgnoreCase));

                            if (parameter != null)
                                DbManager.SetParameterValue(selectIncrementalChangesCommand, parameter.ColumnName, parameter.Value);
                        }
                    }

                    // Create a changes table with Sync scopes columns
                    var changesTable = changesSet.Tables[syncTable.TableName, syncTable.SchemaName];

                    // Get the reader
                    using (var dataReader = selectIncrementalChangesCommand.ExecuteReader())
                    {
                        // Create the container that will contains all the rows for this table

                        while (dataReader.Read())
                        {
                            var row = CreateSyncRowFromReader(dataReader, changesTable, excludingScopeId, isNew, lastTimestamp);

                            if (row.RowState != DataRowState.Deleted && row.RowState != DataRowState.Modified)
                                continue;

                            tableSelectedChanges.TotalChanges++;

                            // Set the correct state to be applied
                            if (row.RowState == DataRowState.Deleted)
                                tableSelectedChanges.Deletes++;
                            else if (row.RowState == DataRowState.Modified)
                                tableSelectedChanges.Upserts++;

                            changesTable.Rows.Add(row);
                        }

                        dataReader.Close();
                    }


                    selectIncrementalChangesCommand.Dispose();
                    // add the stats to global stats
                    if (tableSelectedChanges.Deletes > 0 || tableSelectedChanges.Upserts > 0)
                        changes.TableChangesSelected.Add(tableSelectedChanges);

                    // Progress & Interceptor
                    context.SyncStage = SyncStage.TableChangesSelected;
                    var args = new TableChangesSelectedArgs(context, tableSelectedChanges, connection, transaction);
                    this.ReportProgress(context, progress, args);
                    await this.InterceptAsync(args).ConfigureAwait(false);
                }

                // add changes to batchinfo
                batchInfo.AddChanges(changesSet);

                // Create a new in-memory batch info with an the changes
                return (batchInfo, changes);

            }
            catch (Exception)
            {
                throw;
            }



        }

        /// <summary>
        /// Set common parameters to SelectChanges Sql command
        /// </summary>
        private static void SetSelectChangesCommonParameters(SyncContext context, Guid excludingScopeId, bool isNew, long lastTimestamp, DbCommand selectIncrementalChangesCommand)
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
            DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_min_timestamp", lastTimestamp);
            DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_scope_id", excludingScopeId);
            DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_scope_is_new", isNewScope);
            DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_scope_is_reinit", isReinit);
        }

        /// <summary>
        /// Enumerate all internal changes, no batch mode
        /// </summary>
        internal async Task<(BatchInfo, DatabaseChangesSelected)> EnumerateChangesInBatchesInternalAsync(
             SyncContext context, Guid excludingScopeId, bool isNew, long lastTimestamp, int downloadBatchSizeInKB,
             SyncSet schema, string batchDirectory,
             DbConnection connection, DbTransaction transaction,
             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            // memory size total
            double rowsMemorySize = 0L;

            // numbers of batch files generated
            var batchIndex = 0;

            // this batch info won't be in memory, it will be be batched
            // batchinfo generate a schema clone with scope columns
            var batchInfo = new BatchInfo(false, schema, batchDirectory);

            // Create stats object to store changes count
            var changes = new DatabaseChangesSelected();

            try
            {
                // create the in memory changes set
                var changesSet = new SyncSet
                {
                    CaseSensitive = schema.CaseSensitive,
                    CultureInfoName = schema.CultureInfoName,
                    ScopeName = schema.ScopeName,
                    DataSourceName = schema.DataSourceName
                };

                foreach (var syncTable in schema.Tables)
                {
                    // if we are in upload stage, so check if table is not download only
                    if (context.SyncWay == SyncWay.Upload && syncTable.SyncDirection == SyncDirection.DownloadOnly)
                        continue;

                    // if we are in download stage, so check if table is not download only
                    if (context.SyncWay == SyncWay.Download && syncTable.SyncDirection == SyncDirection.UploadOnly)
                        continue;

                    var builder = this.GetDatabaseBuilder(syncTable);
                    var syncAdapter = builder.CreateSyncAdapter(connection, transaction);

                    // get ordered columns that are mutables and pkeys
                    var orderedNames = syncTable.GetMutableColumnsWithPrimaryKeys().Select(c => c.ColumnName);

                    // raise before event
                    context.SyncStage = SyncStage.TableChangesSelecting;
                    var tableChangesSelectingArgs = new TableChangesSelectingArgs(context, syncTable.TableName, connection, transaction);
                    // launc interceptor if any
                    await this.InterceptAsync(tableChangesSelectingArgs).ConfigureAwait(false);

                    // Get Command
                    DbCommand selectIncrementalChangesCommand;
                    DbCommandType dbCommandType;

                    if (this.CanBeServerProvider && context.Parameters != null && context.Parameters.Count > 0 && schema.Filters.Count > 0)
                    {
                        var tableFilters = schema.Filters.Where(syncTable);

                        if (tableFilters != null && tableFilters.Count() > 0)
                        {
                            dbCommandType = DbCommandType.SelectChangesWitFilters;
                            selectIncrementalChangesCommand = syncAdapter.GetCommand(dbCommandType, tableFilters);
                            if (selectIncrementalChangesCommand == null)
                                throw new Exception("Missing command 'SelectIncrementalChangesCommand' ");
                            syncAdapter.SetCommandParameters(dbCommandType, selectIncrementalChangesCommand, tableFilters);
                        }
                        else
                        {
                            dbCommandType = DbCommandType.SelectChanges;
                            selectIncrementalChangesCommand = syncAdapter.GetCommand(dbCommandType);
                            if (selectIncrementalChangesCommand == null)
                                throw new Exception("Missing command 'SelectIncrementalChangesCommand' ");
                            syncAdapter.SetCommandParameters(dbCommandType, selectIncrementalChangesCommand);
                        }
                    }
                    else
                    {
                        dbCommandType = DbCommandType.SelectChanges;
                        selectIncrementalChangesCommand = syncAdapter.GetCommand(dbCommandType);
                        if (selectIncrementalChangesCommand == null)
                            throw new Exception("Missing command 'SelectIncrementalChangesCommand' ");
                        syncAdapter.SetCommandParameters(dbCommandType, selectIncrementalChangesCommand);
                    }

                    try
                    {
                        // Set commons parameters
                        SetSelectChangesCommonParameters(context, excludingScopeId, isNew, lastTimestamp, selectIncrementalChangesCommand);

                        // Set filter parameters if any
                        // Only on server side
                        if (this.CanBeServerProvider && context.Parameters != null && context.Parameters.Count > 0 && schema.Filters.Count > 0)
                        {
                            var filterTable = schema.Filters.Where(syncTable);

                            foreach (var filter in filterTable)
                            {
                                var parameter = context.Parameters.FirstOrDefault(p => p.ColumnName.Equals(filter.ColumnName, StringComparison.InvariantCultureIgnoreCase) && p.TableName.Equals(filter.TableName, StringComparison.InvariantCultureIgnoreCase));

                                if (parameter != null)
                                    DbManager.SetParameterValue(selectIncrementalChangesCommand, parameter.ColumnName, parameter.Value);
                            }
                        }

                        // Statistics
                        var tableChangesSelected = new TableChangesSelected(syncTable.TableName);

                        // Get the reader
                        using (var dataReader = selectIncrementalChangesCommand.ExecuteReader())
                        {
                            // Create a chnages table with scope columns
                            var changesSetTable = DbSyncAdapter.CreateChangesTable(syncTable, changesSet);

                            while (dataReader.Read())
                            {
                                var row = CreateSyncRowFromReader(dataReader, changesSetTable, excludingScopeId, isNew, lastTimestamp);

                                if (row.RowState != DataRowState.Deleted && row.RowState != DataRowState.Modified)
                                    continue;

                                var fieldsSize = ContainerTable.GetRowSizeFromDataRow(row.ToArray());
                                var finalFieldSize = fieldsSize / 1024d;

                                if (finalFieldSize > downloadBatchSizeInKB)
                                {
                                    var exc = $"Row is too big ({finalFieldSize} kb.) for the current Configuration.DownloadBatchSizeInKB ({downloadBatchSizeInKB} kb.) Aborting Sync...";
                                    throw new Exception(exc);
                                }

                                // Calculate the new memory size
                                rowsMemorySize += finalFieldSize;

                                tableChangesSelected.TotalChanges++;

                                // Set the correct state to be applied
                                if (row.RowState == DataRowState.Deleted)
                                    tableChangesSelected.Deletes++;
                                else if (row.RowState == DataRowState.Modified)
                                    tableChangesSelected.Upserts++;

                                // Create an ordered item array
                                changesSetTable.Rows.Add(row);

                                // We exceed the memorySize, so we can add it to a batch
                                if (rowsMemorySize > downloadBatchSizeInKB)
                                {
                                    // add changes to batchinfo
                                    batchInfo.AddChanges(changesSet, batchIndex, false);

                                    // increment batch index
                                    batchIndex++;

                                    // we know the datas are serialized here, so we can flush  the set
                                    changesSet.Clear();

                                    // Recreate an empty ContainerSet and a ContainerTable
                                    changesSet = new SyncSet
                                    {
                                        CaseSensitive = schema.CaseSensitive,
                                        CultureInfoName = schema.CultureInfoName,
                                        ScopeName = schema.ScopeName,
                                        DataSourceName = schema.DataSourceName
                                    };

                                    changesSetTable = DbSyncAdapter.CreateChangesTable(syncTable, changesSet);

                                    // Init the row memory size
                                    rowsMemorySize = 0L;

                                }
                            }

                            context.SyncStage = SyncStage.TableChangesSelected;

                            // Init the row memory size
                            rowsMemorySize = 0L;

                            // close reader to be able to use connection in interceptor 
                            dataReader.Close();

                            if (tableChangesSelected.Deletes > 0 || tableChangesSelected.Upserts > 0)
                                changes.TableChangesSelected.Add(tableChangesSelected);

                            // Event progress & interceptor
                            context.SyncStage = SyncStage.TableChangesSelected;
                            var tableChangesSelectedArgs = new TableChangesSelectedArgs(context, tableChangesSelected, connection, transaction);
                            this.ReportProgress(context, progress, tableChangesSelectedArgs);
                            await this.InterceptAsync(tableChangesSelectedArgs).ConfigureAwait(false);
                        }
                    }
                    catch (Exception)
                    {
                        throw;
                    }
                    finally
                    {
                    }
                }

                // We are in batch mode, and we are at the last batchpart info
                if (changesSet != null && changesSet.HasTables)
                    batchInfo.AddChanges(changesSet, batchIndex, true);

            }
            catch (Exception)
            {
                throw;
            }

            // Check the last index as the last batch
            batchInfo.EnsureLastBatch();

            return (batchInfo, changes);

        }






        /// <summary>
        /// Read a row from a reader and set a dictionary with all column name / column value
        /// </summary>
        private SyncRow CreateSyncRowFromReader(IDataReader dataReader, SyncTable table, Guid excludingScopeId, bool isNew, long lastTimestamp)
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
