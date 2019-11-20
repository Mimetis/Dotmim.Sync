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
                    (batchInfo, changesSelected) = this.GetEmptyChanges(message.BatchSize, message.BatchDirectory);
                else if (message.BatchSize == 0)
                    (batchInfo, changesSelected) = await this.EnumerateChangesInternalAsync(context, message.ExcludingScopeId, message.IsNew, message.LastTimestamp,  message.Schema, message.Filters, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                else
                    (batchInfo, changesSelected) = await this.EnumerateChangesInBatchesInternalAsync(context, message.ExcludingScopeId, message.IsNew, message.LastTimestamp, message.BatchSize, message.Schema, message.BatchDirectory, message.Filters, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

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
        internal (BatchInfo, DatabaseChangesSelected) GetEmptyChanges(int downloadBatchSizeInKB, string batchDirectory)
        {
            // Get config
            var isBatched = downloadBatchSizeInKB > 0;

            // create the in memory changes set
            var changesSet = new DmSet(SyncSchema.DMSET_NAME);

            // Create the batch info, in memory
            var batchInfo = new BatchInfo(!isBatched, batchDirectory);

            // add changes to batchInfo
            batchInfo.AddChanges(changesSet);

            // Create a new in-memory batch info with an the changes DmSet
            return (batchInfo, new DatabaseChangesSelected());

        }

        /// <summary>
        /// Enumerate all internal changes, no batch mode
        /// </summary>
        internal async Task<(BatchInfo, DatabaseChangesSelected)> EnumerateChangesInternalAsync(
            SyncContext context, Guid excludingScopeId, bool isNew, long lastTimestamp, DmSet configTables, ICollection<FilterClause> filters,
            DbConnection connection, DbTransaction transaction,
            CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            // create the in memory changes set
            var changesSet = new DmSet(SyncSchema.DMSET_NAME);

            // Create the batch info, in memory
            // No need to geneate a directory name, since we are in memory
            var batchInfo = new BatchInfo(true);

            try
            {
                // changes that will be returned as selected changes
                var changes = new DatabaseChangesSelected();

                foreach (var tableDescription in configTables.Tables)
                {
                    // if we are in upload stage, so check if table is not download only
                    if (context.SyncWay == SyncWay.Upload && tableDescription.SyncDirection == SyncDirection.DownloadOnly)
                        continue;

                    // if we are in download stage, so check if table is not download only
                    if (context.SyncWay == SyncWay.Download && tableDescription.SyncDirection == SyncDirection.UploadOnly)
                        continue;

                    var builder = this.GetDatabaseBuilder(tableDescription);
                    var syncAdapter = builder.CreateSyncAdapter(connection, transaction);

                    // raise before event
                    context.SyncStage = SyncStage.TableChangesSelecting;
                    // launch any interceptor
                    await this.InterceptAsync(new TableChangesSelectingArgs(context, tableDescription.TableName, connection, transaction)).ConfigureAwait(false);

                    // selected changes for the current table
                    var tableSelectedChanges = new TableChangesSelected
                    {
                        TableName = tableDescription.TableName
                    };

                    // Get Command
                    DbCommand selectIncrementalChangesCommand;
                    DbCommandType dbCommandType;

                    if (this.CanBeServerProvider && context.Parameters != null && context.Parameters.Count > 0 && filters != null && filters.Count > 0)
                    {
                        var tableFilters = filters
                                        .Where(f => f.TableName.Equals(tableDescription.TableName, StringComparison.InvariantCultureIgnoreCase));

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



                    // Get a clone of the table with tracking columns
                    var dmTableChanges = this.BuildChangesTable(tableDescription.TableName, configTables);

                    SetSelectChangesCommonParameters(context, excludingScopeId, isNew, lastTimestamp, selectIncrementalChangesCommand);

                    // Set filter parameters if any
                    if (this.CanBeServerProvider && context.Parameters != null && context.Parameters.Count > 0 && filters != null && filters.Count > 0)
                    {
                        var tableFilters = filters
                            .Where(f => f.TableName.Equals(tableDescription.TableName, StringComparison.InvariantCultureIgnoreCase)).ToList();

                        if (tableFilters != null && tableFilters.Count > 0)
                        {
                            foreach (var filter in tableFilters)
                            {
                                var parameter = context.Parameters.FirstOrDefault(p => p.ColumnName.Equals(filter.ColumnName, StringComparison.InvariantCultureIgnoreCase) && p.TableName.Equals(filter.TableName, StringComparison.InvariantCultureIgnoreCase));

                                if (parameter != null)
                                    DbManager.SetParameterValue(selectIncrementalChangesCommand, parameter.ColumnName, parameter.Value);
                            }
                        }
                    }

                    this.AddTrackingColumns<int>(dmTableChanges, "sync_row_is_tombstone");

                    // Get the reader
                    using (var dataReader = selectIncrementalChangesCommand.ExecuteReader())
                    {
                        while (dataReader.Read())
                        {
                            var dataRow = this.CreateRowFromReader(dataReader, dmTableChanges);

                            //DmRow dataRow = dmTableChanges.NewRow();

                            // assuming the row is not inserted / modified
                            var state = DmRowState.Unchanged;

                            // get if the current row is inserted, modified, deleted
                            state = this.GetStateFromDmRow(dataRow, excludingScopeId, isNew, lastTimestamp);

                            if (state != DmRowState.Deleted && state != DmRowState.Modified && state != DmRowState.Added)
                                continue;

                            // add row
                            dmTableChanges.Rows.Add(dataRow);

                            // acceptchanges before modifying 
                            dataRow.AcceptChanges();
                            tableSelectedChanges.TotalChanges++;

                            // Set the correct state to be applied
                            if (state == DmRowState.Deleted)
                            {
                                dataRow.Delete();
                                tableSelectedChanges.Deletes++;
                            }
                            else if (state == DmRowState.Added)
                            {
                                dataRow.SetAdded();
                                tableSelectedChanges.Inserts++;
                            }
                            else if (state == DmRowState.Modified)
                            {
                                dataRow.SetModified();
                                tableSelectedChanges.Updates++;
                            }
                        }

                        // Since we dont need this column anymore, remove it
                        this.RemoveTrackingColumns(dmTableChanges, "sync_row_is_tombstone");

                        // add it to the DmSet
                        changesSet.Tables.Add(dmTableChanges);

                    }

                    selectIncrementalChangesCommand.Dispose();
                    // add the stats to global stats
                    changes.TableChangesSelected.Add(tableSelectedChanges);

                    // Progress & Interceptor
                    context.SyncStage = SyncStage.TableChangesSelected;
                    var args = new TableChangesSelectedArgs(context, tableSelectedChanges, connection, transaction);
                    this.ReportProgress(context, progress, args);
                    await this.InterceptAsync(args).ConfigureAwait(false);
                }

                // add changes to batchinfo
                batchInfo.AddChanges(changesSet);

                // Create a new in-memory batch info with an the changes DmSet
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
             SyncContext context, Guid excludingScopeId, bool isNew, long lastTimestamp, int downloadBatchSizeInKB, DmSet configTables, string batchDirectory,  ICollection<FilterClause> filters,
             DbConnection connection, DbTransaction transaction,
             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            DmTable dmTable = null;
            // memory size total
            double memorySizeFromDmRows = 0L;

            var batchIndex = 0;

            // this batch info won't be in memory, it will be be batched
            var batchInfo = new BatchInfo(false, batchDirectory);

            // Create stats object to store changes count
            var changes = new DatabaseChangesSelected();

            try
            {
                // create the in memory changes set
                var changesSet = new DmSet(configTables.DmSetName);

                foreach (var tableDescription in configTables.Tables)
                {
                    // if we are in upload stage, so check if table is not download only
                    if (context.SyncWay == SyncWay.Upload && tableDescription.SyncDirection == SyncDirection.DownloadOnly)
                        continue;

                    // if we are in download stage, so check if table is not download only
                    if (context.SyncWay == SyncWay.Download && tableDescription.SyncDirection == SyncDirection.UploadOnly)
                        continue;

                    var builder = this.GetDatabaseBuilder(tableDescription);
                    var syncAdapter = builder.CreateSyncAdapter(connection, transaction);

                    // raise before event
                    context.SyncStage = SyncStage.TableChangesSelecting;
                    var tableChangesSelectingArgs = new TableChangesSelectingArgs(context, tableDescription.TableName, connection, transaction);
                    // launc interceptor if any
                    await this.InterceptAsync(tableChangesSelectingArgs).ConfigureAwait(false);

                    // Get Command
                    DbCommand selectIncrementalChangesCommand;
                    DbCommandType dbCommandType;

                    if (this.CanBeServerProvider && context.Parameters != null && context.Parameters.Count > 0 && filters != null && filters.Count > 0)
                    {
                        var tableFilters = filters
                                        .Where(f => f.TableName.Equals(tableDescription.TableName, StringComparison.InvariantCultureIgnoreCase));

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



                    dmTable = this.BuildChangesTable(tableDescription.TableName, configTables);

                    try
                    {
                        // Set commons parameters
                        SetSelectChangesCommonParameters(context, excludingScopeId, isNew, lastTimestamp, selectIncrementalChangesCommand);

                        // Set filter parameters if any
                        // Only on server side
                        if (this.CanBeServerProvider && context.Parameters != null && context.Parameters.Count > 0 && filters != null && filters.Count > 0)
                        {
                            var filterTable = filters.Where(f => f.TableName.Equals(tableDescription.TableName, StringComparison.InvariantCultureIgnoreCase)).ToList();

                            if (filterTable != null && filterTable.Count > 0)
                            {
                                foreach (var filter in filterTable)
                                {
                                    var parameter = context.Parameters.FirstOrDefault(p => p.ColumnName.Equals(filter.ColumnName, StringComparison.InvariantCultureIgnoreCase) && p.TableName.Equals(filter.TableName, StringComparison.InvariantCultureIgnoreCase));

                                    if (parameter != null)
                                        DbManager.SetParameterValue(selectIncrementalChangesCommand, parameter.ColumnName, parameter.Value);
                                }
                            }
                        }

                        this.AddTrackingColumns<int>(dmTable, "sync_row_is_tombstone");

                        // Statistics
                        var tableChangesSelected = new TableChangesSelected
                        {
                            TableName = tableDescription.TableName
                        };

                        changes.TableChangesSelected.Add(tableChangesSelected);

                        // Get the reader
                        using (var dataReader = selectIncrementalChangesCommand.ExecuteReader())
                        {
                            while (dataReader.Read())
                            {
                                var dmRow = this.CreateRowFromReader(dataReader, dmTable);

                                var state = DmRowState.Unchanged;

                                state = this.GetStateFromDmRow(dmRow, excludingScopeId, isNew, lastTimestamp);

                                // If the row is not deleted inserted or modified, go next
                                if (state != DmRowState.Deleted && state != DmRowState.Modified && state != DmRowState.Added)
                                    continue;

                                var fieldsSize = DmTableSurrogate.GetRowSizeFromDataRow(dmRow);
                                var dmRowSize = fieldsSize / 1024d;

                                if (dmRowSize > downloadBatchSizeInKB)
                                {
                                    var exc = $"Row is too big ({dmRowSize} kb.) for the current Configuration.DownloadBatchSizeInKB ({downloadBatchSizeInKB} kb.) Aborting Sync...";
                                    throw new Exception(exc);
                                }

                                // Calculate the new memory size
                                memorySizeFromDmRows = memorySizeFromDmRows + dmRowSize;

                                // add row
                                dmTable.Rows.Add(dmRow);
                                tableChangesSelected.TotalChanges++;

                                // acceptchanges before modifying 
                                dmRow.AcceptChanges();

                                // Set the correct state to be applied
                                if (state == DmRowState.Deleted)
                                {
                                    dmRow.Delete();
                                    tableChangesSelected.Deletes++;
                                }
                                else if (state == DmRowState.Added)
                                {
                                    dmRow.SetAdded();
                                    tableChangesSelected.Inserts++;
                                }
                                else if (state == DmRowState.Modified)
                                {
                                    dmRow.SetModified();
                                    tableChangesSelected.Updates++;
                                }

                                // We exceed the memorySize, so we can add it to a batch
                                if (memorySizeFromDmRows > downloadBatchSizeInKB)
                                {
                                    // Since we dont need this column anymore, remove it
                                    this.RemoveTrackingColumns(dmTable, "sync_row_is_tombstone");

                                    changesSet.Tables.Add(dmTable);

                                    // add changes to batchinfo
                                    batchInfo.AddChanges(changesSet, batchIndex, false);

                                    // increment batch index
                                    batchIndex++;

                                    changesSet.Clear();

                                    // Recreate an empty DmSet, then a dmTable clone
                                    changesSet = new DmSet(configTables.DmSetName);
                                    dmTable = dmTable.Clone();
                                    this.AddTrackingColumns<int>(dmTable, "sync_row_is_tombstone");

                                    // Init the row memory size
                                    memorySizeFromDmRows = 0L;

                                    // SyncProgress & interceptor
                                    context.SyncStage = SyncStage.TableChangesSelected;
                                    var loopTableChangesSelectedArgs = new TableChangesSelectedArgs(context, tableChangesSelected, connection, transaction);
                                    this.ReportProgress(context, progress, loopTableChangesSelectedArgs);
                                    await this.InterceptAsync(loopTableChangesSelectedArgs).ConfigureAwait(false);
                                }
                            }

                            // Since we dont need this column anymore, remove it
                            this.RemoveTrackingColumns(dmTable, "sync_row_is_tombstone");

                            context.SyncStage = SyncStage.TableChangesSelected;

                            changesSet.Tables.Add(dmTable);

                            // Init the row memory size
                            memorySizeFromDmRows = 0L;

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
                if (changesSet != null && changesSet.HasTables && changesSet.HasChanges())
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
        /// Create a DmRow from a IDataReader
        /// </summary>
        private DmRow CreateRowFromReader(IDataReader dataReader, DmTable dmTable)
        {
            // we have an insert / update or delete
            var dataRow = dmTable.NewRow();

            for (var i = 0; i < dataReader.FieldCount; i++)
            {
                var columnName = dataReader.GetName(i);
                var dmRowObject = dataReader.GetValue(i);

                if (dmRowObject != DBNull.Value)
                {
                    if (dmRowObject != null)
                    {
                        var columnType = dmTable.Columns[columnName].DataType;
                        var dmRowObjectType = dmRowObject.GetType();

                        if (dmRowObjectType != columnType && columnType != typeof(object))
                        {
                            if (columnType == typeof(Guid) && (dmRowObject as string) != null)
                                dmRowObject = new Guid(dmRowObject.ToString());
                            else if (columnType == typeof(Guid) && dmRowObject.GetType() == typeof(byte[]))
                                dmRowObject = dataReader.GetGuid(i);
                            else if (columnType == typeof(int) && dmRowObjectType != typeof(int))
                                dmRowObject = Convert.ToInt32(dmRowObject);
                            else if (columnType == typeof(uint) && dmRowObjectType != typeof(uint))
                                dmRowObject = Convert.ToUInt32(dmRowObject);
                            else if (columnType == typeof(short) && dmRowObjectType != typeof(short))
                                dmRowObject = Convert.ToInt16(dmRowObject);
                            else if (columnType == typeof(ushort) && dmRowObjectType != typeof(ushort))
                                dmRowObject = Convert.ToUInt16(dmRowObject);
                            else if (columnType == typeof(long) && dmRowObjectType != typeof(long))
                                dmRowObject = Convert.ToInt64(dmRowObject);
                            else if (columnType == typeof(ulong) && dmRowObjectType != typeof(ulong))
                                dmRowObject = Convert.ToUInt64(dmRowObject);
                            else if (columnType == typeof(byte) && dmRowObjectType != typeof(byte))
                                dmRowObject = Convert.ToByte(dmRowObject);
                            else if (columnType == typeof(char) && dmRowObjectType != typeof(char))
                                dmRowObject = Convert.ToChar(dmRowObject);
                            else if (columnType == typeof(DateTime) && dmRowObjectType != typeof(DateTime))
                                dmRowObject = Convert.ToDateTime(dmRowObject);
                            else if (columnType == typeof(decimal) && dmRowObjectType != typeof(decimal))
                                dmRowObject = Convert.ToDecimal(dmRowObject);
                            else if (columnType == typeof(double) && dmRowObjectType != typeof(double))
                                dmRowObject = Convert.ToDouble(dmRowObject);
                            else if (columnType == typeof(sbyte) && dmRowObjectType != typeof(sbyte))
                                dmRowObject = Convert.ToSByte(dmRowObject);
                            else if (columnType == typeof(float) && dmRowObjectType != typeof(float))
                                dmRowObject = Convert.ToSingle(dmRowObject);
                            else if (columnType == typeof(string) && dmRowObjectType != typeof(string))
                                dmRowObject = Convert.ToString(dmRowObject);
                            else if (columnType == typeof(bool) && dmRowObjectType != typeof(bool))
                                dmRowObject = Convert.ToBoolean(dmRowObject);
                            else if (dmRowObjectType != columnType)
                            {
                                var t = dmRowObject.GetType();
                                var converter = columnType.GetConverter();
                                if (converter != null && converter.CanConvertFrom(t))
                                    dmRowObject = converter.ConvertFrom(dmRowObject);
                            }
                        }
                    }
                    dataRow[columnName] = dmRowObject;
                }
            }

            return dataRow;
        }

        private DmTable BuildChangesTable(string tableName, DmSet configTables)
        {
            var dmTable = configTables.Tables[tableName].Clone();

            // Adding the tracking columns
            this.AddTrackingColumns<Guid>(dmTable, "create_scope_id");
            this.AddTrackingColumns<long>(dmTable, "create_timestamp");
            this.AddTrackingColumns<Guid>(dmTable, "update_scope_id");
            this.AddTrackingColumns<long>(dmTable, "update_timestamp");

            // Since we can have some deleted rows, the Changes table should have only null columns (except PrimaryKeys)

            foreach (var c in dmTable.Columns)
            {
                var isPrimaryKey = dmTable.PrimaryKey.Columns.Any(cc => dmTable.IsEqual(cc.ColumnName, c.ColumnName));

                if (!isPrimaryKey)
                    c.AllowDBNull = true;
            }

            return dmTable;

        }


        /// <summary>
        /// Get a DmRow state to know is we have an inserted, updated, or deleted row to apply
        /// </summary>
        private DmRowState GetStateFromDmRow(DmRow dataRow, Guid excludingScopeId, bool isNew, long lastTimestamp)
        {
            var dmRowState = DmRowState.Unchanged;

            var isTombstone = Convert.ToInt64(dataRow["sync_row_is_tombstone"]) > 0;

            if (isTombstone)
                dmRowState = DmRowState.Deleted;
            else
            {
                var createdTimeStamp = DbManager.ParseTimestamp(dataRow["create_timestamp"]);
                var updatedTimeStamp = DbManager.ParseTimestamp(dataRow["update_timestamp"]);
                var updateScopeIdRow = dataRow["update_scope_id"];
                var createScopeIdRow = dataRow["create_scope_id"];

                var updateScopeId = (updateScopeIdRow != DBNull.Value && updateScopeIdRow != null) ? (Guid)updateScopeIdRow : (Guid?)null;
                var createScopeId = (createScopeIdRow != DBNull.Value && createScopeIdRow != null) ? (Guid)createScopeIdRow : (Guid?)null;

                var isLocallyCreated = !createScopeId.HasValue;
                var islocallyUpdated = !updateScopeId.HasValue || updateScopeId.Value != excludingScopeId;


                // Check if a row is modified :
                // 1) Row is not new
                // 2) Row update is AFTER last sync of asker
                // 3) Row insert is BEFORE last sync of asker (if insert is after last sync, it's not an update, it's an insert)
                if (!isNew && islocallyUpdated && updatedTimeStamp > lastTimestamp && (createdTimeStamp <= lastTimestamp || !isLocallyCreated))
                    dmRowState = DmRowState.Modified;
                else if (isNew || (isLocallyCreated && createdTimeStamp >= lastTimestamp))
                    dmRowState = DmRowState.Added;
                // The line has been updated from an other host
                else if (islocallyUpdated && updateScopeId.HasValue && updateScopeId.Value != excludingScopeId)
                    dmRowState = DmRowState.Modified;
                else
                {
                    dmRowState = DmRowState.Unchanged;
                    Debug.WriteLine($"Row is in Unchanegd state. " +
                        $"\tscopeInfo.Id:{excludingScopeId}, scopeInfo.IsNewScope :{isNew}, scopeInfo.LastTimestamp:{lastTimestamp}" +
                        $"\tcreateScopeId:{createScopeId}, updateScopeId:{updateScopeId}, createdTimeStamp:{createdTimeStamp}, updatedTimeStamp:{updatedTimeStamp}.");
                }
            }

            return dmRowState;
        }

    }
}
