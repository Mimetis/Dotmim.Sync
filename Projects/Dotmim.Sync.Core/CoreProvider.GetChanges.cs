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

            // batch info containing changes
            BatchInfo batchInfo;

            // Statistics about changes that are selected
            DatabaseChangesSelected changesSelected;

            if (context.SyncWay == SyncWay.Upload && context.SyncType == SyncType.Reinitialize)
            {
                (batchInfo, changesSelected) = this.GetEmptyChanges(message);
                return (context, batchInfo, changesSelected);
            }

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
                    throw new OutOfDateException();
            }

            // create local directory
            if (message.BatchSize > 0 && !string.IsNullOrEmpty(message.BatchDirectory) && !Directory.Exists(message.BatchDirectory))
                Directory.CreateDirectory(message.BatchDirectory);

            // numbers of batch files generated
            var batchIndex = 0;

            // Check if we are in batch mode
            var isBatch = message.BatchSize > 0;

            // Create stats object to store changes count
            var changes = new DatabaseChangesSelected();

            // create the in memory changes set
            var changesSet = new SyncSet(message.Schema.ScopeName);

            // Create a Schema set without readonly tables, attached to memory changes
            foreach (var table in message.Schema.Tables)
                DbSyncAdapter.CreateChangesTable(message.Schema.Tables[table.TableName, table.SchemaName], changesSet);

            // Create a batch info in memory (if !isBatch) or serialized on disk (if isBatch)
            // batchinfo generate a schema clone with scope columns if needed
            batchInfo = new BatchInfo(!isBatch, changesSet, message.BatchDirectory);

            // Clear tables, we will add only the ones we need in the batch info
            changesSet.Clear();

            foreach (var syncTable in message.Schema.Tables)
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
                var tableChangesSelectingArgs = new TableChangesSelectingArgs(context, syncTable.TableName, connection, transaction);
                // launch interceptor if any
                await this.InterceptAsync(tableChangesSelectingArgs).ConfigureAwait(false);

                // Get Command
                var selectIncrementalChangesCommand = this.GetSelectChangesCommand(context, syncAdapter, syncTable, message.IsNew);

                // Set parameters
                this.SetSelectChangesCommonParameters(context, syncTable, message.ExcludingScopeId, message.IsNew, message.LastTimestamp, selectIncrementalChangesCommand);

                // Statistics
                var tableChangesSelected = new TableChangesSelected(syncTable.TableName);

                // Get the reader
                using (var dataReader = selectIncrementalChangesCommand.ExecuteReader())
                {
                    // memory size total
                    double rowsMemorySize = 0L;

                    // Create a chnages table with scope columns
                    var changesSetTable = DbSyncAdapter.CreateChangesTable(message.Schema.Tables[syncTable.TableName, syncTable.SchemaName], changesSet);

                    while (dataReader.Read())
                    {
                        // Create a row from dataReader
                        var row = CreateSyncRowFromReader(dataReader, changesSetTable, message.LocalScopeId);

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

                            // add changes to batchinfo
                            batchInfo.AddChanges(changesSet, batchIndex, false);

                            // increment batch index
                            batchIndex++;

                            // we know the datas are serialized here, so we can flush  the set
                            changesSet.Clear();

                            // Recreate an empty ContainerSet and a ContainerTable
                            changesSet = new SyncSet(message.Schema.ScopeName);

                            changesSetTable = DbSyncAdapter.CreateChangesTable(message.Schema.Tables[syncTable.TableName, syncTable.SchemaName], changesSet);

                            // Init the row memory size
                            rowsMemorySize = 0L;

                        }
                    }
                }

                selectIncrementalChangesCommand.Dispose();

                context.SyncStage = SyncStage.TableChangesSelected;

                if (tableChangesSelected.Deletes > 0 || tableChangesSelected.Upserts > 0)
                    changes.TableChangesSelected.Add(tableChangesSelected);

                // Event progress & interceptor
                context.SyncStage = SyncStage.TableChangesSelected;
                var tableChangesSelectedArgs = new TableChangesSelectedArgs(context, tableChangesSelected, connection, transaction);
                this.ReportProgress(context, progress, tableChangesSelectedArgs);
                await this.InterceptAsync(tableChangesSelectedArgs).ConfigureAwait(false);

            }

            // We are in batch mode, and we are at the last batchpart info
            if (changesSet != null && changesSet.HasTables)
                batchInfo.AddChanges(changesSet, batchIndex, true);

            // Check the last index as the last batch
            batchInfo.EnsureLastBatch();

            return (context, batchInfo, changes);

        }


        /// <summary>
        /// Generate an empty BatchInfo
        /// </summary>
        internal (BatchInfo, DatabaseChangesSelected) GetEmptyChanges(MessageGetChangesBatch message)
        {
            // Get config
            var isBatched = message.BatchSize > 0;

            // create the in memory changes set
            var changesSet = new SyncSet(message.Schema.ScopeName);

            // Create a Schema set without readonly tables, attached to memory changes
            foreach (var table in message.Schema.Tables)
                DbSyncAdapter.CreateChangesTable(message.Schema.Tables[table.TableName, table.SchemaName], changesSet);

            // Create the batch info, in memory
            var batchInfo = new BatchInfo(!isBatched, changesSet, message.BatchDirectory); ;

            // add changes to batchInfo
            batchInfo.AddChanges(new SyncSet());

            // Create a new empty in-memory batch info
            return (batchInfo, new DatabaseChangesSelected());

        }


        /// <summary>
        /// Get the correct Select changes command 
        /// Can be either
        /// - SelectInitializedChanges              : All changes for first sync
        /// - SelectChanges                         : All changes filtered by timestamp
        /// - SelectInitializedChangesWithFilters   : All changes for first sync with filters
        /// - SelectChangesWithFilters              : All changes filtered by timestamp with filters
        /// </summary>
        private DbCommand GetSelectChangesCommand(SyncContext context, DbSyncAdapter syncAdapter, SyncTable syncTable, bool isNew)
        {
            DbCommand selectIncrementalChangesCommand;
            DbCommandType dbCommandType;

            List<SyncFilter> tableFilters = null;

            // Check if we have parameters specified
            var hasValidParameters = context.Parameters != null && context.Parameters.Count > 0;

            // Sqlite does not have any filter, since he can't be server side
            if (this.CanBeServerProvider && hasValidParameters)
                tableFilters = syncTable.GetFilters();

            var hasFilters = tableFilters != null && tableFilters.Count > 0;

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
            selectIncrementalChangesCommand = syncAdapter.GetCommand(dbCommandType, tableFilters);

            if (selectIncrementalChangesCommand == null)
                throw new MissingCommandException(dbCommandType.ToString());

            // Add common parameters
            syncAdapter.SetCommandParameters(dbCommandType, selectIncrementalChangesCommand, tableFilters);

            return selectIncrementalChangesCommand;

        }

        /// <summary>
        /// Set common parameters to SelectChanges Sql command
        /// </summary>
        private void SetSelectChangesCommonParameters(SyncContext context, SyncTable syncTable, Guid? excludingScopeId, bool isNew, long lastTimestamp, DbCommand selectIncrementalChangesCommand)
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
            DbTableManagerFactory.SetParameterValue(selectIncrementalChangesCommand, "sync_min_timestamp", lastTimestamp);
            DbTableManagerFactory.SetParameterValue(selectIncrementalChangesCommand, "sync_scope_id", excludingScopeId.HasValue ? (object)excludingScopeId.Value : DBNull.Value);
            DbTableManagerFactory.SetParameterValue(selectIncrementalChangesCommand, "sync_scope_is_new", isNewScope);
            DbTableManagerFactory.SetParameterValue(selectIncrementalChangesCommand, "sync_scope_is_reinit", isReinit);

            // Check filters
            List<SyncFilter> tableFilters = null;

            // Check if we have parameters specified
            var hasValidParameters = context.Parameters != null && context.Parameters.Count > 0;

            // Sqlite does not have any filter, since he can't be server side
            if (this.CanBeServerProvider && hasValidParameters)
                tableFilters = syncTable.GetFilters();

            var hasFilters = tableFilters != null && tableFilters.Count > 0;

            if (!hasFilters)
                return;

            foreach (var filter in tableFilters)
            {
                var parameter = context.Parameters.FirstOrDefault(p =>
                {

                    var sc = SyncGlobalization.DataSourceStringComparison;

                    var sn = filter.SchemaName == null ? string.Empty : filter.SchemaName;
                    var otherSn = p.SchemaName == null ? string.Empty : p.SchemaName;

                    return p.ColumnName.Equals(filter.ColumnName, sc) &&
                           p.TableName.Equals(filter.TableName, sc) &&
                           sn.Equals(otherSn, sc);
                });

                if (parameter != null)
                    DbTableManagerFactory.SetParameterValue(selectIncrementalChangesCommand, parameter.ColumnName, parameter.Value);
            }
        }

        /// <summary>
        /// Create a new SyncRow from a dataReader.
        /// </summary>
        private SyncRow CreateSyncRowFromReader(IDataReader dataReader, SyncTable table, Guid localScopeId)
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
                    var readerScopeId = dataReader.GetValue(i);

                    // if update_scope_id is null, so the row owner is the local database
                    // if update_scope_id is not null, the row owner is someone else
                    if (readerScopeId == DBNull.Value || readerScopeId == null)
                        row.UpdateScopeId = localScopeId;
                    else if (SyncTypeConverter.TryConvertTo<Guid>(readerScopeId, out var updateScopeIdObject))
                        row.UpdateScopeId = (Guid)updateScopeIdObject;
                    else
                        throw new Exception("Impossible to parse row['update_scope_id']");

                    continue;
                }

                var columnValueObject = dataReader.GetValue(i);
                var columnValue = columnValueObject == DBNull.Value ? null : columnValueObject;

                row[columnName] = columnValue;

            }

            // during initialization, row["update_scope_id"] is not part of the data reader
            // so we affect the local scope id owner manually
            if (!row.UpdateScopeId.HasValue)
                row.UpdateScopeId = localScopeId;

            row.RowState = isTombstone ? DataRowState.Deleted : DataRowState.Modified;

            return row;
        }




    }
}
