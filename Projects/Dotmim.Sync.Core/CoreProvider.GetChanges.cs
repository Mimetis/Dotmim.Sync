using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using Dotmim.Sync.Serialization;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
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
        public virtual async Task<(SyncContext, BatchInfo, ChangesSelected)> GetChangeBatchAsync(SyncContext context, ScopeInfo scopeInfo)
        {
            try
            {
                if (scopeInfo == null)
                    throw new ArgumentException("ClientScope is null");

                var configuration = GetCacheConfiguration();

                // check batchSize if not > then Configuration.DownloadBatchSizeInKB
                if (configuration.DownloadBatchSizeInKB > 0)
                    Debug.WriteLine($"Enumeration data cache size selected: {configuration.DownloadBatchSizeInKB} Kb");

                // bacth info containing changes
                BatchInfo batchInfo;

                // Statistics about changes that are selected
                ChangesSelected changesSelected;

                (context, batchInfo, changesSelected) = await this.GetChanges(context, scopeInfo);

                // Check if the remote is not outdated
                var isOutdated = this.IsRemoteOutdated();

                if (isOutdated)
                    throw new Exception("OutDatedPeer");

                return (context, batchInfo, changesSelected);
            }
            catch (DbException dbex)
            {
                throw SyncException.CreateDbException(context.SyncStage, dbex);
            }
            catch (Exception ex)
            {
                if (ex is SyncException)
                    throw;
                else
                    throw SyncException.CreateUnknowException(context.SyncStage, ex);
            }
        }

        internal async Task<(SyncContext, BatchInfo, ChangesSelected)> GetChanges(SyncContext context, ScopeInfo scopeInfo)
        {
            BatchInfo batchInfo = null;
            try
            {
                Debug.WriteLine("GetChanges called: _syncBatchProducer is null");

                var configuration = GetCacheConfiguration();

                // Check if the remote is not outdated
                var isOutdated = this.IsRemoteOutdated();

                // Get a chance to make the sync even if it's outdated
                if (isOutdated && this.SyncOutdated != null)
                {
                    Debug.WriteLine("Raising Sync Remote Outdated Event");
                    var outdatedEventArgs = new OutdatedEventArgs();
                    this.SyncOutdated(this, outdatedEventArgs);
                    Debug.WriteLine($"Action taken : {outdatedEventArgs.Action.ToString()}");

                    if (outdatedEventArgs.Action == OutdatedSyncAction.PartialSync)
                        Debug.WriteLine("Attempting Partial Sync");
                }


                // the sync is still outdated, abort it
                if (isOutdated)
                {
                    Debug.WriteLine("Aborting Sync");
                    return (context, null, null);
                }

                // Statistics about changes that are selected
                ChangesSelected changesSelected;

                // if we try a Reinitialize action, don't get any changes from client
                // else get changes from batch or in memory methods
                if (context.SyncWay == SyncWay.Upload && context.SyncType == SyncType.Reinitialize)
                    (batchInfo, changesSelected) = this.GetEmptyChanges(context, scopeInfo);
                else if (configuration.DownloadBatchSizeInKB == 0)
                    (batchInfo, changesSelected) = await this.EnumerateChangesInternal(context, scopeInfo);
                else
                    (batchInfo, changesSelected) = await this.EnumerateChangesInBatchesInternal(context, scopeInfo);

                return (context, batchInfo, changesSelected);
            }
            catch (Exception)
            {
                throw;
            }

        }

        /// <summary>
        /// Generate an empty BatchInfo
        /// </summary>
        internal (BatchInfo, ChangesSelected) GetEmptyChanges(SyncContext context, ScopeInfo scopeInfo)
        {
            // Get config
            var configuration = GetCacheConfiguration();
            var isBatched = configuration.DownloadBatchSizeInKB > 0;

            // create the in memory changes set
            DmSet changesSet = new DmSet(configuration.ScopeSet.DmSetName);

            // Create the batch info, in memory
            var batchInfo = new BatchInfo();
            batchInfo.InMemory = !isBatched;

            if (!isBatched)
                batchInfo.Directory = BatchInfo.GenerateNewDirectoryName();

            // generate the batchpartinfo
            var bpi = batchInfo.GenerateBatchInfo(0, changesSet, configuration.BatchDirectory);
            bpi.IsLastBatch = true;

            // Create a new in-memory batch info with an the changes DmSet
            return (batchInfo, new ChangesSelected());

        }

        /// <summary>
        /// Enumerate all internal changes, no batch mode
        /// </summary>
        internal async Task<(BatchInfo, ChangesSelected)> EnumerateChangesInternal(SyncContext context, ScopeInfo scopeInfo)
        {
            Debug.WriteLine($"----- Enumerating Changes for Scope \"{scopeInfo.Name}\" -----");
            Debug.WriteLine("");
            Debug.WriteLine("");

            // Get config
            var configuration = GetCacheConfiguration();

            // create the in memory changes set
            DmSet changesSet = new DmSet(configuration.ScopeSet.DmSetName);

            // Create the batch info, in memory
            var batchInfo = new BatchInfo();
            batchInfo.InMemory = true;

            using (var connection = this.CreateConnection())
            {
                // Open the connection
                await connection.OpenAsync();

                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        // changes that will be returned as selected changes
                        ChangesSelected changes = new ChangesSelected();

                        foreach (var tableDescription in configuration)
                        {
                            // if we are in upload stage, so check if table is not download only
                            if (context.SyncWay == SyncWay.Upload && tableDescription.SyncDirection == SyncDirection.DownloadOnly)
                                continue;

                            // if we are in download stage, so check if table is not download only
                            if (context.SyncWay == SyncWay.Download && tableDescription.SyncDirection == SyncDirection.UploadOnly)
                                continue;

                            var builder = this.GetDatabaseBuilder(tableDescription);
                            var syncAdapter = builder.CreateSyncAdapter(connection, transaction);
                            syncAdapter.ConflictApplyAction = configuration.GetApplyAction();

                            // raise before event
                            context.SyncStage = SyncStage.ChangesSelecting;
                            var beforeArgs = new TableChangesSelectingEventArgs(this.ProviderTypeName, context.SyncStage, tableDescription.TableName);
                            this.TryRaiseProgressEvent(beforeArgs, this.TableChangesSelecting);

                            // selected changes for the current table
                            TableChangesSelected tableSelectedChanges = new TableChangesSelected
                            {
                                TableName = tableDescription.TableName
                            };

                            // Get Command
                            DbCommand selectIncrementalChangesCommand;
                            DbCommandType dbCommandType;

                            if (this.CanBeServerProvider && context.Parameters != null && context.Parameters.Count > 0 && configuration.Filters != null && configuration.Filters.Count > 0)
                            {
                                var filtersName = configuration.Filters
                                                .Where(f => f.TableName.Equals(tableDescription.TableName, StringComparison.InvariantCultureIgnoreCase))
                                                .Select(f => f.ColumnName);

                                if (filtersName != null && filtersName.Count() > 0)
                                {
                                    dbCommandType = DbCommandType.SelectChangesWitFilters;
                                    selectIncrementalChangesCommand = syncAdapter.GetCommand(dbCommandType, filtersName);
                                }
                                else
                                {
                                    dbCommandType = DbCommandType.SelectChanges;
                                    selectIncrementalChangesCommand = syncAdapter.GetCommand(dbCommandType);
                                }
                            }
                            else
                            {
                                dbCommandType = DbCommandType.SelectChanges;
                                selectIncrementalChangesCommand = syncAdapter.GetCommand(dbCommandType);
                            }

                            if (selectIncrementalChangesCommand == null)
                            {
                                var exc = "Missing command 'SelectIncrementalChangesCommand' ";
                                Debug.WriteLine(exc);
                                throw new Exception(exc);
                            }

                            // Deriving Parameters
                            syncAdapter.SetCommandParameters(dbCommandType, selectIncrementalChangesCommand);

                            // Get a clone of the table with tracking columns
                            var dmTableChanges = BuildChangesTable(tableDescription.TableName);

                            SetSelectChangesCommonParameters(context, scopeInfo, selectIncrementalChangesCommand);

                            // Set filter parameters if any
                            if (this.CanBeServerProvider && context.Parameters != null && context.Parameters.Count > 0 && configuration.Filters != null && configuration.Filters.Count > 0)
                            {
                                var filters = configuration.Filters.Where(f => f.TableName.Equals(tableDescription.TableName, StringComparison.InvariantCultureIgnoreCase)).ToList();

                                if (filters != null && filters.Count > 0)
                                {
                                    foreach (var filter in filters)
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
                                    DmRow dataRow = CreateRowFromReader(dataReader, dmTableChanges);

                                    //DmRow dataRow = dmTableChanges.NewRow();

                                    // assuming the row is not inserted / modified
                                    DmRowState state = DmRowState.Unchanged;

                                    // get if the current row is inserted, modified, deleted
                                    state = GetStateFromDmRow(dataRow, scopeInfo);

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

                            // add the stats to global stats
                            changes.TableChangesSelected.Add(tableSelectedChanges);

                            // Raise event for this table
                            context.SyncStage = SyncStage.ChangesSelected;
                            var args = new TableChangesSelectedEventArgs(this.ProviderTypeName, SyncStage.ChangesSelected, tableSelectedChanges);
                            this.TryRaiseProgressEvent(args, this.TableChangesSelected);

                            Debug.WriteLine($"--- End Table \"{tableDescription.TableName}\" ---");
                            Debug.WriteLine("");
                        }


                        transaction.Commit();

                        // generate the batchpartinfo
                        batchInfo.GenerateBatchInfo(0, changesSet, configuration.BatchDirectory);

                        // Create a new in-memory batch info with an the changes DmSet
                        return (batchInfo, changes);

                    }
                    catch (Exception dbException)
                    {
                        Debug.WriteLine($"Caught exception while enumerating changes\n{dbException}\n");
                        throw;
                    }
                    finally
                    {
                        if (connection != null && connection.State == ConnectionState.Open)
                            connection.Close();
                    }

                }

            }
        }

        /// <summary>
        /// Set common parameters to SelectChanges Sql command
        /// </summary>
        private static void SetSelectChangesCommonParameters(SyncContext context, ScopeInfo scopeInfo, DbCommand selectIncrementalChangesCommand)
        {
            // Generate the isNewScope Flag.
            var isNewScope = scopeInfo.IsNewScope ? 1 : 0;
            var lastTimeStamp = scopeInfo.LastTimestamp;
            int isReinit = context.SyncType == SyncType.Reinitialize ? 1 : 0;

            switch (context.SyncWay)
            {
                case SyncWay.Upload:
                    // Overwrite if we are in Reinitialize mode (not RenitializeWithUpload)
                    isNewScope = context.SyncType == SyncType.Reinitialize ? 1 : isNewScope;
                    lastTimeStamp = context.SyncType == SyncType.Reinitialize ? 0 : lastTimeStamp;
                    isReinit = context.SyncType == SyncType.Reinitialize ? 1 : 0;
                    break;
                case SyncWay.Download:
                    // Ovewrite on bot Reinitialize and ReinitializeWithUpload
                    isNewScope = context.SyncType != SyncType.Normal ? 1 : isNewScope;
                    lastTimeStamp = context.SyncType != SyncType.Normal ? 0 : lastTimeStamp;
                    isReinit = context.SyncType != SyncType.Normal ? 1 : 0;
                    break;
                default:
                    break;
            }

            // Set the parameters
            DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_min_timestamp", lastTimeStamp);
            DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_scope_id", scopeInfo.Id);
            DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_scope_is_new", isNewScope);
            DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_scope_is_reinit", isReinit);

            scopeInfo.IsNewScope = isNewScope == 1 ? true : false;
            scopeInfo.LastTimestamp = lastTimeStamp;

        }

        /// <summary>
        /// Enumerate all internal changes, no batch mode
        /// </summary>
        internal async Task<(BatchInfo, ChangesSelected)> EnumerateChangesInBatchesInternal(SyncContext context, ScopeInfo scopeInfo)
        {
            Debug.WriteLine($"----- Enumerating Changes for Scope \"{scopeInfo.Name}\" -----");
            Debug.WriteLine("");
            Debug.WriteLine("");
            var configuration = GetCacheConfiguration();

            // memory size total
            double memorySizeFromDmRows = 0L;

            int batchIndex = 0;

            // this batch info won't be in memory, it will be be batched
            BatchInfo batchInfo = new BatchInfo();
            // directory where all files will be stored
            batchInfo.Directory = BatchInfo.GenerateNewDirectoryName();
            // not in memory since we serialized all files in the tmp directory
            batchInfo.InMemory = false;

            // Create stats object to store changes count
            ChangesSelected changes = new ChangesSelected();

            using (var connection = this.CreateConnection())
            {
                try
                {
                    // Open the connection
                    await connection.OpenAsync();

                    using (var transaction = connection.BeginTransaction())
                    {
                        // create the in memory changes set
                        DmSet changesSet = new DmSet(configuration.ScopeSet.DmSetName);

                        foreach (var tableDescription in configuration)
                        {
                            // if we are in upload stage, so check if table is not download only
                            if (context.SyncWay == SyncWay.Upload && tableDescription.SyncDirection == SyncDirection.DownloadOnly)
                                continue;

                            // if we are in download stage, so check if table is not download only
                            if (context.SyncWay == SyncWay.Download && tableDescription.SyncDirection == SyncDirection.UploadOnly)
                                continue;

                            var builder = this.GetDatabaseBuilder(tableDescription);
                            var syncAdapter = builder.CreateSyncAdapter(connection, transaction);
                            syncAdapter.ConflictApplyAction = configuration.GetApplyAction();

                            // raise before event
                            context.SyncStage = SyncStage.ChangesSelecting;
                            var beforeArgs = new TableChangesSelectingEventArgs(this.ProviderTypeName, context.SyncStage, tableDescription.TableName);
                            this.TryRaiseProgressEvent(beforeArgs, this.TableChangesSelecting);

                            // Get Command
                            DbCommand selectIncrementalChangesCommand;
                            DbCommandType dbCommandType;

                            if (this.CanBeServerProvider && context.Parameters != null && context.Parameters.Count > 0 && configuration.Filters != null && configuration.Filters.Count > 0)
                            {
                                var filtersName = configuration.Filters
                                                .Where(f => f.TableName.Equals(tableDescription.TableName, StringComparison.InvariantCultureIgnoreCase))
                                                .Select(f => f.ColumnName);

                                if (filtersName != null && filtersName.Count() > 0)
                                {
                                    dbCommandType = DbCommandType.SelectChangesWitFilters;
                                    selectIncrementalChangesCommand = syncAdapter.GetCommand(dbCommandType, filtersName);
                                }
                                else
                                {
                                    dbCommandType = DbCommandType.SelectChanges;
                                    selectIncrementalChangesCommand = syncAdapter.GetCommand(dbCommandType);
                                }
                            }
                            else
                            {
                                dbCommandType = DbCommandType.SelectChanges;
                                selectIncrementalChangesCommand = syncAdapter.GetCommand(dbCommandType);
                            }

                            // Deriving Parameters
                            syncAdapter.SetCommandParameters(DbCommandType.SelectChanges, selectIncrementalChangesCommand);

                            if (selectIncrementalChangesCommand == null)
                            {
                                var exc = "Missing command 'SelectIncrementalChangesCommand' ";
                                Debug.WriteLine(exc);
                                throw new Exception(exc);
                            }

                            var dmTable = BuildChangesTable(tableDescription.TableName);

                            try
                            {
                                // Set commons parameters
                                SetSelectChangesCommonParameters(context, scopeInfo, selectIncrementalChangesCommand);

                                // Set filter parameters if any
                                // Only on server side
                                if (this.CanBeServerProvider && context.Parameters != null && context.Parameters.Count > 0 && configuration.Filters != null && configuration.Filters.Count > 0)
                                {
                                    var filters = configuration.Filters.Where(f => f.TableName.Equals(tableDescription.TableName, StringComparison.InvariantCultureIgnoreCase)).ToList();

                                    if (filters != null && filters.Count > 0)
                                    {
                                        foreach (var filter in filters)
                                        {
                                            var parameter = context.Parameters.FirstOrDefault(p => p.ColumnName.Equals(filter.ColumnName, StringComparison.InvariantCultureIgnoreCase) && p.TableName.Equals(filter.TableName, StringComparison.InvariantCultureIgnoreCase));

                                            if (parameter != null)
                                                DbManager.SetParameterValue(selectIncrementalChangesCommand, parameter.ColumnName, parameter.Value);
                                        }
                                    }
                                }

                                this.AddTrackingColumns<int>(dmTable, "sync_row_is_tombstone");

                                // Statistics
                                TableChangesSelected tableChangesSelected = new TableChangesSelected
                                {
                                    TableName = tableDescription.TableName
                                };

                                changes.TableChangesSelected.Add(tableChangesSelected);

                                // Get the reader
                                using (var dataReader = selectIncrementalChangesCommand.ExecuteReader())
                                {
                                    while (dataReader.Read())
                                    {
                                        DmRow dmRow = CreateRowFromReader(dataReader, dmTable);

                                        DmRowState state = DmRowState.Unchanged;

                                        state = GetStateFromDmRow(dmRow, scopeInfo);

                                        // If the row is not deleted inserted or modified, go next
                                        if (state != DmRowState.Deleted && state != DmRowState.Modified && state != DmRowState.Added)
                                            continue;

                                        var fieldsSize = DmTableSurrogate.GetRowSizeFromDataRow(dmRow);
                                        var dmRowSize = fieldsSize / 1024d;

                                        if (dmRowSize > configuration.DownloadBatchSizeInKB)
                                        {
                                            var exc = $"Row is too big ({dmRowSize} kb.) for the current Configuration.DownloadBatchSizeInKB ({configuration.DownloadBatchSizeInKB} kb.) Aborting Sync...";
                                            Debug.WriteLine(exc);
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
                                        if (memorySizeFromDmRows > configuration.DownloadBatchSizeInKB)
                                        {
                                            // Since we dont need this column anymore, remove it
                                            this.RemoveTrackingColumns(dmTable, "sync_row_is_tombstone");

                                            changesSet.Tables.Add(dmTable);

                                            // generate the batch part info
                                            batchInfo.GenerateBatchInfo(batchIndex, changesSet, configuration.BatchDirectory);

                                            // increment batch index
                                            batchIndex++;

                                            changesSet.Clear();

                                            // Recreate an empty DmSet, then a dmTable clone
                                            changesSet = new DmSet(configuration.ScopeSet.DmSetName);
                                            dmTable = dmTable.Clone();
                                            this.AddTrackingColumns<int>(dmTable, "sync_row_is_tombstone");

                                            // Init the row memory size
                                            memorySizeFromDmRows = 0L;

                                            //// raise SyncProgress Event
                                            //// in batch mode, we could have a table on mulitple batchs
                                            //// so try to get it
                                            //var existSelectedChanges = changes.TableChangesSelected.FirstOrDefault(sc => string.Equals(sc.TableName, tableDescription.TableName));
                                            //if (existSelectedChanges == null)
                                            //{
                                            //    existSelectedChanges = tableChangesSelected;
                                            //    changes.TableChangesSelected.Add(tableChangesSelected);
                                            //}
                                            //else
                                            //{
                                            //    existSelectedChanges.Deletes += tableChangesSelected.Deletes;
                                            //    existSelectedChanges.Inserts += tableChangesSelected.Inserts;
                                            //    existSelectedChanges.Updates += tableChangesSelected.Updates;
                                            //    existSelectedChanges.TotalChanges += tableChangesSelected.TotalChanges;
                                            //}

                                            // add stats for a SyncProgress event
                                            context.SyncStage = SyncStage.ChangesSelected;
                                            var args2 = new TableChangesSelectedEventArgs
                                                (this.ProviderTypeName, SyncStage.ChangesSelected, tableChangesSelected);

                                            this.TryRaiseProgressEvent(args2, this.TableChangesSelected);

                                            //// reinit stats 
                                            //tableChangesSelected = new TableChangesSelected();
                                            //tableChangesSelected.TableName = tableDescription.TableName;

                                        }
                                    }

                                    // Since we dont need this column anymore, remove it
                                    this.RemoveTrackingColumns(dmTable, "sync_row_is_tombstone");

                                    context.SyncStage = SyncStage.ChangesSelected;

                                    changesSet.Tables.Add(dmTable);

                                    // Init the row memory size
                                    memorySizeFromDmRows = 0L;

                                    //// raise SyncProgress Event
                                    //var esc = changes.TableChangesSelected.FirstOrDefault(sc => string.Equals(sc.TableName, tableDescription.TableName));
                                    //if (esc == null)
                                    //{
                                    //    esc = tableChangesSelected;
                                    //    changes.TableChangesSelected.Add(esc);
                                    //}
                                    //else
                                    //{
                                    //    esc.Deletes += tableChangesSelected.Deletes;
                                    //    esc.Inserts += tableChangesSelected.Inserts;
                                    //    esc.Updates += tableChangesSelected.Updates;
                                    //    esc.TotalChanges += tableChangesSelected.TotalChanges;
                                    //}

                                    // Event progress
                                    context.SyncStage = SyncStage.ChangesSelected;
                                    var args = new TableChangesSelectedEventArgs(this.ProviderTypeName, SyncStage.ChangesSelected, tableChangesSelected);
                                    this.TryRaiseProgressEvent(args, this.TableChangesSelected);
                                }
                            }
                            catch (Exception dbException)
                            {
                                Debug.WriteLine($"Caught exception while enumerating changes\n{dbException}\n");
                                throw;
                            }
                            finally
                            {

                                Debug.WriteLine($"--- End Table \"{tableDescription.TableName}\" ---");
                                Debug.WriteLine("");
                            }
                        }

                        // We are in batch mode, and we are at the last batchpart info
                        var batchPartInfo = batchInfo.GenerateBatchInfo(batchIndex, changesSet, configuration.BatchDirectory);

                        if (batchPartInfo != null)
                            batchPartInfo.IsLastBatch = true;

                        transaction.Commit();
                    }

                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    if (connection != null && connection.State == ConnectionState.Open)
                        connection.Close();
                }


            }
            Debug.WriteLine($"--- End Enumerating Changes for Scope \"{scopeInfo.Name}\" ---");
            Debug.WriteLine("");

            return (batchInfo, changes);
        }

        /// <summary>
        /// Create a DmRow from a IDataReader
        /// </summary>
        private DmRow CreateRowFromReader(IDataReader dataReader, DmTable dmTable)
        {
            // we have an insert / update or delete
            DmRow dataRow = dmTable.NewRow();

            for (int i = 0; i < dataReader.FieldCount; i++)
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
                            else if (columnType == typeof(Int32) && dmRowObjectType != typeof(Int32))
                                dmRowObject = Convert.ToInt32(dmRowObject);
                            else if (columnType == typeof(UInt32) && dmRowObjectType != typeof(UInt32))
                                dmRowObject = Convert.ToUInt32(dmRowObject);
                            else if (columnType == typeof(Int16) && dmRowObjectType != typeof(Int16))
                                dmRowObject = Convert.ToInt16(dmRowObject);
                            else if (columnType == typeof(UInt16) && dmRowObjectType != typeof(UInt16))
                                dmRowObject = Convert.ToUInt16(dmRowObject);
                            else if (columnType == typeof(Int64) && dmRowObjectType != typeof(Int64))
                                dmRowObject = Convert.ToInt64(dmRowObject);
                            else if (columnType == typeof(UInt64) && dmRowObjectType != typeof(UInt64))
                                dmRowObject = Convert.ToUInt64(dmRowObject);
                            else if (columnType == typeof(Byte) && dmRowObjectType != typeof(Byte))
                                dmRowObject = Convert.ToByte(dmRowObject);
                            else if (columnType == typeof(Char) && dmRowObjectType != typeof(Char))
                                dmRowObject = Convert.ToChar(dmRowObject);
                            else if (columnType == typeof(DateTime) && dmRowObjectType != typeof(DateTime))
                                dmRowObject = Convert.ToDateTime(dmRowObject);
                            else if (columnType == typeof(Decimal) && dmRowObjectType != typeof(Decimal))
                                dmRowObject = Convert.ToDecimal(dmRowObject);
                            else if (columnType == typeof(Double) && dmRowObjectType != typeof(Double))
                                dmRowObject = Convert.ToDouble(dmRowObject);
                            else if (columnType == typeof(SByte) && dmRowObjectType != typeof(SByte))
                                dmRowObject = Convert.ToSByte(dmRowObject);
                            else if (columnType == typeof(Single) && dmRowObjectType != typeof(Single))
                                dmRowObject = Convert.ToSingle(dmRowObject);
                            else if (columnType == typeof(String) && dmRowObjectType != typeof(String))
                                dmRowObject = Convert.ToString(dmRowObject);
                            else if (columnType == typeof(Boolean) && dmRowObjectType != typeof(Boolean))
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


        private DmTable BuildChangesTable(string tableName)
        {
            var configuration = GetCacheConfiguration();

            var dmTable = configuration[tableName].Clone();

            // Adding the tracking columns
            AddTrackingColumns<Guid>(dmTable, "create_scope_id");
            AddTrackingColumns<long>(dmTable, "create_timestamp");
            AddTrackingColumns<Guid>(dmTable, "update_scope_id");
            AddTrackingColumns<long>(dmTable, "update_timestamp");

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
        private DmRowState GetStateFromDmRow(DmRow dataRow, ScopeInfo scopeInfo)
        {
            DmRowState dmRowState = DmRowState.Unchanged;

            var isTombstone = Convert.ToInt64(dataRow["sync_row_is_tombstone"]) > 0;

            if (isTombstone)
                dmRowState = DmRowState.Deleted;
            else
            {
                var createdTimeStamp = DbManager.ParseTimestamp(dataRow["create_timestamp"]);
                var updatedTimeStamp = DbManager.ParseTimestamp(dataRow["update_timestamp"]);
                var updateScopeIdRow = dataRow["update_scope_id"];
                var createScopeIdRow = dataRow["create_scope_id"];

                Guid? updateScopeId = (updateScopeIdRow != DBNull.Value && updateScopeIdRow != null) ? (Guid)updateScopeIdRow : (Guid?)null;
                Guid? createScopeId = (createScopeIdRow != DBNull.Value && createScopeIdRow != null) ? (Guid)createScopeIdRow : (Guid?)null;

                var isLocallyCreated = !createScopeId.HasValue;
                var islocallyUpdated = !updateScopeId.HasValue || updateScopeId.Value != scopeInfo.Id;


                // Check if a row is modified :
                // 1) Row is not new
                // 2) Row update is AFTER last sync of asker
                // 3) Row insert is BEFORE last sync of asker (if insert is after last sync, it's not an update, it's an insert)
                if (!scopeInfo.IsNewScope && islocallyUpdated && updatedTimeStamp > scopeInfo.LastTimestamp && (createdTimeStamp < scopeInfo.LastTimestamp || !isLocallyCreated))
                    dmRowState = DmRowState.Modified;
                else if (scopeInfo.IsNewScope || (isLocallyCreated && createdTimeStamp > scopeInfo.LastTimestamp))
                    dmRowState = DmRowState.Added;
                // The line has been updated from an other host
                else if (islocallyUpdated && updateScopeId.HasValue && updateScopeId.Value != scopeInfo.Id)
                    dmRowState = DmRowState.Modified;
                else
                {
                    dmRowState = DmRowState.Unchanged;
                    Debug.WriteLine($"Row is in Unchanegd state. " +
                        $"\tscopeInfo.Id:{scopeInfo.Id}, scopeInfo.IsNewScope :{scopeInfo.IsNewScope}, scopeInfo.LastTimestamp:{scopeInfo.LastTimestamp}" +
                        $"\tcreateScopeId:{createScopeId}, updateScopeId:{updateScopeId}, createdTimeStamp:{createdTimeStamp}, updatedTimeStamp:{updatedTimeStamp}.");
                }
            }

            return dmRowState;
        }

    }
}
