
using Dotmim.Sync.Core.Common;
using Dotmim.Sync.Core.Context;
using Dotmim.Sync.Core.Enumerations;
using Dotmim.Sync.Core.Log;
using Dotmim.Sync.Core.Scope;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Core.Adapter
{

    /// <summary>
    /// The SyncAdapter is the datasource manager for ONE table
    /// Should be implemented by every database provider and provide every SQL action
    /// </summary>
    public abstract class SyncAdapter
    {
        public delegate ApplyAction ConflictActionDelegate(SyncConflict conflict, DbConnection connection, DbTransaction transaction);

        public ConflictActionDelegate ConflictActionInvoker;

        /// <summary>
        /// Get or Set the InsertCommand : Insert one item in the datasource
        /// </summary>
        public DbCommand InsertCommand { get; set; }

        /// <summary>
        /// Get or set the UpdateCommand : Update one item in the datasource
        /// </summary>
        public DbCommand UpdateCommand { get; set; }

        /// <summary>
        /// Get or Set the delete command : Delete one item in the datasource
        /// </summary>
        public DbCommand DeleteCommand { get; set; }

        /// <summary>
        /// Get or Set the insert metadata command : Insert metadatas for one item
        /// </summary>
        public DbCommand InsertMetadataCommand { get; set; }

        /// <summary>
        /// Get or Set the update metadata command : Update the metadatas for one item
        /// </summary>
        public DbCommand UpdateMetadataCommand { get; set; }

        /// <summary>
        /// Get or Set the delete metadata command : delete the metadatas for one item
        /// </summary>
        public DbCommand DeleteMetadataCommand { get; set; }

        /// <summary>
        /// Get or Set the select row command : Retrieve one row by its Key
        /// </summary>
        public DbCommand SelectRowCommand { get; set; }

        /// <summary>
        /// Get or set the selecte metadata command : Retrieve the item metadatas
        /// </summary>
        public DbCommand SelectMetadataCommand { get; set; }

        /// <summary>
        /// Get or Set the select incremental changes command : Retrieve all changes since last sync
        /// </summary>
        public DbCommand SelectIncrementalChangesCommand { get; set; }

        /// <summary>
        /// Get or Set the bulk insert command : When batch mode is enabled, insert rows in batch
        /// </summary>
        public DbCommand BulkInsertCommand { get; set; }

        /// <summary>
        /// Get or Set the bulk update command : When batch mode is enabled, update rows in batch
        /// </summary>
        public DbCommand BulkUpdateCommand { get; set; }

        /// <summary>
        /// Get or Set the bulk delete command : When batch mode is enabled, delete rows in batch
        /// </summary>
        public DbCommand BulkDeleteCommand { get; set; }

        /// <summary>
        /// Get or Set the connection used for this adapter. Could be already opened ... or not
        /// </summary>
        public DbConnection Connection { get; set; }

        /// <summary>
        /// Get or Set the transaction used during operation
        /// </summary>
        public DbTransaction Transaction { get; set; }

        /// <summary>
        ///  Get or Set the sync configguration : Contains all columns read from the config data xml field for a scope name
        /// </summary>
        public ScopeConfigDataAdapter ScopeConfigDataAdapter { get; set; }

        /// <summary>
        /// Get or Set the remote table name
        /// </summary>
        public string RemoteTableName { get; set; }

        /// <summary>
        /// Get or Set the row id columns
        /// </summary>
        public List<string> RowIdColumns { get; set; } = new List<string>();

        /// <summary>
        /// Get or set the Local Table Name
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// Get or Set the current step (could be only Added, Modified, Deleted)
        /// </summary>
        internal DmRowState applyType { get; set; }
       
        /// <summary>
        /// Set command parameters value mapped to Row
        /// </summary>
        internal void SetColumnParameters(DbCommand command, DmRow row)
        {
            foreach (DbParameter parameter in command.Parameters)
            {
                // foreach parameter, check if we have a column 
                if (!string.IsNullOrEmpty(parameter.SourceColumn))
                {
                    if (row.Table.Columns.Contains(parameter.SourceColumn))
                    {
                        var value = row[parameter.SourceColumn];
                        DbHelper.SetParameterValue(command, parameter.ParameterName, value);
                    }
                }

            }

            // return value
            var syncRowCountParam = DbHelper.GetParameter(command, "sync_row_count");

            if (syncRowCountParam != null)
                syncRowCountParam.Direction = ParameterDirection.Output;
        }

        /// <summary>
        /// Insert or update a metadata line
        /// </summary>
        internal int InsertOrUpdateMetadatas(DbCommand command, DmRow row, ScopeInfo scope)
        {
            int rowsApplied = 0;

            if (command == null)
            {
                var exc = $"Missing command for apply metadata ";
                Logger.Current.Error(exc);
                throw new Exception(exc);
            }

            // Set the id parameter
            this.SetColumnParameters(command, row);

            DbHelper.SetParameterValue(command, "sync_scope_name", scope.Name);
            DbHelper.SetParameterValue(command, "sync_row_is_tombstone", row.RowState == DmRowState.Deleted ? 1 : 0);
            DbHelper.SetParameterValue(command, "create_timestamp", scope.LastTimestamp);
            DbHelper.SetParameterValue(command, "update_timestamp", scope.LastTimestamp);
   
            try
            {
                var alreadyOpened = this.Connection.State == ConnectionState.Open;

                // OPen Connection
                if (!alreadyOpened)
                    this.Connection.Open();

                if (this.Transaction != null)
                    command.Transaction = this.Transaction;

                command.ExecuteNonQuery();

                // get the row count
                rowsApplied = DbHelper.GetSyncIntOutParameter("sync_row_count", command);
            }
            catch (DbException ex)
            {
                Logger.Current.Error(ex.Message);
                throw;
            }

            return rowsApplied;
        }

        /// <summary>
        /// Try to get a source row
        /// </summary>
        /// <returns></returns>
        private DmTable GetRow(DmRow sourceRow)
        {
            // Get the row in the local repository
            var selectCommand = this.SelectRowCommand;

            var alreadyOpened = this.Connection.State == ConnectionState.Open;

            // Open Connection
            if (!alreadyOpened)
                this.Connection.Open();

            if (this.Transaction != null)
                selectCommand.Transaction = this.Transaction;

            this.SetColumnParameters(selectCommand, sourceRow);
  
            var dmTableSelected = new DmTable(this.RemoteTableName);
            try
            {

                using (var reader = selectCommand.ExecuteReader())
                {
                    dmTableSelected.Fill(reader);
                }

            }
            catch (Exception ex)
            {
                Logger.Current.Error("Server Error on Getting a row : " + ex.Message);
                throw;
            }

            return dmTableSelected;
        }

        /// <summary>
        /// Launch apply bulk changes
        /// </summary>
        /// <returns></returns>
        public DmView ApplyBulkChanges(DmView dmChanges,  ScopeInfo fromScope, List<SyncConflict> conflicts)
        {
            DbCommand bulkCommand = null;

            if (this.applyType == DmRowState.Added)
                bulkCommand = this.BulkInsertCommand;
            else if (this.applyType == DmRowState.Modified)
                bulkCommand = this.BulkUpdateCommand;
            else if (this.applyType == DmRowState.Deleted)
                bulkCommand = this.BulkDeleteCommand;
            else
                throw new Exception("DmRowState not valid during ApplyBulkChanges operation");

            if (this.Transaction != null && this.Transaction.Connection != null)
                bulkCommand.Transaction = this.Transaction;

            DmTable batchDmTable = dmChanges.Table.Clone();
            DmTable failedDmtable = new DmTable { Culture = CultureInfo.InvariantCulture };

            // Create the schema for failed rows (just add the Primary keys)
            this.AddSchemaForFailedRowsTable(batchDmTable, failedDmtable);

            int batchCount = 0;
            int rowCount = 0;

            foreach (var dmRow in dmChanges)
            {
                // Cancel the delete state to be able to get the row, more simplier
                if (applyType == DmRowState.Deleted)
                    dmRow.RejectChanges();

                // Load the datarow
                DmRow dataRow = batchDmTable.LoadDataRow(dmRow.ItemArray, false);

                // Apply the delete
                // is it mandatory ?
                if (applyType == DmRowState.Deleted)
                    dmRow.Delete();

                batchCount++;
                rowCount++;

                if (batchCount != 75 && rowCount != dmChanges.Count)
                    continue;

                // Since the update and create timestamp come from remote, change name for the bulk operations
                batchDmTable.Columns["update_timestamp"].ColumnName = "update_peer_timestamp";
                batchDmTable.Columns["create_timestamp"].ColumnName = "create_peer_timestamp";

                // execute the batch, through the provider
                ExecuteBatchCommand(bulkCommand, batchDmTable, failedDmtable, fromScope);

                batchDmTable = dmChanges.Table.Clone();
                batchCount = 0;
            }

            // Update table progress 
            //tableProgress.ChangesApplied = dmChanges.Count - failedDmtable.Rows.Count;

            if (failedDmtable.Rows.Count == 0)
                return dmChanges;


            // Check all conflicts raised
            var failedFilter = new Predicate<DmRow>(row => failedDmtable.FindByKey(row.GetKeyValues()) != null);
            var appliedFilter = new Predicate<DmRow>(row => failedDmtable.FindByKey(row.GetKeyValues()) == null);

            // New View
            var dmFailedRows = new DmView(dmChanges, failedFilter);
            var dmAppliedRows = new DmView(dmChanges, appliedFilter);

            // Generate a conflict and add it
            foreach (var dmFailedRow in dmFailedRows)
                conflicts.Add(GetConflict(dmFailedRow));

            // return applied rows - failed rows (generating a conflict)
            return dmAppliedRows;
        }

        /// <summary>
        /// Try to apply changes on the server.
        /// Internally will call ApplyInsert / ApplyUpdate or ApplyDelete
        /// </summary>
        /// <param name="dmChanges">Changes from remote</param>
        /// <returns>every lines not updated on the server side</returns>
        internal DmView ApplyChanges(DmView dmChanges, ScopeInfo scope, List<SyncConflict> conflicts)
        {
            DmView appliedRows = new DmView(dmChanges);

            foreach (var dmRow in dmChanges)
            {
                bool operationComplete = false;

                if (applyType == DmRowState.Added)
                    operationComplete = this.ApplyInsert(dmRow);
                else if (applyType == DmRowState.Modified)
                    operationComplete = this.ApplyUpdate(dmRow, scope, false);
                //else if (applyType == DmRowState.Deleted)
                //    operationComplete = this.ApplyDelete(row, forceWrite);

                // if no pb, go to next row
                if (operationComplete)
                {
                    appliedRows.Add(dmRow);
                    continue;
                }

                // Generate a conflict and add it
                conflicts.Add(GetConflict(dmRow));
            }

            return appliedRows;
        }

        /// <summary>
        /// Apply a single insert in the current data source
        /// </summary>
        internal bool ApplyInsert(DmRow remoteRow)
        {
            var command = this.InsertCommand;

            // Set the parameters value from row
            SetColumnParameters(command, remoteRow);

            var alreadyOpened = this.Connection.State == ConnectionState.Open;

            // Open Connection
            if (!alreadyOpened)
                this.Connection.Open();

            int rowInsertedCount = 0;
            try
            {
                if (this.Transaction != null)
                    command.Transaction = this.Transaction;

                command.ExecuteNonQuery();

                // get the row count
                rowInsertedCount = DbHelper.GetSyncIntOutParameter("sync_row_count", command);
            }
            catch (ArgumentException ex)
            {
                Logger.Current.Error(ex.Message);
                throw;
            }
            catch (DbException ex)
            {
                Logger.Current.Error(ex.Message);
                return false;
            }

            return rowInsertedCount > 0;

        }

        /// <summary>
        /// Apply a single update in the current datasource. if forceWrite, override conflict situation and force the update
        /// </summary>
        internal bool ApplyUpdate(DmRow sourceRow, ScopeInfo scope, bool forceWrite)
        {
            var command = this.UpdateCommand;

            // Set the parameters value from row
            SetColumnParameters(command, sourceRow);

            // special parameters for update
            DbHelper.SetParameterValue(command, "sync_force_write", (forceWrite ? 1 : 0));
            DbHelper.SetParameterValue(command, "sync_min_timestamp", scope.LastTimestamp);

            var alreadyOpened = this.Connection.State == ConnectionState.Open;

            int rowInsertedCount = 0;
            try
            {
                // OPen Connection
                if (!alreadyOpened)
                    this.Connection.Open();

                if (this.Transaction != null)
                    command.Transaction = this.Transaction;

                command.ExecuteNonQuery();

                // get the row count
                rowInsertedCount = DbHelper.GetSyncIntOutParameter("sync_row_count", command);
            }
            catch (ArgumentException ex)
            {
                Logger.Current.Error(ex.Message);
                throw;
            }
            catch (DbException ex)
            {
                Logger.Current.Error(ex.Message);
                return false;
            }

            return rowInsertedCount > 0;
        }

        /// <summary>
        /// We have a conflict, try to get the source (server) row and generate a conflict
        /// </summary>
        private SyncConflict GetConflict(DmRow dmRow)
        {
            DmRow destinationRow = null;

            // Problem during operation
            var dmTableSelected = GetRow(dmRow);

            ConflictType dbConflictType = ConflictType.ErrorsOccurred;

            // Can't find the row
            if (dmTableSelected.Rows.Count == 0)
            {
                var errorMessage = "Change Application failed due to Row not Found on the server";

                Logger.Current.Error($"Conflict detected with error: {errorMessage}");

                dbConflictType = applyType == DmRowState.Added ? ConflictType.LocalNoRowRemoteInsert : ConflictType.LocalNoRowRemoteUpdate;
            }
            else
            {
                // We have a problem and found the row on the server side
                destinationRow = dmTableSelected.Rows[0];
            }

            // On est sur un conflit d'insert - insert
            if (applyType == DmRowState.Added)
                dbConflictType = destinationRow == null ? ConflictType.LocalNoRowRemoteUpdate : ConflictType.LocalInsertRemoteInsert;
            else if (applyType == DmRowState.Modified)
                dbConflictType = destinationRow == null ? ConflictType.LocalNoRowRemoteUpdate : ConflictType.LocalUpdateRemoteUpdate;
            else if (applyType == DmRowState.Deleted)
                dbConflictType = ConflictType.LocalDeleteRemoteDelete;

            // if we found metadata and it's a tombstone, so it's an update conflict
            if (destinationRow != null && (bool)destinationRow["sync_row_is_tombstone"] == true)
                dbConflictType = ConflictType.LocalDeleteRemoteUpdate;

            // TODO
            //dbConflictType == ConflictType.LocalDeleteRemoteUpdate; -- OK
            //dbConflictType == ConflictType.LocalUpdateRemoteDelete; -- NOK

            var conflict = new SyncConflict(dbConflictType);
            conflict.AddRemoteRow(dmRow);
            if (destinationRow != null)
                conflict.AddLocalRow(destinationRow);

            dmTableSelected.Clear();

            return conflict;
        }

        /// <summary>
        /// Get if the error is a primarykey exception
        /// </summary>
        public abstract bool IsPrimaryKeyViolation(Exception Error);

        /// <summary>
        /// Get a command and set parameters
        /// </summary>
        public abstract void SetCommandSessionParameters(DbCommand command, ScopeConfigDataAdapter config);

        /// <summary>
        /// Execute a batch command
        /// </summary>
        public abstract void ExecuteBatchCommand(DbCommand cmd, DmTable applyTable, DmTable failedRows, ScopeInfo scope);

        void AddSchemaForFailedRowsTable(DmTable applyTable, DmTable failedRows)
        {
            if (failedRows.Columns.Count == 0)
            {
                foreach (string rowIdColumn in RowIdColumns)
                    failedRows.Columns.Add(applyTable.Columns[rowIdColumn].ColumnName, applyTable.Columns[rowIdColumn].DataType);

                DmColumn[] keys = new DmColumn[RowIdColumns.Count];

                for (int i = 0; i < RowIdColumns.Count; i++)
                    keys[i] = failedRows.columns[i];

                failedRows.PrimaryKey = new DmKey(keys);
            }
        }

        /// <summary>
        /// Handle a conflict
        /// </summary>
        internal ChangeApplicationAction HandleConflict(SyncConflict conflict, ScopeInfo scope, out DmRow finalRow)
        {
            finalRow = null;
            // Default beahvior : Server wins
            ApplyAction conflictApplyAction = ApplyAction.Continue;

            // Default behavior
            if (this.ConflictActionInvoker == null)
                conflictApplyAction = ApplyAction.Continue;
            else
                conflictApplyAction = this.ConflictActionInvoker(conflict, this.Connection, this.Transaction);

            // Default behavior and an error occured
            if (conflictApplyAction == ApplyAction.Rollback)
            {
                Logger.Current.Info("Rollback all operation");

                return ChangeApplicationAction.RollbackTransaction;
            }

            // Server wins
            if (conflictApplyAction == ApplyAction.Continue)
            {
                Logger.Current.Info("Local Wins, update metadata");

                if (conflict.LocalChange.Rows != null && conflict.LocalChange.Rows.Count > 0)
                {
                    var localRow = conflict.LocalChange.Rows[0];
                    // TODO : Différencier le timestamp de mise à jour ou de création
                    var rowsApplied = this.InsertOrUpdateMetadatas(this.UpdateMetadataCommand, localRow, scope);

                    if (rowsApplied < 1)
                        throw new Exception("No metadatas rows found, can't update the server side");

                    finalRow = localRow;

                    return ChangeApplicationAction.Continue;
                }

               // tableProgress.ChangesFailed += 1;
                return ChangeApplicationAction.RollbackTransaction;
            }

            // We gonna apply with force the remote line
            if (conflictApplyAction == ApplyAction.RetryWithForceWrite)
            {
                if (conflict.RemoteChange.Rows.Count == 0)
                {
                    Logger.Current.Error("Cant find a remote row");
                    return ChangeApplicationAction.RollbackTransaction;
                }

                var row = conflict.RemoteChange.Rows[0];
                bool operationComplete = false;

                if (conflict.Type == ConflictType.LocalNoRowRemoteUpdate || conflict.Type == ConflictType.LocalNoRowRemoteInsert)
                    operationComplete = this.ApplyInsert(row);
                else if (applyType == DmRowState.Added)
                    operationComplete = this.ApplyInsert(row);
                else if (applyType == DmRowState.Modified)
                    operationComplete = this.ApplyUpdate(row, scope, true);

                var rowsApplied = this.InsertOrUpdateMetadatas(this.InsertMetadataCommand, row, scope);
                if (rowsApplied < 1)
                    throw new Exception("No metadatas rows found, can't update the server side");

                finalRow = row;

                //After a force update, there is a problem, so raise exception
                if (!operationComplete)
                {
                    var ex = $"Can't force operation for applyType {applyType}";
                    Logger.Current.Error(ex);
                    finalRow = null;
                    return ChangeApplicationAction.RollbackTransaction;
                }

                // tableProgress.ChangesApplied += 1;
                return ChangeApplicationAction.Continue;
            }

            return ChangeApplicationAction.RollbackTransaction;

        }

        /// <summary>
        /// Create a typed adapter
        /// </summary>
        internal DbCommand BuildCommandForProc(string procName)
        {
            var command = this.Connection.CreateCommand();
            command.CommandText = procName;
            command.CommandType = CommandType.StoredProcedure;
            command.Connection = this.Connection;
            this.SetCommandSessionParameters(command, this.ScopeConfigDataAdapter);
            return command;
        }

        /// <summary>
        /// Build the adapter with the scope config containing every tables / informations
        /// </summary>
        public virtual void BuildAdapter(DbConnection connection, ScopeConfigDataAdapter config)
        {
            this.Connection = connection;
            this.ScopeConfigDataAdapter = config;

            ObjectNameParser objectNameParser = new ObjectNameParser(config.TableName);
            ObjectNameParser objectNameParser1 = new ObjectNameParser(config.GlobalTableName);

            this.RemoteTableName = objectNameParser1.UnquotedString;

            foreach (var column in config.Columns)
            {
                if (!column.IsPrimaryKey)
                    continue;
                this.RowIdColumns.Add(column.UnquotedName);
            }

            this.SelectIncrementalChangesCommand = BuildCommandForProc(config.SelectChangesProcName);
            this.SelectRowCommand = BuildCommandForProc(config.SelectRowProcName);
            this.InsertCommand = BuildCommandForProc(config.InsertProcName);
            this.UpdateCommand = BuildCommandForProc(config.UpdateProcName);
            this.DeleteCommand = BuildCommandForProc(config.DeleteProcName);
            this.InsertMetadataCommand = BuildCommandForProc(config.InsertMetadataProcName);
            this.UpdateMetadataCommand = BuildCommandForProc(config.UpdateMetadataProcName);
            this.DeleteMetadataCommand = BuildCommandForProc(config.DeleteMetadataProcName);

            // Optional
            if (!string.IsNullOrEmpty(config.BulkInsertProcName))
                this.BulkInsertCommand = BuildCommandForProc(config.BulkInsertProcName);
            if (!string.IsNullOrEmpty(config.BulkUpdateProcName))
                this.BulkUpdateCommand = BuildCommandForProc(config.BulkUpdateProcName);
            if (!string.IsNullOrEmpty(config.BulkDeleteProcName))
                this.BulkDeleteCommand = BuildCommandForProc(config.BulkDeleteProcName);

        }

     
        private void TraceRowInfo(DmRow row, bool succeeded)
        {
            string pKstr = "";
            foreach (string rowIdColumn in RowIdColumns)
            {
                object obj = pKstr;
                object[] item = { obj, rowIdColumn, "=\"", row[rowIdColumn], "\" " };
                pKstr = string.Concat(item);
            }

            string empty = string.Empty;
            switch (applyType)
            {
                case DmRowState.Added:
                    {
                        empty = (succeeded ? "Inserted" : "Failed to insert");
                        Logger.Current.Debug($"{empty} row with PK using bulk apply: {pKstr} on {this.Connection.Database}");
                        return;
                    }
                case DmRowState.Modified:
                    {
                        empty = (succeeded ? "Updated" : "Failed to update");
                        Logger.Current.Debug($"{empty} row with PK using bulk apply: {pKstr} on {this.Connection.Database}");
                        return;
                    }
                case DmRowState.Deleted:
                    {
                        empty = (succeeded ? "Deleted" : "Failed to delete");
                        Logger.Current.Debug($"{empty} row with PK using bulk apply: {pKstr} on {this.Connection.Database}");
                        break;
                    }
                default:
                    {
                        return;
                    }
            }
        }

        /// <summary>
        /// Create a DmTable from a datareader fields
        /// </summary>
        internal DmTable BuildDataTable(DbDataReader enumQueryResults)
        {
            DmTable dataTable = new DmTable(RemoteTableName);

            List<DmColumn> keyColumns = new List<DmColumn>();
            for (int i = 0; i < enumQueryResults.FieldCount; i++)
            {
                var columnName = enumQueryResults.GetName(i);
                var columnType = enumQueryResults.GetFieldType(i);
                DmColumn column = DmColumn.CreateColumn(columnName, columnType);

                if (enumQueryResults.CanGetColumnSchema())
                {
                    DbColumn dbColumn = enumQueryResults.GetColumnSchema()[i];
                    column.AllowDBNull = dbColumn.AllowDBNull.HasValue ? dbColumn.AllowDBNull.Value : false;
                    column.AutoIncrement = dbColumn.IsAutoIncrement.HasValue ? dbColumn.IsAutoIncrement.Value : false;
                }
                if (RowIdColumns.Contains(columnName))
                    keyColumns.Add(column);

                dataTable.Columns.Add(column);

            }
            dataTable.PrimaryKey = new DmKey(keyColumns.ToArray());

            return dataTable;
        }

        /// <summary>
        /// Build a DmTable from the ScopeConfigDataAdapter definition
        /// </summary>
        internal DmTable BuildDataTable()
        {
            DmTable dmTable = new DmTable(RemoteTableName);
            List<DmColumn> keyColumns = new List<DmColumn>();

            foreach (var column in this.ScopeConfigDataAdapter.Columns)
            {
                Type type = SyncDataTypeMapping.GetType(column.Type);
                DmColumn dmColumn = DmColumn.CreateColumn(column.UnquotedName, type);
                dmColumn.AllowDBNull = column.IsNullable;
                dmColumn.AutoIncrement = column.AutoIncrementSeedSpecified;

                if (column.IsPrimaryKey)
                    keyColumns.Add(dmColumn);

                dmTable.Columns.Add(dmColumn);
            }

            dmTable.PrimaryKey = new DmKey(keyColumns.ToArray());

            return dmTable;

        }
    }

}
