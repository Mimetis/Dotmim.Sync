using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Log;
using Dotmim.Sync.Manager;
using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using Dotmim.Sync.Builders;
using System.Diagnostics;
using System.ComponentModel;
using System.Threading.Tasks;
using Dotmim.Sync.Filter;

namespace Dotmim.Sync
{


    /// <summary>
    /// The SyncAdapter is the datasource manager for ONE table
    /// Should be implemented by every database provider and provide every SQL action
    /// </summary>
    public abstract class DbSyncAdapter
    {
        private const int BATCH_SIZE = 1000;

        /// <summary>
        /// Gets the table description, a dmTable with no rows
        /// </summary>
        public DmTable TableDescription { get; private set; }

        /// <summary>
        /// Get or Set the current step (could be only Added, Modified, Deleted)
        /// </summary>
        internal DmRowState ApplyType { get; set; }

        /// <summary>
        /// Get if the error is a primarykey exception
        /// </summary>
        public abstract bool IsPrimaryKeyViolation(Exception exception);

        /// <summary>
        /// Gets if the error is a unique key constraint exception
        /// </summary>
        public abstract bool IsUniqueKeyViolation(Exception exception);

        /// <summary>
        /// Gets a command from the current adapter
        /// </summary>
        public abstract DbCommand GetCommand(DbCommandType commandType, IEnumerable<FilterClause> filters = null);

        /// <summary>
        /// Set parameters on a command
        /// </summary>
        public abstract void SetCommandParameters(DbCommandType commandType, DbCommand command, IEnumerable<FilterClause> filters = null);

        /// <summary>
        /// Execute a batch command
        /// </summary>
        public abstract void ExecuteBatchCommand(DbCommand cmd, DmView applyTable, DmTable failedRows, ScopeInfo scope);

        /// <summary>
        /// Gets the current connection. could be opened
        /// </summary>
        public abstract DbConnection Connection { get; }

        /// <summary>
        /// Gets the current transaction. could be null
        /// </summary>
        public abstract DbTransaction Transaction { get; }

        /// <summary>
        /// Create a Sync Adapter
        /// </summary>
        public DbSyncAdapter(DmTable tableDescription)
        {
            this.TableDescription = tableDescription;
        }


        /// <summary>
        /// Set command parameters value mapped to Row
        /// </summary>
        internal void SetColumnParametersValues(DbCommand command, DmRow row)
        {
            foreach (DbParameter parameter in command.Parameters)
            {
                // foreach parameter, check if we have a column 
                if (!string.IsNullOrEmpty(parameter.SourceColumn))
                {
                    if (row.Table.Columns.Contains(parameter.SourceColumn))
                    {
                        object value = null;
                        if (row.RowState == DmRowState.Deleted)
                            value = row[parameter.SourceColumn, DmRowVersion.Original];
                        else
                            value = row[parameter.SourceColumn];

                        DbManager.SetParameterValue(command, parameter.ParameterName, value);
                    }
                }

            }

            // return value
            var syncRowCountParam = DbManager.GetParameter(command, "sync_row_count");

            if (syncRowCountParam != null)
                syncRowCountParam.Direction = ParameterDirection.Output;
        }

        /// <summary>
        /// Insert or update a metadata line
        /// </summary>
        internal bool InsertOrUpdateMetadatas(DbCommand command, DmRow row, Guid? fromScopeId)
        {
            int rowsApplied = 0;

            if (command == null)
                throw new Exception("Missing command for apply metadata ");

            // Set the id parameter
            this.SetColumnParametersValues(command, row);

            DmRowVersion version = row.RowState == DmRowState.Deleted ? DmRowVersion.Original : DmRowVersion.Current;

            long createTimestamp = row["create_timestamp", version] != null ? Convert.ToInt64(row["create_timestamp", version]) : 0;
            long updateTimestamp = row["update_timestamp", version] != null ? Convert.ToInt64(row["update_timestamp", version]) : 0;
            Guid? create_scope_id = row["create_scope_id", version] != null ? (Guid?)(row["create_scope_id", version]) : null;
            Guid? update_scope_id = row["update_scope_id", version] != null ? (Guid?)(row["update_scope_id", version]) : null;

            // Override create and update scope id to reflect who change the value
            // if it's an update, the createscope is staying the same (because not present in dbCommand)
            Guid? createScopeId = fromScopeId.HasValue ? fromScopeId : create_scope_id;
            Guid? updateScopeId = fromScopeId.HasValue ? fromScopeId : update_scope_id;

            // some proc stock does not differentiate update_scope_id and create_scope_id and use sync_scope_id
            DbManager.SetParameterValue(command, "sync_scope_id", createScopeId);
            // else they use create_scope_id and update_scope_id
            DbManager.SetParameterValue(command, "create_scope_id", createScopeId);
            DbManager.SetParameterValue(command, "update_scope_id", updateScopeId);

            // 2 choices for getting deleted
            bool isTombstone = false;

            if (row.RowState == DmRowState.Deleted)
                isTombstone = true;

            if (row.Table != null && row.Table.Columns.Contains("sync_row_is_tombstone"))
            {

                var rowValue = row["sync_row_is_tombstone", version] != null && row["sync_row_is_tombstone", version] != DBNull.Value ? row["sync_row_is_tombstone", version] : false;

                if (rowValue.GetType() == typeof(bool))
                {
                    isTombstone = (bool)rowValue;
                }
                else
                {
                    isTombstone = Convert.ToInt64(rowValue) > 0;
                }
                //else
                //{
                //    string rowValueString = rowValue.ToString();
                //    if (Boolean.TryParse(rowValueString.Trim(), out Boolean v))
                //    {
                //        isTombstone = v;
                //    }
                //    else if (rowValueString.Trim() == "0")
                //    {
                //        isTombstone = false;
                //    }
                //    else if (rowValueString.Trim() == "1")
                //    {
                //        isTombstone = true;
                //    }
                //    else
                //    {
                //        var converter = TypeDescriptor.GetConverter(typeof(bool));
                //        if (converter.CanConvertFrom(rowValue.GetType()))
                //        {
                //            isTombstone = (bool)converter.ConvertFrom(rowValue);
                //        }
                //        else
                //        {
                //            isTombstone = false;
                //        }

                //    }
                //}
            }

            DbManager.SetParameterValue(command, "sync_row_is_tombstone", isTombstone ? 1 : 0);
            DbManager.SetParameterValue(command, "create_timestamp", createTimestamp);
            DbManager.SetParameterValue(command, "update_timestamp", updateTimestamp);

            var alreadyOpened = Connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    Connection.Open();

                if (Transaction != null)
                    command.Transaction = Transaction;

                rowsApplied = command.ExecuteNonQuery();
            }
            finally
            {
                // Close Connection
                if (!alreadyOpened)
                    Connection.Close();
            }
            return rowsApplied > 0;
        }

        /// <summary>
        /// Try to get a source row
        /// </summary>
        /// <returns></returns>
        internal DmTable GetRow(DmRow sourceRow)
        {
            // Get the row in the local repository
            using (DbCommand selectCommand = GetCommand(DbCommandType.SelectRow))
            {
                // Deriving Parameters
                this.SetCommandParameters(DbCommandType.SelectRow, selectCommand);

                this.SetColumnParametersValues(selectCommand, sourceRow);

                var alreadyOpened = Connection.State == ConnectionState.Open;

                // Open Connection
                if (!alreadyOpened)
                    Connection.Open();

                if (Transaction != null)
                    selectCommand.Transaction = Transaction;

                var dmTableSelected = new DmTable(this.TableDescription.TableName);
                try
                {
                    using (var reader = selectCommand.ExecuteReader())
                        dmTableSelected.Fill(reader);

                    // set the pkey since we will need them later
                    var pkeys = new DmColumn[this.TableDescription.PrimaryKey.Columns.Length];
                    for (int i = 0; i < pkeys.Length; i++)
                    {
                        var pkName = this.TableDescription.PrimaryKey.Columns[i].ColumnName;
                        pkeys[i] = dmTableSelected.Columns.First(dm => this.TableDescription.IsEqual(dm.ColumnName, pkName));
                    }
                    dmTableSelected.PrimaryKey = new DmKey(pkeys);
                }
                finally
                {
                    // Close Connection
                    if (!alreadyOpened)
                        Connection.Close();
                }

                return dmTableSelected;
            }

        }

        /// <summary>
        /// Launch apply bulk changes
        /// </summary>
        /// <returns></returns>
        public int ApplyBulkChanges(DmView dmChanges, ScopeInfo fromScope, List<SyncConflict> conflicts)
        {
            DbCommand bulkCommand = null;

            if (this.ApplyType == DmRowState.Added)
            {
                bulkCommand = this.GetCommand(DbCommandType.BulkInsertRows);
                this.SetCommandParameters(DbCommandType.BulkInsertRows, bulkCommand);
            }
            else if (this.ApplyType == DmRowState.Modified)
            {
                bulkCommand = this.GetCommand(DbCommandType.BulkUpdateRows);
                this.SetCommandParameters(DbCommandType.BulkUpdateRows, bulkCommand);
            }
            else if (this.ApplyType == DmRowState.Deleted)
            {
                bulkCommand = this.GetCommand(DbCommandType.BulkDeleteRows);
                this.SetCommandParameters(DbCommandType.BulkDeleteRows, bulkCommand);
            }
            else
            {
                throw new Exception("DmRowState not valid during ApplyBulkChanges operation");
            }

            if (Transaction != null && Transaction.Connection != null)
                bulkCommand.Transaction = Transaction;

            //DmTable batchDmTable = dmChanges.Table.Clone();
            DmTable failedDmtable = new DmTable { Culture = CultureInfo.InvariantCulture };

            // Create the schema for failed rows (just add the Primary keys)
            this.AddSchemaForFailedRowsTable(failedDmtable);

            // Since the update and create timestamp come from remote, change name for the bulk operations
            var update_timestamp_column = dmChanges.Table.Columns["update_timestamp"].ColumnName;
            dmChanges.Table.Columns["update_timestamp"].ColumnName = "update_timestamp";
            var create_timestamp_column = dmChanges.Table.Columns["create_timestamp"].ColumnName;
            dmChanges.Table.Columns["create_timestamp"].ColumnName = "create_timestamp";

            // Make some parts of BATCH_SIZE 
            for (int step = 0; step < dmChanges.Count; step += BATCH_SIZE)
            {
                // get upper bound max value
                var taken = step + BATCH_SIZE >= dmChanges.Count ? dmChanges.Count - step : BATCH_SIZE;

                using (var dmStepChanges = dmChanges.Take(step, taken))
                {
                    // execute the batch, through the provider
                    ExecuteBatchCommand(bulkCommand, dmStepChanges, failedDmtable, fromScope);
                }
            }

            // Disposing command
            if (bulkCommand != null)
            {
                bulkCommand.Dispose();
                bulkCommand = null;
            }

            // Since the update and create timestamp come from remote, change name for the bulk operations
            dmChanges.Table.Columns["update_timestamp"].ColumnName = update_timestamp_column;
            dmChanges.Table.Columns["create_timestamp"].ColumnName = create_timestamp_column;

            //foreach (var dmRow in dmChanges)
            //{
            //    // Cancel the delete state to be able to get the row, more simplier
            //    if (applyType == DmRowState.Deleted)
            //        dmRow.RejectChanges();

            //    // Load the datarow
            //    DmRow dataRow = batchDmTable.LoadDataRow(dmRow.ItemArray, false);

            //    // Apply the delete
            //    // is it mandatory ?
            //    if (applyType == DmRowState.Deleted)
            //        dmRow.Delete();

            //    batchCount++;
            //    rowCount++;

            //    if (batchCount < BATCH_SIZE && rowCount < dmChanges.Count)
            //        continue;

            //    // Since the update and create timestamp come from remote, change name for the bulk operations
            //    batchDmTable.Columns["update_timestamp"].ColumnName = "update_timestamp";
            //    batchDmTable.Columns["create_timestamp"].ColumnName = "create_timestamp";

            //    // execute the batch, through the provider
            //    ExecuteBatchCommand(bulkCommand, batchDmTable, failedDmtable, fromScope);

            //    // Clear the batch
            //    batchDmTable.Clear();

            //    // Recreate a Clone
            //    // TODO : Evaluate if it's necessary
            //    batchDmTable = dmChanges.Table.Clone();
            //    batchCount = 0;
            //}

            // Update table progress 
            //tableProgress.ChangesApplied = dmChanges.Count - failedDmtable.Rows.Count;

            if (failedDmtable.Rows.Count == 0)
                return dmChanges.Count;

            // Check all conflicts raised
            var failedFilter = new Predicate<DmRow>(row =>
            {
                if (row.RowState == DmRowState.Deleted)
                    return failedDmtable.FindByKey(row.GetKeyValues(DmRowVersion.Original)) != null;
                else
                    return failedDmtable.FindByKey(row.GetKeyValues()) != null;
            });

            // New View
            var dmFailedRows = new DmView(dmChanges, failedFilter);

            // Generate a conflict and add it
            foreach (var dmFailedRow in dmFailedRows)
                conflicts.Add(GetConflict(dmFailedRow));

            int failedRows = dmFailedRows.Count;

            // Dispose the failed view
            dmFailedRows.Dispose();
            dmFailedRows = null;

            // return applied rows - failed rows (generating a conflict)
            return dmChanges.Count - failedRows;
        }


        private void UpdateMetadatas(DbCommandType dbCommandType, DmRow dmRow, ScopeInfo scope)
        {
            using (var dbCommand = this.GetCommand(dbCommandType))
            {
                this.SetCommandParameters(dbCommandType, dbCommand);
                this.InsertOrUpdateMetadatas(dbCommand, dmRow, scope.Id);
            }
        }

        /// <summary>
        /// Try to apply changes on the server.
        /// Internally will call ApplyInsert / ApplyUpdate or ApplyDelete
        /// </summary>
        /// <param name="dmChanges">Changes from remote</param>
        /// <returns>every lines not updated on the server side</returns>
        internal int ApplyChanges(DmView dmChanges, ScopeInfo scope, List<SyncConflict> conflicts)
        {
            int appliedRows = 0;

            foreach (var dmRow in dmChanges)
            {
                bool operationComplete = false;

                try
                {
                    if (ApplyType == DmRowState.Added)
                    {
                        operationComplete = this.ApplyInsert(dmRow, scope, false);
                        if (operationComplete)
                            UpdateMetadatas(DbCommandType.InsertMetadata, dmRow, scope);
                    }
                    else if (ApplyType == DmRowState.Modified)
                    {
                        operationComplete = this.ApplyUpdate(dmRow, scope, false);
                        if (operationComplete)
                            UpdateMetadatas(DbCommandType.UpdateMetadata, dmRow, scope);
                    }
                    else if (ApplyType == DmRowState.Deleted)
                    {
                        operationComplete = this.ApplyDelete(dmRow, scope, false);
                        if (operationComplete)
                            UpdateMetadatas(DbCommandType.UpdateMetadata, dmRow, scope);
                    }

                    if (operationComplete)
                        // if no pb, increment then go to next row
                        appliedRows++;
                    else
                        // Generate a conflict and add it
                        conflicts.Add(GetConflict(dmRow));

                }
                catch (Exception ex)
                {
                    if (this.IsUniqueKeyViolation(ex))
                    {

                        // Generate the conflict
                        var conflict = new SyncConflict(ConflictType.UniqueKeyConstraint);

                        // Add the row as Remote row
                        conflict.AddRemoteRow(dmRow);

                        // Get the local row
                        var localTable = GetRow(dmRow);
                        if (localTable.Rows.Count > 0)
                            conflict.AddLocalRow(localTable.Rows[0]);

                        conflicts.Add(conflict);

                        localTable.Clear();
                    }
                    else
                    {
                        throw;
                    }
                }
            }

            return appliedRows;
        }

        /// <summary>
        /// Apply a single insert in the current data source
        /// </summary>
        internal bool ApplyInsert(DmRow row, ScopeInfo scope, bool forceWrite)
        {
            using (var command = this.GetCommand(DbCommandType.InsertRow))
            {

                // Deriving Parameters
                this.SetCommandParameters(DbCommandType.InsertRow, command);

                // Set the parameters value from row
                this.SetColumnParametersValues(command, row);

                // Set the special parameters for insert
                AddCommonParametersValues(command, scope.Id, scope.Timestamp, false, forceWrite);

                var alreadyOpened = Connection.State == ConnectionState.Open;

                // Open Connection
                if (!alreadyOpened)
                    Connection.Open();

                int rowInsertedCount = 0;
                try
                {
                    if (Transaction != null)
                        command.Transaction = Transaction;

                    rowInsertedCount = command.ExecuteNonQuery();
                }

                finally
                {
                    // Open Connection
                    if (!alreadyOpened)
                        Connection.Close();

                }

                return rowInsertedCount > 0;
            }
        }

        /// <summary>
        /// Apply a delete on a row
        /// </summary>
        internal bool ApplyDelete(DmRow sourceRow, ScopeInfo scope, bool forceWrite)
        {
            using (var command = this.GetCommand(DbCommandType.DeleteRow))
            {
                // Deriving Parameters
                this.SetCommandParameters(DbCommandType.DeleteRow, command);

                // Set the parameters value from row
                this.SetColumnParametersValues(command, sourceRow);

                // Set the special parameters for update
                this.AddCommonParametersValues(command, scope.Id, scope.Timestamp, true, forceWrite);

                var alreadyOpened = Connection.State == ConnectionState.Open;

                int rowInsertedCount = 0;
                try
                {
                    // OPen Connection
                    if (!alreadyOpened)
                        Connection.Open();

                    if (Transaction != null)
                        command.Transaction = Transaction;

                    rowInsertedCount = command.ExecuteNonQuery();

                }
                finally
                {
                    if (!alreadyOpened)
                        Connection.Close();
                }

                return rowInsertedCount > 0;
            }
        }

        /// <summary>
        /// Apply a single update in the current datasource. if forceWrite, override conflict situation and force the update
        /// </summary>
        internal bool ApplyUpdate(DmRow sourceRow, ScopeInfo scope, bool forceWrite)
        {

            bool hasUpdatableColumns = true;

            if (sourceRow.Table != null)
                hasUpdatableColumns = sourceRow.Table.MutableColumnsAndNotAutoInc.Any( c => c.ColumnName.ToLowerInvariant() != "create_timestamp" && c.ColumnName.ToLowerInvariant() != "update_timestamp" && c.ColumnName.ToLowerInvariant() != "create_scope_id" && c.ColumnName.ToLowerInvariant() != "update_scope_id");

            if (!hasUpdatableColumns)
                return false;

            using (var command = this.GetCommand(DbCommandType.UpdateRow))
            {
                // Deriving Parameters
                this.SetCommandParameters(DbCommandType.UpdateRow, command);

                // Set the parameters value from row
                this.SetColumnParametersValues(command, sourceRow);

                // Set the special parameters for update
                AddCommonParametersValues(command, scope.Id, scope.Timestamp, false, forceWrite);

                var alreadyOpened = Connection.State == ConnectionState.Open;

                int rowInsertedCount = 0;
                try
                {
                    if (!alreadyOpened)
                        Connection.Open();

                    if (Transaction != null)
                        command.Transaction = Transaction;

                    rowInsertedCount = command.ExecuteNonQuery();
                }
                finally
                {
                    if (!alreadyOpened)
                        Connection.Close();
                }
                return rowInsertedCount > 0;
            }
        }

        /// <summary>
        /// Reset a table, deleting rows from table and tracking_table
        /// </summary>
        internal bool ResetTable(DmTable tableDescription)
        {
            using (var command = this.GetCommand(DbCommandType.Reset))
            {
                // Deriving Parameters
                this.SetCommandParameters(DbCommandType.Reset, command);

                var alreadyOpened = Connection.State == ConnectionState.Open;

                int rowCount = 0;
                try
                {
                    if (!alreadyOpened)
                        Connection.Open();

                    if (Transaction != null)
                        command.Transaction = Transaction;

                    rowCount = command.ExecuteNonQuery();
                }
                finally
                {
                    if (!alreadyOpened)
                        Connection.Close();
                }
                return rowCount > 0;
            }
        }


        /// <summary>
        /// Reset a table, deleting rows from table and tracking_table
        /// </summary>
        internal bool DisableConstraints()
        {
            using (var command = this.GetCommand(DbCommandType.DisableConstraints))
            {
                var alreadyOpened = Connection.State == ConnectionState.Open;

                int rowCount = 0;
                try
                {
                    if (!alreadyOpened)
                        Connection.Open();

                    if (Transaction != null)
                        command.Transaction = Transaction;

                    rowCount = command.ExecuteNonQuery();
                }
                finally
                {
                    if (!alreadyOpened)
                        Connection.Close();
                }
                return rowCount > 0;
            }
        }


        /// <summary>
        /// Reset a table, deleting rows from table and tracking_table
        /// </summary>
        internal bool EnableConstraints()
        {
            using (var command = this.GetCommand(DbCommandType.EnableConstraints))
            {
                var alreadyOpened = Connection.State == ConnectionState.Open;

                int rowCount = 0;
                try
                {
                    if (!alreadyOpened)
                        Connection.Open();

                    if (Transaction != null)
                        command.Transaction = Transaction;

                    rowCount = command.ExecuteNonQuery();
                }
                finally
                {
                    if (!alreadyOpened)
                        Connection.Close();
                }
                return rowCount > 0;
            }
        }



        /// <summary>
        /// Add common parameters which could be part of the command
        /// if not found, no set done
        /// </summary>
        private void AddCommonParametersValues(DbCommand command, Guid id, long lastTimestamp, bool isDeleted, bool forceWrite)
        {
            // Dotmim.Sync parameters
            DbManager.SetParameterValue(command, "sync_force_write", (forceWrite ? 1 : 0));
            DbManager.SetParameterValue(command, "sync_min_timestamp", lastTimestamp);
            DbManager.SetParameterValue(command, "sync_scope_id", id);
            DbManager.SetParameterValue(command, "sync_row_is_tombstone", isDeleted);
        }

        /// <summary>
        /// We have a conflict, try to get the source row and generate a conflict
        /// </summary>
        private SyncConflict GetConflict(DmRow dmRow)
        {
            DmRow localRow = null;

            // Problem during operation
            // Getting the row involved in the conflict 
            var localTable = GetRow(dmRow);

            ConflictType dbConflictType = ConflictType.ErrorsOccurred;

            // Can't find the row on the server datastore
            if (localTable.Rows.Count == 0)
            {
                if (ApplyType == DmRowState.Added)
                    dbConflictType = ConflictType.RemoteInsertLocalNoRow;
                else if (ApplyType == DmRowState.Modified)
                    dbConflictType = ConflictType.RemoteUpdateLocalNoRow;
                else if (ApplyType == DmRowState.Deleted)
                    dbConflictType = ConflictType.RemoteDeleteLocalNoRow;
            }
            else
            {
                // We have a problem and found the row on the server side
                localRow = localTable.Rows[0];

                var isTombstone = Convert.ToBoolean(localRow["sync_row_is_tombstone"]);

                // the row on local is deleted
                if (isTombstone)
                {
                    if (ApplyType == DmRowState.Added)
                        dbConflictType = ConflictType.RemoteInsertLocalDelete;
                    else if (ApplyType == DmRowState.Modified)
                        dbConflictType = ConflictType.RemoteUpdateLocalDelete;
                    else if (ApplyType == DmRowState.Deleted)
                        dbConflictType = ConflictType.RemoteDeleteLocalDelete;
                }
                else
                {
                    var createTimestamp = localRow["create_timestamp"] != DBNull.Value ? (long)localRow["create_timestamp"] : 0L;
                    var updateTimestamp = localRow["update_timestamp"] != DBNull.Value ? (long)localRow["update_timestamp"] : 0L;
                    switch (ApplyType)
                    {
                        case DmRowState.Added:
                            dbConflictType = updateTimestamp == 0 ? ConflictType.RemoteInsertLocalInsert : ConflictType.RemoteInsertLocalUpdate;
                            break;
                        case DmRowState.Modified:
                            dbConflictType = updateTimestamp == 0 ? ConflictType.RemoteUpdateLocalInsert : ConflictType.RemoteUpdateLocalUpdate;
                            break;
                        case DmRowState.Deleted:
                            dbConflictType = updateTimestamp == 0 ? ConflictType.RemoteDeleteLocalInsert : ConflictType.RemoteDeleteLocalUpdate;
                            break;
                    }
                }
            }
            // Generate the conflict
            var conflict = new SyncConflict(dbConflictType);
            conflict.AddRemoteRow(dmRow);

            if (localRow != null)
                conflict.AddLocalRow(localRow);

            localTable.Clear();

            return conflict;
        }

        /// <summary>
        /// Adding failed rows when used by a bulk operation
        /// </summary>
        private void AddSchemaForFailedRowsTable(DmTable failedRows)
        {
            if (failedRows.Columns.Count == 0)
            {
                foreach (var rowIdColumn in this.TableDescription.PrimaryKey.Columns)
                    failedRows.Columns.Add(rowIdColumn.ColumnName, rowIdColumn.DataType);

                DmColumn[] keys = new DmColumn[this.TableDescription.PrimaryKey.Columns.Length];

                for (int i = 0; i < this.TableDescription.PrimaryKey.Columns.Length; i++)
                    keys[i] = failedRows.Columns[i];

                failedRows.PrimaryKey = new DmKey(keys);
            }
        }


        /// <summary>
        /// Trace info
        /// </summary>
        private void TraceRowInfo(DmRow row, bool succeeded)
        {
            string pKstr = "";
            foreach (var rowIdColumn in this.TableDescription.PrimaryKey.Columns)
            {
                object obj = pKstr;
                object[] item = { obj, rowIdColumn.ColumnName, "=\"", row[rowIdColumn], "\" " };
                pKstr = string.Concat(item);
            }

            string empty = string.Empty;
            switch (ApplyType)
            {
                case DmRowState.Added:
                    {
                        empty = (succeeded ? "Inserted" : "Failed to insert");
                        Debug.WriteLine($"{empty} row with PK using bulk apply: {pKstr} on {Connection.Database}");
                        return;
                    }
                case DmRowState.Modified:
                    {
                        empty = (succeeded ? "Updated" : "Failed to update");
                        Debug.WriteLine($"{empty} row with PK using bulk apply: {pKstr} on {Connection.Database}");
                        return;
                    }
                case DmRowState.Deleted:
                    {
                        empty = (succeeded ? "Deleted" : "Failed to delete");
                        Debug.WriteLine($"{empty} row with PK using bulk apply: {pKstr} on {Connection.Database}");
                        break;
                    }
                default:
                    {
                        return;
                    }
            }
        }
    }
}
