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
using System.Reflection;

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
        /// Gets the table description
        /// </summary>
        public SyncTable TableDescription { get; private set; }

        /// <summary>
        /// Get or Set the current step (could be only Added, Modified, Deleted)
        /// </summary>
        internal DataRowState ApplyType { get; set; }

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
        public abstract DbCommand GetCommand(DbCommandType commandType, IEnumerable<SyncFilter> filters = null);

        /// <summary>
        /// Set parameters on a command
        /// </summary>
        public abstract void SetCommandParameters(DbCommandType commandType, DbCommand command, IEnumerable<SyncFilter> filters = null);

        /// <summary>
        /// Execute a batch command
        /// </summary>
        public abstract void ExecuteBatchCommand(DbCommand cmd, IEnumerable<SyncRow> arrayItems, SyncTable schemaChangesTable, SyncTable failedRows, Guid applyingScopeId, long lastTimestamp);

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
        public DbSyncAdapter(SyncTable tableDescription) => this.TableDescription = tableDescription;


        /// <summary>
        /// Set command parameters value mapped to Row
        /// </summary>
        internal void SetColumnParametersValues(DbCommand command, SyncRow row)
        {
            if (row.Table == null)
                throw new ArgumentException("Schema table columns does not correspond to row values");

            var schemaTable = row.Table;

            foreach (DbParameter parameter in command.Parameters)
            {
                // foreach parameter, check if we have a column 
                if (!string.IsNullOrEmpty(parameter.SourceColumn))
                {
                    var column = schemaTable.Columns.FirstOrDefault(parameter.SourceColumn);
                    if (column != null)
                    {
                        object value = row[column];
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
        internal bool InsertOrUpdateMetadatas(DbCommand command, SyncRow row, Guid? fromScopeId)
        {
            if (row.Table == null)
                throw new ArgumentException("Schema table columns does not exist");

            if (command == null)
                throw new Exception("Missing command for apply metadata ");

            int rowsApplied = 0;

            var schemaTable = row.Table;

            // Set the id parameter
            this.SetColumnParametersValues(command, row);

            // 2 choices for getting deleted
            bool isTombstone = false;

            // Firstly check the state
            isTombstone = row.RowState == DataRowState.Deleted;

            // TODO : Secondly check the sync is tombstone column. Should we do this ?
            var isTombstoneColumn = schemaTable.Columns.FirstOrDefault("sync_row_is_tombstone");

            if (isTombstoneColumn != null)
            {
                var rowValue = row[schemaTable.Columns.IndexOf(isTombstoneColumn)];

                if (rowValue == null)
                    isTombstone = false;
                else
                    isTombstone = rowValue.GetType() == typeof(bool) ? (bool)rowValue : Convert.ToInt64(rowValue) > 0;
            }

            DbManager.SetParameterValue(command, "sync_row_is_tombstone", isTombstone ? 1 : 0);
            DbManager.SetParameterValue(command, "sync_scope_id", fromScopeId);

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
        internal SyncRow GetRow(SyncRow primaryKeyRow, SyncTable schema)
        {
            // Get the row in the local repository
            using (var selectCommand = GetCommand(DbCommandType.SelectRow))
            {
                // Deriving Parameters
                this.SetCommandParameters(DbCommandType.SelectRow, selectCommand);

                // set the primary keys columns as parameters
                this.SetColumnParametersValues(selectCommand, primaryKeyRow);

                var alreadyOpened = Connection.State == ConnectionState.Open;

                // Open Connection
                if (!alreadyOpened)
                    Connection.Open();

                if (Transaction != null)
                    selectCommand.Transaction = Transaction;

                // Create a select table based on the schema in parameter + scope columns
                var changesSet = schema.Schema.Clone(false);
                var selectTable = CreateChangesTable(schema, changesSet);
                SyncRow syncRow = null;
                try
                {
                    using (var dataReader = selectCommand.ExecuteReader())
                    {
                        while (dataReader.Read())
                        {
                            // Create a new empty row
                            syncRow = selectTable.NewRow();

                            for (var i = 0; i < dataReader.FieldCount; i++)
                            {
                                var columnName = dataReader.GetName(i);

                                // if we have the tombstone value, do not add it to the table
                                if (columnName == "sync_row_is_tombstone")
                                {
                                    var isTombstone = Convert.ToInt64(dataReader.GetValue(i)) > 0;
                                    syncRow.RowState = isTombstone ? DataRowState.Deleted : DataRowState.Unchanged;
                                    continue;
                                }

                                var columnValueObject = dataReader.GetValue(i);
                                var columnValue = columnValueObject == DBNull.Value ? null : columnValueObject;
                                syncRow[columnName] = columnValue;
                            }
                        }
                    }
                }
                finally
                {
                    // Close Connection
                    if (!alreadyOpened)
                        Connection.Close();
                }

                if (syncRow != null)
                    syncRow.RowState = primaryKeyRow.RowState;

                return syncRow;
            }

        }

        /// <summary>
        /// Launch apply bulk changes
        /// </summary>
        /// <returns></returns>
        public int ApplyBulkChanges(SyncTable changesTable, Guid applyingScopeId, long lastTimestamp, List<SyncConflict> conflicts)
        {
            DbCommand bulkCommand;
            if (this.ApplyType == DataRowState.Modified)
            {
                bulkCommand = this.GetCommand(DbCommandType.BulkUpdateRows);
                this.SetCommandParameters(DbCommandType.BulkUpdateRows, bulkCommand);
            }
            else if (this.ApplyType == DataRowState.Deleted)
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

            // Create
            var failedPrimaryKeysTable = changesTable.Schema.Clone().Tables[changesTable.TableName, changesTable.SchemaName];
            //AddSchemaForFailedRowsTable(failedPrimaryKeysTable);

            // get the items count
            var itemsArrayCount = changesTable.Rows.Count;

            // Make some parts of BATCH_SIZE 
            for (int step = 0; step < itemsArrayCount; step += BATCH_SIZE)
            {
                // get upper bound max value
                var taken = step + BATCH_SIZE >= itemsArrayCount ? itemsArrayCount - step : BATCH_SIZE;

                var arrayStepChanges = changesTable.Rows.ToList().Skip(step).Take(taken);

                // execute the batch, through the provider
                ExecuteBatchCommand(bulkCommand, arrayStepChanges, changesTable, failedPrimaryKeysTable, applyingScopeId, lastTimestamp);
            }

            // Disposing command
            if (bulkCommand != null)
                bulkCommand.Dispose();

            if (failedPrimaryKeysTable.Rows.Count == 0)
                return itemsArrayCount;

            // Get local and remote row and create the conflict object
            foreach (var failedRow in failedPrimaryKeysTable.Rows)
            {
                failedRow.RowState = this.ApplyType;

                // Get the row that caused the problem, from the remote side (client)
                var remoteConflictRows = changesTable.Rows.GetRowsByPrimaryKeys(failedRow);
                if (remoteConflictRows.Count() == 0)
                    throw new Exception("Cant find changes row who is in conflict");
                var remoteConflictRow = remoteConflictRows.ToList()[0];

                var localConflictRow = GetRow(failedRow, changesTable);

                conflicts.Add(GetConflict(remoteConflictRow, localConflictRow));
            }

            // return applied rows minus failed rows
            return itemsArrayCount - failedPrimaryKeysTable.Rows.Count;
        }


        /// <summary>
        /// We have a conflict, try to get the source row and generate a conflict
        /// </summary>
        private SyncConflict GetConflict(SyncRow remoteConflictRow, SyncRow localConflictRow)
        {

            var dbConflictType = ConflictType.ErrorsOccurred;

            // Can't find the row on the server datastore
            if (localConflictRow == null)
            {
                if (ApplyType == DataRowState.Modified)
                    dbConflictType = ConflictType.RemoteUpdateLocalNoRow;
                else if (ApplyType == DataRowState.Deleted)
                    dbConflictType = ConflictType.RemoteDeleteLocalNoRow;
            }
            else
            {
                // the row on local is deleted
                if (localConflictRow.RowState == DataRowState.Deleted)
                {
                    if (ApplyType == DataRowState.Modified)
                        dbConflictType = ConflictType.RemoteUpdateLocalDelete;
                    else if (ApplyType == DataRowState.Deleted)
                        dbConflictType = ConflictType.RemoteDeleteLocalDelete;
                }
                else
                {
                    dbConflictType = ConflictType.RemoteUpdateLocalUpdate;
                }
            }
            // Generate the conflict
            var conflict = new SyncConflict(dbConflictType);
            conflict.AddRemoteRow(remoteConflictRow);

            if (localConflictRow != null)
                conflict.AddLocalRow(localConflictRow);

            return conflict;
        }

        private void UpdateMetadatas(DbCommandType dbCommandType, SyncRow row, Guid applyingScopeId, long lastTimestamp)
        {
            using (var dbCommand = this.GetCommand(dbCommandType))
            {
                this.SetCommandParameters(dbCommandType, dbCommand);
                this.InsertOrUpdateMetadatas(dbCommand, row, applyingScopeId);
            }
        }

        /// <summary>
        /// Try to apply changes on the server.
        /// Internally will call ApplyInsert / ApplyUpdate or ApplyDelete
        /// </summary>
        /// <param name="changes">Changes from remote</param>
        /// <returns>every lines not updated on the server side</returns>
        internal int ApplyChanges(SyncTable changesTable, Guid applyingScopeId, long lastTimestamp, List<SyncConflict> conflicts)
        {
            int appliedRows = 0;

            foreach (var row in changesTable.Rows)
            {
                bool operationComplete = false;

                try
                {
                    if (ApplyType == DataRowState.Modified)
                    {
                        operationComplete = this.ApplyUpdate(row, applyingScopeId, lastTimestamp, false);
                        if (operationComplete)
                        {
                            UpdateMetadatas(DbCommandType.UpdateMetadata, row, applyingScopeId, lastTimestamp);
                        }
                    }
                    else if (ApplyType == DataRowState.Deleted)
                    {
                        operationComplete = this.ApplyDelete(row, applyingScopeId, lastTimestamp, false);
                        if (operationComplete)
                        {
                            UpdateMetadatas(DbCommandType.UpdateMetadata, row, applyingScopeId, lastTimestamp);
                        }
                    }

                    if (operationComplete)
                        // if no pb, increment then go to next row
                        appliedRows++;
                    else
                    {
                        var localConflictRow = GetRow(row, changesTable);
                        conflicts.Add(GetConflict(row, localConflictRow));
                    }

                }
                catch (Exception ex)
                {
                    if (this.IsUniqueKeyViolation(ex))
                    {
                        // Generate the conflict
                        var conflict = new SyncConflict(ConflictType.UniqueKeyConstraint);

                        // Add the row as Remote row
                        conflict.AddRemoteRow(row);

                        // Get the local row
                        var localRow = GetRow(row, changesTable);
                        if (localRow != null)
                            conflict.AddLocalRow(localRow);

                        conflicts.Add(conflict);
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
        /// Apply a delete on a row
        /// </summary>
        internal bool ApplyDelete(SyncRow row, Guid applyingScopeId, long lastTimestamp, bool forceWrite)
        {
            if (row.Table == null)
                throw new ArgumentException("Schema table is not present in the row");

            using (var command = this.GetCommand(DbCommandType.DeleteRow))
            {
                // Deriving Parameters
                this.SetCommandParameters(DbCommandType.DeleteRow, command);

                // Set the parameters value from row
                this.SetColumnParametersValues(command, row);

                // Set the special parameters for update
                this.AddScopeParametersValues(command, applyingScopeId, lastTimestamp, true, forceWrite);

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
        internal bool ApplyUpdate(SyncRow row, Guid applyingScopeId, long lastTimestamp, bool forceWrite)
        {
            if (row.Table == null)
                throw new ArgumentException("Schema table is not present in the row");

            using (var command = this.GetCommand(DbCommandType.UpdateRow))
            {
                // Deriving Parameters
                this.SetCommandParameters(DbCommandType.UpdateRow, command);

                // Set the parameters value from row
                this.SetColumnParametersValues(command, row);

                // Set the special parameters for update
                AddScopeParametersValues(command, applyingScopeId, lastTimestamp, false, forceWrite);

                var alreadyOpened = Connection.State == ConnectionState.Open;

                int rowUpdatedCount = 0;
                try
                {
                    if (!alreadyOpened)
                        Connection.Open();

                    if (Transaction != null)
                        command.Transaction = Transaction;

                    rowUpdatedCount = command.ExecuteNonQuery();
                }
                finally
                {
                    if (!alreadyOpened)
                        Connection.Close();
                }
                return rowUpdatedCount > 0;
            }
        }

        /// <summary>
        /// Reset a table, deleting rows from table and tracking_table
        /// </summary>
        internal bool ResetTable()
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
        private void AddScopeParametersValues(DbCommand command, Guid id, long lastTimestamp, bool isDeleted, bool forceWrite)
        {
            // Dotmim.Sync parameters
            DbManager.SetParameterValue(command, "sync_force_write", (forceWrite ? 1 : 0));
            DbManager.SetParameterValue(command, "sync_min_timestamp", lastTimestamp);
            DbManager.SetParameterValue(command, "sync_scope_id", id);
            DbManager.SetParameterValue(command, "sync_row_is_tombstone", isDeleted);
        }

        /// <summary>
        /// Create a change table with scope columns and tombstone column
        /// </summary>
        public static SyncTable CreateChangesTable(SyncTable syncTable, SyncSet owner)
        {
            if (syncTable.Schema == null)
                throw new ArgumentException("Schema can't be null when creating a changes table");

            // Create an empty sync table without columns
            var changesTable = new SyncTable(syncTable.TableName, syncTable.SchemaName)
            {
                OriginalProvider = syncTable.OriginalProvider,
                SyncDirection = syncTable.SyncDirection
            };

            // Adding primary keys
            foreach (var pkey in syncTable.PrimaryKeys)
                changesTable.PrimaryKeys.Add(pkey);

            // get ordered columns that are mutables and pkeys
            var orderedNames = syncTable.GetMutableColumnsWithPrimaryKeys();

            foreach (var c in orderedNames)
                changesTable.Columns.Add(c.Clone());

            owner.Tables.Add(changesTable);

            return changesTable;
        }
    }
}
