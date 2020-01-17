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
        public abstract void ExecuteBatchCommand(DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> arrayItems, SyncTable schemaChangesTable, SyncTable failedRows, long lastTimestamp);

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
                    var column = schemaTable.Columns.FirstOrDefault(sc => sc.ColumnName.Equals(parameter.SourceColumn, SyncGlobalization.DataSourceStringComparison));
                    if (column != null)
                    {
                        object value = row[column];
                        DbTableManagerFactory.SetParameterValue(command, parameter.ParameterName, value);
                    }
                }

            }

            // return value
            var syncRowCountParam = DbTableManagerFactory.GetParameter(command, "sync_row_count");

            if (syncRowCountParam != null)
                syncRowCountParam.Direction = ParameterDirection.Output;
        }

        /// <summary>
        /// Insert or update a metadata line
        /// </summary>
        internal bool InsertOrUpdateMetadatas(DbCommand command, SyncRow row, Guid senderScopeId)
        {
            if (row.Table == null)
                throw new ArgumentException("Schema table columns does not exist");

            int rowsApplied = 0;

            var schemaTable = row.Table;

            // Set the id parameter
            this.SetColumnParametersValues(command, row);

            // 2 choices for getting deleted
            // Firstly check the state
            var isTombstone = row.RowState == DataRowState.Deleted;

            // TODO : Secondly check the sync is tombstone column. Should we do this ?
            var isTombstoneColumn = schemaTable.Columns.FirstOrDefault(sc => sc.ColumnName.Equals("sync_row_is_tombstone", SyncGlobalization.DataSourceStringComparison));

            if (isTombstoneColumn != null)
            {
                var rowValue = row[schemaTable.Columns.IndexOf(isTombstoneColumn)];

                if (rowValue == null)
                    isTombstone = false;
                else
                    isTombstone = rowValue.GetType() == typeof(bool) ? (bool)rowValue : Convert.ToInt64(rowValue) > 0;
            }


            DbTableManagerFactory.SetParameterValue(command, "sync_row_is_tombstone", isTombstone ? 1 : 0);
            DbTableManagerFactory.SetParameterValue(command, "sync_scope_id", senderScopeId);

            var alreadyOpened = Connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                Connection.Open();

            if (Transaction != null)
                command.Transaction = Transaction;

            rowsApplied = command.ExecuteNonQuery();

            // Close Connection
            if (!alreadyOpened)
                Connection.Close();

            return rowsApplied > 0;
        }

        /// <summary>
        /// Try to get a source row
        /// </summary>
        /// <returns></returns>
        internal SyncRow GetRow(Guid localScopeId, SyncRow primaryKeyRow, SyncTable schema)
        {
            // Get the row in the local repository
            using (var selectCommand = GetCommand(DbCommandType.SelectRow))
            {
                if (selectCommand == null)
                    throw new MissingCommandException(DbCommandType.SelectRow.ToString());

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

                            if (columnName == "update_scope_id")
                            {
                                var readerScopeId = dataReader.GetValue(i);

                                // if update_scope_id is null, so the row owner is the local database
                                // if update_scope_id is not null, the row owner is someone else
                                if (readerScopeId == DBNull.Value || readerScopeId == null)
                                    syncRow.UpdateScopeId = localScopeId;
                                else if (SyncTypeConverter.TryConvertTo<Guid>(readerScopeId, out var updateScopeIdObject))
                                    syncRow.UpdateScopeId = (Guid)updateScopeIdObject;
                                else
                                    throw new Exception("Impossible to parse row['update_scope_id']");

                                continue;
                            }

                            var columnValueObject = dataReader.GetValue(i);
                            var columnValue = columnValueObject == DBNull.Value ? null : columnValueObject;
                            syncRow[columnName] = columnValue;
                        }
                    }
                }

                // Close Connection
                if (!alreadyOpened)
                    Connection.Close();


                if (syncRow != null)
                    syncRow.RowState = primaryKeyRow.RowState;

                return syncRow;
            }

        }

        /// <summary>
        /// Launch apply bulk changes
        /// </summary>
        /// <returns></returns>
        public int ApplyBulkChanges(Guid localScopeId, Guid senderScopeId, SyncTable changesTable, long lastTimestamp, List<SyncConflict> conflicts)
        {
            DbCommand bulkCommand;
            if (this.ApplyType == DataRowState.Modified)
            {
                bulkCommand = this.GetCommand(DbCommandType.BulkUpdateRows);
                if (bulkCommand == null)
                    throw new MissingCommandException(DbCommandType.BulkUpdateRows.ToString());

                this.SetCommandParameters(DbCommandType.BulkUpdateRows, bulkCommand);
            }
            else if (this.ApplyType == DataRowState.Deleted)
            {
                bulkCommand = this.GetCommand(DbCommandType.BulkDeleteRows);
                if (bulkCommand == null)
                    throw new MissingCommandException(DbCommandType.BulkDeleteRows.ToString());

                this.SetCommandParameters(DbCommandType.BulkDeleteRows, bulkCommand);
            }
            else
            {
                throw new Exception("RowState not valid during ApplyBulkChanges operation");
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
                ExecuteBatchCommand(bulkCommand, senderScopeId, arrayStepChanges, changesTable, failedPrimaryKeysTable, lastTimestamp);
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

                var localConflictRow = GetRow(localScopeId, failedRow, changesTable);

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
                    dbConflictType = ConflictType.RemoteExistsLocalNotExists;
                else if (ApplyType == DataRowState.Deleted)
                    dbConflictType = ConflictType.RemoteIsDeletedLocalNotExists;
            }
            else
            {
                // the row on local is deleted
                if (localConflictRow.RowState == DataRowState.Deleted)
                {
                    if (ApplyType == DataRowState.Modified)
                        dbConflictType = ConflictType.RemoteExistsLocalIsDeleted;
                    else if (ApplyType == DataRowState.Deleted)
                        dbConflictType = ConflictType.RemoteIsDeletedLocalIsDeleted;
                }
                else
                {
                    dbConflictType = ConflictType.RemoteExistsLocalExists;
                }
            }
            // Generate the conflict
            var conflict = new SyncConflict(dbConflictType);
            conflict.AddRemoteRow(remoteConflictRow);

            if (localConflictRow != null)
                conflict.AddLocalRow(localConflictRow);

            return conflict;
        }

        private void UpdateMetadatas(DbCommandType dbCommandType, SyncRow row, Guid senderScopeId)
        {
            using (var dbCommand = this.GetCommand(dbCommandType))
            {
                if (dbCommand == null)
                    throw new MissingCommandException(dbCommandType.ToString());

                this.SetCommandParameters(dbCommandType, dbCommand);
                this.InsertOrUpdateMetadatas(dbCommand, row, senderScopeId);
            }
        }

        /// <summary>
        /// Try to apply changes on the server.
        /// Internally will call ApplyInsert / ApplyUpdate or ApplyDelete
        /// </summary>
        /// <param name="changes">Changes from remote</param>
        /// <returns>every lines not updated on the server side</returns>
        internal int ApplyChanges(Guid localScopeId, Guid senderScopeId, SyncTable changesTable, long lastTimestamp, List<SyncConflict> conflicts)
        {
            int appliedRows = 0;

            foreach (var row in changesTable.Rows)
            {
                bool operationComplete = false;

                try
                {
                    if (ApplyType == DataRowState.Modified)
                    {
                        operationComplete = this.ApplyUpdate(row, lastTimestamp, senderScopeId, false);

                        if (operationComplete)
                            UpdateMetadatas(DbCommandType.UpdateMetadata, row, senderScopeId);
                    }
                    else if (ApplyType == DataRowState.Deleted)
                    {
                        operationComplete = this.ApplyDelete(row, lastTimestamp, senderScopeId, false);

                        if (operationComplete)
                            UpdateMetadatas(DbCommandType.UpdateMetadata, row, senderScopeId);
                    }

                    if (operationComplete)
                        // if no pb, increment then go to next row
                        appliedRows++;
                    else
                    {
                        var localConflictRow = GetRow(localScopeId, row, changesTable);
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
                        var localRow = GetRow(localScopeId, row, changesTable);
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
        internal bool ApplyDelete(SyncRow row, long lastTimestamp, Guid? senderScopeId, bool forceWrite)
        {
            if (row.Table == null)
                throw new ArgumentException("Schema table is not present in the row");

            using (var command = this.GetCommand(DbCommandType.DeleteRow))
            {
                if (command == null)
                    throw new MissingCommandException(DbCommandType.DeleteRow.ToString());

                // Deriving Parameters
                this.SetCommandParameters(DbCommandType.DeleteRow, command);

                // Set the parameters value from row
                this.SetColumnParametersValues(command, row);

                // Set the special parameters for update
                this.AddScopeParametersValues(command, senderScopeId, lastTimestamp, true, forceWrite);

                var alreadyOpened = Connection.State == ConnectionState.Open;

                // OPen Connection
                if (!alreadyOpened)
                    Connection.Open();

                if (Transaction != null)
                    command.Transaction = Transaction;

                var rowInsertedCount = command.ExecuteNonQuery();


                if (!alreadyOpened)
                    Connection.Close();


                return rowInsertedCount > 0;
            }
        }

        /// <summary>
        /// Apply a single update in the current datasource. if forceWrite, override conflict situation and force the update
        /// </summary>
        internal bool ApplyUpdate(SyncRow row, long lastTimestamp, Guid? senderScopeId, bool forceWrite)
        {
            if (row.Table == null)
                throw new ArgumentException("Schema table is not present in the row");

            using (var command = this.GetCommand(DbCommandType.UpdateRow))
            {
                if (command == null)
                    throw new MissingCommandException(DbCommandType.UpdateRow.ToString());

                // Deriving Parameters
                this.SetCommandParameters(DbCommandType.UpdateRow, command);

                // Set the parameters value from row
                this.SetColumnParametersValues(command, row);

                // Set the special parameters for update
                AddScopeParametersValues(command, senderScopeId, lastTimestamp, false, forceWrite);

                var alreadyOpened = Connection.State == ConnectionState.Open;
                
                if (!alreadyOpened)
                    Connection.Open();

                if (Transaction != null)
                    command.Transaction = Transaction;

                var rowUpdatedCount = command.ExecuteNonQuery();
                if (!alreadyOpened)
                    Connection.Close();
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
                if (command == null)
                    throw new MissingCommandException(DbCommandType.Reset.ToString());

                // Deriving Parameters
                this.SetCommandParameters(DbCommandType.Reset, command);

                var alreadyOpened = Connection.State == ConnectionState.Open;

                int rowCount = 0;
                if (!alreadyOpened)
                    Connection.Open();

                if (Transaction != null)
                    command.Transaction = Transaction;

                rowCount = command.ExecuteNonQuery();

                if (!alreadyOpened)
                    Connection.Close();

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
                if (command == null)
                    throw new MissingCommandException(DbCommandType.DisableConstraints.ToString());

                // set parameters if needed
                this.SetCommandParameters(DbCommandType.DisableConstraints, command);

                var alreadyOpened = Connection.State == ConnectionState.Open;

                int rowCount = 0;

                if (!alreadyOpened)
                    Connection.Open();

                if (Transaction != null)
                    command.Transaction = Transaction;

                rowCount = command.ExecuteNonQuery();

                if (!alreadyOpened)
                    Connection.Close();

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
                if (command == null)
                    throw new MissingCommandException(DbCommandType.EnableConstraints.ToString());

                // set parameters if needed
                this.SetCommandParameters(DbCommandType.EnableConstraints, command);

                var alreadyOpened = Connection.State == ConnectionState.Open;

                int rowCount = 0;

                if (!alreadyOpened)
                    Connection.Open();

                if (Transaction != null)
                    command.Transaction = Transaction;

                rowCount = command.ExecuteNonQuery();

                if (!alreadyOpened)
                    Connection.Close();

                return rowCount > 0;
            }
        }

        /// <summary>
        /// Add common parameters which could be part of the command
        /// if not found, no set done
        /// </summary>
        private void AddScopeParametersValues(DbCommand command, Guid? id, long lastTimestamp, bool isDeleted, bool forceWrite)
        {
            // Dotmim.Sync parameters
            DbTableManagerFactory.SetParameterValue(command, "sync_force_write", (forceWrite ? 1 : 0));
            DbTableManagerFactory.SetParameterValue(command, "sync_min_timestamp", lastTimestamp);
            DbTableManagerFactory.SetParameterValue(command, "sync_scope_id", id.HasValue ? (object)id.Value : DBNull.Value);
            DbTableManagerFactory.SetParameterValue(command, "sync_row_is_tombstone", isDeleted);
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
