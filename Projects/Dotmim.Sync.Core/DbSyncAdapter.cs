﻿using Dotmim.Sync.Enumerations;
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

namespace Dotmim.Sync
{


    /// <summary>
    /// The SyncAdapter is the datasource manager for ONE table
    /// Should be implemented by every database provider and provide every SQL action
    /// </summary>
    public abstract class DbSyncAdapter
    {
        private const int BATCH_SIZE = 1000;

        public delegate (ApplyAction, DmRow) ConflictActionDelegate(SyncConflict conflict, ConflictResolutionPolicy policy, DbConnection connection, DbTransaction transaction = null);

        public ConflictActionDelegate ConflictActionInvoker = null;

        internal ApplyAction ConflictApplyAction { get; set; } = ApplyAction.Continue;

        /// <summary>
        /// Gets the table description, a dmTable with no rows
        /// </summary>
        public DmTable TableDescription { get; private set; }

        /// <summary>
        /// Get or Set the current step (could be only Added, Modified, Deleted)
        /// </summary>
        internal DmRowState applyType { get; set; }

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
        public abstract DbCommand GetCommand(DbCommandType commandType, IEnumerable<string> additionals = null);

        /// <summary>
        /// Set parameters on a command
        /// </summary>
        public abstract void SetCommandParameters(DbCommandType commandType, DbCommand command);

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
                isTombstone = row["sync_row_is_tombstone", version] != null && row["sync_row_is_tombstone", version] != DBNull.Value ? (bool)row["sync_row_is_tombstone", version] : false;

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
        private DmTable GetRow(DmRow sourceRow)
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

            if (this.applyType == DmRowState.Added)
            {
                bulkCommand = this.GetCommand(DbCommandType.BulkInsertRows);
                this.SetCommandParameters(DbCommandType.BulkInsertRows, bulkCommand);
            }
            else if (this.applyType == DmRowState.Modified)
            {
                bulkCommand = this.GetCommand(DbCommandType.BulkUpdateRows);
                this.SetCommandParameters(DbCommandType.BulkUpdateRows, bulkCommand);
            }
            else if (this.applyType == DmRowState.Deleted)
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
                    if (applyType == DmRowState.Added)
                    {
                        operationComplete = this.ApplyInsert(dmRow, scope, false);
                        if (operationComplete)
                            UpdateMetadatas(DbCommandType.InsertMetadata, dmRow, scope);
                    }
                    else if (applyType == DmRowState.Modified)
                    {
                        operationComplete = this.ApplyUpdate(dmRow, scope, false);
                        if (operationComplete)
                            UpdateMetadatas(DbCommandType.UpdateMetadata, dmRow, scope);
                    }
                    else if (applyType == DmRowState.Deleted)
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
                var errorMessage = "Change Application failed due to Row not Found on the server";

                if (applyType == DmRowState.Added)
                    dbConflictType = ConflictType.RemoteInsertLocalNoRow;
                else if (applyType == DmRowState.Modified)
                    dbConflictType = ConflictType.RemoteUpdateLocalNoRow;
                else if (applyType == DmRowState.Deleted)
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
                    if (applyType == DmRowState.Added)
                        dbConflictType = ConflictType.RemoteInsertLocalDelete;
                    else if (applyType == DmRowState.Modified)
                        dbConflictType = ConflictType.RemoteUpdateLocalDelete;
                    else if (applyType == DmRowState.Deleted)
                        dbConflictType = ConflictType.RemoteDeleteLocalDelete;
                }
                else
                {
                    var createTimestamp = localRow["create_timestamp"] != DBNull.Value ? (long)localRow["create_timestamp"] : 0L;
                    var updateTimestamp = localRow["update_timestamp"] != DBNull.Value ? (long)localRow["update_timestamp"] : 0L;
                    switch (applyType)
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
        /// Handle a conflict
        /// </summary>
        internal ChangeApplicationAction HandleConflict(SyncConflict conflict, ConflictResolutionPolicy policy, ScopeInfo scope, long fromScopeLocalTimeStamp, out DmRow finalRow)
        {
            finalRow = null;

            // overwrite apply action if we handle it (ie : user wants to change the action)
            if (this.ConflictActionInvoker != null)
                (ConflictApplyAction, finalRow) = this.ConflictActionInvoker(conflict, policy, Connection, Transaction);

            // Default behavior and an error occured
            if (ConflictApplyAction == ApplyAction.Rollback)
            {
                conflict.ErrorMessage = "Rollback action taken on conflict";
                conflict.Type = ConflictType.ErrorsOccurred;

                return ChangeApplicationAction.Rollback;
            }

            // Local provider wins, update metadata
            if (ConflictApplyAction == ApplyAction.Continue)
            {
                var isMergeAction = finalRow != null;
                var row = isMergeAction ? finalRow : conflict.LocalRow;

                // Conflict on a line that is not present on the datasource
                if (row == null)
                    return ChangeApplicationAction.Continue;

                if (row != null)
                {
                    // if we have a merge action, we apply the row on the server
                    if (isMergeAction)
                    {
                        bool isUpdated = false;
                        bool isInserted = false;
                        // Insert metadata is a merge, actually
                        DbCommandType commandType = DbCommandType.UpdateMetadata;

                        isUpdated = this.ApplyUpdate(row, scope, true);

                        if (!isUpdated)
                        {
                            // Insert the row
                            isInserted = this.ApplyInsert(row, scope, true);
                            // Then update the row to mark this row as updated from server
                            // and get it back to client 
                            isUpdated = this.ApplyUpdate(row, scope, true);

                            commandType = DbCommandType.InsertMetadata;
                        }

                        if (!isUpdated && !isInserted)
                            throw new Exception("Can't update the merge row.");


                        // IF we have insert the row in the server side, to resolve the conflict
                        // Whe should update the metadatas correctly
                        if (isUpdated && isInserted)
                        {
                            using (var metadataCommand = GetCommand(commandType))
                            {
                                // getting the row updated from server
                                var dmTableRow = GetRow(row);
                                row = dmTableRow.Rows[0];

                                // Deriving Parameters
                                this.SetCommandParameters(commandType, metadataCommand);

                                // Set the id parameter
                                this.SetColumnParametersValues(metadataCommand, row);

                                DmRowVersion version = row.RowState == DmRowState.Deleted ? DmRowVersion.Original : DmRowVersion.Current;

                                Guid? create_scope_id = row["create_scope_id"] != DBNull.Value ? (Guid?)row["create_scope_id"] : null;
                                long createTimestamp = row["create_timestamp", version] != DBNull.Value ? Convert.ToInt64(row["create_timestamp", version]) : 0;

                                // The trick is to force the row to be "created before last sync"
                                // Even if we just inserted it
                                // to be able to get the row in state Updated (and not Added)
                                row["create_scope_id"] = create_scope_id;
                                row["create_timestamp"] = fromScopeLocalTimeStamp - 1;

                                // Update scope id is set to server side
                                Guid? update_scope_id = row["update_scope_id"] != DBNull.Value ? (Guid?)row["update_scope_id"] : null;
                                long updateTimestamp = row["update_timestamp", version] != DBNull.Value ? Convert.ToInt64(row["update_timestamp", version]) : 0;

                                row["update_scope_id"] = null;
                                row["update_timestamp"] = updateTimestamp;


                                // apply local row, set scope.id to null becoz applied locally
                                var rowsApplied = this.InsertOrUpdateMetadatas(metadataCommand, row, null);

                                if (!rowsApplied)
                                    throw new Exception("No metadatas rows found, can't update the server side");

                            }
                        }
                    }

                    finalRow = isMergeAction ? row : conflict.LocalRow;

                    return ChangeApplicationAction.Continue;
                }
                return ChangeApplicationAction.Rollback;
            }

            // We gonna apply with force the line
            if (ConflictApplyAction == ApplyAction.RetryWithForceWrite)
            {
                if (conflict.RemoteRow == null)
                {
                    // TODO : Should Raise an error ?
                    return ChangeApplicationAction.Rollback;
                }

                bool operationComplete = false;

                // create a localscope to override values
                var localScope = new ScopeInfo { Name = scope.Name, Timestamp = fromScopeLocalTimeStamp };

                DbCommandType commandType = DbCommandType.InsertMetadata;

                switch (conflict.Type)
                {
                    // Remote source has row, Local don't have the row, so insert it
                    case ConflictType.RemoteUpdateLocalNoRow:
                    case ConflictType.RemoteInsertLocalNoRow:
                        operationComplete = this.ApplyInsert(conflict.RemoteRow, localScope, true);
                        commandType = DbCommandType.InsertMetadata;
                        break;

                    // Conflict, but both have delete the row, so nothing to do
                    case ConflictType.RemoteDeleteLocalDelete:
                    case ConflictType.RemoteDeleteLocalNoRow:
                        operationComplete = true;
                        break;

                    // The remote has delete the row, and local has insert or update it
                    // So delete the local row
                    case ConflictType.RemoteDeleteLocalUpdate:
                    case ConflictType.RemoteDeleteLocalInsert:
                        operationComplete = this.ApplyDelete(conflict.RemoteRow, localScope, true);
                        commandType = DbCommandType.UpdateMetadata;
                        break;


                    // Remote insert and local delete, sor insert again on local
                    // but tracking line exist, so make an update on metadata
                    case ConflictType.RemoteInsertLocalDelete:
                    case ConflictType.RemoteUpdateLocalDelete:
                        operationComplete = this.ApplyInsert(conflict.RemoteRow, localScope, true);
                        commandType = DbCommandType.UpdateMetadata;
                        break;

                    // Remote insert and local insert/ update, take the remote row and update the local row
                    case ConflictType.RemoteUpdateLocalInsert:
                    case ConflictType.RemoteUpdateLocalUpdate:
                    case ConflictType.RemoteInsertLocalInsert:
                    case ConflictType.RemoteInsertLocalUpdate:
                        operationComplete = this.ApplyUpdate(conflict.RemoteRow, localScope, true);
                        commandType = DbCommandType.UpdateMetadata;
                        break;

                    case ConflictType.RemoteCleanedupDeleteLocalUpdate:
                    case ConflictType.ErrorsOccurred:
                        return ChangeApplicationAction.Rollback;
                }


                using (var metadataCommand = GetCommand(commandType))
                {
                    // Deriving Parameters
                    this.SetCommandParameters(commandType, metadataCommand);

                    // force applying client row, so apply scope.id (client scope here)
                    var rowsApplied = this.InsertOrUpdateMetadatas(metadataCommand, conflict.RemoteRow, scope.Id);
                    if (!rowsApplied)
                        throw new Exception("No metadatas rows found, can't update the server side");
                }

                finalRow = conflict.RemoteRow;

                //After a force update, there is a problem, so raise exception
                if (!operationComplete)
                {
                    var ex = $"Can't force operation for applyType {applyType}";
                    finalRow = null;
                    return ChangeApplicationAction.Continue;
                }

                // tableProgress.ChangesApplied += 1;
                return ChangeApplicationAction.Continue;
            }

            return ChangeApplicationAction.Rollback;

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
            switch (applyType)
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
