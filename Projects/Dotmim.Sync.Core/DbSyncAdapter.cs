using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
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

using System.Reflection;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;

namespace Dotmim.Sync
{
    /// <summary>
    /// The SyncAdapter is the datasource manager for ONE table
    /// Should be implemented by every database provider and provide every SQL action
    /// </summary>
    public abstract class DbSyncAdapter
    {
        private const int BATCH_SIZE = 1000;


        // Internal commands cache
        private ConcurrentDictionary<string, Lazy<SyncCommand>> commands = new ConcurrentDictionary<string, Lazy<SyncCommand>>();

        /// <summary>
        /// Gets the table description
        /// </summary>
        public SyncTable TableDescription { get; private set; }

        /// <summary>
        /// Gets the setup used 
        /// </summary>
        public SyncSetup Setup { get; }

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
        public abstract DbCommand GetCommand(DbCommandType commandType, SyncFilter filter = null);

        /// <summary>
        /// Add parameters to a command
        /// </summary>
        public abstract Task AddCommandParametersAsync(DbCommandType commandType, DbCommand command, DbConnection connection, DbTransaction transaction, SyncFilter filter = null);

        /// <summary>
        /// Execute a batch command
        /// </summary>
        public abstract Task ExecuteBatchCommandAsync(DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> arrayItems, SyncTable schemaChangesTable,
                                                      SyncTable failedRows, long lastTimestamp, DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Create a Sync Adapter
        /// </summary>
        public DbSyncAdapter(SyncTable tableDescription, SyncSetup setup)
        {
            this.TableDescription = tableDescription;
            this.Setup = setup;
        }

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
                if (!string.IsNullOrEmpty(parameter.SourceColumn))
                {
                    // foreach parameter, check if we have a column 
                    var column = schemaTable.Columns[parameter.SourceColumn];

                    if (column != null)
                    {
                        object value = row[column] ?? DBNull.Value;
                        DbSyncAdapter.SetParameterValue(command, parameter.ParameterName, value);
                    }
                }

            }

            // return value
            var syncRowCountParam = DbSyncAdapter.GetParameter(command, "sync_row_count");

            if (syncRowCountParam != null)
            {
                syncRowCountParam.Direction = ParameterDirection.Output;
                syncRowCountParam.Value = DBNull.Value;
            }
        }


        /// <summary>
        /// Remove a Command from internal shared dictionary
        /// </summary>
        internal void RemoveCommands() => this.commands.Clear();

        /// <summary>
        /// Get the command from provider, check connection is opened, affect connection and transaction
        /// Prepare the command parameters and add scope parameters
        /// </summary>
        internal async Task<DbCommand> PrepareCommandAsync(DbCommandType commandType, DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
        {
            // Create the key
            var commandKey = $"{connection.DataSource}-{connection.Database}-{this.TableDescription.GetFullName()}-{commandType}";

            // Get a lazy command instance
            var lazyCommand = commands.GetOrAdd(commandKey, k => new Lazy<SyncCommand>(() => new SyncCommand(GetCommand(commandType, filter))));

            // Get the concrete instance
            var command = lazyCommand.Value.DbCommand;

            if (command == null)
                throw new MissingCommandException(commandType.ToString());

            if (connection == null)
                throw new MissingConnectionException();

            if (connection.State != ConnectionState.Open)
                throw new ConnectionClosedException(connection);

            command.Connection = connection;

            if (transaction != null)
                command.Transaction = transaction;

            // lazyCommand.Metadata is a boolean indicating if the command is already prepared on the server
            if (lazyCommand.Value.IsPrepared == true)
                return command;

            // Add Parameters
            await this.AddCommandParametersAsync(commandType, command, connection, transaction, filter).ConfigureAwait(false);

            // Testing The Prepare() performance increase
            command.Prepare();

            // Adding this command as prepared
            lazyCommand.Value.IsPrepared = true;

            commands.AddOrUpdate(commandKey, lazyCommand, (key, lc) => new Lazy<SyncCommand>(() => lc.Value));

            return command;
        }

        /// <summary>
        /// Try to get a source row
        /// </summary>
        /// <returns></returns>
        internal async Task<SyncRow> GetRowAsync(Guid localScopeId, SyncRow primaryKeyRow, SyncTable schema, DbConnection connection, DbTransaction transaction)
        {
            // Get the row in the local repository
            var command = await this.PrepareCommandAsync(DbCommandType.SelectRow, connection, transaction);

            // set the primary keys columns as parameters
            this.SetColumnParametersValues(command, primaryKeyRow);

            // Create a select table based on the schema in parameter + scope columns
            var changesSet = schema.Schema.Clone(false);
            var selectTable = CreateChangesTable(schema, changesSet);
            SyncRow syncRow = null;

            using (var dataReader = await command.ExecuteReaderAsync().ConfigureAwait(false))
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
                            syncRow.RowState = isTombstone ? DataRowState.Deleted : DataRowState.Modified;
                            continue;
                        }

                        if (columnName == "update_scope_id")
                        {
                            var readerScopeId = dataReader.GetValue(i);

                            //// if update_scope_id is null, so the row owner is the local database
                            //// if update_scope_id is not null, the row owner is someone else
                            //if (readerScopeId == DBNull.Value || readerScopeId == null)
                            //    syncRow.UpdateScopeId = localScopeId;
                            //else if (SyncTypeConverter.TryConvertTo<Guid>(readerScopeId, out var updateScopeIdObject))
                            //    syncRow.UpdateScopeId = (Guid)updateScopeIdObject;
                            //else
                            //    throw new Exception("Impossible to parse row['update_scope_id']");

                            continue;
                        }

                        var columnValueObject = dataReader.GetValue(i);
                        var columnValue = columnValueObject == DBNull.Value ? null : columnValueObject;
                        syncRow[columnName] = columnValue;
                    }
                }
            }

            // if syncRow is not a deleted row, we can check for which kind of row it is.
            if (syncRow != null && syncRow.RowState == DataRowState.Unchanged)
                syncRow.RowState = DataRowState.Modified;

            return syncRow;
        }

        /// <summary>
        /// Launch apply bulk changes
        /// </summary>
        /// <returns></returns>
        internal async Task<int> ApplyBulkChangesAsync(SyncContext context, Guid localScopeId, Guid senderScopeId, SyncTable changesTable, long lastTimestamp,
                                                     List<SyncConflict> conflicts, InterceptorWrapper<TableChangesBatchApplyingArgs> iTableChangesBatchApplying, 
                                                     DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
        {
            var dbCommandType = this.ApplyType switch
            {
                DataRowState.Deleted => DbCommandType.BulkDeleteRows,
                DataRowState.Modified => DbCommandType.BulkUpdateRows,
                _ => throw new UnknownException("RowState not valid during ApplyBulkChanges operation"),
            };

            DbCommand command = await this.PrepareCommandAsync(dbCommandType, connection, transaction);

            // Launch any interceptor if available
            if (iTableChangesBatchApplying != null)
            {
                var action = new TableChangesBatchApplyingArgs(context, changesTable, this.ApplyType, command, connection, transaction);
                await iTableChangesBatchApplying.RunAsync(action, cancellationToken);

                if (action.Cancel || action.Command == null)
                    return 0;

                // get the correct pointer to the command from the interceptor in case user change the whole instance
                command = action.Command;
            }

            // Create
            var failedPrimaryKeysTable = changesTable.Schema.Clone().Tables[changesTable.TableName, changesTable.SchemaName];
            //AddSchemaForFailedRowsTable(failedPrimaryKeysTable);

            // get the items count
            var itemsArrayCount = changesTable.Rows.Count;

            // Make some parts of BATCH_SIZE
            await Task.Run(async () =>
            {
                var rowsList = changesTable.Rows.ToList();
                for (int step = 0; step < itemsArrayCount; step += BATCH_SIZE)
                {
                    // get upper bound max value
                    var taken = step + BATCH_SIZE >= itemsArrayCount ? itemsArrayCount - step : BATCH_SIZE;

                    var arrayStepChanges = rowsList.Skip(step).Take(taken);

                    // execute the batch, through the provider
                    await ExecuteBatchCommandAsync(command, senderScopeId, arrayStepChanges, changesTable, failedPrimaryKeysTable, lastTimestamp, connection, transaction).ConfigureAwait(false);
                }

                if (failedPrimaryKeysTable.Rows.Count > 0)
                {
                    // Get local and remote row and create the conflict object
                    foreach (var failedRow in failedPrimaryKeysTable.Rows)
                    {
                        //failedRow.RowState = this.ApplyType;

                        // Get the row that caused the problem, from the remote side (client)
                        var remoteConflictRows = changesTable.Rows.GetRowsByPrimaryKeys(failedRow);

                        if (remoteConflictRows.Count() == 0)
                            throw new Exception("Cant find changes row who is in conflict");

                        var remoteConflictRow = remoteConflictRows.ToList()[0];

                        var localConflictRow = await GetRowAsync(localScopeId, failedRow, changesTable, connection, transaction).ConfigureAwait(false);

                        conflicts.Add(GetConflict(remoteConflictRow, localConflictRow));
                    }
                }
            });

            // return applied rows minus failed rows
            return itemsArrayCount - failedPrimaryKeysTable.Rows.Count;
        }


   
        /// <summary>
        /// Try to apply changes on the server.
        /// Internally will call ApplyUpdate or ApplyDelete and will return
        /// </summary>
        /// <param name="changes">Changes</param>
        /// <returns>every lines not updated / deleted in the destination data source</returns>
        internal async Task<int> ApplyChangesAsync(SyncContext context, Guid localScopeId, Guid senderScopeId, SyncTable changesTable, long lastTimestamp, 
                                                   List<SyncConflict> conflicts, InterceptorWrapper<TableChangesBatchApplyingArgs> iTableChangesBatchApplying, 
                                                   DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
        {

            var dbCommandType = this.ApplyType switch
            {
                DataRowState.Deleted => DbCommandType.DeleteRow,
                DataRowState.Modified => DbCommandType.UpdateRow,
                _ => throw new UnknownException("RowState not valid during ApplyChangesAsync operation"),
            };

            var command = await this.PrepareCommandAsync(dbCommandType, connection, transaction);

            // Launch any interceptor if available
            if (iTableChangesBatchApplying != null)
            {
                var action = new TableChangesBatchApplyingArgs(context, changesTable, this.ApplyType, command, connection, transaction);
                await iTableChangesBatchApplying.RunAsync(action, cancellationToken);

                if (action.Cancel || action.Command == null)
                    return 0;

                // get the correct pointer to the command from the interceptor in case user change the whole instance
                command = action.Command;
            }

            int appliedRows = 0;

            // Making an async call on all the rows to ensure we don't freeze any client UI
            appliedRows = await Task.Run(async () =>
            {
                int appliedRowsTmp = 0;

                foreach (var row in changesTable.Rows)
                {
                    try
                    {
                        if (row.Table == null)
                            throw new ArgumentException("Schema table is not present in the row");

                        // Set the parameters value from row 
                        this.SetColumnParametersValues(command, row);

                        // Set the special parameters for update
                        AddScopeParametersValues(command, senderScopeId, lastTimestamp, ApplyType == DataRowState.Deleted, false);

                        var rowAppliedCount = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                        // Check if we have a return value instead
                        var syncRowCountParam = GetParameter(command, "sync_row_count");

                        if (syncRowCountParam != null)
                            rowAppliedCount = (int)syncRowCountParam.Value;

                        if (rowAppliedCount > 0)
                            appliedRowsTmp++;
                        else
                            conflicts.Add(GetConflict(row, await GetRowAsync(localScopeId, row, changesTable, connection, transaction).ConfigureAwait(false)));

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
                            var localRow = await GetRowAsync(localScopeId, row, changesTable, connection, transaction).ConfigureAwait(false);
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

                return appliedRowsTmp;

            }).ConfigureAwait(false);

            return appliedRows;
        }




        /// <summary>
        /// Apply a delete on a row
        /// </summary>
        internal async Task<bool> ApplyDeleteAsync(SyncRow row, long lastTimestamp, Guid? senderScopeId, bool forceWrite, DbConnection connection, DbTransaction transaction)
        {
            if (row.Table == null)
                throw new ArgumentException("Schema table is not present in the row");

            var command = await this.PrepareCommandAsync(DbCommandType.DeleteRow, connection, transaction);

            // Set the parameters value from row
            this.SetColumnParametersValues(command, row);

            // Set the special parameters for update
            this.AddScopeParametersValues(command, senderScopeId, lastTimestamp, true, forceWrite);

            var rowDeletedCount = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

            // Check if we have a return value instead
            var syncRowCountParam = DbSyncAdapter.GetParameter(command, "sync_row_count");

            if (syncRowCountParam != null)
                rowDeletedCount = (int)syncRowCountParam.Value;

            return rowDeletedCount > 0;
        }

        /// <summary>
        /// Apply a single update in the current datasource. if forceWrite, override conflict situation and force the update
        /// </summary>
        internal async Task<bool> ApplyUpdateAsync(SyncRow row, long lastTimestamp, Guid? senderScopeId, bool forceWrite, DbConnection connection, DbTransaction transaction)
        {
            if (row.Table == null)
                throw new ArgumentException("Schema table is not present in the row");

            var command = await this.PrepareCommandAsync(DbCommandType.UpdateRow, connection, transaction);

            // Set the parameters value from row
            this.SetColumnParametersValues(command, row);

            // Set the special parameters for update
            AddScopeParametersValues(command, senderScopeId, lastTimestamp, false, forceWrite);

            var rowUpdatedCount = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

            // Check if we have a return value instead
            var syncRowCountParam = DbSyncAdapter.GetParameter(command, "sync_row_count");

            if (syncRowCountParam != null)
                rowUpdatedCount = (int)syncRowCountParam.Value;

            return rowUpdatedCount > 0;
        }

        /// <summary>
        /// We have a conflict, try to get the source row and generate a conflict
        /// </summary>
        private SyncConflict GetConflict(SyncRow remoteConflictRow, SyncRow localConflictRow)
        {

            var dbConflictType = ConflictType.ErrorsOccurred;

            if (remoteConflictRow == null)
                throw new UnknownException("THAT can't happen...");


            // local row is null
            if (localConflictRow == null && remoteConflictRow.RowState == DataRowState.Modified)
                dbConflictType = ConflictType.RemoteExistsLocalNotExists;
            else if (localConflictRow == null && remoteConflictRow.RowState == DataRowState.Deleted)
                dbConflictType = ConflictType.RemoteIsDeletedLocalNotExists;

            //// remote row is null. Can not happen
            //else if (remoteConflictRow == null && localConflictRow.RowState == DataRowState.Modified)
            //    dbConflictType = ConflictType.RemoteNotExistsLocalExists;
            //else if (remoteConflictRow == null && localConflictRow.RowState == DataRowState.Deleted)
            //    dbConflictType = ConflictType.RemoteNotExistsLocalIsDeleted;

            else if (remoteConflictRow.RowState == DataRowState.Deleted && localConflictRow.RowState == DataRowState.Deleted)
                dbConflictType = ConflictType.RemoteIsDeletedLocalIsDeleted;
            else if (remoteConflictRow.RowState == DataRowState.Modified && localConflictRow.RowState == DataRowState.Deleted)
                dbConflictType = ConflictType.RemoteExistsLocalIsDeleted;
            else if (remoteConflictRow.RowState == DataRowState.Deleted && localConflictRow.RowState == DataRowState.Modified)
                dbConflictType = ConflictType.RemoteIsDeletedLocalExists;
            else if (remoteConflictRow.RowState == DataRowState.Modified && localConflictRow.RowState == DataRowState.Modified)
                dbConflictType = ConflictType.RemoteExistsLocalExists;

            // Generate the conflict
            var conflict = new SyncConflict(dbConflictType);
            conflict.AddRemoteRow(remoteConflictRow);

            if (localConflictRow != null)
                conflict.AddLocalRow(localConflictRow);

            return conflict;
        }

        internal async Task<int> UpdateUntrackedRowsAsync(DbConnection connection, DbTransaction transaction)
        {
            // Get correct Select incremental changes command 
            var command = await this.PrepareCommandAsync(DbCommandType.UpdateUntrackedRows, connection, transaction);

            // Execute
            var rowAffected = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

            // Check if we have a return value instead
            var syncRowCountParam = DbSyncAdapter.GetParameter(command, "sync_row_count");

            if (syncRowCountParam != null)
                rowAffected = (int)syncRowCountParam.Value;

            return rowAffected;
        }

        /// <summary>
        /// Delete all metadatas from one table before a timestamp limit
        /// </summary>
        internal async Task<int> DeleteMetadatasAsync(long timestampLimit, DbConnection connection, DbTransaction transaction)
        {
            var command = await this.PrepareCommandAsync(DbCommandType.DeleteMetadata, connection, transaction);

            // Set the special parameters for delete metadata
            DbSyncAdapter.SetParameterValue(command, "sync_row_timestamp", timestampLimit);

            var metadataDeletedRowsCount = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

            // Check if we have a return value instead
            var syncRowCountParam = DbSyncAdapter.GetParameter(command, "sync_row_count");

            if (syncRowCountParam != null)
                metadataDeletedRowsCount = (int)syncRowCountParam.Value;

            return metadataDeletedRowsCount;

        }

        /// <summary>
        /// Update a metadata row
        /// </summary>
        internal async Task<bool> UpdateMetadatasAsync(SyncRow row, Guid? senderScopeId, bool forceWrite, DbConnection connection, DbTransaction transaction)
        {
            var command = await this.PrepareCommandAsync(DbCommandType.UpdateMetadata, connection, transaction);

            // Set the parameters value from row
            this.SetColumnParametersValues(command, row);

            // Set the special parameters for update
            AddScopeParametersValues(command, senderScopeId, 0, row.RowState == DataRowState.Deleted, forceWrite);

            var metadataUpdatedRowsCount = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

            // Check if we have a return value instead
            var syncRowCountParam = DbSyncAdapter.GetParameter(command, "sync_row_count");

            if (syncRowCountParam != null)
                metadataUpdatedRowsCount = (int)syncRowCountParam.Value;

            return metadataUpdatedRowsCount > 0;
        }

        /// <summary>
        /// Reset a table, deleting rows from table and tracking_table
        /// </summary>
        internal async Task<bool> ResetTableAsync(DbConnection connection, DbTransaction transaction)
        {
            var command = await this.PrepareCommandAsync(DbCommandType.Reset, connection, transaction);

            var rowCount = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

            return rowCount > 0;

        }

        /// <summary>
        /// Reset a table, deleting rows from table and tracking_table
        /// </summary>
        public virtual async Task DisableConstraintsAsync(DbConnection connection, DbTransaction transaction)
        {
            var command = await this.PrepareCommandAsync(DbCommandType.DisableConstraints, connection, transaction);

            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Reset a table, deleting rows from table and tracking_table
        /// </summary>
        public virtual async Task EnableConstraintsAsync(DbConnection connection, DbTransaction transaction)
        {
            var command = await this.PrepareCommandAsync(DbCommandType.EnableConstraints, connection, transaction);

            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Add common parameters which could be part of the command
        /// if not found, no set done
        /// </summary>
        internal void AddScopeParametersValues(DbCommand command, Guid? id, long lastTimestamp, bool isDeleted, bool forceWrite)
        {
            // Dotmim.Sync parameters
            DbSyncAdapter.SetParameterValue(command, "sync_force_write", forceWrite ? 1 : 0);
            DbSyncAdapter.SetParameterValue(command, "sync_min_timestamp", lastTimestamp);
            DbSyncAdapter.SetParameterValue(command, "sync_scope_id", id.HasValue ? (object)id.Value : DBNull.Value);
            DbSyncAdapter.SetParameterValue(command, "sync_row_is_tombstone", isDeleted);
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


        /// <summary>
        /// Get a parameter even if it's a @param or :param or param
        /// </summary>
        public static DbParameter GetParameter(DbCommand command, string parameterName)
        {
            if (command == null)
                return null;

            if (command.Parameters.Contains($"@{parameterName}"))
                return command.Parameters[$"@{parameterName}"];

            if (command.Parameters.Contains($":{parameterName}"))
                return command.Parameters[$":{parameterName}"];

            if (command.Parameters.Contains($"in_{parameterName}"))
                return command.Parameters[$"in_{parameterName}"];

            if (!command.Parameters.Contains(parameterName))
                return null;

            return command.Parameters[parameterName];
        }

        /// <summary>
        /// Set a parameter value
        /// </summary>
        public static void SetParameterValue(DbCommand command, string parameterName, object value)
        {
            var parameter = GetParameter(command, parameterName);
            if (parameter == null)
                return;

            if (value == null || value == DBNull.Value)
                parameter.Value = DBNull.Value;
            else
                parameter.Value = SyncTypeConverter.TryConvertFromDbType(value, parameter.DbType);


        }

        public static int GetSyncIntOutParameter(string parameter, DbCommand command)
        {
            DbParameter dbParameter = GetParameter(command, parameter);
            if (dbParameter == null || dbParameter.Value == null || string.IsNullOrEmpty(dbParameter.Value.ToString()))
                return 0;

            return int.Parse(dbParameter.Value.ToString(), CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Parse a time stamp value
        /// </summary>
        public static long ParseTimestamp(object obj)
        {
            if (obj == DBNull.Value)
                return 0;

            if (obj is long || obj is int || obj is ulong || obj is uint || obj is decimal)
                return Convert.ToInt64(obj, NumberFormatInfo.InvariantInfo);
            long timestamp;
            if (obj is string str)
            {
                long.TryParse(str, NumberStyles.HexNumber, CultureInfo.InvariantCulture.NumberFormat, out timestamp);
                return timestamp;
            }

            if (!(obj is byte[] numArray))
                return 0;

            var stringBuilder = new StringBuilder();
            for (int i = 0; i < numArray.Length; i++)
            {
                string str1 = numArray[i].ToString("X", NumberFormatInfo.InvariantInfo);
                stringBuilder.Append((str1.Length == 1 ? string.Concat("0", str1) : str1));
            }

            long.TryParse(stringBuilder.ToString(), NumberStyles.HexNumber, CultureInfo.InvariantCulture.NumberFormat, out timestamp);
            return timestamp;
        }


    }
}
