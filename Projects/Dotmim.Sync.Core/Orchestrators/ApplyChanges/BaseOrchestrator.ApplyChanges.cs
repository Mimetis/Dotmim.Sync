
using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {

        internal virtual async Task InternalApplyCleanErrorsAsync(ScopeInfo scopeInfo, SyncContext context,
                             BatchInfo lastSyncErrorsBatchInfo, MessageApplyChanges message, DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (lastSyncErrorsBatchInfo == null)
                return;

            context.SyncStage = SyncStage.ChangesApplying;

            var schemaTables = message.Schema.Tables.SortByDependencies(tab => tab.GetRelations().Select(r => r.GetParentTable()));

            foreach (var schemaTable in schemaTables)
            {
                var tableChangesApplied = message.ChangesApplied?.TableChangesApplied?.FirstOrDefault(tca =>
                {
                    var sc = SyncGlobalization.DataSourceStringComparison;

                    var sn = tca.SchemaName == null ? string.Empty : tca.SchemaName;
                    var otherSn = schemaTable.SchemaName == null ? string.Empty : schemaTable.SchemaName;

                    return tca.TableName.Equals(schemaTable.TableName, sc) &&
                            sn.Equals(otherSn, sc);
                });


                // tmp sync table with only writable columns
                var changesSet = schemaTable.Schema.Clone(false);
                var schemaChangesTable = CreateChangesTable(schemaTable, changesSet);

                // get bpi from changes to be applied
                var bpiTables = message.Changes.GetBatchPartsInfo(schemaTable);

                if (bpiTables == null || !bpiTables.Any())
                    continue;

                var tableBpis = lastSyncErrorsBatchInfo.GetBatchPartsInfo(schemaTable);

                if (tableBpis == null || !tableBpis.Any())
                    continue;

                var localSerializerReader = new LocalJsonSerializer();
                var localSerializerWriter = new LocalJsonSerializer();

                // Load in memory failed rows for this table
                var failedRows = new List<SyncRow>();

                var lastSyncErrorsBpiFullPath = lastSyncErrorsBatchInfo.GetBatchPartInfoPath(tableBpis.ToList()[0]).FullPath;
                foreach (var syncRow in localSerializerReader.ReadRowsFromFile(lastSyncErrorsBpiFullPath, schemaChangesTable))
                    failedRows.Add(syncRow);

                // Open again the same file
                await localSerializerWriter.OpenFileAsync(lastSyncErrorsBpiFullPath, schemaChangesTable).ConfigureAwait(false);


                foreach (var batchPartInfo in bpiTables)
                {
                    // Get full path of my batchpartinfo
                    var fullPath = message.Changes.GetBatchPartInfoPath(batchPartInfo).FullPath;

                    foreach (var syncRow in localSerializerReader.ReadRowsFromFile(fullPath, schemaChangesTable))
                    {
                        var rowIsInBatch = SyncRows.GetRowByPrimaryKeys(syncRow, failedRows, schemaTable);

                        // we found the row in the batch, that means the failed row is currently in a progress of being updated
                        // we can remove it from failedRowsTable
                        if (rowIsInBatch != null)
                        {
                            failedRows.Remove(rowIsInBatch);

                            if (tableChangesApplied != null && tableChangesApplied.Failed > 0)
                                tableChangesApplied.Failed--;
                        }

                        if (failedRows.Count <= 0)
                            break;
                    }

                    if (failedRows.Count <= 0)
                        break;
                }

                foreach (var row in failedRows)
                    await localSerializerWriter.WriteRowToFileAsync(row, schemaChangesTable).ConfigureAwait(false);

                await localSerializerWriter.CloseFileAsync();

                if (failedRows.Count <= 0 && File.Exists(lastSyncErrorsBpiFullPath))
                    File.Delete(lastSyncErrorsBpiFullPath);
            }
        }

        /// <summary>
        /// Apply changes : Delete / Insert / Update
        /// the fromScope is local client scope when this method is called from server
        /// the fromScope is server scope when this method is called from client
        /// </summary>
        internal virtual async Task<Exception>
            InternalApplyChangesAsync(ScopeInfo scopeInfo, SyncContext context, MessageApplyChanges message, DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            context.SyncStage = SyncStage.ChangesApplying;

            message.ChangesApplied ??= new DatabaseChangesApplied();
            message.FailedRows ??= message.Schema.Clone();

            // Check if we have some data available
            var hasChanges = message.Changes.HasData();

            // critical exception that causes rollback
            Exception failureException = null;

            try
            {
                // if we have changes or if we are in re init mode
                if (hasChanges || context.SyncType != SyncType.Normal)
                {
                    var schemaTables = message.Schema.Tables.SortByDependencies(tab => tab.GetRelations().Select(r => r.GetParentTable()));

                    // create local directory
                    if (!string.IsNullOrEmpty(message.BatchDirectory) && !Directory.Exists(message.BatchDirectory))
                        Directory.CreateDirectory(message.BatchDirectory);


                    // Disable check constraints
                    // Because Sqlite does not support "PRAGMA foreign_keys=OFF" Inside a transaction
                    // Report this disabling constraints brefore opening a transaction
                    if (this.Options.DisableConstraintsOnApplyChanges)
                    {
                        foreach (var table in schemaTables)
                            context = await this.InternalDisableConstraintsAsync(scopeInfo, context, table, connection, transaction).ConfigureAwait(false);
                    }

                    // -----------------------------------------------------
                    // 0) Check if we are in a reinit mode (Check also SyncWay to be sure we don't reset tables on server, then check if we don't have already applied a snapshot)
                    // -----------------------------------------------------
                    if (context.SyncWay == SyncWay.Download && context.SyncType != SyncType.Normal && !message.SnapshoteApplied)
                    {
                        foreach (var table in schemaTables.Reverse())
                            context = await this.InternalResetTableAsync(scopeInfo, context, table, connection, transaction).ConfigureAwait(false);
                    }

                    // Trying to change order (from deletes-upserts to upserts-deletes)
                    // see https://github.com/Mimetis/Dotmim.Sync/discussions/453#discussioncomment-380530

                    // -----------------------------------------------------
                    // 1) Applying Inserts and Updates. Apply in table order
                    // -----------------------------------------------------
                    if (hasChanges)
                    {
                        foreach (var table in schemaTables)
                        {
                            failureException = await this.InternalApplyTableChangesAsync(scopeInfo, context, table, message, message.FailedRows.Tables[table.TableName, table.SchemaName],
                                        connection, transaction, SyncRowState.Modified, message.ChangesApplied,
                                        cancellationToken, progress).ConfigureAwait(false);

                            if (failureException != null)
                                break;
                        }
                    }

                    // -----------------------------------------------------
                    // 2) Applying Deletes. Do not apply deletes if we are in a new database
                    // -----------------------------------------------------
                    if (!message.IsNew && hasChanges && failureException == null)
                    {
                        foreach (var table in schemaTables.Reverse())
                        {
                            failureException = await this.InternalApplyTableChangesAsync(scopeInfo, context, table, message, message.FailedRows.Tables[table.TableName, table.SchemaName],
                                connection, transaction, SyncRowState.Deleted, message.ChangesApplied,
                                cancellationToken, progress).ConfigureAwait(false);

                            if (failureException != null)
                                break;
                        }
                    }

                    // Re enable check constraints
                    if (this.Options.DisableConstraintsOnApplyChanges)
                        foreach (var table in schemaTables)
                            context = await this.InternalEnableConstraintsAsync(scopeInfo, context, table, connection, transaction).ConfigureAwait(false);

                }

                // if we set option to clean folder && message allows to clean (it won't allow to clean if batch is an error batch)
                if (this.Options.CleanFolder)
                {
                    // Before cleaning, check if we are not applying changes from a snapshotdirectory
                    var cleanFolder = await this.InternalCanCleanFolderAsync(scopeInfo.Name, context.Parameters, message.Changes, cancellationToken, progress).ConfigureAwait(false);

                    // clear the changes because we don't need them anymore
                    if (cleanFolder)
                        message.Changes.TryRemoveDirectory();
                }


                return failureException;
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
            }
        }

        /// <summary>
        /// Apply changes internal method for one type of query: Insert, Update or Delete for every batch from a table
        /// </summary>
        internal virtual async Task<Exception> InternalApplyTableChangesAsync(ScopeInfo scopeInfo, SyncContext context, SyncTable schemaTable,
            MessageApplyChanges message, SyncTable errorsTable,
            DbConnection connection, DbTransaction transaction, SyncRowState applyType, DatabaseChangesApplied changesApplied,
            CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (this.Provider == null)
                return default;

            context.SyncStage = SyncStage.ChangesApplying;

            var setupTable = scopeInfo.Setup.Tables[schemaTable.TableName, schemaTable.SchemaName];

            if (setupTable == null)
                return default;

            // Only table schema is replicated, no datas are applied
            if (setupTable.SyncDirection == SyncDirection.None)
                return default;

            // if we are in upload stage, so check if table is not download only
            if (context.SyncWay == SyncWay.Upload && setupTable.SyncDirection == SyncDirection.DownloadOnly)
                return default;

            // if we are in download stage, so check if table is not download only
            if (context.SyncWay == SyncWay.Download && setupTable.SyncDirection == SyncDirection.UploadOnly)
                return default;

            var hasChanges = message.Changes.HasData(schemaTable.TableName, schemaTable.SchemaName);

            // Each table in the messages contains scope columns. Don't forget it
            if (!hasChanges)
                return default;

            // what kind of command to execute
            var init = message.IsNew || context.SyncType != SyncType.Normal;
            DbCommandType dbCommandType = applyType == SyncRowState.Deleted ? DbCommandType.DeleteRows : (init ? DbCommandType.InsertRows : DbCommandType.UpdateRows);

            // tmp sync table with only writable columns
            var changesSet = schemaTable.Schema.Clone(false);
            var schemaChangesTable = CreateChangesTable(schemaTable, changesSet);

            // get executioning adapter
            var syncAdapter = this.GetSyncAdapter(scopeInfo.Name, schemaChangesTable, scopeInfo.Setup);

            IEnumerable<BatchPartInfo> bpiTables;
            // Get command
            DbCommand command;
            bool isBatch;
            string cmdText;

            await using (var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false))
            {
                (command, isBatch) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, dbCommandType, null,
                runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress);

                if (command == null)
                    return default;

                bpiTables = message.Changes.GetBatchPartsInfo(schemaTable);

                // launch interceptor if any
                var args = new TableChangesApplyingArgs(context, message.Changes, bpiTables, schemaTable, applyType, command, connection, transaction);
                await this.InterceptAsync(args, progress, cancellationToken).ConfigureAwait(false);

                if (args.Cancel || args.Command == null)
                    return default;

                command = args.Command;
                cmdText = command.CommandText;
            }

            TableChangesApplied tableChangesApplied = null;

            var localSerializer = new LocalJsonSerializer();

            // If someone has an interceptor on deserializing, we read the row and intercept
            var interceptorsReading = this.interceptors.GetInterceptors<DeserializingRowArgs>();
            if (interceptorsReading.Count > 0)
            {
                localSerializer.OnReadingRow(async (schemaTable, rowString) =>
                {
                    var args = new DeserializingRowArgs(context, schemaTable, rowString);
                    await this.InterceptAsync(args, progress, cancellationToken).ConfigureAwait(false);
                    return args.Result;
                });
            }

            // Failure exception if any
            Exception failureException = null;

            // Conflicts occured when trying to apply rows
            var conflictRows = new List<SyncRow>();

            // failed rows that were ignored
            var failedRows = 0;

            // Errors occured when trying to apply rows
            var errorsRows = new List<(SyncRow SyncRow, Exception Exception)>();

            // Applied row for this particular BPI
            var appliedRows = 0;

            // conflict resolved count
            int conflictsResolvedCount = 0;

            foreach (var batchPartInfo in bpiTables)
            {

                // Rows fetch (either of the good state or not) from the BPI
                var rowsFetched = 0;

                // Get full path of my batchpartinfo
                var fullPath = message.Changes.GetBatchPartInfoPath(batchPartInfo).FullPath;

                // accumulating rows
                var batchRows = new List<SyncRow>();

                await using var runner = await this.GetConnectionAsync(context, Options.TransactionMode == TransactionMode.PerBatch ? SyncMode.WithTransaction : SyncMode.NoTransaction, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (isBatch)
                {
                    foreach (var syncRow in localSerializer.ReadRowsFromFile(fullPath, schemaChangesTable))
                    {
                        rowsFetched++;

                        // Adding rows to the batch rows
                        if (batchRows.Count < this.Provider.BulkBatchMaxLinesCount)
                        {
                            if (applyType == SyncRowState.Modified && (syncRow.RowState == SyncRowState.RetryModifiedOnNextSync || syncRow.RowState == SyncRowState.Modified))
                                batchRows.Add(syncRow);
                            else if (applyType == SyncRowState.Deleted && (syncRow.RowState == SyncRowState.RetryDeletedOnNextSync || syncRow.RowState == SyncRowState.Deleted))
                                batchRows.Add(syncRow);
                            else if (syncRow.RowState == SyncRowState.ApplyModifiedFailed || syncRow.RowState == SyncRowState.ApplyDeletedFailed)
                                errorsRows.Add((syncRow, new Exception("Row failed to be applied on last sync")));

                            if (rowsFetched < batchPartInfo.RowsCount && batchRows.Count < this.Provider.BulkBatchMaxLinesCount)
                                continue;
                        }
                        if (batchRows.Count <= 0)
                            continue;

                        command.CommandText = cmdText;
                        command.Connection = runner.Connection;
                        command.Transaction = runner.Transaction;

                        var (rowAppliedCount, conflictSyncRows, errorException) = await this.InternalApplyBatchRowsAsync(context, command, batchRows, schemaChangesTable, applyType, message, dbCommandType, syncAdapter,
                                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        if (errorException == null)
                        {
                            // Add applied rows
                            appliedRows += rowAppliedCount;

                            // Check conflicts
                            if (conflictSyncRows != null && conflictSyncRows.Count > 0)
                                foreach (var conflictRow in conflictSyncRows)
                                    conflictRows.Add(conflictRow);

                        }
                        else
                        {
                            // we have an error in the entire batch
                            // try to fallback to row per row
                            // and see if we can still continue to insert rows (excepted the error one) and manage the error

                            // fallback to row per row
                            syncAdapter.UseBulkOperations = false;
                            (command, isBatch) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, dbCommandType, null,
                                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress);
                            cmdText = command.CommandText;


                            foreach (var batchRow in batchRows)
                            {
                                if (batchRow.RowState == SyncRowState.ApplyModifiedFailed || batchRow.RowState == SyncRowState.ApplyDeletedFailed)
                                {
                                    errorsRows.Add((batchRow, new Exception("Row failed to be applied on last sync")));
                                    continue;
                                }

                                if (applyType == SyncRowState.Modified && batchRow.RowState != SyncRowState.RetryModifiedOnNextSync && batchRow.RowState != SyncRowState.Modified)
                                    continue;

                                if (applyType == SyncRowState.Deleted && batchRow.RowState != SyncRowState.RetryDeletedOnNextSync && batchRow.RowState != SyncRowState.Deleted)
                                    continue;

                                command.CommandText = cmdText;
                                command.Connection = runner.Connection;
                                command.Transaction = runner.Transaction;

                                var (singleRowAppliedCount, singleErrorException) = await this.InternalApplySingleRowAsync(context, command, batchRow, schemaChangesTable, applyType, message, dbCommandType,
                                            runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                                if (singleRowAppliedCount > 0)
                                    appliedRows++;
                                else if (singleErrorException != null)
                                    errorsRows.Add((batchRow, singleErrorException));
                                else
                                    conflictRows.Add(batchRow);
                            }

                            // revert back bulk operation
                            syncAdapter.UseBulkOperations = true;
                        }
                        batchRows.Clear();
                    }
                }
                else
                {
                    foreach (var syncRow in localSerializer.ReadRowsFromFile(fullPath, schemaChangesTable))
                    {
                        if (syncRow.RowState == SyncRowState.ApplyModifiedFailed || syncRow.RowState == SyncRowState.ApplyDeletedFailed)
                        {
                            errorsRows.Add((syncRow, new Exception("Row failed to be applied on last sync")));
                            continue;
                        }

                        if (applyType == SyncRowState.Modified && syncRow.RowState != SyncRowState.RetryModifiedOnNextSync && syncRow.RowState != SyncRowState.Modified)
                            continue;

                        if (applyType == SyncRowState.Deleted && syncRow.RowState != SyncRowState.RetryDeletedOnNextSync && syncRow.RowState != SyncRowState.Deleted)
                            continue;

                        command.CommandText = cmdText;
                        command.Connection = runner.Connection;
                        command.Transaction = runner.Transaction;

                        var (rowAppliedCount, errorException) = await this.InternalApplySingleRowAsync(context, command, syncRow, schemaChangesTable, applyType, message, dbCommandType,
                            runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (rowAppliedCount > 0)
                            appliedRows++;
                        else if (errorException != null)
                            errorsRows.Add((syncRow, errorException));
                        else
                            conflictRows.Add(syncRow);
                    }
                }

                await runner.CommitAsync().ConfigureAwait(false);
            }

            if ((conflictRows != null && conflictRows.Count > 0) || (errorsRows != null && errorsRows.Count > 0))
            {
                await using var runnerError = await this.GetConnectionAsync(context, Options.TransactionMode == TransactionMode.None ? SyncMode.NoTransaction : SyncMode.WithTransaction, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // If conflicts occured
                foreach (var conflictRow in conflictRows)
                {
                    var (applied, conflictResolved, exception) =
                        await this.HandleConflictAsync(scopeInfo, context, message.LocalScopeId, message.SenderScopeId, conflictRow, schemaChangesTable,
                                                       message.Policy, message.LastTimestamp,
                                                       runnerError.Connection, runnerError.Transaction, runnerError.CancellationToken, runnerError.Progress).ConfigureAwait(false);

                    if (exception != null)
                    {
                        errorsRows.Add((conflictRow, exception));
                    }
                    else
                    {
                        conflictsResolvedCount += conflictResolved ? 1 : 0;
                        appliedRows += applied ? 1 : 0;
                    }
                }

                // If errors occured
                bool shouldRollbackTransaction = false;
                foreach (var errorRow in errorsRows)
                {
                    if ((errorRow.SyncRow.RowState == SyncRowState.ApplyModifiedFailed || errorRow.SyncRow.RowState == SyncRowState.Modified || errorRow.SyncRow.RowState == SyncRowState.RetryModifiedOnNextSync)
                        && applyType == SyncRowState.Deleted)
                        continue;

                    if ((errorRow.SyncRow.RowState == SyncRowState.ApplyDeletedFailed || errorRow.SyncRow.RowState == SyncRowState.Deleted || errorRow.SyncRow.RowState == SyncRowState.RetryDeletedOnNextSync)
                        && applyType == SyncRowState.Modified)
                        continue;

                    ErrorAction errorAction;
                    (errorAction, failureException) = await this.HandleErrorAsync(
                                        scopeInfo, context, errorRow.SyncRow, applyType, schemaChangesTable,
                                        errorRow.Exception, message.SenderScopeId, message.LastTimestamp,
                                        runnerError.Connection, runnerError.Transaction, runnerError.CancellationToken, runnerError.Progress).ConfigureAwait(false);

                    // If failed, add the row to the final output failure rows
                    var existingRow = SyncRows.GetRowByPrimaryKeys(errorRow.SyncRow, errorsTable.Rows, errorsTable);

                    if (existingRow != null)
                        errorsTable.Rows.Remove(existingRow);

                    // User decides error should be logged
                    if (errorAction != ErrorAction.Resolved)
                        errorsTable.Rows.Add(errorRow.SyncRow);

                    // final throw if any error coming back from HandleErrorAsync
                    if (errorAction != ErrorAction.Ignore)
                    {
                        if (errorAction == ErrorAction.Throw)
                        {
                            failedRows++;
                            shouldRollbackTransaction = true;
                            // Break because a critical error has been raised and we don't want to continue
                            break;
                        }
                        else
                        {
                            failedRows += errorAction == ErrorAction.Log ? 1 : 0;
                            appliedRows += errorAction == ErrorAction.Resolved ? 1 : 0;
                        }

                    }
                }

                // Close file
                if (localSerializer.IsOpen)
                    await localSerializer.CloseFileAsync().ConfigureAwait(false);

                if (shouldRollbackTransaction)
                    await runnerError.RollbackAsync().ConfigureAwait(false);
                else
                    await runnerError.CommitAsync().ConfigureAwait(false);
            }

            // Only Upsert DatabaseChangesApplied if we make an upsert/ delete from the batch or resolved any conflict
            if (appliedRows > 0 || conflictsResolvedCount > 0 || failedRows > 0)
            {
                // We may have multiple batch files, so we can have multipe sync tables with the same name
                // We can say that a syncTable may be contained in several files
                // That's why we should get an applied changes instance if already exists from a previous batch file
                tableChangesApplied = changesApplied.TableChangesApplied.FirstOrDefault(tca =>
                {
                    var sc = SyncGlobalization.DataSourceStringComparison;

                    var sn = tca.SchemaName == null ? string.Empty : tca.SchemaName;
                    var otherSn = schemaTable.SchemaName == null ? string.Empty : schemaTable.SchemaName;

                    return tca.TableName.Equals(schemaTable.TableName, sc) &&
                           sn.Equals(otherSn, sc) &&
                           tca.State == applyType;
                });

                if (tableChangesApplied == null)
                {
                    tableChangesApplied = new TableChangesApplied
                    {
                        TableName = schemaTable.TableName,
                        SchemaName = schemaTable.SchemaName,
                        Applied = appliedRows,
                        ResolvedConflicts = conflictsResolvedCount,
                        Failed = failedRows,
                        State = applyType,
                        TotalRowsCount = message.Changes.RowsCount,
                        TotalAppliedCount = changesApplied.TotalAppliedChanges + appliedRows
                    };
                    changesApplied.TableChangesApplied.Add(tableChangesApplied);
                }
                else
                {
                    tableChangesApplied.Applied += appliedRows;
                    tableChangesApplied.TotalAppliedCount = changesApplied.TotalAppliedChanges;
                    tableChangesApplied.ResolvedConflicts += conflictsResolvedCount;
                    tableChangesApplied.Failed += failedRows;
                }

                // we've got 0.25% to fill here 
                var progresspct = appliedRows * 0.25d / tableChangesApplied.TotalRowsCount;
                context.ProgressPercentage += progresspct;
            }

            schemaChangesTable.Dispose();
            schemaChangesTable = null;
            changesSet.Dispose();
            changesSet = null;

            // Report the overall changes applied for the current table
            if (tableChangesApplied != null)
            {
                if (connection == null)
                    connection = this.Provider.CreateConnection();

                var tableChangesAppliedArgs = new TableChangesAppliedArgs(context, tableChangesApplied, connection, transaction);
                // We don't report progress if we do not have applied any changes on the table, to limit verbosity of Progress
                await this.InterceptAsync(tableChangesAppliedArgs, progress, cancellationToken).ConfigureAwait(false);
            }

            if (command != null)
                command.Dispose();

            return failureException;
        }


        internal virtual async Task<(int rowAppliedCount, Exception errorException)> InternalApplySingleRowAsync(SyncContext context, DbCommand command, SyncRow syncRow, SyncTable schemaChangesTable,
            SyncRowState applyType, MessageApplyChanges message, DbCommandType dbCommandType,
             DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var batchArgs = new RowsChangesApplyingArgs(context, message.Changes, new List<SyncRow> { syncRow }, schemaChangesTable, applyType, command, connection, transaction);
            await this.InterceptAsync(batchArgs, progress, cancellationToken).ConfigureAwait(false);

            if (batchArgs.Cancel || batchArgs.Command == null || batchArgs.SyncRows == null || batchArgs.SyncRows.Count() <= 0)
                return (-1, null);

            // get the correct pointer to the command from the interceptor in case user change the whole instance
            command = batchArgs.Command;

            // Set the parameters value from row 
            this.SetColumnParametersValues(command, batchArgs.SyncRows.First());

            // Set the special parameters for update
            this.AddScopeParametersValues(command, message.SenderScopeId, message.LastTimestamp, applyType == SyncRowState.Deleted, false);

            await this.InterceptAsync(new ExecuteCommandArgs(context, command, dbCommandType, connection, transaction),
                progress, cancellationToken).ConfigureAwait(false);

            int rowAppliedCount = 0;
            Exception errorException = null;
            try
            {
                rowAppliedCount = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                // Check if we have a return value instead
                var syncRowCountParam = InternalGetParameter(command, "sync_row_count");

                if (syncRowCountParam != null && syncRowCountParam.Value != null && syncRowCountParam.Value != DBNull.Value)
                    rowAppliedCount = (int)syncRowCountParam.Value;
            }
            catch (Exception ex)
            {
                errorException = ex;
            }

            return (rowAppliedCount, errorException);
        }


        internal virtual async Task<(int rowAppliedCount, SyncRows conflicts, Exception errorException)> InternalApplyBatchRowsAsync(SyncContext context, DbCommand command, List<SyncRow> batchRows, SyncTable schemaChangesTable,
             SyncRowState applyType, MessageApplyChanges message, DbCommandType dbCommandType, DbSyncAdapter syncAdapter,
             DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var conflictRowsTable = schemaChangesTable.Schema.Clone().Tables[schemaChangesTable.TableName, schemaChangesTable.SchemaName];

            var batchArgs = new RowsChangesApplyingArgs(context, message.Changes, batchRows, schemaChangesTable, applyType, command, connection, transaction);
            await this.InterceptAsync(batchArgs, progress, cancellationToken).ConfigureAwait(false);

            if (batchArgs.Cancel || batchArgs.Command == null || batchArgs.SyncRows == null || batchArgs.SyncRows.Count <= 0)
                return (-1, null, null);

            // get the correct pointer to the command from the interceptor in case user change the whole instance
            command = batchArgs.Command;

            await this.InterceptAsync(new ExecuteCommandArgs(context, command, dbCommandType, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            Exception errorException = null;

            try
            {
                // execute the batch, through the provider
                await syncAdapter.ExecuteBatchCommandAsync(command, message.SenderScopeId, batchArgs.SyncRows, schemaChangesTable, conflictRowsTable, message.LastTimestamp, connection, transaction).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                errorException = ex;
            }

            var rowAppliedCount = errorException != null ? 0 : batchRows.Count - conflictRowsTable.Rows.Count;

            return (rowAppliedCount, conflictRowsTable.Rows, errorException);
        }
    }
}
