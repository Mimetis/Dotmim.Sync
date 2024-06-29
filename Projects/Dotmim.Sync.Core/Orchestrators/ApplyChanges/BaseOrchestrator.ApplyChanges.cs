
using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {
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
                    this.Logger.LogInformation($@"[InternalApplyChangesAsync]. directory {{directoryName}} BatchPartsInfo count: {{BatchPartsInfoCount}} RowsCount {{RowsCount}}",
                        message.Changes.DirectoryName, message.Changes.BatchPartsInfo.Count, message.Changes.RowsCount);

                    var schemaTables = message.Schema.Tables.SortByDependencies(tab => tab.GetRelations().Select(r => r.GetParentTable()));

                    // create local directory
                    if (!string.IsNullOrEmpty(message.BatchDirectory) && !Directory.Exists(message.BatchDirectory))
                        Directory.CreateDirectory(message.BatchDirectory);

                    // Disable check constraints
                    // Because Sqlite does not support "PRAGMA foreign_keys=OFF" Inside a transaction
                    // Report this disabling constraints brefore opening a transaction
                    if (this.Options.DisableConstraintsOnApplyChanges && this.Provider.ConstraintsLevelAction == ConstraintsLevelAction.OnSessionLevel)
                    {
                        foreach (var table in schemaTables)
                            context = await this.InternalDisableConstraintsAsync(scopeInfo, context, table, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    }

                    // -----------------------------------------------------
                    // 0) Check if we are in a reinit mode (Check also SyncWay to be sure we don't reset tables on server, then check if we don't have already applied a snapshot)
                    // -----------------------------------------------------
                    if (context.SyncWay == SyncWay.Download && context.SyncType != SyncType.Normal && !message.SnapshoteApplied)
                    {
                        foreach (var table in schemaTables.Reverse())
                            context = await this.InternalResetTableAsync(scopeInfo, context, table, connection, transaction,
                                        cancellationToken, progress).ConfigureAwait(false);
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
                    if (this.Options.DisableConstraintsOnApplyChanges && this.Provider.ConstraintsLevelAction == ConstraintsLevelAction.OnSessionLevel)
                        foreach (var table in schemaTables)
                            context = await this.InternalEnableConstraintsAsync(scopeInfo, context, table, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                }

                // if we set option to clean folder && message allows to clean (it won't allow to clean if batch is an error batch)
                if (this.Options.CleanFolder)
                {
                    // Before cleaning, check if we are not applying changes from a snapshotdirectory
                    var cleanFolder = await this.InternalCanCleanFolderAsync(scopeInfo.Name, context.Parameters, message.Changes, cancellationToken, progress).ConfigureAwait(false);

                    // clear the changes because we don't need them anymore
                    if (cleanFolder)
                    {
                        this.Logger.LogInformation($@"[InternalApplyChangesAsync]. Cleaning directory {{directoryName}}.", message.Changes.DirectoryName);
                        message.Changes.TryRemoveDirectory();
                    }
                }

                this.Logger.LogInformation($@"[InternalApplyChangesAsync]. return exception {{failureException}} ", failureException != null ? failureException.Message : "No Exception");

                return failureException;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
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
            DbCommandType dbPreCommandType = applyType == SyncRowState.Deleted ? DbCommandType.PreDeleteRows : (init ? DbCommandType.PreInsertRows : DbCommandType.PreUpdateRows);

            this.Logger.LogInformation($@"[InternalApplyTableChangesAsync]. table {{tableName}}. init {{init}} command type {{dbCommandType}}", schemaTable.GetFullName(), init, dbCommandType);

            // tmp sync table with only writable columns
            var changesSet = schemaTable.Schema.Clone(false);
            var schemaChangesTable = CreateChangesTable(schemaTable, changesSet);

            // get executioning adapter
            var syncAdapter = this.GetSyncAdapter(scopeInfo.Name, schemaChangesTable, scopeInfo.Setup);

            TableChangesApplied tableChangesApplied = null;

            using var localSerializer = new LocalJsonSerializer(this, context);

            // conflict resolved count
            int conflictsResolvedCount = 0;

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

            string batchPartInfoFileName = null;

            IEnumerable<BatchPartInfo> bpiTables;
            // Get command
            DbCommand command = null;
            bool isBatch = false;
            string cmdText;

            bpiTables = message.Changes.GetBatchPartsInfo(schemaTable);

            // launch interceptor if any
            var args = new TableChangesApplyingArgs(context, message.Changes, bpiTables, schemaTable, applyType, command, connection, transaction);
            await this.InterceptAsync(args, progress, cancellationToken).ConfigureAwait(false);

            foreach (var batchPartInfo in bpiTables)
            {
                // Get full path of my batchpartinfo
                var fullPath = message.Changes.GetBatchPartInfoPath(batchPartInfo);

                if (batchPartInfo.State != SyncRowState.None && batchPartInfo.State != applyType)
                    continue;

                var batchChangesApplyingArgs = new BatchChangesApplyingArgs(context, message.Changes, batchPartInfo, schemaTable, applyType, command, connection, transaction);
                // We don't report progress if we do not have applied any changes on the table, to limit verbosity of Progress
                await this.InterceptAsync(batchChangesApplyingArgs, progress, cancellationToken).ConfigureAwait(false);

                // for error handling
                batchPartInfoFileName = batchPartInfo.FileName;

                this.Logger.LogInformation($@"[InternalApplyTableChangesAsync]. Directory name {{directoryName}}. BatchParts count {{BatchPartsInfoCount}}", message.Changes.DirectoryName, message.Changes.BatchPartsInfo.Count);

                // If we have a transient error happening, and we are rerunning the tranaction,
                // raising an interceptor
                var onRetry = new Func<Exception, int, TimeSpan, object, Task>((ex, cpt, ts, arg) =>
                    this.InterceptAsync(new TransientErrorOccuredArgs(context, connection, ex, cpt, ts), progress, cancellationToken));

                // Defining my retry policy
                SyncPolicy retryPolicy = Options.TransactionMode != TransactionMode.AllOrNothing
                 ? retryPolicy = SyncPolicy.WaitAndRetryForever(retryAttempt => TimeSpan.FromMilliseconds(500 * retryAttempt), (ex, arg) => this.Provider.ShouldRetryOn(ex), onRetry)
                 : retryPolicy = SyncPolicy.WaitAndRetry(0, TimeSpan.Zero);

                var applyChangesPolicyResult = await retryPolicy.ExecuteAsync(async () =>
                {
                    // Connection & Transaction runner
                    DbConnectionRunner runner = null;

                    // Conflicts occured when trying to apply rows
                    var conflictRows = new List<SyncRow>();

                    // failed rows that were ignored
                    var failedRows = 0;

                    // Errors occured when trying to apply rows
                    var errorsRows = new List<(SyncRow SyncRow, Exception Exception)>();

                    // Applied row for this particular BPI
                    var appliedRows = 0;

                    try
                    {
                        runner = await this.GetConnectionAsync(context, Options.TransactionMode == TransactionMode.PerBatch ? SyncMode.WithTransaction : SyncMode.NoTransaction, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Disable check constraints for provider supporting only at table level
                        if (this.Options.DisableConstraintsOnApplyChanges && this.Provider.ConstraintsLevelAction == ConstraintsLevelAction.OnTableLevel)
                            await this.InternalDisableConstraintsAsync(scopeInfo, context, schemaTable, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        // Pre command if exists
                        var (preCommand, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, dbPreCommandType,
                                runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        if (preCommand != null)
                        {
                            await this.InterceptAsync(new ExecuteCommandArgs(context, preCommand, dbPreCommandType, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                            await preCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
                            preCommand.Dispose();
                        }

                        (command, isBatch) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, dbCommandType,
                                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        if (command == null)
                            return (0, default, default, 0);

                        // Rows fetch from the BPI
                        var rowsFetched = 0;

                        // accumulating rows
                        var batchRows = new List<SyncRow>();

                        if (isBatch)
                        {
                            foreach (var syncRow in localSerializer.GetRowsFromFile(fullPath, schemaChangesTable))
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
                                    //if transient error, let the policy tries again, instead of going for 1 by 1 row
                                    var transientError = this.Provider.ShouldRetryOn(errorException);

                                    if (transientError)
                                        throw errorException;

                                    // we have an error in the entire batch
                                    // try to fallback to row per row
                                    // and see if we can still continue to insert rows (excepted the error one) and manage the error
                                    this.Logger.LogInformation($"[InternalApplyTableChangesAsync]. Using per line apply since we had an error on batch mode : {{errorException}}", errorException.Message);

                                    // fallback to row per row
                                    var fallbackArgs = new RowsChangesFallbackFromBatchToSingleRowApplyingArgs(context, errorException, message.Changes, batchRows, schemaChangesTable, applyType, command,
                                            runner.Connection, runner.Transaction);

                                    await this.InterceptAsync(fallbackArgs, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                                    syncAdapter.UseBulkOperations = false;
                                    (command, isBatch) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, dbCommandType,
                                                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

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

                                        var (singleRowAppliedCount, singleErrorException) = await this.InternalApplySingleRowAsync(context, command, batchRow, schemaChangesTable, syncAdapter, applyType, message, dbCommandType,
                                                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                                        if (singleRowAppliedCount > 0)
                                            appliedRows++;
                                        else if (singleErrorException != null && this.Provider.ShouldRetryOn(singleErrorException))
                                            throw singleErrorException;
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
                            command.Connection = runner.Connection;
                            command.Transaction = runner.Transaction;

                            foreach (var syncRow in localSerializer.GetRowsFromFile(fullPath, schemaChangesTable))
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

                                var (rowAppliedCount, errorException) = await this.InternalApplySingleRowAsync(context, command, syncRow, schemaChangesTable, syncAdapter, applyType, message, dbCommandType,
                                        runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                                if (rowAppliedCount > 0)
                                    appliedRows++;
                                else if (errorException != null && this.Provider.ShouldRetryOn(errorException))
                                    throw errorException;
                                else if (errorException != null)
                                    errorsRows.Add((syncRow, errorException));
                                else
                                    conflictRows.Add(syncRow);
                            }
                        }

                        // Enable check constraints for provider supporting only at table level
                        if (this.Options.DisableConstraintsOnApplyChanges && this.Provider.ConstraintsLevelAction == ConstraintsLevelAction.OnTableLevel)
                            await this.InternalEnableConstraintsAsync(scopeInfo, context, schemaTable, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        await runner.CommitAsync().ConfigureAwait(false);

                        return (appliedRows, conflictRows, errorsRows, failedRows);
                    }
                    catch (Exception ex)
                    {
                        if (runner != null)
                        {
                            await runner.RollbackAsync($"InternalApplyTableChangesAsync during apply changes. Error:{ex.Message}").ConfigureAwait(false);
                            await runner.DisposeAsync().ConfigureAwait(false);
                        }
                        throw GetSyncError(context, ex);
                    }
                    finally
                    {
                        // Close file
                        if (localSerializer.IsOpen)
                            localSerializer.CloseFile();
                    }

                }).ConfigureAwait(false);

                var batchChangesAppliedArgs = new BatchChangesAppliedArgs(context, message.Changes, batchPartInfo, schemaTable, applyType, command, connection, transaction);
                // We don't report progress if we do not have applied any changes on the table, to limit verbosity of Progress
                await this.InterceptAsync(batchChangesAppliedArgs, progress, cancellationToken).ConfigureAwait(false);

                appliedRows += applyChangesPolicyResult.appliedRows;
                failedRows += applyChangesPolicyResult.failedRows;

                if (applyChangesPolicyResult.conflictRows != null && applyChangesPolicyResult.conflictRows.Count > 0)
                    applyChangesPolicyResult.conflictRows.ForEach(sr => conflictRows.Add(sr));
                if (applyChangesPolicyResult.errorsRows != null && applyChangesPolicyResult.errorsRows.Count > 0)
                    applyChangesPolicyResult.errorsRows.ForEach(sr => errorsRows.Add(sr));
            }

            try
            {
                var ce = await InternalApplyConflictsAndErrorsAsync(scopeInfo, context, schemaChangesTable, applyType, errorsTable, conflictRows, errorsRows, message,
                      connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                appliedRows += ce.appliedRows;
                failedRows += ce.failedRows;
                conflictsResolvedCount += ce.conflictsResolvedCount;
                failureException = ce.failureException;

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

                    connection ??= this.Provider.CreateConnection();
                    var tableChangesAppliedArgs = new TableChangesAppliedArgs(context, tableChangesApplied, connection, transaction);
                    // We don't report progress if we do not have applied any changes on the table, to limit verbosity of Progress
                    await this.InterceptAsync(tableChangesAppliedArgs, progress, cancellationToken).ConfigureAwait(false);

                }

                schemaChangesTable.Dispose();
                schemaChangesTable = null;
                changesSet.Dispose();
                changesSet = null;

                if (command != null)
                    command.Dispose();
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

            this.Logger.LogInformation($@"[InternalApplyTableChangesAsync]. return exception {{failureException}} ", failureException != null ? failureException.Message : "No Exception");

            return failureException;
        }



        internal virtual async Task<(int rowAppliedCount, Exception errorException)> InternalApplySingleRowAsync(SyncContext context, DbCommand command,
            SyncRow syncRow, SyncTable schemaChangesTable, DbSyncAdapter syncAdapter,
            SyncRowState applyType, MessageApplyChanges message, DbCommandType dbCommandType,
             DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var batchArgs = new RowsChangesApplyingArgs(context, message.Changes, new List<SyncRow> { syncRow }, schemaChangesTable, applyType, command, connection, transaction);
            await this.InterceptAsync(batchArgs, progress, cancellationToken).ConfigureAwait(false);

            if (batchArgs.Cancel || batchArgs.Command == null || batchArgs.SyncRows == null || batchArgs.SyncRows.Count() <= 0)
                return (-1, null);

            Exception errorException = null;
            int rowAppliedCount = 0;

            try
            {
                // get the correct pointer to the command from the interceptor in case user change the whole instance
                command = batchArgs.Command;

                // Set the parameters value from row 
                this.InternalSetCommandParametersValues(context, command, dbCommandType, syncAdapter, connection, transaction, cancellationToken, progress,
                    batchArgs.SyncRows[0], message.SenderScopeId, message.LastTimestamp, applyType == SyncRowState.Deleted, false);

                await this.InterceptAsync(new ExecuteCommandArgs(context, command, dbCommandType, connection, transaction),
                    progress, cancellationToken).ConfigureAwait(false);

                rowAppliedCount = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                // Check if we have a return value instead
                var syncRowCountParam = syncAdapter.GetParameter(context, command, "sync_row_count");

                // Check if we have an handled error
                var syncErrorText = syncAdapter.GetParameter(context, command, "sync_error_text");

                if (syncRowCountParam != null && syncRowCountParam.Value != null && syncRowCountParam.Value != DBNull.Value)
                    rowAppliedCount = (int)syncRowCountParam.Value;

                if (syncErrorText != null && syncErrorText.Value != null && syncErrorText.Value != DBNull.Value)
                    throw new Exception(syncErrorText.Value.ToString());
            }
            catch (Exception ex)
            {
                string errorMessage = $"{ex.Message}\nCommand Text:{command.CommandText}\nCommand Type:{Enum.GetName(typeof(DbCommandType), dbCommandType)}";
                errorException = new Exception(errorMessage, ex);
            }

            var rowAppliedArgs = new RowsChangesAppliedArgs(context, message.Changes, new List<SyncRow> { batchArgs.SyncRows[0] }, schemaChangesTable, applyType, rowAppliedCount, errorException, connection, transaction);
            await this.InterceptAsync(rowAppliedArgs, progress, cancellationToken).ConfigureAwait(false);

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
                await syncAdapter.ExecuteBatchCommandAsync(context, command, message.SenderScopeId, batchArgs.SyncRows, schemaChangesTable, conflictRowsTable, message.LastTimestamp, connection, transaction).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                string errorMessage = $"{ex.Message}\nCommand Text:{command.CommandText}\nCommand Type:{Enum.GetName(typeof(DbCommandType), dbCommandType)}";
                errorException = new Exception(errorMessage, ex);
            }

            var rowAppliedCount = errorException != null ? 0 : batchRows.Count - conflictRowsTable.Rows.Count;

            var rowAppliedArgs = new RowsChangesAppliedArgs(context, message.Changes, batchRows, schemaChangesTable, applyType, rowAppliedCount, errorException, connection, transaction);
            await this.InterceptAsync(rowAppliedArgs, progress, cancellationToken).ConfigureAwait(false);

            return (rowAppliedCount, conflictRowsTable.Rows, errorException);
        }


        internal virtual async Task<(int appliedRows, int conflictsResolvedCount, int failedRows, Exception failureException)> InternalApplyConflictsAndErrorsAsync(ScopeInfo scopeInfo, SyncContext context,
            SyncTable schemaChangesTable, SyncRowState applyType, SyncTable errorsTable,
            List<SyncRow> conflictRows, List<(SyncRow SyncRow, Exception Exception)> errorsRows, MessageApplyChanges message, DbConnection connection, DbTransaction transaction,
                         CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            int conflictsResolvedCount = 0;
            int appliedRows = 0;
            int failedRows = 0;
            Exception failureException = null;

            if (!(conflictRows?.Count > 0) && !(errorsRows?.Count > 0))
            {
                return (appliedRows, conflictsResolvedCount, failedRows, failureException);
            }

            var runnerError = await this.GetConnectionAsync(context, Options.TransactionMode == TransactionMode.None ? SyncMode.NoTransaction : SyncMode.WithTransaction, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            await using (runnerError)
            {
                // Disable check constraints for provider supporting only at table level
                if (this.Options.DisableConstraintsOnApplyChanges && this.Provider.ConstraintsLevelAction == ConstraintsLevelAction.OnTableLevel)
                    await this.InternalDisableConstraintsAsync(scopeInfo, context, schemaChangesTable, runnerError.Connection, runnerError.Transaction, runnerError.CancellationToken, runnerError.Progress).ConfigureAwait(false);

                // If conflicts occured
                foreach (var conflictRow in conflictRows)
                {
                    this.Logger.LogInformation($@"[InternalApplyTableChangesAsync]. Handle {{conflictsCount}} conflitcts", conflictRows.Count);


                    var (applied, conflictResolved, exception) =
                        await this.HandleConflictAsync(scopeInfo, context, message.Changes, message.LocalScopeId, message.SenderScopeId, conflictRow, schemaChangesTable,
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
                    this.Logger.LogInformation($@"[InternalApplyTableChangesAsync]. Handle {{errorsRowsCount}} errors", errorsRows.Count);

                    if ((errorRow.SyncRow.RowState == SyncRowState.ApplyModifiedFailed || errorRow.SyncRow.RowState == SyncRowState.Modified || errorRow.SyncRow.RowState == SyncRowState.RetryModifiedOnNextSync)
                        && applyType == SyncRowState.Deleted)
                        continue;

                    if ((errorRow.SyncRow.RowState == SyncRowState.ApplyDeletedFailed || errorRow.SyncRow.RowState == SyncRowState.Deleted || errorRow.SyncRow.RowState == SyncRowState.RetryDeletedOnNextSync)
                        && applyType == SyncRowState.Modified)
                        continue;

                    ErrorAction errorAction;
                    (errorAction, failureException) = await this.HandleErrorAsync(
                                        scopeInfo, context, message.Changes, errorRow.SyncRow, applyType, schemaChangesTable,
                                        errorRow.Exception, message.SenderScopeId, message.LastTimestamp,
                                        runnerError.Connection, runnerError.Transaction, runnerError.CancellationToken, runnerError.Progress).ConfigureAwait(false);


                    // check if we have already the row in errorsTable
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

                // Enable check constraints for provider supporting only at table level
                if (this.Options.DisableConstraintsOnApplyChanges && this.Provider.ConstraintsLevelAction == ConstraintsLevelAction.OnTableLevel)
                    await this.InternalEnableConstraintsAsync(scopeInfo, context, schemaChangesTable, runnerError.Connection, runnerError.Transaction, runnerError.CancellationToken, runnerError.Progress).ConfigureAwait(false);

                if (shouldRollbackTransaction)
                    await runnerError.RollbackAsync($"Rollback because we can't resolve errors. Failure:{failureException?.Message}").ConfigureAwait(false);
                else
                    await runnerError.CommitAsync().ConfigureAwait(false);
            }

            return (appliedRows, conflictsResolvedCount, failedRows, failureException);
        }

        internal virtual async Task InternalApplyCleanErrorsAsync(ScopeInfo scopeInfo, SyncContext context,
                         BatchInfo lastSyncErrorsBatchInfo, MessageApplyChanges message, DbConnection connection, DbTransaction transaction,
                         CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (lastSyncErrorsBatchInfo == null)
                return;
            try
            {
                context.SyncStage = SyncStage.ChangesApplying;

                var schemaTables = message.Schema.Tables.SortByDependencies(tab => tab.GetRelations().Select(r => r.GetParentTable()));

                this.Logger.LogInformation($@"[InternalApplyCleanErrorsAsync]. Directory name {{directoryName}}. BatchParts count {{BatchPartsInfoCount}}", lastSyncErrorsBatchInfo.DirectoryName, lastSyncErrorsBatchInfo.BatchPartsInfo.Count);

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

                    using var localSerializerReader = new LocalJsonSerializer(this, context);
                    using var localSerializerWriter = new LocalJsonSerializer(this, context);

                    // Load in memory failed rows for this table
                    var failedRows = new List<SyncRow>();

                    // Read already present lines
                    var lastSyncErrorsBpiFullPath = lastSyncErrorsBatchInfo.GetBatchPartInfoPath(tableBpis.ToList()[0]);

                    var syncRows = localSerializerReader.GetRowsFromFile(lastSyncErrorsBpiFullPath, schemaChangesTable);
                    failedRows.AddRange(syncRows);

                    // Open again the same file
                    await localSerializerWriter.OpenFileAsync(lastSyncErrorsBpiFullPath, schemaChangesTable, SyncRowState.None).ConfigureAwait(false);

                    foreach (var batchPartInfo in bpiTables)
                    {
                        // Get full path of my batchpartinfo
                        var fullPath = message.Changes.GetBatchPartInfoPath(batchPartInfo);

                        foreach (var syncRow in localSerializerReader.GetRowsFromFile(fullPath, schemaChangesTable))
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

                    if (failedRows.Count <= 0 && File.Exists(lastSyncErrorsBpiFullPath))
                        File.Delete(lastSyncErrorsBpiFullPath);

                    this.Logger.LogInformation($@"[InternalApplyCleanErrorsAsync]. schemaTable {{schemaTableName}} failedRows count {{failedRowsCount}}", schemaTable.GetFullName(), failedRows.Count);
                }
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }



    }
}
