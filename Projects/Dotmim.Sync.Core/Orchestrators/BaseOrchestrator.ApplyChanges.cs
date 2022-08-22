using Dotmim.Sync.Args;
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
        internal virtual async Task<(SyncContext, DatabaseChangesApplied)>
            InternalApplyChangesAsync(IScopeInfo scopeInfo, SyncContext context, MessageApplyChanges message, DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            context.SyncStage = SyncStage.ChangesApplying;
            // call interceptor
            var databaseChangesApplyingArgs = new DatabaseChangesApplyingArgs(context, message, connection, transaction);
            await this.InterceptAsync(databaseChangesApplyingArgs, progress, cancellationToken).ConfigureAwait(false);

            var changesApplied = new DatabaseChangesApplied();

            // Check if we have some data available
            var hasChanges = message.BatchInfo.HasData();

            try
            {

                // if we have changes or if we are in re init mode
                if (hasChanges || context.SyncType != SyncType.Normal)
                {
                    var schemaTables = message.Schema.Tables.SortByDependencies(tab => tab.GetRelations().Select(r => r.GetParentTable()));

                    // Disable check constraints
                    // Because Sqlite does not support "PRAGMA foreign_keys=OFF" Inside a transaction
                    // Report this disabling constraints brefore opening a transaction
                    if (message.DisableConstraintsOnApplyChanges)
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
                            context = await this.InternalApplyTableChangesAsync(scopeInfo, context, table, message,
                                        connection, transaction, DataRowState.Modified, changesApplied,
                                        cancellationToken, progress).ConfigureAwait(false);

                        }
                    }

                    // -----------------------------------------------------
                    // 2) Applying Deletes. Do not apply deletes if we are in a new database
                    // -----------------------------------------------------
                    if (!message.IsNew && hasChanges)
                    {
                        foreach (var table in schemaTables.Reverse())
                        {
                            context = await this.InternalApplyTableChangesAsync(scopeInfo, context, table, message,
                                connection, transaction, DataRowState.Deleted, changesApplied,
                                cancellationToken, progress).ConfigureAwait(false);
                        }
                    }

                    // Re enable check constraints
                    if (message.DisableConstraintsOnApplyChanges)
                        foreach (var table in schemaTables)
                            context = await this.InternalEnableConstraintsAsync(scopeInfo, context, table, connection, transaction).ConfigureAwait(false);

                    // Dispose data
                    message.BatchInfo.Clear(false);
                }

                // Before cleaning, check if we are not applying changes from a snapshotdirectory
                var cleanFolder = message.CleanFolder;

                if (cleanFolder)
                    cleanFolder = await this.InternalCanCleanFolderAsync(scopeInfo.Name, context.Parameters, message.BatchInfo, cancellationToken, progress).ConfigureAwait(false);

                // clear the changes because we don't need them anymore
                message.BatchInfo.Clear(cleanFolder);

                // Just get one for event raising
                if (connection == null)
                    connection = this.Provider.CreateConnection();

                var databaseChangesAppliedArgs = new DatabaseChangesAppliedArgs(context, changesApplied, connection, transaction);
                await this.InterceptAsync(databaseChangesAppliedArgs, progress, cancellationToken).ConfigureAwait(false);

                return (context, changesApplied);
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
        private async Task<SyncContext> InternalApplyTableChangesAsync(IScopeInfo scopeInfo, SyncContext context, SyncTable schemaTable, MessageApplyChanges message,
            DbConnection connection, DbTransaction transaction, DataRowState applyType, DatabaseChangesApplied changesApplied,
            CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (this.Provider == null)
                return context;

            context.SyncStage = SyncStage.ChangesApplying;

            var setupTable = scopeInfo.Setup.Tables[schemaTable.TableName, schemaTable.SchemaName];

            if (setupTable == null)
                return context;

            // Only table schema is replicated, no datas are applied
            if (setupTable.SyncDirection == SyncDirection.None)
                return context;

            // if we are in upload stage, so check if table is not download only
            if (context.SyncWay == SyncWay.Upload && setupTable.SyncDirection == SyncDirection.DownloadOnly)
                return context;

            // if we are in download stage, so check if table is not download only
            if (context.SyncWay == SyncWay.Download && setupTable.SyncDirection == SyncDirection.UploadOnly)
                return context;

            var hasChanges = message.BatchInfo.HasData(schemaTable.TableName, schemaTable.SchemaName);

            // Each table in the messages contains scope columns. Don't forget it
            if (!hasChanges)
                return context;

            // what kind of command to execute
            var init = message.IsNew || context.SyncType != SyncType.Normal;
            DbCommandType dbCommandType = applyType == DataRowState.Deleted ? DbCommandType.DeleteRows : (init ? DbCommandType.InsertRows : DbCommandType.UpdateRows);

            // tmp sync table with only writable columns
            var changesSet = schemaTable.Schema.Clone(false);
            var schemaChangesTable = CreateChangesTable(schemaTable, changesSet);

            // get executioning adapter
            var syncAdapter = this.GetSyncAdapter(schemaChangesTable, scopeInfo);


            IEnumerable<BatchPartInfo> bpiTables;
            // Get command
            DbCommand command;
            bool isBatch;
            string cmdText;

            await using (var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false))
            {
                (command, isBatch) = await this.GetCommandAsync(scopeInfo, context, schemaChangesTable, dbCommandType, null,
                runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress);

                if (command == null) return context;
                
                bpiTables = message.BatchInfo.GetBatchPartsInfo(schemaTable);

                // launch interceptor if any
                var args = new TableChangesApplyingArgs(context, message.BatchInfo, bpiTables, schemaTable, applyType, command, connection, transaction);
                await this.InterceptAsync(args, progress, cancellationToken).ConfigureAwait(false);

                if (args.Cancel || args.Command == null)
                    return context;

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

            // I've got all files for my table
            // applied rows for this bpi
            foreach (var batchPartInfo in bpiTables)
            {
                // Conflicts occured when trying to apply rows
                var conflictRows = new List<SyncRow>();

                // Errors occured when trying to apply rows
                var errorsRows = new List<(SyncRow SyncRow, Exception Exception)>();

                // Applied row for this particular BPI
                var appliedRows = 0;

                // failed rows that were ignored
                var failedRows = 0;

                // Rows fetch (either of the good state or not) from the BPI
                var rowsFetched = 0;

                // Get full path of my batchpartinfo
                var fullPath = message.BatchInfo.GetBatchPartInfoPath(batchPartInfo).FullPath;

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
                            if (syncRow.RowState == applyType)
                                batchRows.Add(syncRow);

                            if (rowsFetched < batchPartInfo.RowsCount && batchRows.Count < this.Provider.BulkBatchMaxLinesCount)
                                continue;
                        }
                        if (batchRows.Count <= 0)
                            continue;

                        var conflictRowsTable = schemaChangesTable.Schema.Clone().Tables[schemaChangesTable.TableName, schemaChangesTable.SchemaName];

                        command.CommandText = cmdText;
                        command.Connection = runner.Connection;
                        command.Transaction = runner.Transaction;

                        var batchArgs = new RowsChangesApplyingArgs(context, message.BatchInfo, batchRows, schemaChangesTable, applyType, command, runner.Connection, runner.Transaction);
                        await this.InterceptAsync(batchArgs, progress, cancellationToken).ConfigureAwait(false);

                        if (batchArgs.Cancel || batchArgs.Command == null || batchArgs.SyncRows == null || batchArgs.SyncRows.Count <= 0)
                            continue;

                        // get the correct pointer to the command from the interceptor in case user change the whole instance
                        command = batchArgs.Command;

                        await this.InterceptAsync(new ExecuteCommandArgs(context, command, dbCommandType, runner.Connection, runner.Transaction), progress, cancellationToken).ConfigureAwait(false);

                        // execute the batch, through the provider
                        await syncAdapter.ExecuteBatchCommandAsync(command, message.SenderScopeId, batchArgs.SyncRows, schemaChangesTable, conflictRowsTable, message.LastTimestamp, runner.Connection, runner.Transaction).ConfigureAwait(false);

                        foreach (var failedRow in conflictRowsTable.Rows)
                            conflictRows.Add(failedRow);

                        //rows minus failed rows
                        appliedRows += batchRows.Count - conflictRowsTable.Rows.Count;

                        batchRows.Clear();
                    }
                }
                else
                {
                    foreach (var syncRow in localSerializer.ReadRowsFromFile(fullPath, schemaChangesTable))
                    {
                        rowsFetched++;

                        if (syncRow.RowState != applyType)
                            continue;

                        command.CommandText = cmdText;
                        command.Connection = runner.Connection;
                        command.Transaction = runner.Transaction;

                        var batchArgs = new RowsChangesApplyingArgs(context, message.BatchInfo, new List<SyncRow> { syncRow }, schemaChangesTable, applyType, command, runner.Connection, runner.Transaction);
                        await this.InterceptAsync(batchArgs, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                        if (batchArgs.Cancel || batchArgs.Command == null || batchArgs.SyncRows == null || batchArgs.SyncRows.Count() <= 0)
                            continue;

                        // get the correct pointer to the command from the interceptor in case user change the whole instance
                        command = batchArgs.Command;

                        // Set the parameters value from row 
                        this.SetColumnParametersValues(command, batchArgs.SyncRows.First());

                        // Set the special parameters for update
                        this.AddScopeParametersValues(command, message.SenderScopeId, message.LastTimestamp, applyType == DataRowState.Deleted, false);

                        await this.InterceptAsync(new ExecuteCommandArgs(context, command, dbCommandType, runner.Connection, runner.Transaction),
                            runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                        int rowAppliedCount = 0;
                        Exception errorException = null;
                        DbParameter syncRowCountParam = null;
                        try
                        {
                            rowAppliedCount = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                            // Check if we have a return value instead
                            syncRowCountParam = GetParameter(command, "sync_row_count");

                            if (syncRowCountParam != null && syncRowCountParam.Value != null && syncRowCountParam.Value != DBNull.Value)
                                rowAppliedCount = (int)syncRowCountParam.Value;
                        }
                        catch (Exception ex)
                        {
                            errorException = ex;
                        }

                        if (rowAppliedCount > 0)
                            appliedRows++;
                        else if (errorException != null)
                            errorsRows.Add((syncRow, errorException));
                        else
                            conflictRows.Add(syncRow);
                    }
                }

                // conflict resolved count
                int conflictsResolvedCount = 0;

                // If conflicts occured
                foreach (var conflictRow in conflictRows)
                {
                    TableConflictErrorApplied tableConflictApplied;
                    (context, tableConflictApplied) =
                        await this.HandleConflictAsync(scopeInfo, context, message.LocalScopeId, message.SenderScopeId, conflictRow, schemaChangesTable,
                                                       message.Policy, message.LastTimestamp, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    if (tableConflictApplied.Exception != null)
                    {
                        errorsRows.Add((conflictRow, tableConflictApplied.Exception));
                    }
                    else
                    {
                        conflictsResolvedCount += tableConflictApplied.HasBeenResolved ? 1 : 0;
                        appliedRows += tableConflictApplied.HasBeenApplied ? 1 : 0;
                    }
                }

                // If errors occured
                foreach (var errorRow in errorsRows)
                {
                    TableConflictErrorApplied tableErrorApplied;
                    (context, tableErrorApplied) = await this.HandleErrorAsync(scopeInfo, context, errorRow.SyncRow, applyType,
                                                    schemaChangesTable, errorRow.Exception,
                                                    message.SenderScopeId, message.LastTimestamp,
                                                    connection, transaction, cancellationToken, progress).ConfigureAwait(false);


                    if (tableErrorApplied.Exception != null)
                    {
                        // Last chance, now we can throw the exception
                        throw tableErrorApplied.Exception;
                    }
                    else
                    {
                        appliedRows += tableErrorApplied.HasBeenApplied ? 1 : 0;
                        // to be a failed row, we need to have a row
                        // -- resolved (no exception on action)
                        // -- not applied (otherwise it's an applied row
                        failedRows += (tableErrorApplied.HasBeenResolved && !tableErrorApplied.HasBeenApplied) ? 1 : 0;
                    }
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
                            TotalRowsCount = message.BatchInfo.RowsCount,
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

                await runner.CommitAsync().ConfigureAwait(false);
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

            return context;
        }
    }
}
