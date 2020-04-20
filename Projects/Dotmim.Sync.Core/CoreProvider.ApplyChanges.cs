using Dotmim.Sync.Enumerations;
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
    public abstract partial class CoreProvider
    {
        /// <summary>
        /// Apply changes : Delete / Insert / Update
        /// the fromScope is local client scope when this method is called from server
        /// the fromScope is server scope when this method is called from client
        /// </summary>
        public virtual async Task<(SyncContext, DatabaseChangesApplied)>
            ApplyChangesAsync(SyncContext context, MessageApplyChanges message, DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var changesApplied = new DatabaseChangesApplied();

            // Check if we have some data available
            var hasChanges = await message.Changes.HasDataAsync(this.Orchestrator);

            if (!hasChanges)
            {
                this.Orchestrator.logger.LogDebug(SyncEventsId.ApplyChanges, changesApplied);
                return (context, changesApplied);
            }

            // Disable check constraints
            // Because Sqlite does not support "PRAGMA foreign_keys=OFF" Inside a transaction
            // Report this disabling constraints brefore opening a transaction
            if (message.DisableConstraintsOnApplyChanges)
                foreach (var table in message.Schema.Tables.Reverse())
                    await this.DisableConstraintsAsync(context, table, message.Setup, connection, transaction);

            // -----------------------------------------------------
            // 0) Check if we are in a reinit mode
            // -----------------------------------------------------
            if (context.SyncWay == SyncWay.Download && context.SyncType != SyncType.Normal)
                await this.ResetInternalAsync(context, message.Schema, message.Setup, connection, transaction);

            // -----------------------------------------------------
            // 1) Applying deletes. Do not apply deletes if we are in a new database
            // -----------------------------------------------------
            if (!message.IsNew)
            {
                // for delete we must go from Up to Down
                foreach (var table in message.Schema.Tables.Reverse())
                {
                    await this.ApplyChangesInternalAsync(table, context, message, connection,
                        transaction, DataRowState.Deleted, changesApplied, cancellationToken, progress).ConfigureAwait(false);
                }
            }

            // -----------------------------------------------------
            // 2) Applying Inserts and Updates. Apply in table order
            // -----------------------------------------------------
            foreach (var table in message.Schema.Tables)
            {
                await this.ApplyChangesInternalAsync(table, context, message, connection,
                    transaction, DataRowState.Modified, changesApplied, cancellationToken, progress).ConfigureAwait(false);
            }

            // Re enable check constraints
            if (message.DisableConstraintsOnApplyChanges)
                foreach (var table in message.Schema.Tables)
                    await this.EnableConstraintsAsync(context, table, message.Setup, connection, transaction);


            // Before cleaning, check if we are not applying changes from a snapshotdirectory
            var cleanFolder = message.CleanFolder;
            if (cleanFolder && !String.IsNullOrEmpty(this.Options.SnapshotsDirectory) && !String.IsNullOrEmpty(message.Changes.DirectoryRoot))
            {
                var snapshotDirectory = new DirectoryInfo(Path.Combine(this.Options.SnapshotsDirectory, context.ScopeName)).FullName;
                var messageBatchInfoDirectory = new DirectoryInfo(message.Changes.DirectoryRoot).FullName;

                // If we are getting batches from a snapshot folder, do not delete it
                if (snapshotDirectory.Equals(messageBatchInfoDirectory, SyncGlobalization.DataSourceStringComparison))
                    cleanFolder = false;

            }
            // clear the changes because we don't need them anymore
            message.Changes.Clear(cleanFolder);

            this.Orchestrator.logger.LogDebug(SyncEventsId.ApplyChanges, changesApplied);

            return (context, changesApplied);

        }

        /// <summary>
        /// Here we are reseting all tables and tracking tables to be able to Reinitialize completely
        /// </summary>
        private async Task ResetInternalAsync(SyncContext context, SyncSet schema, SyncSetup setup, DbConnection connection, DbTransaction transaction)
        {
            if (schema == null || schema.Tables.Count <= 0)
                return;

            for (var i = 0; i < schema.Tables.Count; i++)
            {
                var tableDescription = schema.Tables[schema.Tables.Count - i - 1];
                
                this.Orchestrator.logger.LogDebug(SyncEventsId.ResetTable, tableDescription);

                var builder = this.GetTableBuilder(tableDescription, setup);
                var syncAdapter = builder.CreateSyncAdapter(connection, transaction);

                // reset table
                await syncAdapter.ResetTableAsync();
            }
        }


        /// <summary>
        /// Apply changes internal method for one Insert or Update or Delete for every dbSyncAdapter
        /// </summary>
        internal async Task ApplyChangesInternalAsync(
            SyncTable schemaTable,
            SyncContext context,
            MessageApplyChanges message,
            DbConnection connection,
            DbTransaction transaction,
            DataRowState applyType,
            DatabaseChangesApplied changesApplied,
            CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            this.Orchestrator.logger.LogDebug(SyncEventsId.ApplyChanges, message);

            // if we are in upload stage, so check if table is not download only
            if (context.SyncWay == SyncWay.Upload && schemaTable.SyncDirection == SyncDirection.DownloadOnly)
                return;

            // if we are in download stage, so check if table is not download only
            if (context.SyncWay == SyncWay.Download && schemaTable.SyncDirection == SyncDirection.UploadOnly)
                return;

            var builder = this.GetTableBuilder(schemaTable, message.Setup);

            var syncAdapter = builder.CreateSyncAdapter(connection, transaction);
            syncAdapter.ApplyType = applyType;

            var hasChanges = await message.Changes.HasDataAsync(this.Orchestrator);
            // Each table in the messages contains scope columns. Don't forget it
            if (hasChanges)
            {
                // getting the table to be applied
                // we may have multiple batch files, so we can have multipe sync tables with the same name
                // We can say that dmTable may be contained in several files
                foreach (var syncTable in message.Changes.GetTable(schemaTable.TableName, schemaTable.SchemaName, this.Orchestrator))
                {
                    if (syncTable == null || syncTable.Rows == null || syncTable.Rows.Count == 0)
                        continue;

                    // Creating a filtered view of my rows with the correct applyType
                    var filteredRows = syncTable.Rows.Where(r => r.RowState == applyType);

                    // no filtered rows, go next container table
                    if (filteredRows.Count() == 0)
                        continue;

                    // Conflicts occured when trying to apply rows
                    var conflicts = new List<SyncConflict>();

                    // Create an empty Set that wil contains filtered rows to apply
                    // Need Schema for culture & case sensitive properties
                    var changesSet = syncTable.Schema.Clone(false);
                    var schemaChangesTable = syncTable.Clone();
                    changesSet.Tables.Add(schemaChangesTable);
                    schemaChangesTable.Rows.AddRange(filteredRows.ToList());

                    if (this.Orchestrator.logger.IsEnabled(LogLevel.Trace))
                        foreach(var row in schemaChangesTable.Rows)
                            this.Orchestrator.logger.LogTrace(SyncEventsId.ApplyChanges, row);

                    // Launch any interceptor if available
                    await this.Orchestrator.InterceptAsync(new TableChangesApplyingArgs(context, schemaChangesTable, applyType, connection, transaction), cancellationToken).ConfigureAwait(false);

                    int rowsApplied = 0;

                    if (message.UseBulkOperations && this.SupportBulkOperations)
                        rowsApplied = await syncAdapter.ApplyBulkChangesAsync(message.LocalScopeId, message.SenderScopeId, schemaChangesTable, message.LastTimestamp, conflicts);
                    else
                        rowsApplied = await syncAdapter.ApplyChangesAsync(message.LocalScopeId, message.SenderScopeId, schemaChangesTable, message.LastTimestamp, conflicts);

                    // resolving conflicts
                    var (rowsAppliedCount, conflictsResolvedCount, syncErrorsCount) =
                        await ResolveConflictsAsync(context, message.LocalScopeId, message.SenderScopeId, syncAdapter, conflicts, message, connection, transaction).ConfigureAwait(false);

                    // Add conflict rows applied that are correctly resolved, as applied
                    rowsApplied += rowsAppliedCount;

                    // Handle sync progress for this syncadapter (so this table)
                    var changedFailed = filteredRows.Count() - conflictsResolvedCount - rowsApplied;

                    // We may have multiple batch files, so we can have multipe sync tables with the same name
                    // We can say that a syncTable may be contained in several files
                    // That's why we should get an applied changes instance if already exists from a previous batch file
                    var existAppliedChanges = changesApplied.TableChangesApplied.FirstOrDefault(tca =>
                    {
                        var sc = SyncGlobalization.DataSourceStringComparison;

                        var sn = tca.SchemaName == null ? string.Empty : tca.SchemaName;
                        var otherSn = schemaTable.SchemaName == null ? string.Empty : schemaTable.SchemaName;

                        return tca.TableName.Equals(schemaTable.TableName, sc) &&
                               sn.Equals(otherSn, sc) &&
                               tca.State == applyType;
                    });

                    if (existAppliedChanges == null)
                    {
                        existAppliedChanges = new TableChangesApplied
                        {
                            TableName = schemaTable.TableName,
                            SchemaName = schemaTable.SchemaName,
                            Applied = rowsApplied,
                            ResolvedConflicts = conflictsResolvedCount,
                            Failed = changedFailed,
                            State = applyType
                        };
                        changesApplied.TableChangesApplied.Add(existAppliedChanges);
                    }
                    else
                    {
                        existAppliedChanges.Applied += rowsApplied;
                        existAppliedChanges.ResolvedConflicts += conflictsResolvedCount;
                        existAppliedChanges.Failed += changedFailed;
                    }

                    var tableChangesAppliedArgs = new TableChangesAppliedArgs(context, existAppliedChanges, connection, transaction);

                    // We don't report progress if we do not have applied any changes on the table, to limit verbosity of Progress
                    if (tableChangesAppliedArgs.TableChangesApplied.Applied > 0 || tableChangesAppliedArgs.TableChangesApplied.Failed > 0 || tableChangesAppliedArgs.TableChangesApplied.ResolvedConflicts > 0)
                    {
                        await this.Orchestrator.InterceptAsync(tableChangesAppliedArgs, cancellationToken).ConfigureAwait(false);
                        this.Orchestrator.ReportProgress(context, progress, tableChangesAppliedArgs, connection, transaction);

                        this.Orchestrator.logger.LogDebug(SyncEventsId.ApplyChanges, tableChangesAppliedArgs);
                    }
                }
            }
        }


        private async Task<(int rowsAppliedCount, int conflictsResolvedCount, int syncErrorsCount)> ResolveConflictsAsync(SyncContext context, Guid localScopeId, Guid senderScopeId, DbSyncAdapter syncAdapter, List<SyncConflict> conflicts, MessageApplyChanges message, DbConnection connection, DbTransaction transaction)
        {
            int rowsAppliedCount = 0;
            int conflictsResolvedCount = 0;
            int syncErrorsCount = 0;

            // If conflicts occured
            // Eventuall, conflicts are resolved on server side.
            if (conflicts == null || conflicts.Count <= 0)
                return (rowsAppliedCount, conflictsResolvedCount, syncErrorsCount);


            foreach (var conflict in conflicts)
            {
                var fromScopeLocalTimeStamp = message.LastTimestamp;

                this.Orchestrator.logger.LogDebug(SyncEventsId.ResolveConflicts, conflict);
                this.Orchestrator.logger.LogDebug(SyncEventsId.ResolveConflicts, new
                {
                    LocalScopeId = localScopeId,
                    SenderScopeId = senderScopeId,
                    FromScopeLocalTimeStamp= fromScopeLocalTimeStamp,
                    message.Policy
                }); ;


                var (conflictResolvedCount, resolvedRow, rowAppliedCount) =
                    await this.HandleConflictAsync(localScopeId, senderScopeId, syncAdapter, context, conflict,
                                                   message.Policy,
                                                   fromScopeLocalTimeStamp, connection, transaction).ConfigureAwait(false);

                if (resolvedRow != null)
                {
                    conflictsResolvedCount += conflictResolvedCount;
                    rowsAppliedCount += rowAppliedCount;
                }

            }

            return (rowsAppliedCount, conflictsResolvedCount, syncErrorsCount);

        }

        /// <summary>
        /// Handle a conflict
        /// The int returned is the conflict count I need 
        /// </summary>
        /// changeApplicationAction, conflictCount, resolvedRow, conflictApplyInt
        internal async Task<(int conflictResolvedCount, SyncRow resolvedRow, int rowAppliedCount)> HandleConflictAsync(
                                Guid localScopeId, Guid senderScopeId,
                                DbSyncAdapter syncAdapter, SyncContext context, SyncConflict conflict,
                                ConflictResolutionPolicy policy, long lastTimestamp,
                                DbConnection connection, DbTransaction transaction)
        {

            SyncRow finalRow;
            ApplyAction conflictApplyAction;
            int rowAppliedCount = 0;

            (conflictApplyAction, finalRow) = await this.GetConflictActionAsync(context, conflict, policy, connection, transaction).ConfigureAwait(false);

            // Conflict rollbacked by user
            if (conflictApplyAction == ApplyAction.Rollback)
                throw new RollbackException("Rollback action taken on conflict");

            // Local provider wins, update metadata
            if (conflictApplyAction == ApplyAction.Continue)
            {
                var isMergeAction = finalRow != null;
                var row = isMergeAction ? finalRow : conflict.LocalRow;

                // Conflict on a line that is not present on the datasource
                if (row == null)
                    return (0, finalRow, 0);

                // if we have a merge action, we apply the row on the server
                if (isMergeAction)
                {

                    // if merge, we update locally the row and let the update_scope_id set to null
                    var isUpdated = await syncAdapter.ApplyUpdateAsync(row, lastTimestamp, null, true);
                    // We don't update metadatas so the row is updated (on server side) 
                    // and is mark as updated locally.
                    // and will be returned back to sender, since it's a merge, and we need it on the client

                    if (!isUpdated)
                        throw new Exception("Can't update the merge row.");
                }

                finalRow = isMergeAction ? row : conflict.LocalRow;

                // We don't do anything, since we let the original row. so we resolved one conflict but applied no rows
                return (conflictResolvedCount: 1, finalRow, rowAppliedCount: 0);

            }

            // We gonna apply with force the line
            if (conflictApplyAction == ApplyAction.RetryWithForceWrite)
            {
                // TODO : Should Raise an error ?
                if (conflict.RemoteRow == null)
                    return (0, finalRow, 0);

                bool operationComplete = false;

                switch (conflict.Type)
                {
                    // Remote source has row, Local don't have the row, so insert it
                    case ConflictType.RemoteExistsLocalExists:
                    case ConflictType.RemoteExistsLocalNotExists:
                    case ConflictType.RemoteExistsLocalIsDeleted:
                    case ConflictType.UniqueKeyConstraint:
                        operationComplete = await syncAdapter.ApplyUpdateAsync(conflict.RemoteRow, lastTimestamp, senderScopeId, true);
                        rowAppliedCount = 1;
                        break;

                    // Conflict, but both have delete the row, so nothing to do
                    case ConflictType.RemoteIsDeletedLocalIsDeleted:
                    case ConflictType.RemoteIsDeletedLocalNotExists:
                        operationComplete = true;
                        rowAppliedCount = 0;
                        break;

                    // The remote has delete the row, and local has insert or update it
                    // So delete the local row
                    case ConflictType.RemoteIsDeletedLocalExists:
                        operationComplete = await syncAdapter.ApplyDeleteAsync(conflict.RemoteRow, lastTimestamp, senderScopeId, true);
                        rowAppliedCount = 1;
                        break;


                    case ConflictType.RemoteCleanedupDeleteLocalUpdate:
                    case ConflictType.ErrorsOccurred:
                        return (0, finalRow, 0);
                }

                finalRow = conflict.RemoteRow;

                //After a force update, there is a problem, so raise exception
                if (!operationComplete)
                {
                    finalRow = null;
                    return (0, finalRow, rowAppliedCount);
                }

                return (1, finalRow, rowAppliedCount);
            }

            return (0, finalRow, 0);
        }



        /// <summary>
        /// A conflict has occured, we try to ask for the solution to the user
        /// </summary>
        internal async Task<(ApplyAction, SyncRow)> GetConflictActionAsync(SyncContext context, SyncConflict conflict, ConflictResolutionPolicy policy, DbConnection connection, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            var conflictAction = ConflictResolution.ServerWins;

            if (policy == ConflictResolutionPolicy.ClientWins)
                conflictAction = ConflictResolution.ClientWins;

            // Interceptor
            var arg = new ApplyChangesFailedArgs(context, conflict, conflictAction, connection, transaction);
            this.Orchestrator.logger.LogDebug(SyncEventsId.ResolveConflicts, arg);

            await this.Orchestrator.InterceptAsync(arg, cancellationToken).ConfigureAwait(false);

            // if ConflictAction is ServerWins or MergeRow it's Ok to set to Continue
            var action = ApplyAction.Continue;

            // Change action only if we choose ClientWins or Rollback.
            // for ServerWins or MergeRow, action is Continue
            if (arg.Resolution == ConflictResolution.ClientWins)
                action = ApplyAction.RetryWithForceWrite;
            else if (arg.Resolution == ConflictResolution.Rollback)
                action = ApplyAction.Rollback;

            var finalRow = arg.Resolution == ConflictResolution.MergeRow ? arg.FinalRow : null;

            // returning the action to take, and actually the finalRow if action is set to Merge
            return (action, finalRow);
        }

        /// <summary>
        /// Disabling all constraints on synced tables
        /// Since we can disable at the database level
        /// Just check for one available table and execute for the whole db
        /// </summary>
        internal Task DisableConstraintsAsync(SyncContext context, SyncTable table, SyncSetup setup, DbConnection connection, DbTransaction transaction = null)
        {

            var builder = this.GetTableBuilder(table, setup);
            var syncAdapter = builder.CreateSyncAdapter(connection, transaction);

            // disable constraints
            return syncAdapter.DisableConstraintsAsync();
        }


        /// <summary>
        /// Enabling all constraints on synced tables
        /// Since we can disable at the database level
        /// Just check for one available table and execute for the whole db
        /// </summary>
        internal Task EnableConstraintsAsync(SyncContext context, SyncTable table, SyncSetup setup, DbConnection connection, DbTransaction transaction)
        {
            var builder = this.GetTableBuilder(table, setup);
            var syncAdapter = builder.CreateSyncAdapter(connection, transaction);

            // enable table
            return syncAdapter.EnableConstraintsAsync();
        }


    }
}
