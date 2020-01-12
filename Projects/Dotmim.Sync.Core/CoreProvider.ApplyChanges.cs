using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Messages;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
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
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            var changeApplicationAction = ChangeApplicationAction.Continue;
            var changesApplied = new DatabaseChangesApplied();


            // Check if we have some data available
            if (!message.Changes.HasData())
                return (context, changesApplied);

            context.SyncStage = SyncStage.DatabaseChangesApplying;

            // Launch any interceptor if available
            await this.InterceptAsync(new DatabaseChangesApplyingArgs(context, connection, transaction)).ConfigureAwait(false);

            // Disable check constraints
            // Because Sqlite does not support "PRAGMA foreign_keys=OFF" Inside a transaction
            // Report this disabling constraints brefore opening a transaction
            if (message.DisableConstraintsOnApplyChanges)
                this.DisableConstraints(context, message.Schema, connection, transaction);

            // -----------------------------------------------------
            // 0) Check if we are in a reinit mode
            // -----------------------------------------------------
            if (context.SyncWay == SyncWay.Download && context.SyncType != SyncType.Normal)
            {
                changeApplicationAction = this.ResetInternal(context, message.Schema, connection, transaction);


                // Rollback
                if (changeApplicationAction == ChangeApplicationAction.Rollback)
                    throw new RollbackException("Rollback during reset tables");
            }

            // -----------------------------------------------------
            // 1) Applying deletes. Do not apply deletes if we are in a new database
            // -----------------------------------------------------
            if (!message.IsNew)
            {
                // for delete we must go from Up to Down
                foreach (var table in message.Schema.Tables.Reverse())
                {
                    changeApplicationAction = await this.ApplyChangesInternalAsync(table, context, message, connection,
                        transaction, DataRowState.Deleted, changesApplied, cancellationToken, progress).ConfigureAwait(false);
                }

                // Rollback
                if (changeApplicationAction == ChangeApplicationAction.Rollback)
                    throw new RollbackException("Rollback during applying deletes");
            }

            // -----------------------------------------------------
            // 2) Applying Inserts and Updates. Apply in table order
            // -----------------------------------------------------
            foreach (var table in message.Schema.Tables)
            {
                changeApplicationAction = await this.ApplyChangesInternalAsync(table, context, message, connection,
                    transaction, DataRowState.Modified, changesApplied, cancellationToken, progress).ConfigureAwait(false);

                // Rollback
                if (changeApplicationAction == ChangeApplicationAction.Rollback)
                    throw new RollbackException("Rollback during applying updates");
            }


            // Progress & Interceptor
            context.SyncStage = SyncStage.DatabaseChangesApplied;
            var databaseChangesAppliedArgs = new DatabaseChangesAppliedArgs(context, changesApplied, connection, transaction);
            this.ReportProgress(context, progress, databaseChangesAppliedArgs, connection, transaction);
            await this.InterceptAsync(databaseChangesAppliedArgs).ConfigureAwait(false);

            // Re enable check constraints
            if (message.DisableConstraintsOnApplyChanges)
                this.EnableConstraints(context, message.Schema, connection, transaction);

            // clear the changes because we don't need them anymore
            message.Changes.Clear(false);

            return (context, changesApplied);

        }

        /// <summary>
        /// Here we are reseting all tables and tracking tables to be able to Reinitialize completely
        /// </summary>
        private ChangeApplicationAction ResetInternal(SyncContext context, SyncSet schema, DbConnection connection, DbTransaction transaction)
        {
            if (schema == null || schema.Tables.Count <= 0)
                return ChangeApplicationAction.Continue;

            for (var i = 0; i < schema.Tables.Count; i++)
            {
                var tableDescription = schema.Tables[schema.Tables.Count - i - 1];
                var builder = this.GetDatabaseBuilder(tableDescription);
                var syncAdapter = builder.CreateSyncAdapter(connection, transaction);

                // reset table
                syncAdapter.ResetTable();

            }
            return ChangeApplicationAction.Continue;
        }


        /// <summary>
        /// Apply changes internal method for one Insert or Update or Delete for every dbSyncAdapter
        /// </summary>
        internal async Task<ChangeApplicationAction> ApplyChangesInternalAsync(
            SyncTable schemaTable,
            SyncContext context,
            MessageApplyChanges message,
            DbConnection connection,
            DbTransaction transaction,
            DataRowState applyType,
            DatabaseChangesApplied changesApplied,
            CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {


            // if we are in upload stage, so check if table is not download only
            if (context.SyncWay == SyncWay.Upload && schemaTable.SyncDirection == SyncDirection.DownloadOnly)
                return ChangeApplicationAction.Continue;

            // if we are in download stage, so check if table is not download only
            if (context.SyncWay == SyncWay.Download && schemaTable.SyncDirection == SyncDirection.UploadOnly)
                return ChangeApplicationAction.Continue;

            var builder = this.GetDatabaseBuilder(schemaTable);

            var syncAdapter = builder.CreateSyncAdapter(connection, transaction);
            syncAdapter.ApplyType = applyType;


            // Each table in the messages contains scope columns. Don't forget it
            if (message.Changes.HasData())
            {
                // getting the table to be applied
                // we may have multiple batch files, so we can have multipe sync tables with the same name
                // We can say that dmTable may be contained in several files
                foreach (var syncTable in message.Changes.GetTable(schemaTable.TableName, schemaTable.SchemaName))
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

                    context.SyncStage = SyncStage.TableChangesApplying;
                    // Launch any interceptor if available
                    await this.InterceptAsync(new TableChangesApplyingArgs(context, filteredRows, schemaTable, applyType, connection, transaction)).ConfigureAwait(false);

                    // Create an empty Set that wil contains filtered rows to apply
                    // Need Schema for culture & case sensitive properties
                    var changesSet = syncTable.Schema.Clone(false);
                    var schemaChangesTable = syncTable.Clone();
                    changesSet.Tables.Add(schemaChangesTable);
                    schemaChangesTable.Rows.AddRange(filteredRows.ToList());

                    int rowsApplied = 0;

                    if (message.UseBulkOperations && this.SupportBulkOperations)
                        rowsApplied = syncAdapter.ApplyBulkChanges(message.LocalScopeId, schemaChangesTable, message.LastTimestamp, conflicts);
                    else
                        rowsApplied = syncAdapter.ApplyChanges(message.LocalScopeId, schemaChangesTable, message.LastTimestamp, conflicts);


                    // resolving conflicts
                    (var changeApplicationAction, var conflictRowsApplied) = await ResolveConflictsAsync(context, message.LocalScopeId, syncAdapter, conflicts, message, connection, transaction).ConfigureAwait(false);

                    if (changeApplicationAction == ChangeApplicationAction.Rollback)
                        return ChangeApplicationAction.Rollback;

                    // Add conflict rows that are correctly resolved, as applied
                    rowsApplied += conflictRowsApplied;

                    // Handle sync progress for this syncadapter (so this table)
                    var changedFailed = filteredRows.Count() - rowsApplied;

                    // raise SyncProgress Event
                    var existAppliedChanges = changesApplied.TableChangesApplied.FirstOrDefault(
                            sc => string.Equals(sc.Table.TableName, schemaTable.TableName, SyncGlobalization.DataSourceStringComparison) && sc.State == applyType);

                    if (existAppliedChanges == null)
                    {
                        existAppliedChanges = new TableChangesApplied
                        {
                            Table = schemaTable,
                            Applied = rowsApplied,
                            Failed = changedFailed,
                            State = applyType
                        };
                        changesApplied.TableChangesApplied.Add(existAppliedChanges);
                    }
                    else
                    {
                        existAppliedChanges.Applied += rowsApplied;
                        existAppliedChanges.Failed += changedFailed;
                    }

                    // Progress & Interceptor
                    context.SyncStage = SyncStage.TableChangesApplied;
                    var tableChangesAppliedArgs = new TableChangesAppliedArgs(context, existAppliedChanges, connection, transaction);
                    this.ReportProgress(context, progress, tableChangesAppliedArgs, connection, transaction);
                    await this.InterceptAsync(tableChangesAppliedArgs).ConfigureAwait(false);



                }
            }

            return ChangeApplicationAction.Continue;
        }


        private async Task<(ChangeApplicationAction, int)> ResolveConflictsAsync(SyncContext context, Guid localScopeId, DbSyncAdapter syncAdapter, List<SyncConflict> conflicts, MessageApplyChanges message, DbConnection connection, DbTransaction transaction)
        {
            // If conflicts occured
            // Eventuall, conflicts are resolved on server side.
            if (conflicts == null || conflicts.Count <= 0)
                return (ChangeApplicationAction.Continue, 0);

            int rowsApplied = 0;

            foreach (var conflict in conflicts)
            {
                var fromScopeLocalTimeStamp = message.LastTimestamp;

                var (changeApplicationAction, conflictCount, resolvedRow, conflictApplyInt) =
                    await this.HandleConflictAsync(localScopeId, syncAdapter, context, conflict,
                                                   message.Policy,
                                                   fromScopeLocalTimeStamp, connection, transaction).ConfigureAwait(false);

                if (changeApplicationAction == ChangeApplicationAction.Continue)
                {
                    if (resolvedRow != null)
                    {
                        context.TotalSyncConflicts += conflictCount;
                        rowsApplied += conflictApplyInt;
                    }
                }
                else
                {
                    context.TotalSyncErrors++;
                    return (ChangeApplicationAction.Rollback, rowsApplied);
                }
            }

            return (ChangeApplicationAction.Continue, rowsApplied);

        }


        /// <summary>
        /// A conflict has occured, we try to ask for the solution to the user
        /// </summary>
        internal async Task<(ApplyAction, SyncRow)> GetConflictActionAsync(SyncContext context, SyncConflict conflict, ConflictResolutionPolicy policy, DbConnection connection, DbTransaction transaction = null)
        {
            var conflictAction = ConflictResolution.ServerWins;

            if (policy == ConflictResolutionPolicy.ClientWins)
                conflictAction = ConflictResolution.ClientWins;

            // Interceptor
            var arg = new ApplyChangesFailedArgs(context, conflict, conflictAction, connection, transaction);
            await this.InterceptAsync(arg).ConfigureAwait(false);

            // if ConflictAction is ServerWins or MergeRow it's Ok to set to Continue
            var action = ApplyAction.Continue;

            if (arg.Resolution == ConflictResolution.ClientWins)
                action = ApplyAction.RetryWithForceWrite;

            var finalRow = arg.Resolution == ConflictResolution.MergeRow ? arg.FinalRow : null;

            // returning the action to take, and actually the finalRow if action is set to Merge
            return (action, finalRow);
        }

        /// <summary>
        /// Handle a conflict
        /// The int returned is the conflict count I need 
        /// </summary>
        internal async Task<(ChangeApplicationAction, int, SyncRow, int)> HandleConflictAsync(
                                Guid localScopeId,
                                DbSyncAdapter syncAdapter, SyncContext context, SyncConflict conflict,
                                ConflictResolutionPolicy policy, long lastTimestamp,
                                DbConnection connection, DbTransaction transaction)
        {

            SyncRow finalRow;
            ApplyAction conflictApplyAction;
            int conflictResolved = 0;

            (conflictApplyAction, finalRow) = await this.GetConflictActionAsync(context, conflict, policy, connection, transaction).ConfigureAwait(false);

            // Default behavior and an error occured
            if (conflictApplyAction == ApplyAction.Rollback)
            {
                conflict.ErrorMessage = "Rollback action taken on conflict";
                conflict.Type = ConflictType.ErrorsOccurred;

                return (ChangeApplicationAction.Rollback, 0, null, 0);
            }

            // Local provider wins, update metadata
            if (conflictApplyAction == ApplyAction.Continue)
            {
                var isMergeAction = finalRow != null;
                var row = isMergeAction ? finalRow : conflict.LocalRow;

                // Conflict on a line that is not present on the datasource
                if (row == null)
                    return (ChangeApplicationAction.Continue, 0, finalRow, 0);

                if (row != null)
                {
                    // if we have a merge action, we apply the row on the server
                    if (isMergeAction)
                    {

                        // if merge, we update locally the row and let the update_scope_id set to null
                        var isUpdated = syncAdapter.ApplyUpdate(row, lastTimestamp, true);
                        // We don't update metadatas so the row is updated (on server side) 
                        // and is mark as updated locally.
                        // and will be returned back to sender, since it's a merge, and we need it on the client

                        if (!isUpdated)
                            throw new Exception("Can't update the merge row.");
                    }

                    finalRow = isMergeAction ? row : conflict.LocalRow;

                    // We don't do anything on the local provider, so we do not need to return a +1 on syncConflicts count
                    return (ChangeApplicationAction.Continue, 0, finalRow, 1);
                }
                return (ChangeApplicationAction.Rollback, 0, finalRow, 1);
            }

            // We gonna apply with force the line
            if (conflictApplyAction == ApplyAction.RetryWithForceWrite)
            {
                // TODO : Should Raise an error ?
                if (conflict.RemoteRow == null)
                    return (ChangeApplicationAction.Rollback, 0, finalRow, 0);

                bool operationComplete = false;

                //var commandType = DbCommandType.UpdateMetadata;
                bool needToUpdateMetadata = true;

                switch (conflict.Type)
                {
                    // Remote source has row, Local don't have the row, so insert it
                    case ConflictType.RemoteExistsLocalExists:
                    case ConflictType.RemoteExistsLocalNotExists:
                    case ConflictType.RemoteExistsLocalIsDeleted:
                    case ConflictType.UniqueKeyConstraint:
                        operationComplete = syncAdapter.ApplyUpdate(conflict.RemoteRow, lastTimestamp, true);
                        conflictResolved = 1;
                        break;

                    // Conflict, but both have delete the row, so nothing to do
                    case ConflictType.RemoteIsDeletedLocalIsDeleted:
                    case ConflictType.RemoteIsDeletedLocalNotExists:
                        operationComplete = true;
                        needToUpdateMetadata = false;
                        conflictResolved = 0;
                        break;

                    // The remote has delete the row, and local has insert or update it
                    // So delete the local row
                    case ConflictType.RemoteIsDeletedLocalExists:
                        operationComplete = syncAdapter.ApplyDelete(conflict.RemoteRow, lastTimestamp, true);
                        conflictResolved = 1;
                        break;


                    case ConflictType.RemoteCleanedupDeleteLocalUpdate:
                    case ConflictType.ErrorsOccurred:
                        return (ChangeApplicationAction.Rollback, 0, finalRow, 0);
                }

                if (needToUpdateMetadata)
                {
                    using (var metadataCommand = syncAdapter.GetCommand(DbCommandType.UpdateMetadata))
                    {
                        if (metadataCommand == null)
                            throw new MissingCommandException(DbCommandType.UpdateMetadata.ToString());

                        // Deriving Parameters
                        syncAdapter.SetCommandParameters(DbCommandType.UpdateMetadata, metadataCommand);

                        // force applying client row, so apply scope.id (client scope here)
                        var rowsApplied = syncAdapter.InsertOrUpdateMetadatas(metadataCommand, conflict.RemoteRow, false);

                        if (!rowsApplied)
                            throw new MetadataException(syncAdapter.TableDescription.TableName);
                    }
                }

                finalRow = conflict.RemoteRow;

                //After a force update, there is a problem, so raise exception
                if (!operationComplete)
                {
                    finalRow = null;
                    return (ChangeApplicationAction.Continue, 0, finalRow, conflictResolved);
                }

                return (ChangeApplicationAction.Continue, 1, finalRow, conflictResolved);
            }

            return (ChangeApplicationAction.Rollback, 0, finalRow, 0);
        }


        /// <summary>
        /// Disabling all constraints on synced tables
        /// Since we can disable at the database level
        /// Just check for one available table and execute for the whole db
        /// </summary>
        internal void DisableConstraints(SyncContext context, SyncSet schema, DbConnection connection, DbTransaction transaction = null)
        {
            if (schema == null || schema.Tables.Count <= 0)
                return;

            // arbitrary table
            var tableDescription = schema.Tables[0];

            var builder = this.GetDatabaseBuilder(tableDescription);
            var syncAdapter = builder.CreateSyncAdapter(connection, transaction);

            // disable constraints
            syncAdapter.DisableConstraints();


        }


        /// <summary>
        /// Enabling all constraints on synced tables
        /// Since we can disable at the database level
        /// Just check for one available table and execute for the whole db
        /// </summary>
        private ChangeApplicationAction EnableConstraints(SyncContext context, SyncSet schema, DbConnection connection, DbTransaction transaction)
        {
            if (schema == null || schema.Tables.Count <= 0)
                return ChangeApplicationAction.Continue;

            // arbitrary table
            var tableDescription = schema.Tables[0];

            var builder = this.GetDatabaseBuilder(tableDescription);
            var syncAdapter = builder.CreateSyncAdapter(connection, transaction);

            // reset table
            syncAdapter.EnableConstraints();

            return ChangeApplicationAction.Continue;
        }


    }
}
