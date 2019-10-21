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

            try
            {
                context.SyncStage = SyncStage.DatabaseChangesApplying;

                // Launch any interceptor if available
                await this.InterceptAsync(new DatabaseChangesApplyingArgs(context, connection, transaction)).ConfigureAwait(false);

                // Disable check constraints
                if (message.DisableConstraintsOnApplyChanges)
                    changeApplicationAction = this.DisableConstraints(context, message.Schema, connection, transaction, message.FromScope);

                // -----------------------------------------------------
                // 0) Check if we are in a reinit mode
                // -----------------------------------------------------
                if (context.SyncWay == SyncWay.Download && context.SyncType != SyncType.Normal)
                {
                    changeApplicationAction = this.ResetInternal(context, message.Schema, connection, transaction, message.FromScope);

                    // Rollback
                    if (changeApplicationAction == ChangeApplicationAction.Rollback)
                        throw new SyncException("Rollback during reset tables", context.SyncStage, SyncExceptionType.Rollback);
                }

                // -----------------------------------------------------
                // 1) Applying deletes. Do not apply deletes if we are in a new database
                // -----------------------------------------------------
                if (!message.FromScope.IsNewScope)
                {
                    // for delete we must go from Up to Down
                    foreach (var table in message.Schema.Tables.Reverse())
                    {
                        changeApplicationAction = await this.ApplyChangesInternalAsync(table, context, message, connection,
                            transaction, DmRowState.Deleted, changesApplied, cancellationToken, progress).ConfigureAwait(false);
                    }

                    // Rollback
                    if (changeApplicationAction == ChangeApplicationAction.Rollback)
                        RaiseRollbackException(context, "Rollback during applying deletes");
                }

                // -----------------------------------------------------
                // 2) Applying Inserts and Updates. Apply in table order
                // -----------------------------------------------------
                foreach (var table in message.Schema.Tables)
                {
                    changeApplicationAction = await this.ApplyChangesInternalAsync(table, context, message, connection,
                        transaction, DmRowState.Added, changesApplied, cancellationToken, progress).ConfigureAwait(false);

                    // Rollback
                    if (changeApplicationAction == ChangeApplicationAction.Rollback)
                        RaiseRollbackException(context, "Rollback during applying inserts");

                    changeApplicationAction = await this.ApplyChangesInternalAsync(table, context, message, connection,
                        transaction, DmRowState.Modified, changesApplied, cancellationToken, progress).ConfigureAwait(false);

                    // Rollback
                    if (changeApplicationAction == ChangeApplicationAction.Rollback)
                        RaiseRollbackException(context, "Rollback during applying updates");
                }


                // Progress & Interceptor
                context.SyncStage = SyncStage.DatabaseChangesApplied;
                var databaseChangesAppliedArgs = new DatabaseChangesAppliedArgs(context, changesApplied, connection, transaction);
                this.ReportProgress(context, progress, databaseChangesAppliedArgs, connection, transaction);
                await this.InterceptAsync(databaseChangesAppliedArgs).ConfigureAwait(false);

                // Re enable check constraints
                if (message.DisableConstraintsOnApplyChanges)
                    changeApplicationAction = this.EnableConstraints(context, message.Schema, connection, transaction, message.FromScope);

                return (context, changesApplied);
            }
            catch (SyncException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new SyncException(ex, SyncStage.TableChangesApplying);
            }
        }

        /// <summary>
        /// Here we are reseting all tables and tracking tables to be able to Reinitialize completely
        /// </summary>
        private ChangeApplicationAction ResetInternal(SyncContext context, DmSet configTables, DbConnection connection, DbTransaction transaction, ScopeInfo fromScope)
        {
            if (configTables == null || configTables.Tables.Count <= 0)
                return ChangeApplicationAction.Continue;

            for (var i = 0; i < configTables.Tables.Count; i++)
            {
                var tableDescription = configTables.Tables[configTables.Tables.Count - i - 1];

                var builder = this.GetDatabaseBuilder(tableDescription);
                var syncAdapter = builder.CreateSyncAdapter(connection, transaction);

                // reset table
                syncAdapter.ResetTable(tableDescription);

            }
            return ChangeApplicationAction.Continue;
        }



        /// <summary>
        /// Disabling all constraints on synced tables
        /// </summary>
        private ChangeApplicationAction DisableConstraints(SyncContext context, DmSet configTables, DbConnection connection, DbTransaction transaction, ScopeInfo fromScope)
        {
            if (configTables == null || configTables.Tables.Count <= 0)
                return ChangeApplicationAction.Continue;

            for (var i = 0; i < configTables.Tables.Count; i++)
            {
                var tableDescription = configTables.Tables[configTables.Tables.Count - i - 1];

                var builder = this.GetDatabaseBuilder(tableDescription);
                var syncAdapter = builder.CreateSyncAdapter(connection, transaction);

                // reset table
                syncAdapter.DisableConstraints();

            }
            return ChangeApplicationAction.Continue;
        }


        /// <summary>
        /// Disabling all constraints on synced tables
        /// </summary>
        private ChangeApplicationAction EnableConstraints(SyncContext context, DmSet configTables, DbConnection connection, DbTransaction transaction, ScopeInfo fromScope)
        {
            if (configTables == null || configTables.Tables.Count <= 0)
                return ChangeApplicationAction.Continue;

            for (var i = 0; i < configTables.Tables.Count; i++)
            {
                var tableDescription = configTables.Tables[configTables.Tables.Count - i - 1];

                var builder = this.GetDatabaseBuilder(tableDescription);
                var syncAdapter = builder.CreateSyncAdapter(connection, transaction);

                // reset table
                syncAdapter.EnableConstraints();

            }
            return ChangeApplicationAction.Continue;
        }


        /// <summary>
        /// Apply changes internal method for one Insert or Update or Delete for every dbSyncAdapter
        /// </summary>
        internal async Task<ChangeApplicationAction> ApplyChangesInternalAsync(
            DmTable table,
            SyncContext context,
            MessageApplyChanges message,
            DbConnection connection,
            DbTransaction transaction,
            DmRowState applyType,
            DatabaseChangesApplied changesApplied,
            CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            var changeApplicationAction = ChangeApplicationAction.Continue;

            // if we are in upload stage, so check if table is not download only
            if (context.SyncWay == SyncWay.Upload && table.SyncDirection == SyncDirection.DownloadOnly)
            {
                return ChangeApplicationAction.Continue;
            }

            // if we are in download stage, so check if table is not download only
            if (context.SyncWay == SyncWay.Download && table.SyncDirection == SyncDirection.UploadOnly)
            {
                return ChangeApplicationAction.Continue;
            }

            var builder = this.GetDatabaseBuilder(table);

            var syncAdapter = builder.CreateSyncAdapter(connection, transaction);
            syncAdapter.ApplyType = applyType;

            if (message.Changes.BatchPartsInfo != null && message.Changes.BatchPartsInfo.Count > 0)
            {
                // getting the table to be applied
                // we may have multiple batch files, so we can have multipe dmTable with the same Name
                // We can say that dmTable may be contained in several files
                foreach (var dmTablePart in message.Changes.GetTable(table.TableName))
                {
                    if (dmTablePart == null || dmTablePart.Rows.Count == 0)
                        continue;

                    // check and filter
                    var dmChangesView = new DmView(dmTablePart, (r) => r.RowState == applyType);

                    if (dmChangesView.Count == 0)
                    {
                        dmChangesView.Dispose();
                        dmChangesView = null;
                        continue;
                    }

                    // Conflicts occured when trying to apply rows
                    var conflicts = new List<SyncConflict>();

                    context.SyncStage = SyncStage.TableChangesApplying;
                    // Launch any interceptor if available
                    await this.InterceptAsync(new TableChangesApplyingArgs(context, table, applyType, connection, transaction)).ConfigureAwait(false);

                    int rowsApplied;
                    // applying the bulkchanges command
                    if (message.UseBulkOperations && this.SupportBulkOperations)
                        rowsApplied = syncAdapter.ApplyBulkChanges(dmChangesView, message.FromScope, conflicts);
                    else
                        rowsApplied = syncAdapter.ApplyChanges(dmChangesView, message.FromScope, conflicts);

                    // If conflicts occured
                    // Eventuall, conflicts are resolved on server side.
                    if (conflicts != null && conflicts.Count > 0)
                    {
                        foreach (var conflict in conflicts)
                        {
                            //var scopeBuilder = this.GetScopeBuilder();
                            //var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(message.ScopeInfoTableName, connection, transaction);
                            //var localTimeStamp = scopeInfoBuilder.GetLocalTimestamp();
                            var fromScopeLocalTimeStamp = message.FromScope.Timestamp;

                            var conflictCount = 0;
                            DmRow resolvedRow = null;
                            (changeApplicationAction, conflictCount, resolvedRow) =
                                await this.HandleConflictAsync(syncAdapter, context, conflict, message.Policy, message.FromScope, fromScopeLocalTimeStamp, connection, transaction).ConfigureAwait(false);

                            if (changeApplicationAction == ChangeApplicationAction.Continue)
                            {
                                // row resolved
                                if (resolvedRow != null)
                                {
                                    context.TotalSyncConflicts += conflictCount;
                                    rowsApplied++;
                                }
                            }
                            else
                            {
                                context.TotalSyncErrors++;
                                // TODO : Should we break at the first error ?
                                return ChangeApplicationAction.Rollback;
                            }
                        }
                    }

                    // Handle sync progress for this syncadapter (so this table)
                    var changedFailed = dmChangesView.Count - rowsApplied;

                    // raise SyncProgress Event
                    var existAppliedChanges = changesApplied.TableChangesApplied.FirstOrDefault(
                            sc => string.Equals(sc.Table.TableName, table.TableName) && sc.State == applyType);

                    if (existAppliedChanges == null)
                    {
                        existAppliedChanges = new TableChangesApplied
                        {
                            Table = new DmTableSurrogate(table),
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

        /// <summary>
        /// A conflict has occured, we try to ask for the solution to the user
        /// </summary>
        internal async Task<(ApplyAction, DmRow)> GetConflictActionAsync(SyncContext context, SyncConflict conflict, ConflictResolutionPolicy policy, DbConnection connection, DbTransaction transaction = null)
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
        internal async Task<(ChangeApplicationAction, int, DmRow)> HandleConflictAsync(DbSyncAdapter syncAdapter, SyncContext context, SyncConflict conflict, ConflictResolutionPolicy policy, ScopeInfo scope, long fromScopeLocalTimeStamp, DbConnection connection, DbTransaction transaction)
        {
            DmRow finalRow;
            ApplyAction conflictApplyAction;
            (conflictApplyAction, finalRow) = await this.GetConflictActionAsync(context, conflict, policy, connection, transaction).ConfigureAwait(false);

            // Default behavior and an error occured
            if (conflictApplyAction == ApplyAction.Rollback)
            {
                conflict.ErrorMessage = "Rollback action taken on conflict";
                conflict.Type = ConflictType.ErrorsOccurred;

                return (ChangeApplicationAction.Rollback, 0, null);
            }

            // Local provider wins, update metadata
            if (conflictApplyAction == ApplyAction.Continue)
            {
                var isMergeAction = finalRow != null;
                var row = isMergeAction ? finalRow : conflict.LocalRow;

                // Conflict on a line that is not present on the datasource
                if (row == null)
                    return (ChangeApplicationAction.Continue, 0, finalRow);

                if (row != null)
                {
                    // if we have a merge action, we apply the row on the server
                    if (isMergeAction)
                    {
                        bool isUpdated = false;
                        bool isInserted = false;
                        // Insert metadata is a merge, actually
                        var commandType = DbCommandType.UpdateMetadata;

                        isUpdated = syncAdapter.ApplyUpdate(row, scope, true);

                        if (!isUpdated)
                        {
                            // Insert the row
                            isInserted = syncAdapter.ApplyInsert(row, scope, true);
                            // Then update the row to mark this row as updated from server
                            // and get it back to client 
                            isUpdated = syncAdapter.ApplyUpdate(row, scope, true);

                            commandType = DbCommandType.InsertMetadata;
                        }

                        if (!isUpdated && !isInserted)
                            throw new Exception("Can't update the merge row.");


                        // IF we have insert the row in the server side, to resolve the conflict
                        // Whe should update the metadatas correctly
                        if (isUpdated || isInserted)
                        {
                            using (var metadataCommand = syncAdapter.GetCommand(commandType))
                            {
                                // getting the row updated from server
                                var dmTableRow = syncAdapter.GetRow(row);
                                row = dmTableRow.Rows[0];

                                // Deriving Parameters
                                syncAdapter.SetCommandParameters(commandType, metadataCommand);

                                // Set the id parameter
                                syncAdapter.SetColumnParametersValues(metadataCommand, row);

                                var version = row.RowState == DmRowState.Deleted ? DmRowVersion.Original : DmRowVersion.Current;

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
                                var rowsApplied = syncAdapter.InsertOrUpdateMetadatas(metadataCommand, row, null);

                                if (!rowsApplied)
                                    throw new Exception("No metadatas rows found, can't update the server side");

                            }
                        }
                    }

                    finalRow = isMergeAction ? row : conflict.LocalRow;

                    // We don't do anything on the local provider, so we do not need to return a +1 on syncConflicts count
                    return (ChangeApplicationAction.Continue, 0, finalRow);
                }
                return (ChangeApplicationAction.Rollback, 0, finalRow);
            }

            // We gonna apply with force the line
            if (conflictApplyAction == ApplyAction.RetryWithForceWrite)
            {
                if (conflict.RemoteRow == null)
                {
                    // TODO : Should Raise an error ?
                    return (ChangeApplicationAction.Rollback, 0, finalRow);
                }

                bool operationComplete = false;

                // create a localscope to override values
                var localScope = new ScopeInfo { Name = scope.Name, Timestamp = fromScopeLocalTimeStamp };

                DbCommandType commandType = DbCommandType.InsertMetadata;
                bool needToUpdateMetadata = true;

                switch (conflict.Type)
                {
                    // Remote source has row, Local don't have the row, so insert it
                    case ConflictType.RemoteUpdateLocalNoRow:
                    case ConflictType.RemoteInsertLocalNoRow:
                        operationComplete = syncAdapter.ApplyInsert(conflict.RemoteRow, localScope, true);
                        commandType = DbCommandType.InsertMetadata;
                        break;

                    // Conflict, but both have delete the row, so nothing to do
                    case ConflictType.RemoteDeleteLocalDelete:
                    case ConflictType.RemoteDeleteLocalNoRow:
                        operationComplete = true;
                        needToUpdateMetadata = false;
                        break;

                    // The remote has delete the row, and local has insert or update it
                    // So delete the local row
                    case ConflictType.RemoteDeleteLocalUpdate:
                    case ConflictType.RemoteDeleteLocalInsert:
                        operationComplete = syncAdapter.ApplyDelete(conflict.RemoteRow, localScope, true);
                        commandType = DbCommandType.UpdateMetadata;
                        break;


                    // Remote insert and local delete, sor insert again on local
                    // but tracking line exist, so make an update on metadata
                    case ConflictType.RemoteInsertLocalDelete:
                    case ConflictType.RemoteUpdateLocalDelete:
                        operationComplete = syncAdapter.ApplyInsert(conflict.RemoteRow, localScope, true);
                        commandType = DbCommandType.UpdateMetadata;
                        break;

                    // Remote insert and local insert/ update, take the remote row and update the local row
                    case ConflictType.RemoteUpdateLocalInsert:
                    case ConflictType.RemoteUpdateLocalUpdate:
                    case ConflictType.RemoteInsertLocalInsert:
                    case ConflictType.RemoteInsertLocalUpdate:
                        operationComplete = syncAdapter.ApplyUpdate(conflict.RemoteRow, localScope, true);
                        commandType = DbCommandType.UpdateMetadata;
                        break;

                    case ConflictType.RemoteCleanedupDeleteLocalUpdate:
                    case ConflictType.ErrorsOccurred:
                        return (ChangeApplicationAction.Rollback, 0, finalRow);
                }

                if (needToUpdateMetadata)
                {
                    using (var metadataCommand = syncAdapter.GetCommand(commandType))
                    {
                        // Deriving Parameters
                        syncAdapter.SetCommandParameters(commandType, metadataCommand);

                        // force applying client row, so apply scope.id (client scope here)
                        var rowsApplied = syncAdapter.InsertOrUpdateMetadatas(metadataCommand, conflict.RemoteRow, scope.Id);
                        if (!rowsApplied)
                            throw new Exception("No metadatas rows found, can't update the server side");
                    }
                }

                finalRow = conflict.RemoteRow;

                //After a force update, there is a problem, so raise exception
                if (!operationComplete)
                {
                    var ex = $"Can't force operation for applyType {syncAdapter.ApplyType}";
                    finalRow = null;
                    return (ChangeApplicationAction.Continue, 0, finalRow);
                }

                // tableProgress.ChangesApplied += 1;
                return (ChangeApplicationAction.Continue, 1, finalRow);
            }

            return (ChangeApplicationAction.Rollback, 0, finalRow);

        }



    }
}
