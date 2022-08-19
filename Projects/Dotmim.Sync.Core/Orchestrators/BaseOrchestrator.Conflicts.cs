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
        /// Handle a conflict
        /// The int returned is the conflict count I need 
        /// </summary>
        private async Task<(SyncContext context, TableConflictErrorApplied tableConflictApplied)>
            HandleConflictAsync(IScopeInfo scopeInfo, SyncContext context,
                                Guid localScopeId, Guid senderScopeId, DbSyncAdapter syncAdapter, SyncRow conflictRow, 
                                SyncTable schemaChangesTable,
                                ConflictResolutionPolicy policy, long? lastTimestamp,
                                DbConnection connection, DbTransaction transaction,
                                CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            TableConflictErrorApplied tableConflictApplied = new TableConflictErrorApplied();
            SyncRow finalRow;
            ConflictType conflictType;
            ApplyAction conflictApplyAction;
            Guid? nullableSenderScopeId;


            // TODO : Should Raise an error ?
            if (conflictRow == null)
            {
                tableConflictApplied.HasBeenResolved = false;
                tableConflictApplied.HasBeenApplied = false;
                return (context, tableConflictApplied);
            }

            (context, conflictApplyAction, conflictType, finalRow, nullableSenderScopeId) =
                 await this.GetConflictActionAsync(scopeInfo, context, localScopeId, syncAdapter, conflictRow, schemaChangesTable,
                policy, senderScopeId, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            // Conflict rollbacked by user
            if (conflictApplyAction == ApplyAction.Rollback)
            {
                tableConflictApplied.HasBeenApplied = false;
                tableConflictApplied.HasBeenResolved = false;
                tableConflictApplied.Exception = new RollbackException("Rollback action taken on conflict");
                return (context, tableConflictApplied);
            }
            else if (conflictApplyAction == ApplyAction.Merge)
            {
                // if merge, we update locally the row and let the sync_update_scope_id set to null
                // We don't update metadatas so the row is updated locally and is marked as updated by the trigger
                // and will be returned back to client if occurs on server
                // and will be returned to the server on next sync if occurs on client
                bool isUpdated;
                Exception exception;

                (context, isUpdated, exception) = await this.InternalApplyUpdateAsync(scopeInfo, context,
                    syncAdapter, finalRow, lastTimestamp, null, true, connection, transaction).ConfigureAwait(false);

                if (!isUpdated && exception == null)
                    exception = new Exception("Can't update the merged row.");

                tableConflictApplied.HasBeenResolved = exception == null;
                tableConflictApplied.HasBeenApplied = exception == null;
                tableConflictApplied.Exception = exception;

                return (context, tableConflictApplied);
            }
            // Local wins
            else if (conflictApplyAction == ApplyAction.Continue)
            {
                // We don't do anything, since we let the original row.
                // so we resolved one conflict but applied no rows
                tableConflictApplied.HasBeenResolved = true;
                tableConflictApplied.HasBeenApplied = false;
                return (context, tableConflictApplied);
            }

            // We gonna apply with force the line. ApplyAction.RetryWithForceWrite
            else
            {
                bool operationComplete = false;
                Exception exception;
                switch (conflictType)
                {
                    // Remote source has row, Local don't have the row, so insert it
                    case ConflictType.RemoteExistsLocalExists:
                    case ConflictType.RemoteExistsLocalNotExists:
                    case ConflictType.RemoteExistsLocalIsDeleted:
                    case ConflictType.UniqueKeyConstraint:
                        (context, operationComplete, exception) = await this.InternalApplyUpdateAsync(scopeInfo, context,
                            syncAdapter, conflictRow, lastTimestamp, nullableSenderScopeId, true, connection, transaction).ConfigureAwait(false);
                        tableConflictApplied.HasBeenApplied = operationComplete;
                        tableConflictApplied.HasBeenResolved = operationComplete;
                        tableConflictApplied.Exception = exception;
                        break;

                    // Conflict, but both have delete the row, so just update the metadata to the right winner
                    case ConflictType.RemoteIsDeletedLocalIsDeleted:
                        (context, operationComplete, exception) = await this.InternalUpdateMetadatasAsync(scopeInfo, context, syncAdapter, conflictRow, nullableSenderScopeId, true, connection, transaction).ConfigureAwait(false);
                        tableConflictApplied.HasBeenApplied = false;
                        tableConflictApplied.HasBeenResolved = operationComplete;
                        tableConflictApplied.Exception = exception;
                        break;

                    // The row does not exists locally, and since it's coming from a deleted state, we can forget it
                    case ConflictType.RemoteIsDeletedLocalNotExists:
                        operationComplete = true;
                        tableConflictApplied.HasBeenApplied = false;
                        tableConflictApplied.HasBeenResolved = true;
                        tableConflictApplied.Exception = null;
                        break;

                    // The remote has delete the row, and local has insert or update it
                    // So delete the local row
                    case ConflictType.RemoteIsDeletedLocalExists:
                        (context, operationComplete, exception) = await this.InternalApplyDeleteAsync(scopeInfo, context, syncAdapter, conflictRow, lastTimestamp, nullableSenderScopeId, true, connection, transaction);

                        // Conflict, but both have delete the row, so just update the metadata to the right winner
                        if (!operationComplete && exception == null)
                        {
                            (context, operationComplete, exception) = await this.InternalUpdateMetadatasAsync(scopeInfo, context, syncAdapter, conflictRow, nullableSenderScopeId, true, connection, transaction);
                            tableConflictApplied.HasBeenApplied = false;
                            tableConflictApplied.HasBeenResolved = operationComplete;
                            tableConflictApplied.Exception = exception;
                        }
                        else
                        {
                            tableConflictApplied.HasBeenApplied = operationComplete;
                            tableConflictApplied.HasBeenResolved = operationComplete;
                            tableConflictApplied.Exception = exception;
                        }
                        break;

                    case ConflictType.ErrorsOccurred:
                        break;
                }
                return (context, tableConflictApplied);
            }
        }

        /// <summary>
        /// A conflict has occured, we try to ask for the solution to the user
        /// </summary>
        private async Task<(SyncContext context, ApplyAction applyAction, ConflictType conflictType,
            SyncRow finalRow, Guid? finalSenderScopeId)>
            GetConflictActionAsync(IScopeInfo scopeInfo, SyncContext context, Guid localScopeId, DbSyncAdapter syncAdapter, SyncRow conflictRow,
            SyncTable schemaChangesTable, ConflictResolutionPolicy policy, Guid senderScopeId, DbConnection connection, DbTransaction transaction,
            CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            // default action
            var resolution = policy == ConflictResolutionPolicy.ClientWins ? ConflictResolution.ClientWins : ConflictResolution.ServerWins;

            // if ConflictAction is ServerWins it's Ok to set to Continue
            var action = ApplyAction.Continue;

            // check the interceptor
            var interceptors = this.interceptors.GetInterceptors<ApplyChangesConflictOccuredArgs>();

            SyncRow finalRow = null;
            Guid? finalSenderScopeId = senderScopeId;

            // default conflict type
            ConflictType conflictType = conflictRow.RowState == DataRowState.Deleted ? ConflictType.RemoteIsDeletedLocalExists : ConflictType.RemoteExistsLocalExists;

            // if is not empty, get the conflict and intercept
            // We don't get the conflict on automatic conflict resolution
            // Since it's an automatic resolution, we don't need to get the local conflict row
            // So far we get the conflict only if an interceptor exists
            if (interceptors.Count > 0)
            {
                // Interceptor
                var arg = new ApplyChangesConflictOccuredArgs(context, this, syncAdapter, conflictRow, schemaChangesTable, resolution, senderScopeId, connection, transaction);
                await this.InterceptAsync(arg, progress, cancellationToken).ConfigureAwait(false);

                resolution = arg.Resolution;
                finalRow = arg.Resolution == ConflictResolution.MergeRow ? arg.FinalRow : null;
                finalSenderScopeId = arg.SenderScopeId;
                conflictType = arg.conflict != null ? arg.conflict.Type : conflictType;
            }
            else
            {
                // Check logger, because we make some reflection here
                if (this.Logger.IsEnabled(LogLevel.Debug))
                {
                    var args = new { Row = conflictRow, Resolution = resolution, Connection = connection, Transaction = transaction };
                    this.Logger.LogDebug(new EventId(SyncEventsId.ApplyChangesFailed.Id, "ApplyChangesFailed"), args);
                }
            }

            // Change action only if we choose ClientWins or Rollback.
            // for ServerWins or MergeRow, action is Continue
            if (resolution == ConflictResolution.ClientWins)
                action = ApplyAction.RetryWithForceWrite;
            else if (resolution == ConflictResolution.MergeRow)
                action = ApplyAction.Merge;
            else if (resolution == ConflictResolution.Rollback)
                action = ApplyAction.Rollback;

            // returning the action to take, and actually the finalRow if action is set to Merge
            return (context, action, conflictType, finalRow, finalSenderScopeId);
        }

        /// <summary>
        /// We have a conflict, try to get the source row and generate a conflict
        /// </summary>
        internal SyncConflict InternalGetConflict(SyncRow remoteConflictRow, SyncRow localConflictRow)
        {

            var dbConflictType = ConflictType.ErrorsOccurred;

            if (remoteConflictRow == null)
                throw new UnknownException("THAT can't happen...");


            // local row is null
            if (localConflictRow == null && remoteConflictRow.RowState == DataRowState.Modified)
                dbConflictType = ConflictType.RemoteExistsLocalNotExists;
            else if (localConflictRow == null && remoteConflictRow.RowState == DataRowState.Deleted)
                dbConflictType = ConflictType.RemoteIsDeletedLocalNotExists;

            //// remote row is null. Can't happen
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

        /// <summary>
        /// Try to get a source row
        /// </summary>
        internal async Task<(SyncContext context, SyncRow syncRow)> InternalGetConflictRowAsync(SyncContext context, DbSyncAdapter syncAdapter,
                    SyncRow primaryKeyRow, SyncTable schema, DbConnection connection, DbTransaction transaction)
        {
            // Get the row in the local repository
            var (command, _) = await syncAdapter.GetCommandAsync(DbCommandType.SelectRow, connection, transaction);

            if (command == null) return (context, null);

            // set the primary keys columns as parameters
            syncAdapter.SetColumnParametersValues(command, primaryKeyRow);

            // Create a select table based on the schema in parameter + scope columns
            var changesSet = schema.Schema.Clone(false);
            var selectTable = DbSyncAdapter.CreateChangesTable(schema, changesSet);

            await this.InterceptAsync(new DbCommandArgs(context, command, connection, transaction)).ConfigureAwait(false);

            using var dataReader = await command.ExecuteReaderAsync().ConfigureAwait(false);

            if (!dataReader.Read())
            {
                dataReader.Close();
                command.Dispose();
                return (context, null);
            }

            // Create a new empty row
            var syncRow = selectTable.NewRow();
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
                if (columnName == "sync_update_scope_id")
                    continue;

                var columnValueObject = dataReader.GetValue(i);
                var columnValue = columnValueObject == DBNull.Value ? null : columnValueObject;
                syncRow[columnName] = columnValue;
            }

            // if syncRow is not a deleted row, we can check for which kind of row it is.
            if (syncRow != null && syncRow.RowState == DataRowState.Unchanged)
                syncRow.RowState = DataRowState.Modified;

            dataReader.Close();
            command.Dispose();

            return (context, syncRow);
        }


    }
}
