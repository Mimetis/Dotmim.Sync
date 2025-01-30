﻿using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains methods to handle conflicts.
    /// </summary>
    public abstract partial class BaseOrchestrator
    {

        /// <summary>
        /// Try to get a source row that is in conflict.
        /// </summary>
        internal async Task<(SyncContext Context, SyncRow SyncRow)> InternalGetConflictRowAsync(ScopeInfo scopeInfo, SyncContext context,
            SyncTable schemaTable, SyncRow primaryKeyRow, DbConnection connection, DbTransaction transaction,
            IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                var syncAdapter = this.GetSyncAdapter(schemaTable, scopeInfo);

                // Get the row in the local repository
                var (command, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, DbCommandType.SelectRow,
                    connection, transaction, default, default).ConfigureAwait(false);

                if (command == null)
                    return (context, null);

                // Set the parameters value from row
                this.InternalSetCommandParametersValues(context, command, DbCommandType.SelectRow, syncAdapter, connection, transaction,
                    primaryKeyRow, progress: progress, cancellationToken: cancellationToken);

                await this.InterceptAsync(new ExecuteCommandArgs(context, command, DbCommandType.SelectRow, connection, transaction), cancellationToken: cancellationToken).ConfigureAwait(false);

                // Create a select table based on the schema in parameter + scope columns
                var changesSet = schemaTable.Schema.Clone(false);
                var selectTable = CreateChangesTable(schemaTable, changesSet);

                using var dataReader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

                var read = await dataReader.ReadAsync(cancellationToken).ConfigureAwait(false);

                if (!read)
                {

#if NET6_0_OR_GREATER
                    await dataReader.CloseAsync().ConfigureAwait(false);
#else
                    dataReader.Close();
#endif
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
                        var isTombstone = SyncTypeConverter.TryConvertTo<long>(dataReader.GetValue(i)) > 0;
                        syncRow.RowState = isTombstone ? SyncRowState.Deleted : SyncRowState.Modified;
                        continue;
                    }

                    if (columnName == "sync_update_scope_id")
                        continue;

                    var columnValueObject = dataReader.GetValue(i);
                    var columnValue = columnValueObject == DBNull.Value ? null : columnValueObject;
                    syncRow[columnName] = columnValue;
                }

                // if syncRow is not a deleted row, we can check for which kind of row it is.
                if (syncRow != null && syncRow.RowState == SyncRowState.None)
                    syncRow.RowState = SyncRowState.Modified;

#if NET6_0_OR_GREATER
                await dataReader.CloseAsync().ConfigureAwait(false);
#else
                dataReader.Close();
#endif

                command.Dispose();

                return (context, syncRow);
            }
            catch (Exception ex)
            {
                string message = null;

                if (primaryKeyRow != null)
                    message += $"Primary Key:{primaryKeyRow}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// We have a conflict, try to get the source row and generate a conflict.
        /// </summary>
        internal SyncConflict InternalGetConflict(SyncContext context, SyncRow remoteConflictRow, SyncRow localConflictRow)
        {
            try
            {
                var dbConflictType = ConflictType.ErrorsOccurred;

                if (remoteConflictRow == null)
                    throw new UnknownException("Remote Conflict Row Should Exists.");

                // local row is null
                if (localConflictRow == null && remoteConflictRow.RowState == SyncRowState.Modified)
                    dbConflictType = ConflictType.RemoteExistsLocalNotExists;
                else if (localConflictRow == null && remoteConflictRow.RowState == SyncRowState.Deleted)
                    dbConflictType = ConflictType.RemoteIsDeletedLocalNotExists;

                //// remote row is null. Can't happen
                // else if (remoteConflictRow == null && localConflictRow.RowState == SyncRowState.Modified)
                //    dbConflictType = ConflictType.RemoteNotExistsLocalExists;
                // else if (remoteConflictRow == null && localConflictRow.RowState == SyncRowState.Deleted)
                //    dbConflictType = ConflictType.RemoteNotExistsLocalIsDeleted;
                else if (remoteConflictRow.RowState == SyncRowState.Deleted && localConflictRow.RowState == SyncRowState.Deleted)
                    dbConflictType = ConflictType.RemoteIsDeletedLocalIsDeleted;
                else if (remoteConflictRow.RowState == SyncRowState.Modified && localConflictRow.RowState == SyncRowState.Deleted)
                    dbConflictType = ConflictType.RemoteExistsLocalIsDeleted;
                else if (remoteConflictRow.RowState == SyncRowState.Deleted && localConflictRow.RowState == SyncRowState.Modified)
                    dbConflictType = ConflictType.RemoteIsDeletedLocalExists;
                else if (remoteConflictRow.RowState == SyncRowState.Modified && localConflictRow.RowState == SyncRowState.Modified)
                    dbConflictType = ConflictType.RemoteExistsLocalExists;

                // Generate the conflict
                var conflict = new SyncConflict(dbConflictType);
                conflict.AddRemoteRow(remoteConflictRow);

                if (localConflictRow != null)
                    conflict.AddLocalRow(localConflictRow);

                return conflict;
            }
            catch (Exception ex)
            {
                string message = null;

                if (localConflictRow != null)
                    message += $"Local Row:{localConflictRow}.";

                if (remoteConflictRow != null)
                    message += $"Remote Row:{remoteConflictRow}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// A conflict has occured, we try to ask for the solution to the user.
        /// </summary>
        private async Task<(ConflictResolution ConflictResolution, ConflictType ConflictType, SyncRow FinalRow, Guid? FinalSenderScopeId)>
            GetConflictResolutionAsync(ScopeInfo scopeInfo, SyncContext context, Guid localScopeId, SyncRow conflictRow,
            SyncTable schemaChangesTable, ConflictResolutionPolicy policy, Guid senderScopeId,
            DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {

            // default action
            var resolution = policy == ConflictResolutionPolicy.ClientWins ? ConflictResolution.ClientWins : ConflictResolution.ServerWins;

            // check the interceptor
            var interceptors = this.Interceptors.GetInterceptors<ApplyChangesConflictOccuredArgs>();

            SyncRow finalRow = null;
            Guid? finalSenderScopeId = senderScopeId;

            // default conflict type
            var conflictType = conflictRow.RowState == SyncRowState.Deleted ? ConflictType.RemoteIsDeletedLocalExists : ConflictType.RemoteExistsLocalExists;

            // if is not empty, get the conflict and intercept
            // We don't get the conflict on automatic conflict resolution
            // Since it's an automatic resolution, we don't need to get the local conflict row
            // So far we get the conflict only if an interceptor exists
            var arg = new ApplyChangesConflictOccuredArgs(scopeInfo, context, this, conflictRow, schemaChangesTable, resolution, senderScopeId, connection, transaction);
            if (interceptors.Count > 0)
            {
                // Interceptor
                await this.InterceptAsync(arg, progress, cancellationToken).ConfigureAwait(false);

                resolution = arg.Resolution;
                finalRow = arg.Resolution == ConflictResolution.MergeRow ? arg.FinalRow : null;
                finalSenderScopeId = arg.SenderScopeId;
                var conflict = await arg.GetSyncConflictAsync().ConfigureAwait(false);
                conflictType = conflict != null ? conflict.Type : conflictType;
            }
            else
            {
                // Check logger, because we make some reflection here
                if (this.Logger.IsEnabled(LogLevel.Debug))
                    this.Logger.LogDebug(new EventId(arg.EventId, "ApplyChangesConflictOccured"), arg);
            }

            // returning the action to take, and actually the finalRow if action is set to Merge
            return (resolution, conflictType, finalRow, finalSenderScopeId);
        }

        /// <summary>
        /// Handle a conflict
        /// The int returned is the conflict count I need.
        /// </summary>
        private async Task<(bool IsApplied, bool IsConflictResolved, Exception Exception)> HandleConflictAsync(ScopeInfo scopeInfo, SyncContext context,
                                BatchInfo batchInfo, Guid localScopeId, Guid senderScopeId, SyncRow conflictRow,
                                SyncTable schemaChangesTable,
                                ConflictResolutionPolicy policy, long? lastTimestamp,
                                DbConnection connection, DbTransaction transaction,
                                IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            var (conflictResolution, conflictType, finalRow, nullableSenderScopeId) =
                 await this.GetConflictResolutionAsync(scopeInfo, context, localScopeId, conflictRow, schemaChangesTable,
                policy, senderScopeId, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

            Exception exception = null;
            var applied = false;
            var conflictResolved = false;
            bool operationComplete;

            switch (conflictResolution)
            {
                case ConflictResolution.ServerWins:
                    applied = false;
                    conflictResolved = true;
                    break;
                case ConflictResolution.ClientWins:

                    switch (conflictType)
                    {
                        // Remote source has row, Local don't have the row, so insert it
                        case ConflictType.RemoteExistsLocalExists:
                        case ConflictType.RemoteExistsLocalNotExists:
                        case ConflictType.RemoteExistsLocalIsDeleted:
                        case ConflictType.UniqueKeyConstraint:
                            (_, operationComplete, exception) = await this.InternalApplyUpdateAsync(scopeInfo, context, batchInfo,
                                conflictRow, schemaChangesTable, lastTimestamp, nullableSenderScopeId, true, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                            applied = operationComplete;
                            conflictResolved = operationComplete && exception == null;
                            break;

                        // Conflict, but both have delete the row, so just update the metadata to the right winner
                        case ConflictType.RemoteIsDeletedLocalIsDeleted:
                            // (_, operationComplete, exception) = await this.InternalUpdateMetadatasAsync(scopeInfo, context,
                            //    conflictRow, schemaChangesTable, nullableSenderScopeId, true, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                            // applied = false;
                            // conflictResolved = operationComplete && exception == null;
                            applied = false;
                            conflictResolved = true;

                            break;

                        // The row does not exists locally, and since it's coming from a deleted state, we can forget it
                        case ConflictType.RemoteIsDeletedLocalNotExists:
                            applied = false;
                            conflictResolved = true;
                            break;

                        // The remote has delete the row, and local has insert or update it
                        // So delete the local row
                        case ConflictType.RemoteIsDeletedLocalExists:
                            (_, operationComplete, exception) = await this.InternalApplyDeleteAsync(scopeInfo, context, batchInfo,
                                conflictRow, schemaChangesTable, lastTimestamp, nullableSenderScopeId, true, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                            // Conflict, but both have delete the row, so just update the metadata to the right winner
                            if (!operationComplete && exception == null)
                            {
                                // IS IT MANDATORY to update to the correct winner ?
                                // if we don't have any rows on one side, we will add a metadata for nothing...
                                // (_, operationComplete, exception) = await this.InternalUpdateMetadatasAsync(scopeInfo, context,
                                //    conflictRow, schemaChangesTable, nullableSenderScopeId, true, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                                // applied = false;
                                // conflictResolved = operationComplete && exception == null;
                                applied = false;
                                conflictResolved = true;
                            }
                            else
                            {
                                applied = operationComplete && exception == null;
                                conflictResolved = operationComplete && exception == null;
                            }

                            break;

                        case ConflictType.ErrorsOccurred:
                            break;
                    }

                    break;
                case ConflictResolution.MergeRow:
                    // if merge, we update locally the row and let the sync_update_scope_id set to null
                    // We don't update metadatas so the row is updated locally and is marked as updated by the trigger
                    // and will be returned back to client if occurs on server
                    // and will be returned to the server on next sync if occurs on client
                    (_, operationComplete, exception) = await this.InternalApplyUpdateAsync(scopeInfo, context, batchInfo,
                        finalRow, schemaChangesTable, lastTimestamp, null, true, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                    if (!operationComplete && exception == null)
                        exception = new Exception("Can't update the merged row locally.");

                    applied = operationComplete && exception == null;
                    conflictResolved = operationComplete && exception == null;

                    break;
                case ConflictResolution.Throw:
                    throw new RollbackException("Rollback action taken on conflict");
            }

            return (applied, conflictResolved, exception);
        }
    }
}