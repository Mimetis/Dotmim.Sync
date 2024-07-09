using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains methods to apply changes.
    /// </summary>
    public abstract partial class BaseOrchestrator
    {
        /// <summary>
        /// Apply a delete on a row. if forceWrite, force the delete.
        /// </summary>
        internal virtual async Task<(SyncContext Context, bool Applied, Exception Exception)> InternalApplyDeleteAsync(
            ScopeInfo scopeInfo, SyncContext context, BatchInfo batchInfo, SyncRow row, SyncTable schemaTable, long? lastTimestamp, Guid? senderScopeId, bool forceWrite,
            DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // get executioning adapter
            var syncAdapter = this.GetSyncAdapter(scopeInfo.Name, schemaTable, scopeInfo.Setup);

            // Pre command if exists
            var (preCommand, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, DbCommandType.PreDeleteRow,
                connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (preCommand != null)
            {
                await this.InterceptAsync(new ExecuteCommandArgs(context, preCommand, DbCommandType.PreDeleteRow, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
                await preCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
                preCommand.Dispose();
            }

            var (command, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, DbCommandType.DeleteRow,
                connection, transaction, default, default).ConfigureAwait(false);

            if (command == null)
                return (context, false, null);

            var batchArgs = new RowsChangesApplyingArgs(context, batchInfo, new List<SyncRow> { row }, schemaTable, SyncRowState.Modified, command, connection, transaction);
            await this.InterceptAsync(batchArgs, progress, cancellationToken).ConfigureAwait(false);

            if (batchArgs.Cancel || batchArgs.Command == null || batchArgs.SyncRows == null || batchArgs.SyncRows.Count <= 0)
                return (context, false, null);

            // get the correct pointer to the command from the interceptor in case user change the whole instance
            command = batchArgs.Command;

            // Set the parameters
            this.InternalSetCommandParametersValues(context, command, DbCommandType.DeleteRow, syncAdapter, connection, transaction, cancellationToken, progress,
            row: row, sync_scope_id: senderScopeId, sync_min_timestamp: lastTimestamp, sync_row_is_tombstone: true, sync_force_write: forceWrite);

            await this.InterceptAsync(new ExecuteCommandArgs(context, command, DbCommandType.DeleteRow, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            Exception exception = null;
            var rowDeletedCount = 0;

            try
            {
                rowDeletedCount = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                // Check if we have a return value instead
                var syncRowCountParam = syncAdapter.GetParameter(context, command, "sync_row_count");

                // Check if we have an handled error
                var syncErrorText = syncAdapter.GetParameter(context, command, "sync_error_text");

                if (syncRowCountParam != null && syncRowCountParam.Value != null && syncRowCountParam.Value != DBNull.Value)
                    rowDeletedCount = (int)syncRowCountParam.Value;

                if (syncErrorText != null && syncErrorText.Value != null && syncErrorText.Value != DBNull.Value)
                    throw new Exception(syncErrorText.Value.ToString());

                command.Dispose();
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            var rowAppliedArgs = new RowsChangesAppliedArgs(context, batchInfo, batchArgs.SyncRows, schemaTable, SyncRowState.Modified, rowDeletedCount, exception, connection, transaction);
            await this.InterceptAsync(rowAppliedArgs, progress, cancellationToken).ConfigureAwait(false);

            return (context, rowDeletedCount > 0, exception);
        }

        /// <summary>
        /// Apply a single update in the current datasource. if forceWrite, force the update.
        /// </summary>
        internal virtual async Task<(SyncContext Context, bool IsApplied, Exception Exception)> InternalApplyUpdateAsync(
            ScopeInfo scopeInfo, SyncContext context, BatchInfo batchInfo, SyncRow row, SyncTable schemaTable, long? lastTimestamp, Guid? senderScopeId, bool forceWrite,
            DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var syncAdapter = this.GetSyncAdapter(scopeInfo.Name, schemaTable, scopeInfo.Setup);

            // Pre command if exists
            var (preCommand, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, DbCommandType.PreUpdateRow,
                connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (preCommand != null)
            {
                await this.InterceptAsync(new ExecuteCommandArgs(context, preCommand, DbCommandType.PreUpdateRow, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
                await preCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
                preCommand.Dispose();
            }

            var (command, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, DbCommandType.UpdateRow,
                connection, transaction, default, default).ConfigureAwait(false);

            if (command == null)
                return (context, false, null);

            var batchArgs = new RowsChangesApplyingArgs(context, batchInfo, new List<SyncRow> { row }, schemaTable, SyncRowState.Modified, command, connection, transaction);
            await this.InterceptAsync(batchArgs, progress, cancellationToken).ConfigureAwait(false);

            if (batchArgs.Cancel || batchArgs.Command == null || batchArgs.SyncRows == null || batchArgs.SyncRows.Count <= 0)
                return (context, false, null);

            // get the correct pointer to the command from the interceptor in case user change the whole instance
            command = batchArgs.Command;

            // Set the parameters value from row
            this.InternalSetCommandParametersValues(context, command, DbCommandType.UpdateRow, syncAdapter, connection, transaction, cancellationToken, progress,
                 row: batchArgs.SyncRows.First(), sync_scope_id: senderScopeId, sync_min_timestamp: lastTimestamp, sync_row_is_tombstone: false, sync_force_write: forceWrite);

            await this.InterceptAsync(new ExecuteCommandArgs(context, command, DbCommandType.UpdateRow, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            Exception exception = null;
            var rowUpdatedCount = 0;
            try
            {
                rowUpdatedCount = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                // Check if we have a return value instead
                var syncRowCountParam = syncAdapter.GetParameter(context, command, "sync_row_count");

                // Check if we have an handled error
                var syncErrorText = syncAdapter.GetParameter(context, command, "sync_error_text");

                if (syncRowCountParam != null && syncRowCountParam.Value != null && syncRowCountParam.Value != DBNull.Value)
                    rowUpdatedCount = (int)syncRowCountParam.Value;

                if (syncErrorText != null && syncErrorText.Value != null && syncErrorText.Value != DBNull.Value)
                    throw new Exception(syncErrorText.Value.ToString());

                command.Dispose();
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            var rowAppliedArgs = new RowsChangesAppliedArgs(context, batchInfo, batchArgs.SyncRows, schemaTable, SyncRowState.Modified, rowUpdatedCount, exception, connection, transaction);
            await this.InterceptAsync(rowAppliedArgs, progress, cancellationToken).ConfigureAwait(false);

            return (context, rowUpdatedCount > 0, exception);
        }
    }
}