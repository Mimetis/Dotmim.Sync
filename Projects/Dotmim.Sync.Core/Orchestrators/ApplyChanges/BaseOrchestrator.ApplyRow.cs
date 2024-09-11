using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data.Common;
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
        internal virtual Task<(SyncContext Context, bool Applied, Exception Exception)> InternalApplyDeleteAsync(
            ScopeInfo scopeInfo, SyncContext context, BatchInfo batchInfo, SyncRow row, SyncTable schemaTable, long? lastTimestamp, Guid? senderScopeId, bool forceWrite,
            DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            return this.InternalApplyChangeAsync(scopeInfo, context, batchInfo, row, schemaTable, lastTimestamp, senderScopeId, forceWrite,
                connection, transaction, progress, cancellationToken, DbCommandType.PreDeleteRow, DbCommandType.DeleteRow);
        }

        /// <summary>
        /// Apply a single update in the current datasource. if forceWrite, force the update.
        /// </summary>
        internal virtual Task<(SyncContext Context, bool IsApplied, Exception Exception)> InternalApplyUpdateAsync(
            ScopeInfo scopeInfo, SyncContext context, BatchInfo batchInfo, SyncRow row, SyncTable schemaTable, long? lastTimestamp, Guid? senderScopeId, bool forceWrite,
            DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            return this.InternalApplyChangeAsync(scopeInfo, context, batchInfo, row, schemaTable, lastTimestamp, senderScopeId, forceWrite,
                connection, transaction, progress, cancellationToken, DbCommandType.PreUpdateRow, DbCommandType.UpdateRow);
        }

        private async Task<(SyncContext Context, bool Applied, Exception Exception)> InternalApplyChangeAsync(
            ScopeInfo scopeInfo, SyncContext context, BatchInfo batchInfo, SyncRow row, SyncTable schemaTable, long? lastTimestamp, Guid? senderScopeId, bool forceWrite,
            DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken, DbCommandType preCommandType, DbCommandType commandType)
        {
            // get executioning adapter
            var syncAdapter = this.GetSyncAdapter(schemaTable, scopeInfo);

            // Pre command if exists
            var (preCommand, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, preCommandType,
                connection, transaction, progress, cancellationToken).ConfigureAwait(false);

            if (preCommand != null)
            {
                try
                {
                    await this.InterceptAsync(new ExecuteCommandArgs(context, preCommand, preCommandType, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
                    await preCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    preCommand.Dispose();
                }
            }

            var (command, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, commandType,
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
            this.InternalSetCommandParametersValues(context, command, commandType, syncAdapter, connection, transaction,
            row, senderScopeId, lastTimestamp, commandType == DbCommandType.DeleteRow, forceWrite, progress, cancellationToken);

            await this.InterceptAsync(new ExecuteCommandArgs(context, command, commandType, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            Exception exception = null;
            var rowCount = 0;

            try
            {
                rowCount = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

                // Check if we have a return value instead
                var syncRowCountParam = syncAdapter.GetParameter(context, command, "sync_row_count");

                // Check if we have an handled error
                var syncErrorText = syncAdapter.GetParameter(context, command, "sync_error_text");

                if (syncRowCountParam != null && syncRowCountParam.Value != null && syncRowCountParam.Value != DBNull.Value)
                    rowCount = (int)syncRowCountParam.Value;

                if (syncErrorText != null && syncErrorText.Value != null && syncErrorText.Value != DBNull.Value)
                    throw new Exception(syncErrorText.Value.ToString());
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            finally
            {
                command.Dispose();
            }

            var rowAppliedArgs = new RowsChangesAppliedArgs(context, batchInfo, batchArgs.SyncRows, schemaTable, SyncRowState.Modified, rowCount, exception, connection, transaction);
            await this.InterceptAsync(rowAppliedArgs, progress, cancellationToken).ConfigureAwait(false);

            return (context, rowCount > 0, exception);
        }
    }
}