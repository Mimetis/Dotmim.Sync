
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
        /// Apply a delete on a row. if forceWrite, force the delete
        /// </summary>
        internal virtual async Task<(SyncContext context, bool applied, Exception exception)> InternalApplyDeleteAsync(
            ScopeInfo scopeInfo, SyncContext context, BatchInfo batchInfo, SyncRow row, SyncTable schemaTable, long? lastTimestamp, Guid? senderScopeId, bool forceWrite, 
            DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // get executioning adapter
            var syncAdapter = this.GetSyncAdapter(scopeInfo.Name, schemaTable, scopeInfo.Setup);

            var (command, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, DbCommandType.DeleteRow, null,
                connection, transaction, default, default).ConfigureAwait(false);

            if (command == null) return (context, false, null);

            var batchArgs = new RowsChangesApplyingArgs(context, batchInfo, new List<SyncRow> { row }, schemaTable, SyncRowState.Modified, command, connection, transaction);
            await this.InterceptAsync(batchArgs, progress, cancellationToken).ConfigureAwait(false);

            if (batchArgs.Cancel || batchArgs.Command == null || batchArgs.SyncRows == null || batchArgs.SyncRows.Count() <= 0)
                return (context, false, null);

            // get the correct pointer to the command from the interceptor in case user change the whole instance
            command = batchArgs.Command;

            // Set the parameters value from row 
            this.SetColumnParametersValues(command, batchArgs.SyncRows.First());

            // Set the special parameters for update
            this.AddScopeParametersValues(command, senderScopeId, lastTimestamp, true, forceWrite);

            await this.InterceptAsync(new ExecuteCommandArgs(context, command, DbCommandType.DeleteRow, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            Exception exception = null;
            int rowDeletedCount = 0;

            try
            {
                rowDeletedCount = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                // Check if we have a return value instead
                var syncRowCountParam = InternalGetParameter(command, "sync_row_count");

                // Check if we have an handled error
                var syncErrorText = InternalGetParameter(command, "sync_error_text");

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
        /// Apply a single update in the current datasource. if forceWrite, force the update
        /// </summary>
        internal virtual async Task<(SyncContext context, bool applied, Exception exception)> InternalApplyUpdateAsync(
            ScopeInfo scopeInfo, SyncContext context, BatchInfo batchInfo, SyncRow row, SyncTable schemaTable, long? lastTimestamp, Guid? senderScopeId, bool forceWrite, 
            DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var syncAdapter = this.GetSyncAdapter(scopeInfo.Name, schemaTable, scopeInfo.Setup);

            var (command, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, DbCommandType.UpdateRow, null,
                connection, transaction, default, default).ConfigureAwait(false);

            if (command == null) return (context, false, null);

            var batchArgs = new RowsChangesApplyingArgs(context, batchInfo, new List<SyncRow> { row }, schemaTable, SyncRowState.Modified, command, connection, transaction);
            await this.InterceptAsync(batchArgs, progress, cancellationToken).ConfigureAwait(false);

            if (batchArgs.Cancel || batchArgs.Command == null || batchArgs.SyncRows == null || batchArgs.SyncRows.Count() <= 0)
                return (context, false, null);

            // get the correct pointer to the command from the interceptor in case user change the whole instance
            command = batchArgs.Command;

            // Set the parameters value from row 
            this.SetColumnParametersValues(command, batchArgs.SyncRows.First());

            // Set the special parameters for update
            this.AddScopeParametersValues(command, senderScopeId, lastTimestamp, false, forceWrite);

            await this.InterceptAsync(new ExecuteCommandArgs(context, command, DbCommandType.UpdateRow, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            Exception exception = null;
            int rowUpdatedCount = 0;
            try
            {
                rowUpdatedCount = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                // Check if we have a return value instead
                var syncRowCountParam = InternalGetParameter(command, "sync_row_count");

                // Check if we have an handled error
                var syncErrorText = InternalGetParameter(command, "sync_error_text");

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
