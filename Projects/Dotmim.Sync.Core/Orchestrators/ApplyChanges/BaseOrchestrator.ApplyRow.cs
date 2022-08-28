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
        /// Apply a delete on a row. if forceWrite, force the delete
        /// </summary>
        internal virtual async Task<(SyncContext context, bool applied, Exception exception)> InternalApplyDeleteAsync(
            ScopeInfo scopeInfo, SyncContext context, SyncRow row, SyncTable schemaTable, long? lastTimestamp, Guid? senderScopeId, bool forceWrite, DbConnection connection, DbTransaction transaction)
        {
            var (command, _) = await this.InternalGetCommandAsync(scopeInfo, context, schemaTable, DbCommandType.DeleteRow, null,
                connection, transaction, default, default).ConfigureAwait(false);

            if (command == null) return (context, false, null);

            // Set the parameters value from row
            this.SetColumnParametersValues(command, row);

            // Set the special parameters for update
            this.AddScopeParametersValues(command, senderScopeId, lastTimestamp, true, forceWrite);

            await this.InterceptAsync(new ExecuteCommandArgs(context, command, DbCommandType.DeleteRow, connection, transaction)).ConfigureAwait(false);

            Exception exception = null;
            int rowDeletedCount = 0;

            try
            {
                rowDeletedCount = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                // Check if we have a return value instead
                var syncRowCountParam = GetParameter(command, "sync_row_count");

                if (syncRowCountParam != null)
                    rowDeletedCount = (int)syncRowCountParam.Value;

                command.Dispose();
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            return (context, rowDeletedCount > 0, exception);
        }

        /// <summary>
        /// Apply a single update in the current datasource. if forceWrite, force the update
        /// </summary>
        internal virtual async Task<(SyncContext context, bool applied, Exception exception)> InternalApplyUpdateAsync(
            ScopeInfo scopeInfo, SyncContext context, SyncRow row, SyncTable schemaTable, long? lastTimestamp, Guid? senderScopeId, bool forceWrite, DbConnection connection, DbTransaction transaction)
        {
            var (command, _) = await this.InternalGetCommandAsync(scopeInfo, context, schemaTable, DbCommandType.UpdateRow, null,
                connection, transaction, default, default).ConfigureAwait(false);

            if (command == null) return (context, false, null);

            // Set the parameters value from row
            this.SetColumnParametersValues(command, row);

            // Set the special parameters for update
            this.AddScopeParametersValues(command, senderScopeId, lastTimestamp, false, forceWrite);

            await this.InterceptAsync(new ExecuteCommandArgs(context, command, DbCommandType.UpdateRow, connection, transaction)).ConfigureAwait(false);

            Exception exception = null;
            int rowUpdatedCount = 0;
            try
            {
                rowUpdatedCount = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                // Check if we have a return value instead
                var syncRowCountParam = GetParameter(command, "sync_row_count");

                if (syncRowCountParam != null)
                    rowUpdatedCount = (int)syncRowCountParam.Value;

                command.Dispose();
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            return (context, rowUpdatedCount > 0, exception);
        }
    }
}
