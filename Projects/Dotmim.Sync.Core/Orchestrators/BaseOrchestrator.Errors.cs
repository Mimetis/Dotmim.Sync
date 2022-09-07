
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
        private async Task<(bool applied, bool failed, Exception ex)>
            HandleErrorAsync(ScopeInfo scopeInfo, SyncContext context, SyncRow errorRow, SyncRowState applyType,
                                SyncTable schemaChangesTable, Exception exception, LocalJsonSerializer localSerializer, string filePath,
                                Guid senderScopeId, long? lastTimestamp,
                                DbConnection connection, DbTransaction transaction,
                                CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var errorRowArgs = new ApplyChangesErrorOccuredArgs(context, errorRow, schemaChangesTable, applyType, exception,
                connection, transaction);

            var errorOccuredArgs = await this.InterceptAsync(errorRowArgs, progress, cancellationToken).ConfigureAwait(false);
            bool hasFailed = false, applied = false;
            Exception operationException = null;

            switch (errorOccuredArgs.Resolution)
            {
                // We have an error, but we continue
                // Row in error is saved to the batch error file
                case ErrorResolution.ContinueOnError:
                    if (!localSerializer.IsOpen)
                        await localSerializer.OpenFileAsync(filePath, schemaChangesTable).ConfigureAwait(false);

                    errorRow.RowState = applyType == SyncRowState.Modified ? SyncRowState.ApplyModifiedFailed : SyncRowState.ApplyDeletedFailed;
                    await localSerializer.WriteRowToFileAsync(errorRow, schemaChangesTable).ConfigureAwait(false);
                    hasFailed = true;
                    applied = false;

                    break;
                // We have an error but at least we try one more time
                case ErrorResolution.RetryOneMoreTime:

                    bool operationComplete;

                    if (applyType == SyncRowState.Deleted)
                    {
                        (context, operationComplete, operationException) = await this.InternalApplyDeleteAsync(
                            scopeInfo, context, errorRow, schemaChangesTable, lastTimestamp, senderScopeId, true,
                            connection, transaction).ConfigureAwait(false);
                    }
                    else
                    {
                        (context, operationComplete, operationException) = await this.InternalApplyUpdateAsync(
                            scopeInfo, context, errorRow, schemaChangesTable, lastTimestamp, senderScopeId, true,
                            connection, transaction).ConfigureAwait(false);
                    }

                    // we have another error raised even if we tried again
                    // row is saved to batch error file
                    if (operationException != null)
                    {
                        if (!localSerializer.IsOpen)
                            await localSerializer.OpenFileAsync(filePath, schemaChangesTable).ConfigureAwait(false);

                        errorRow.RowState = applyType == SyncRowState.Modified ? SyncRowState.ApplyModifiedFailed : SyncRowState.ApplyDeletedFailed;
                        await localSerializer.WriteRowToFileAsync(errorRow, schemaChangesTable).ConfigureAwait(false);
                        hasFailed = true;
                        applied = false;
                    }
                    else
                    {
                        applied = operationComplete;
                        hasFailed = !operationComplete;
                    }


                    break;
                // we mark the row to be tried again on next sync
                case ErrorResolution.RetryOnNextSync:

                    if (!localSerializer.IsOpen)
                        await localSerializer.OpenFileAsync(filePath, schemaChangesTable).ConfigureAwait(false);

                    errorRow.RowState = applyType == SyncRowState.Modified ? SyncRowState.RetryModifiedOnNextSync : SyncRowState.RetryDeletedOnNextSync;
                    await localSerializer.WriteRowToFileAsync(errorRow, schemaChangesTable).ConfigureAwait(false);
                    hasFailed = true;
                    applied = false;

                    break;
                // row is marked as resolved
                case ErrorResolution.Resolved:
                    hasFailed = false;
                    applied = true;
                    break;
                // default case : we throw the error
                case ErrorResolution.Throw:
                    hasFailed = true;
                    applied = false;

                    if (!localSerializer.IsOpen)
                        await localSerializer.OpenFileAsync(filePath, schemaChangesTable).ConfigureAwait(false);

                    errorRow.RowState = applyType == SyncRowState.Modified ? SyncRowState.ApplyModifiedFailed : SyncRowState.ApplyDeletedFailed;
                    await localSerializer.WriteRowToFileAsync(errorRow, schemaChangesTable).ConfigureAwait(false);

                    operationException = exception;
                    break;
            }

            return (applied, hasFailed, operationException);
        }


    }
}