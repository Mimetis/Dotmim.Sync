
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
        private async Task<(ErrorAction errorAction, Exception ex)> HandleErrorAsync(ScopeInfo scopeInfo, SyncContext context, SyncRow errorRow, SyncRowState applyType,
                                SyncTable schemaChangesTable, Exception exception, Guid senderScopeId, long? lastTimestamp,
                                DbConnection connection, DbTransaction transaction,
                                CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var errorRowArgs = new ApplyChangesErrorOccuredArgs(context, errorRow, schemaChangesTable, applyType, exception,
                connection, transaction);

            var errorOccuredArgs = await this.InterceptAsync(errorRowArgs, progress, cancellationToken).ConfigureAwait(false);
            Exception operationException = null;

            // We are handling a previous error already in the batch info
            if (errorRow.RowState == SyncRowState.ApplyModifiedFailed || applyType == SyncRowState.ApplyDeletedFailed)
                return (ErrorAction.Ignore, null);
            
            var errorAction = ErrorAction.Throw;
            switch (errorOccuredArgs.Resolution)
            {
                // We have an error, but we continue
                // Row in error is saved to the batch error file
                case ErrorResolution.ContinueOnError:
                    errorRow.RowState = applyType == SyncRowState.Modified ? SyncRowState.ApplyModifiedFailed : SyncRowState.ApplyDeletedFailed;
                    errorAction = ErrorAction.Log;
                    break;

                // We have an error but at least we try one more time
                case ErrorResolution.RetryOneMoreTimeAndThrowOnError:
                case ErrorResolution.RetryOneMoreTimeAndContinueOnError:

                    bool operationComplete;

                    if (applyType == SyncRowState.Deleted)
                        (context, operationComplete, operationException) = await this.InternalApplyDeleteAsync(scopeInfo, context, errorRow, schemaChangesTable, lastTimestamp, senderScopeId, true,
                            connection, transaction).ConfigureAwait(false);
                    else
                        (context, operationComplete, operationException) = await this.InternalApplyUpdateAsync(scopeInfo, context, errorRow, schemaChangesTable, lastTimestamp, senderScopeId, true,
                            connection, transaction).ConfigureAwait(false);


                    if (operationComplete)
                    {
                        errorAction = ErrorAction.Resolved;
                        operationException = null;
                    }
                    else
                    {
                        // we have another error raised even if we tried again
                        // row is saved to batch error file
                        errorRow.RowState = applyType == SyncRowState.Modified ? SyncRowState.ApplyModifiedFailed : SyncRowState.ApplyDeletedFailed;

                        // Should we silentely log or throw ?
                        errorAction = errorOccuredArgs.Resolution == ErrorResolution.RetryOnNextSync ? ErrorAction.Log : ErrorAction.Throw;
                    }

                    break;

                // we mark the row to be tried again on next sync
                case ErrorResolution.RetryOnNextSync:
                    errorRow.RowState = applyType == SyncRowState.Modified ? SyncRowState.RetryModifiedOnNextSync : SyncRowState.RetryDeletedOnNextSync;
                    errorAction = ErrorAction.Log;
                    break;

                // row is marked as resolved
                case ErrorResolution.Resolved:
                    errorAction = ErrorAction.Resolved;
                    break;

                // default case : we throw the error
                case ErrorResolution.Throw:
                    errorRow.RowState = applyType == SyncRowState.Modified ? SyncRowState.ApplyModifiedFailed : SyncRowState.ApplyDeletedFailed;
                    errorAction = ErrorAction.Throw;
                    operationException = exception;
                    break;
            }

            return (errorAction, operationException);
        }


    }
}