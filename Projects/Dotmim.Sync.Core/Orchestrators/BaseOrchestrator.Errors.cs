
using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
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
        private async Task<(ErrorAction errorAction, ApplyChangesException applyChangesException)> HandleErrorAsync(ScopeInfo scopeInfo, SyncContext context, 
                                BatchInfo batchInfo, SyncRow errorRow, SyncRowState applyType,
                                SyncTable schemaChangesTable, Exception exception, Guid senderScopeId, long? lastTimestamp,
                                DbConnection connection, DbTransaction transaction,
                                CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // We are handling a previous error already in the batch info
            if (errorRow.RowState == SyncRowState.ApplyModifiedFailed || errorRow.RowState == SyncRowState.ApplyDeletedFailed)
                return (ErrorAction.Ignore, null);

            var errorRowArgs = new ApplyChangesErrorOccuredArgs(context, errorRow, schemaChangesTable, applyType, exception, this.Options.ErrorResolutionPolicy,
                connection, transaction);

            var errorOccuredArgs = await this.InterceptAsync(errorRowArgs, progress, cancellationToken).ConfigureAwait(false);
            Exception operationException = null;
            
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
                        (_, operationComplete, operationException) = await this.InternalApplyDeleteAsync(scopeInfo, context, batchInfo, errorRow, schemaChangesTable, lastTimestamp, senderScopeId, true,
                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    else
                        (_, operationComplete, operationException) = await this.InternalApplyUpdateAsync(scopeInfo, context, batchInfo, errorRow, schemaChangesTable, lastTimestamp, senderScopeId, true,
                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);


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

                        if (errorOccuredArgs.Resolution == ErrorResolution.RetryOneMoreTimeAndContinueOnError)
                        {
                            errorAction = ErrorAction.Log;
                            operationException = null;
                        }
                        else
                        {
                            errorAction = ErrorAction.Throw;
                        }
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

            ApplyChangesException applyChangesException = null;
            if (operationException != null)
                applyChangesException = new ApplyChangesException(errorRow, schemaChangesTable, applyType, operationException);   

            return (errorAction, applyChangesException);
        }
    }
}