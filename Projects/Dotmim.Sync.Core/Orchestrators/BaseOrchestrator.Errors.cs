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
        private async Task<(SyncContext context, TableConflictErrorApplied tableConflictError)>
            HandleErrorAsync(IScopeInfo scopeInfo, SyncContext context, SyncRow errorRow, DataRowState applyType,
                                SyncTable schemaChangesTable, Exception exception,
                                Guid senderScopeId, long? lastTimestamp,
                                DbConnection connection, DbTransaction transaction,
                                CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var errorRowArgs = new ApplyChangesErrorOccuredArgs(context, errorRow, schemaChangesTable, applyType, exception, connection, transaction);
            var errorOccuredArgs = await this.InterceptAsync(errorRowArgs, progress, cancellationToken).ConfigureAwait(false);

            TableConflictErrorApplied tableConflictError = new TableConflictErrorApplied();

            switch (errorOccuredArgs.Resolution)
            {
                case ErrorResolution.Throw:
                    tableConflictError.Exception = exception;
                    tableConflictError.HasBeenApplied = false;
                    tableConflictError.HasBeenResolved = false;
                    break;
                case ErrorResolution.RetryOneMoreTime:
                    {
                        Exception operationException;
                        bool operationComplete;

                        if (applyType == DataRowState.Deleted)
                            (context, operationComplete, operationException) = await this.InternalApplyDeleteAsync(
                                scopeInfo, context, errorRow, schemaChangesTable, lastTimestamp, senderScopeId, true, connection, transaction);
                        else
                            (context, operationComplete, operationException) = await this.InternalApplyUpdateAsync(
                                scopeInfo, context, errorRow, schemaChangesTable, lastTimestamp, senderScopeId, true, connection, transaction).ConfigureAwait(false);

                        tableConflictError.Exception = operationException;
                        tableConflictError.HasBeenApplied = operationComplete;
                        tableConflictError.HasBeenResolved = operationException == null;
                        break;
                    }

                case ErrorResolution.ContinueOnError:
                    tableConflictError.Exception = null;
                    tableConflictError.HasBeenApplied = false;
                    tableConflictError.HasBeenResolved = true;
                    break;
                case ErrorResolution.Resolved:
                default:
                    tableConflictError.Exception = null;
                    tableConflictError.HasBeenApplied = true;
                    tableConflictError.HasBeenResolved = true;
                    break;
            }

            return (context, tableConflictError);
        }
    }
}