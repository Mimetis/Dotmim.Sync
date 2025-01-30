using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains the logic to get the current operation type.
    /// </summary>
    public partial class RemoteOrchestrator : BaseOrchestrator
    {

        /// <summary>
        /// Gets the current operation type.
        /// The interceptor is called to allow the user to change the operation type.
        /// </summary>
        internal virtual async Task<(SyncContext Context, SyncOperation Operation)>
          InternalGetOperationAsync(ScopeInfo serverScopeInfo, ScopeInfo clientScopeInfo, ScopeInfoClient scopeInfoClient, SyncContext context,
            DbConnection connection = default, DbTransaction transaction = default,
            IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            try
            {
                var syncOperation = SyncOperation.Normal;

                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.Provisioning, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    var operationArgs = new OperationArgs(context, serverScopeInfo, clientScopeInfo, scopeInfoClient, runner.Connection, runner.Transaction);
                    await this.InterceptAsync(operationArgs, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    syncOperation = operationArgs.Operation;

                    return (context, syncOperation);
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }
    }
}