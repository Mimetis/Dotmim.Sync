using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


namespace Dotmim.Sync
{
    public partial class RemoteOrchestrator : BaseOrchestrator
    {



        internal virtual async Task<(SyncContext context, SyncOperation operation)>
          InternalGetOperationAsync(ScopeInfo serverScopeInfo, ScopeInfo clientScopeInfo, ScopeInfoClient scopeInfoClient, SyncContext context, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                SyncOperation syncOperation = SyncOperation.Normal;

                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
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
                throw GetSyncError(context, ex);
            }

        }

   

    }
}
