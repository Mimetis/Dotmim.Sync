using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {


        /// <summary>
        /// Get the last timestamp from the orchestrator database
        /// </summary>
        public virtual Task<long> GetLocalTimestampAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.None, async (ctx, connection, transaction) =>
        {
            var timestamp = await this.InternalGetLocalTimestampAsync(ctx, connection, transaction, cancellationToken, progress);

            return timestamp;

        }, connection, transaction, cancellationToken);

        /// <summary>
        /// Read a scope info
        /// </summary>
        internal async Task<long> InternalGetLocalTimestampAsync(SyncContext context,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            // we don't care about DbScopeType. That's why we are using a random value DbScopeType.Client...
            var command = scopeBuilder.PrepareCommand(DbScopeCommandType.GetLocalTimestamp, DbScopeType.Client, connection, transaction);

            if (command == null)
                return 0L;

            var action = new LocalTimestampLoadingArgs(context, command, connection, transaction);

            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return 0L;

            long result = Convert.ToInt64(await action.Command.ExecuteScalarAsync().ConfigureAwait(false));

            var loadedArgs = new LocalTimestampLoadedArgs(context, result, connection, transaction);
            await this.InterceptAsync(loadedArgs, cancellationToken).ConfigureAwait(false);

            return loadedArgs.LocalTimestamp;
        }

    }
}
