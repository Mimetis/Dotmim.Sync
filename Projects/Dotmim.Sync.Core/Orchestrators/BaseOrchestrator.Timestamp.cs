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
        public async virtual Task<long> GetLocalTimestampAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            await using var runner = await this.GetConnectionAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
            var ctx = this.GetContext();
            ctx.SyncStage = SyncStage.None;
            try
            {
                return await this.InternalGetLocalTimestampAsync(ctx, runner.Connection, runner.Transaction, cancellationToken, progress);
            }
            catch (Exception ex)
            {
                throw GetSyncError(ex);
            }
        }

        /// <summary>
        /// Read a scope info
        /// </summary>
        internal async Task<long> InternalGetLocalTimestampAsync(SyncContext context,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            // we don't care about DbScopeType. That's why we are using a random value DbScopeType.Client...
            var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetLocalTimestamp, DbScopeType.Client, connection, transaction);

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
