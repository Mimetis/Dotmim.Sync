using Dotmim.Sync.Args;
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
        public async virtual Task<long> GetLocalTimestampAsync(string scopeName = SyncOptions.DefaultScopeName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.Reading, SyncStage.None, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                long timestamp;
                (context, timestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress);

                return timestamp;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Read a scope info
        /// </summary>
        internal virtual async Task<(SyncContext context, long timestamp)> InternalGetLocalTimestampAsync(SyncContext context,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            // we don't care about DbScopeType. That's why we are using a random value DbScopeType.Client...
            using var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetLocalTimestamp, connection, transaction);

            if (command == null)
                return (context, 0L);

            var action = await this.InterceptAsync(new LocalTimestampLoadingArgs(context, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return (context, 0L);

            await this.InterceptAsync(new DbCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            long result = Convert.ToInt64(await action.Command.ExecuteScalarAsync().ConfigureAwait(false));

            var loadedArgs = await this.InterceptAsync(new LocalTimestampLoadedArgs(context, result, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            action.Command.Dispose();

            return (context, loadedArgs.LocalTimestamp);
        }

    }
}
