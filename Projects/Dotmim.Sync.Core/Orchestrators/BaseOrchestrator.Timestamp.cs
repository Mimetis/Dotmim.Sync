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
            try
            {
                await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Reading, SyncStage.None, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                return await this.InternalGetLocalTimestampAsync(scopeName, runner.Connection, runner.Transaction, cancellationToken, progress);
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeName, ex);
            }
        }

        /// <summary>
        /// Read a scope info
        /// </summary>
        internal async Task<long> InternalGetLocalTimestampAsync(string scopeName,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            // we don't care about DbScopeType. That's why we are using a random value DbScopeType.Client...
            var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetLocalTimestamp, connection, transaction);

            if (command == null)
                return 0L;

            var context = this.GetContext(scopeName);

            var action = await this.InterceptAsync(new LocalTimestampLoadingArgs(context, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return 0L;

            await this.InterceptAsync(new DbCommandArgs(context, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            long result = Convert.ToInt64(await action.Command.ExecuteScalarAsync().ConfigureAwait(false));

            var loadedArgs = await this.InterceptAsync(new LocalTimestampLoadedArgs(context, result, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            return loadedArgs.LocalTimestamp;
        }

    }
}
