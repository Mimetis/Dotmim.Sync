using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains the logic to get the last timestamp from the orchestrator database.
    /// </summary>
    public abstract partial class BaseOrchestrator
    {
        /// <summary>
        /// Get the last timestamp from the orchestrator database.
        /// <example>
        /// Example:
        /// <code>
        ///  var remoteOrchestrator = new RemoteOrchestrator(serverProvider);
        ///  var ts = await remoteOrchestrator.GetLocalTimestampAsync()
        /// </code>
        /// </example>
        /// </summary>
        public async virtual Task<long> GetLocalTimestampAsync(string scopeName, DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    long timestamp;
                    (context, timestamp) = await this.InternalGetLocalTimestampAsync(
                        context,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    return timestamp;
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }

        /// <inheritdoc cref="GetLocalTimestampAsync(string, DbConnection, DbTransaction)"/>
        public virtual Task<long> GetLocalTimestampAsync(DbConnection connection = null, DbTransaction transaction = null)
            => this.GetLocalTimestampAsync(SyncOptions.DefaultScopeName, connection, transaction);

        /// <summary>
        /// Read a scope info.
        /// </summary>
        internal virtual async Task<(SyncContext Context, long Timestamp)> InternalGetLocalTimestampAsync(
            SyncContext context,
            DbConnection connection, DbTransaction transaction,
            IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            try
            {
                var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None, connection, transaction, cancellationToken: cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    // we don't care about DbScopeType. That's why we are using a random value DbScopeType.Client...
                    using var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetLocalTimestamp, runner.Connection, runner.Transaction);

                    if (command == null)
                        return (context, 0L);

                    var action = await this.InterceptAsync(new LocalTimestampLoadingArgs(context, command, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (action.Cancel || action.Command == null)
                        return (context, 0L);

                    await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    var result = SyncTypeConverter.TryConvertTo<long>(await action.Command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));

                    var loadedArgs = await this.InterceptAsync(new LocalTimestampLoadedArgs(context, result, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    action.Command.Dispose();

                    return (context, loadedArgs.LocalTimestamp);
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }
    }
}