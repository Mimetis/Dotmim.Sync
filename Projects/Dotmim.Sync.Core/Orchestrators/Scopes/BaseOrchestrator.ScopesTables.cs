using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains the logic to handle scopes tables.
    /// </summary>
    public abstract partial class BaseOrchestrator
    {
        /// <summary>
        /// Create a scope_info_client table in the local data source, if not exists.
        /// <example>
        /// <code>
        ///  await localOrchestrator.CreateScopeInfoClientTableAsync();
        /// </code>
        /// </example>
        /// </summary>
        public async Task<bool> CreateScopeInfoClientTableAsync(DbConnection connection = null, DbTransaction transaction = null)
        {

            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);
            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.None, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    bool exists;
                    (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (!exists)
                    {
                        (context, _) = await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                    }

                    await runner.CommitAsync().ConfigureAwait(false);
                    return exists;
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Create a scope_info table in the local data source, if not exists.
        /// <example>
        /// <code>
        ///  await localOrchestrator.CreateScopeInfoTableAsync();
        /// </code>
        /// </example>
        /// </summary>
        public async Task<bool> CreateScopeInfoTableAsync(DbConnection connection = null, DbTransaction transaction = null)
        {

            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);
            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.None, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    bool exists;
                    (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (!exists)
                    {
                        (context, _) = await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                    }

                    await runner.CommitAsync().ConfigureAwait(false);
                    return exists;
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Check if a scope_info table exists in the local data source.
        /// <example>
        /// <code>
        ///  var exists = await localOrchestrator.ExistScopeInfoTableAsync();
        /// </code>
        /// </example>
        /// </summary>
        public async Task<bool> ExistScopeInfoTableAsync(DbConnection connection = null, DbTransaction transaction = null)
        {

            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);
            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    bool exists;
                    (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    await runner.CommitAsync().ConfigureAwait(false);
                    return exists;
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Check if a scope_info_client table exists in the local data source.
        /// <example>
        /// <code>
        ///  var exists = await localOrchestrator.ExistScopeInfoClientTableAsync();
        /// </code>
        /// </example>
        /// </summary>
        public async Task<bool> ExistScopeInfoClientTableAsync(DbConnection connection = null, DbTransaction transaction = null)
        {

            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);
            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    bool exists;
                    (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    return exists;
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Internal exists scope table routine.
        /// </summary>
        internal virtual async Task<(SyncContext Context, bool IsExisting)> InternalExistsScopeInfoTableAsync(SyncContext context, DbScopeType scopeType,
            DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                var scopeCommandType = scopeType switch
                {
                    DbScopeType.ScopeInfo => DbScopeCommandType.ExistsScopeInfoTable,
                    DbScopeType.ScopeInfoClient => DbScopeCommandType.ExistsScopeInfoClientTable,
                    _ => throw new NotImplementedException(),
                };

                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None, connection, transaction, cancellationToken: cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    // Get exists command
                    using var existsCommand = scopeBuilder.GetCommandAsync(scopeCommandType, runner.Connection, runner.Transaction);

                    if (existsCommand == null)
                        return (context, false);

                    await this.InterceptAsync(new ExecuteCommandArgs(context, existsCommand, default, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    var existsResultObject = await existsCommand.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                    var exists = SyncTypeConverter.TryConvertTo<int>(existsResultObject) > 0;
                    return (context, exists);
                }
            }
            catch (Exception ex)
            {
                string message = null;
                message += $"ScopeType:{scopeType}.";
                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Internal drop scope info table routine.
        /// </summary>
        internal virtual async Task<(SyncContext Context, bool IsDropped)> InternalDropScopeInfoTableAsync(SyncContext context, DbScopeType scopeType,
            DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {

                var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                var scopeCommandType = scopeType switch
                {
                    DbScopeType.ScopeInfo => DbScopeCommandType.DropScopeInfoTable,
                    DbScopeType.ScopeInfoClient => DbScopeCommandType.DropScopeInfoClientTable,
                    _ => throw new NotImplementedException(),
                };

                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None, connection, transaction, cancellationToken: cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    using var command = scopeBuilder.GetCommandAsync(scopeCommandType, runner.Connection, runner.Transaction);

                    if (command == null)
                        return (context, false);

                    var action = new ScopeInfoTableDroppingArgs(context, scopeBuilder.ScopeInfoTableName.ToString(), scopeType, command, runner.Connection, runner.Transaction);
                    await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

                    if (action.Cancel || action.Command == null)
                        return (context, false);

                    await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, runner.Connection, runner.Transaction), progress, cancellationToken).ConfigureAwait(false);

                    await action.Command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

                    await this.InterceptAsync(new ScopeInfoTableDroppedArgs(context, scopeBuilder.ScopeInfoTableName.ToString(), scopeType, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    action.Command.Dispose();

                    return (context, true);
                }
            }
            catch (Exception ex)
            {
                string message = null;
                message += $"ScopeType:{scopeType}.";
                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Internal create scope info table routine.
        /// </summary>
        internal virtual async Task<(SyncContext Context, bool IsCreated)> InternalCreateScopeInfoTableAsync(SyncContext context, DbScopeType scopeType,
            DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                var scopeCommandType = scopeType switch
                {
                    DbScopeType.ScopeInfo => DbScopeCommandType.CreateScopeInfoTable,
                    DbScopeType.ScopeInfoClient => DbScopeCommandType.CreateScopeInfoClientTable,
                    _ => throw new NotImplementedException(),
                };

                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    using var command = scopeBuilder.GetCommandAsync(scopeCommandType, runner.Connection, runner.Transaction);

                    if (command == null)
                        return (context, false);

                    var action = new ScopeInfoTableCreatingArgs(context, scopeBuilder.ScopeInfoTableName.ToString(), scopeType, command, runner.Connection, runner.Transaction);
                    await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

                    if (action.Cancel || action.Command == null)
                        return (context, false);

                    await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    await action.Command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

                    await this.InterceptAsync(new ScopeInfoTableCreatedArgs(context, scopeBuilder.ScopeInfoTableName.ToString(), scopeType, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    action.Command.Dispose();

                    return (context, true);
                }
            }
            catch (Exception ex)
            {
                string message = null;
                message += $"ScopeType:{scopeType}.";
                throw this.GetSyncError(context, ex, message);
            }
        }
    }
}