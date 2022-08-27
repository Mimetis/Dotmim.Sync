using Dotmim.Sync.Args;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {

        /// <summary>
        /// Check if a scope info table exists
        /// </summary>
        public async Task<bool> ExistScopeInfoTableAsync(string scopeName = SyncOptions.DefaultScopeName,  DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                bool exists;
                (context, exists) = await InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                await runner.CommitAsync().ConfigureAwait(false);
                return exists;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        public async Task<bool> ExistScopeInfoClientTableAsync(string scopeName = SyncOptions.DefaultScopeName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                bool exists;
                (context, exists) = await InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                await runner.CommitAsync().ConfigureAwait(false);
                return exists;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Internal exists scope table routine
        /// </summary>
        internal async Task<(SyncContext context, bool exists)> InternalExistsScopeInfoTableAsync(SyncContext context, DbScopeType scopeType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var scopeCommandType = scopeType switch
            {
                DbScopeType.ScopeInfo => DbScopeCommandType.ExistsScopeInfoTable,
                DbScopeType.ScopeInfoClient => DbScopeCommandType.ExistsScopeInfoClientTable,
                _ => throw new NotImplementedException()
            };

            // Get exists command
            using var existsCommand = scopeBuilder.GetCommandAsync(scopeCommandType, connection, transaction);

            if (existsCommand == null)
                return (context, false);

            await this.InterceptAsync(new ExecuteCommandArgs(context, existsCommand, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;
            return (context, exists);
        }

        /// <summary>
        /// Internal drop scope info table routine
        /// </summary>
        internal async Task<(SyncContext context, bool dropped)> InternalDropScopeInfoTableAsync(SyncContext context, DbScopeType scopeType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var scopeCommandType = scopeType switch
            {
                DbScopeType.ScopeInfo => DbScopeCommandType.DropScopeInfoTable,
                DbScopeType.ScopeInfoClient => DbScopeCommandType.DropScopeInfoClientTable,
                _ => throw new NotImplementedException()
            };

            using var command = scopeBuilder.GetCommandAsync(scopeCommandType, connection, transaction);

            if (command == null) return (context, false);

            var action = new ScopeInfoTableDroppingArgs(context, scopeBuilder.ScopeInfoTableName.ToString(), scopeType, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return (context, false);

            await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

            await this.InterceptAsync(new ScopeInfoTableDroppedArgs(context, scopeBuilder.ScopeInfoTableName.ToString(), scopeType, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            action.Command.Dispose();

            return (context, true);
        }

        /// <summary>
        /// Internal create scope info table routine
        /// </summary>
        internal async Task<(SyncContext context, bool created)> InternalCreateScopeInfoTableAsync(SyncContext context, DbScopeType scopeType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var scopeCommandType = scopeType switch
            {
                DbScopeType.ScopeInfo => DbScopeCommandType.CreateScopeInfoTable,
                DbScopeType.ScopeInfoClient => DbScopeCommandType.CreateScopeInfoClientTable,
                _ => throw new NotImplementedException()
            };

            using var command = scopeBuilder.GetCommandAsync(scopeCommandType, connection, transaction);

            if (command == null)
                return (context, false);

            var action = new ScopeInfoTableCreatingArgs(context, scopeBuilder.ScopeInfoTableName.ToString(), scopeType, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return (context, false);

            await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

            await this.InterceptAsync(new ScopeInfoTableCreatedArgs(context, scopeBuilder.ScopeInfoTableName.ToString(), scopeType, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            action.Command.Dispose();

            return (context, true);
        }

    }
}
