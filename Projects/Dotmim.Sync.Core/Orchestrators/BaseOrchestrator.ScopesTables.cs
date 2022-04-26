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
        /// Ensure the scope (Client, Server or ServerHistpory) is created
        /// The scope contains all about last sync, schema and scope and local / remote timestamp 
        /// </summary>
        public virtual async Task<bool> CreateScopeInfoTableAsync(string scopeName, DbScopeType scopeType, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Writing, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                bool hasBeenCreated = false;

                var exists = await InternalExistsScopeInfoTableAsync(scopeName, scopeType, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // should create only if not exists OR if overwrite has been set
                var shouldCreate = !exists || overwrite;
                if (shouldCreate)
                {
                    // Drop trigger if already exists
                    if (exists && overwrite)
                        await InternalDropScopeInfoTableAsync(scopeName, scopeType, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    hasBeenCreated = await InternalCreateScopeInfoTableAsync(scopeName, scopeType, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                await runner.CommitAsync().ConfigureAwait(false);

                return hasBeenCreated;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeName, ex);
            }

        }


        /// <summary>
        /// Check if a scope info table exists
        /// </summary>
        public async Task<bool> ExistScopeInfoTableAsync(string scopeName = SyncOptions.DefaultScopeName, DbScopeType scopeType = DbScopeType.Client, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Reading, SyncStage.None, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var exists = await InternalExistsScopeInfoTableAsync(scopeName, scopeType, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                await runner.CommitAsync().ConfigureAwait(false);
                return exists;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeName, ex);
            }
        }


        /// <summary>
        /// Internal exists scope table routine
        /// </summary>
        internal async Task<bool> InternalExistsScopeInfoTableAsync(string scopeName, DbScopeType scopeType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            // Get exists command
            var existsCommand = scopeBuilder.GetCommandAsync(DbScopeCommandType.ExistsScopeTable, scopeType, connection, transaction);

            if (existsCommand == null)
                return false;

            var ctx = this.GetContext(scopeName);

            await this.InterceptAsync(new DbCommandArgs(ctx, existsCommand, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;
            return exists;
        }

        /// <summary>
        /// Internal drop scope info table routine
        /// </summary>
        internal async Task<bool> InternalDropScopeInfoTableAsync(string scopeName, DbScopeType scopeType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.DropScopeTable, scopeType, connection, transaction);

            if (command == null) return false;

            var ctx = this.GetContext(scopeName);

            var action = new ScopeTableDroppingArgs(ctx, scopeBuilder.ScopeInfoTableName.ToString(), scopeType, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await this.InterceptAsync(new DbCommandArgs(ctx, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

            await this.InterceptAsync(new ScopeTableDroppedArgs(ctx, scopeBuilder.ScopeInfoTableName.ToString(), scopeType, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal create scope info table routine
        /// </summary>
        internal async Task<bool> InternalCreateScopeInfoTableAsync(string scopeName, DbScopeType scopeType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.CreateScopeTable, scopeType, connection, transaction);

            if (command == null)
                return false;

            var ctx = this.GetContext(scopeName);

            var action = new ScopeTableCreatingArgs(ctx, scopeBuilder.ScopeInfoTableName.ToString(), scopeType, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await this.InterceptAsync(new DbCommandArgs(ctx, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

            await this.InterceptAsync(new ScopeTableCreatedArgs(ctx, scopeBuilder.ScopeInfoTableName.ToString(), scopeType, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            return true;
        }

    }
}
