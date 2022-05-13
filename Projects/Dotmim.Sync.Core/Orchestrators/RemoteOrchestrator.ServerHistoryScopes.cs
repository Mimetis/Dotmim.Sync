using Dotmim.Sync.Args;
using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Newtonsoft.Json;
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

        /// <summary>
        /// Get the server scope histories
        /// </summary>
        public virtual async Task<ServerHistoryScopeInfo> GetServerHistoryScopeInfoAsync(string scopeId, string scopeName = SyncOptions.DefaultScopeName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Reading, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var exists = await this.InternalExistsScopeInfoTableAsync(scopeName, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(scopeName, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // Get scope if exists
                var serverHistoryScope = await this.InternalLoadServerHistoryScopeAsync(scopeId, scopeName, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return serverHistoryScope;

            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeName, ex);
            }
        }

        /// <summary>
        /// Update or Insert a server scope row
        /// </summary>
        public virtual async Task<ServerHistoryScopeInfo> SaveServerHistoryScopeInfoAsync(ServerHistoryScopeInfo serverHistoryScopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(serverHistoryScopeInfo.Name, SyncMode.Writing, SyncStage.ScopeWriting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var exists = await this.InternalExistsScopeInfoTableAsync(serverHistoryScopeInfo.Name, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(serverHistoryScopeInfo.Name, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // Write scopes locally
                var scopeInfoUpdated = await this.InternalSaveServerHistoryScopeAsync(serverHistoryScopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return scopeInfoUpdated as ServerHistoryScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(serverHistoryScopeInfo.Name, ex);
            }
        }

        /// <summary>
        /// Internal exists server history scope
        /// </summary>
        internal async Task<bool> InternalExistsServerHistoryScopeInfoAsync(string scopeId, string scopeName, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // Get exists command
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var existsCommand = scopeBuilder.GetCommandAsync(DbScopeCommandType.ExistServerHistoryScopeInfo, connection, transaction);

            if (existsCommand == null) return false;

            var ctx = this.GetContext(scopeName);

            DbSyncAdapter.SetParameterValue(existsCommand, "sync_scope_name", scopeName);
            DbSyncAdapter.SetParameterValue(existsCommand, "sync_scope_Id", scopeId);

            if (existsCommand == null)
                return false;

            await this.InterceptAsync(new DbCommandArgs(ctx, existsCommand, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;
            return exists;
        }

        /// <summary>
        /// Internal load a server history scope by scope name
        /// </summary>
        internal async Task<ServerHistoryScopeInfo> InternalLoadServerHistoryScopeAsync(string scopeId, string scopeName, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetServerHistoryScopeInfo, connection, transaction);

            if (command == null) return null;

            var ctx = this.GetContext(scopeName);

            DbSyncAdapter.SetParameterValue(command, "sync_scope_id", scopeId);
            DbSyncAdapter.SetParameterValue(command, "sync_scope_name", scopeName);

            var action = new ScopeLoadingArgs(ctx, scopeName, DbScopeType.ServerHistory, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return null;

            await this.InterceptAsync(new DbCommandArgs(ctx, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            using DbDataReader reader = await action.Command.ExecuteReaderAsync().ConfigureAwait(false);

            ServerHistoryScopeInfo serverHistoryScopeInfo = null;

            if (reader.Read())
                serverHistoryScopeInfo = InternalReadServerHistoryScopeInfo(reader);

            reader.Close();

            if (serverHistoryScopeInfo.Schema != null)
                serverHistoryScopeInfo.Schema.EnsureSchema();

            var scopeLoadedArgs = new ScopeLoadedArgs(ctx, scopeName, DbScopeType.ServerHistory, serverHistoryScopeInfo, connection, transaction);
            await this.InterceptAsync(scopeLoadedArgs, progress, cancellationToken).ConfigureAwait(false);

            return serverHistoryScopeInfo;
        }

        /// <summary>
        /// Internal load all server histories scopes. scopeName arg is just here for getting context
        /// </summary>
        internal async Task<List<ServerHistoryScopeInfo>> InternalLoadAllServerHistoriesScopesAsync(string scopeName, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetAllServerHistoryScopesInfo, connection, transaction);

            if (command == null) return null;

            var ctx = this.GetContext(scopeName);

            var action = new ScopeLoadingArgs(ctx, scopeName, DbScopeType.ServerHistory, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return null;

            var serverHistoriesScopes = new List<ServerHistoryScopeInfo>();

            await this.InterceptAsync(new DbCommandArgs(ctx, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            using DbDataReader reader = await action.Command.ExecuteReaderAsync().ConfigureAwait(false);

            while (reader.Read())
            {
                var serverHistoryScopeInfo = InternalReadServerHistoryScopeInfo(reader);

                if (serverHistoryScopeInfo.Schema != null)
                    serverHistoryScopeInfo.Schema.EnsureSchema();

                serverHistoriesScopes.Add(serverHistoryScopeInfo);
            }

            reader.Close();

            foreach (var scopeInfo in serverHistoriesScopes)
            {
                var scopeLoadedArgs = new ScopeLoadedArgs(ctx, scopeName, DbScopeType.ServerHistory, scopeInfo, connection, transaction);
                await this.InterceptAsync(scopeLoadedArgs, progress, cancellationToken).ConfigureAwait(false);

            }

            return serverHistoriesScopes;
        }

        private ServerHistoryScopeInfo InternalReadServerHistoryScopeInfo(DbDataReader reader)
        {
            var serverScopeInfo = new ServerHistoryScopeInfo
            {
                Id = reader.GetGuid(reader.GetOrdinal("sync_scope_id")),
                Name = reader["sync_scope_name"] as string,
                LastSync = reader["scope_last_sync"] != DBNull.Value ? (DateTime?)reader.GetDateTime(reader.GetOrdinal("scope_last_sync")) : null,
                LastSyncDuration = reader["scope_last_sync_duration"] != DBNull.Value ? reader.GetInt64(reader.GetOrdinal("scope_last_sync_duration")) : 0L,
                LastSyncTimestamp = reader["scope_last_sync_timestamp"] != DBNull.Value ? reader.GetInt64(reader.GetOrdinal("scope_last_sync_timestamp")) : 0L
            };

            return serverScopeInfo;
        }

        /// <summary>
        /// Internal upsert server scope info in a server scope table
        /// </summary>
        internal async Task<ServerHistoryScopeInfo> InternalSaveServerHistoryScopeAsync(ServerHistoryScopeInfo serverHistoryScopeInfo, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var scopeExists = await InternalExistsServerHistoryScopeInfoAsync(serverHistoryScopeInfo.Id.ToString(), serverHistoryScopeInfo.Name, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            DbCommand command;
            if (scopeExists)
                command = scopeBuilder.GetCommandAsync(DbScopeCommandType.UpdateServerHistoryScopeInfo, connection, transaction);
            else
                command = scopeBuilder.GetCommandAsync(DbScopeCommandType.InsertServerHistoryScopeInfo, connection, transaction);

            if (command == null) return null;

            command = InternalSetSaveServerHistoryScopeParameters(serverHistoryScopeInfo, command);

            var ctx = this.GetContext(serverHistoryScopeInfo.Name);

            var action = new ScopeSavingArgs(ctx, scopeBuilder.ScopeInfoTableName.ToString(), DbScopeType.ServerHistory, serverHistoryScopeInfo, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return default;

            await this.InterceptAsync(new DbCommandArgs(ctx, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            using DbDataReader reader = await action.Command.ExecuteReaderAsync().ConfigureAwait(false);

            reader.Read();

            var newServerHistoryScopeInfo = InternalReadServerHistoryScopeInfo(reader);

            reader.Close();

            await this.InterceptAsync(new ScopeSavedArgs(ctx, scopeBuilder.ScopeInfoTableName.ToString(), DbScopeType.ServerHistory, newServerHistoryScopeInfo, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            return newServerHistoryScopeInfo;
        }

        private DbCommand InternalSetSaveServerHistoryScopeParameters(ServerHistoryScopeInfo serverHistoryScopeInfo, DbCommand command)
        {
            DbSyncAdapter.SetParameterValue(command, "sync_scope_id", serverHistoryScopeInfo.Id.ToString());
            DbSyncAdapter.SetParameterValue(command, "sync_scope_name", serverHistoryScopeInfo.Name);
            DbSyncAdapter.SetParameterValue(command, "scope_last_sync_timestamp", serverHistoryScopeInfo.LastSyncTimestamp);
            DbSyncAdapter.SetParameterValue(command, "scope_last_sync", serverHistoryScopeInfo.LastSync.HasValue ? (object)serverHistoryScopeInfo.LastSync.Value : DBNull.Value);
            DbSyncAdapter.SetParameterValue(command, "scope_last_sync_duration", serverHistoryScopeInfo.LastSyncDuration);

            return command;
        }

    }
}