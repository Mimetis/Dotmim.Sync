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
        public virtual async Task<ServerHistoryScopeInfo>
            GetServerHistoryScopeInfoAsync(string scopeId, string scopeName = SyncOptions.DefaultScopeName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.Reading, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // Get scope if exists
                ServerHistoryScopeInfo serverHistoryScope;
                (context, serverHistoryScope) = await this.InternalLoadServerHistoryScopeAsync(scopeId, context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return serverHistoryScope;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Update or Insert a server scope row
        /// </summary>
        public virtual async Task<ServerHistoryScopeInfo>
            SaveServerHistoryScopeInfoAsync(ServerHistoryScopeInfo serverHistoryScopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), serverHistoryScopeInfo.Name);
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.ScopeWriting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // Write scopes locally
                (context, serverHistoryScopeInfo) = await this.InternalSaveServerHistoryScopeAsync(serverHistoryScopeInfo, context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return serverHistoryScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }



        /// <summary>
        /// Get all scopes histories. scopeName arg is just here for logging purpose and is not used
        /// </summary>
        public virtual async Task<List<ServerHistoryScopeInfo>>
            GetAllServerScopesHistoriesInfosAsync(string scopeName = SyncOptions.DefaultScopeName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);

            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.Reading, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                List<ServerHistoryScopeInfo> localScopes;
                (context, localScopes) = await InternalLoadAllServerHistoriesScopesAsync(context,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return localScopes;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }



        /// <summary>
        /// Internal exists server history scope
        /// </summary>
        internal async Task<(SyncContext context, bool exists)> 
            InternalExistsServerHistoryScopeInfoAsync(string scopeId, string scopeName, SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // Get exists command
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            using var existsCommand = scopeBuilder.GetCommandAsync(DbScopeCommandType.ExistServerHistoryScopeInfo, connection, transaction);

            if (existsCommand == null) return (context, false);

            DbSyncAdapter.SetParameterValue(existsCommand, "sync_scope_name", scopeName);
            DbSyncAdapter.SetParameterValue(existsCommand, "sync_scope_Id", scopeId);

            if (existsCommand == null)
                return (context, false);

            await this.InterceptAsync(new DbCommandArgs(context, existsCommand, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            existsCommand.CommandTimeout = Options.SqlCommandTimeout;

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;
            return (context, exists);
        }

        /// <summary>
        /// Internal load a server history scope by scope name
        /// </summary>
        internal async Task<(SyncContext context, ServerHistoryScopeInfo serverHistoryScopeInfo)> 
            InternalLoadServerHistoryScopeAsync(string scopeId, SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            using var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetServerHistoryScopeInfo, connection, transaction);

            if (command == null) return (context, null);

            DbSyncAdapter.SetParameterValue(command, "sync_scope_id", scopeId);
            DbSyncAdapter.SetParameterValue(command, "sync_scope_name", context.ScopeName);

            await this.InterceptAsync(new DbCommandArgs(context, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            command.CommandTimeout = Options.SqlCommandTimeout;

            using DbDataReader reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

            ServerHistoryScopeInfo serverHistoryScopeInfo = null;

            if (reader.Read())
                serverHistoryScopeInfo = InternalReadServerHistoryScopeInfo(reader);

            reader.Close();

            if (serverHistoryScopeInfo.Schema != null)
                serverHistoryScopeInfo.Schema.EnsureSchema();
            
            command.Dispose();

            return (context, serverHistoryScopeInfo);
        }

        /// <summary>
        /// Internal load all server histories scopes. scopeName arg is just here for getting context
        /// </summary>
        internal async Task<(SyncContext context, List<ServerHistoryScopeInfo> serverHistoryScopeInfos)> 
            InternalLoadAllServerHistoriesScopesAsync(SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            using var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetAllServerHistoryScopesInfo, connection, transaction);

            if (command == null) return (context, null);

            var serverHistoriesScopes = new List<ServerHistoryScopeInfo>();

            await this.InterceptAsync(new DbCommandArgs(context, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            command.CommandTimeout = Options.SqlCommandTimeout;

            using DbDataReader reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

            while (reader.Read())
            {
                var serverHistoryScopeInfo = InternalReadServerHistoryScopeInfo(reader);

                if (serverHistoryScopeInfo.Schema != null)
                    serverHistoryScopeInfo.Schema.EnsureSchema();

                serverHistoriesScopes.Add(serverHistoryScopeInfo);
            }

            reader.Close();

            command.Dispose();

            return (context, serverHistoriesScopes);
        }

        private ServerHistoryScopeInfo InternalReadServerHistoryScopeInfo(DbDataReader reader)
        {
            var serverScopeInfo = new ServerHistoryScopeInfo
            {
                Id = reader.GetGuid(reader.GetOrdinal("sync_scope_id")),
                Name = reader["sync_scope_name"] as string,
                LastSync = reader["scope_last_sync"] != DBNull.Value ? reader.GetDateTime(reader.GetOrdinal("scope_last_sync")) : null,
                LastSyncDuration = reader["scope_last_sync_duration"] != DBNull.Value ? reader.GetInt64(reader.GetOrdinal("scope_last_sync_duration")) : 0L,
                LastSyncTimestamp = reader["scope_last_sync_timestamp"] != DBNull.Value ? reader.GetInt64(reader.GetOrdinal("scope_last_sync_timestamp")) : 0L,
                Properties = reader["scope_properties"] != DBNull.Value ? reader.GetString(reader.GetOrdinal("scope_properties")) : null
            };

            return serverScopeInfo;
        }

        /// <summary>
        /// Internal upsert server scope info in a server scope table
        /// </summary>
        internal async Task<(SyncContext context, ServerHistoryScopeInfo serverHistoryScopeInfo)> 
            InternalSaveServerHistoryScopeAsync(ServerHistoryScopeInfo serverHistoryScopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            bool scopeExists;
            (context, scopeExists) = await InternalExistsServerHistoryScopeInfoAsync(serverHistoryScopeInfo.Id.ToString(), serverHistoryScopeInfo.Name, context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            DbCommand command;
            if (scopeExists)
                command = scopeBuilder.GetCommandAsync(DbScopeCommandType.UpdateServerHistoryScopeInfo, connection, transaction);
            else
                command = scopeBuilder.GetCommandAsync(DbScopeCommandType.InsertServerHistoryScopeInfo, connection, transaction);

            if (command == null) return (context, null);

            InternalSetSaveServerHistoryScopeParameters(serverHistoryScopeInfo, command);

            var action = new ScopeSavingArgs(context, scopeBuilder.ScopeInfoTableName.ToString(), DbScopeType.ServerHistory, serverHistoryScopeInfo, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return default;

            await this.InterceptAsync(new DbCommandArgs(context, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            command.CommandTimeout = Options.SqlCommandTimeout;

            using DbDataReader reader = await action.Command.ExecuteReaderAsync().ConfigureAwait(false);

            reader.Read();

            var newServerHistoryScopeInfo = InternalReadServerHistoryScopeInfo(reader);

            reader.Close();

            await this.InterceptAsync(new ScopeSavedArgs(context, scopeBuilder.ScopeInfoTableName.ToString(), DbScopeType.ServerHistory, newServerHistoryScopeInfo, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            action.Command.Dispose();

            return (context, newServerHistoryScopeInfo);
        }

        private DbCommand InternalSetSaveServerHistoryScopeParameters(ServerHistoryScopeInfo serverHistoryScopeInfo, DbCommand command)
        {
            DbSyncAdapter.SetParameterValue(command, "sync_scope_id", serverHistoryScopeInfo.Id.ToString());
            DbSyncAdapter.SetParameterValue(command, "sync_scope_name", serverHistoryScopeInfo.Name);
            DbSyncAdapter.SetParameterValue(command, "scope_last_sync_timestamp", serverHistoryScopeInfo.LastSyncTimestamp);
            DbSyncAdapter.SetParameterValue(command, "scope_last_sync", serverHistoryScopeInfo.LastSync.HasValue ? (object)serverHistoryScopeInfo.LastSync.Value : DBNull.Value);
            DbSyncAdapter.SetParameterValue(command, "scope_last_sync_duration", serverHistoryScopeInfo.LastSyncDuration);
            DbSyncAdapter.SetParameterValue(command, "scope_properties", serverHistoryScopeInfo.Properties);

            return command;
        }

    }
}