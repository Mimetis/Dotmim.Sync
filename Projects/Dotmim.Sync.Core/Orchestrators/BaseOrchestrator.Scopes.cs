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
        /// Internal exists scope
        /// </summary>
        internal async Task<bool> InternalExistsScopeInfoAsync(string scopeId, string scopeName, DbScopeType scopeType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // Get exists command
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var existsCommand = scopeBuilder.GetCommandAsync(DbScopeCommandType.ExistScope, scopeType, connection, transaction);

            if (existsCommand == null) return false;

            var ctx = this.GetContext(scopeName);

            // Just in case, in older version we may have sync_scope_name as primary key;
            DbSyncAdapter.SetParameterValue(existsCommand, "sync_scope_name", scopeId);
            // Set primary key value
            DbSyncAdapter.SetParameterValue(existsCommand, "sync_scope_id", scopeId);

            if (existsCommand == null)
                return false;

            await this.InterceptAsync(new DbCommandArgs(ctx, existsCommand, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;
            return exists;
        }


        /// <summary>
        /// Internal load all scopes with the same name
        /// </summary>
        internal async Task<List<IScopeInfo>> InternalGetAllScopesAsync(string scopeName, DbScopeType scopeType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetScopes, scopeType, connection, transaction);

            if (command == null) return null;

            var ctx = this.GetContext(scopeName);

            DbSyncAdapter.SetParameterValue(command, "sync_scope_name", scopeName);

            var action = new ScopeLoadingArgs(ctx, scopeName, scopeType, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return null;

            var scopes = new List<IScopeInfo>();

            await this.InterceptAsync(new DbCommandArgs(ctx, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            using DbDataReader reader = await action.Command.ExecuteReaderAsync().ConfigureAwait(false);

            while (reader.Read())
            {
                IScopeInfo scopeInfo = scopeType switch
                {
                    DbScopeType.Server => ReaderServerScopeInfo(reader),
                    DbScopeType.ServerHistory => ReadServerHistoryScopeInfo(reader),
                    DbScopeType.Client => ReadScopeInfo(reader),
                    _ => throw new NotImplementedException($"Can't get {scopeType} from the reader ")
                };

                if (scopeInfo != null)
                    scopes.Add(scopeInfo);
            }

            reader.Close();

            return scopes;
        }

        /// <summary>
        /// Internal load scope routine
        /// </summary>
        internal async Task<IScopeInfo> InternalGetScopeAsync(string scopeName, DbScopeType scopeType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var ctx = this.GetContext(scopeName);

            var scopes = await InternalGetAllScopesAsync(scopeName, scopeType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (scopes == null || scopes.Count <= 0)
                return default;

            foreach (var scopeInfo in scopes)
            {
                if (scopeInfo.GetType() == typeof(ScopeInfo))
                {
                    var localScopeInfo = scopeInfo as ScopeInfo;
                    localScopeInfo.IsNewScope = localScopeInfo.LastSync == null;
                }

                if (scopeInfo?.Schema != null)
                    scopeInfo.Schema.EnsureSchema();

                var scopeLoadedArgs = new ScopeLoadedArgs<IScopeInfo>(ctx, scopeName, scopeType, scopeInfo, connection, transaction);
                await this.InterceptAsync(scopeLoadedArgs, progress, cancellationToken).ConfigureAwait(false);

            }
            // get first scope
            return scopes.FirstOrDefault();
        }

        /// <summary>
        /// Get scope info
        /// </summary>
        internal Task<IScopeInfo> InternalGetScopeAsync(string scopeName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null) 
            => this.InternalGetScopeAsync(scopeName, this.Side == SyncSide.ClientSide ? DbScopeType.Client : DbScopeType.Server, connection, transaction, cancellationToken, progress);

        internal IScopeInfo InternalCreateScope(string scopeName, DbScopeType scopeType, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // create a new scope id for the current owner (could be server or client as well)
            IScopeInfo scope = scopeType switch
            {
                DbScopeType.Client => new ScopeInfo { Id = Guid.NewGuid(), Name = scopeName, IsNewScope = true, LastSync = null, Version = SyncVersion.Current.ToString() },
                DbScopeType.Server => new ServerScopeInfo { Name = scopeName, LastCleanupTimestamp = 0, Version = SyncVersion.Current.ToString() },
                _ => throw new NotImplementedException($"Type of scope {scopeName} is not implemented when trying to get a single instance")
            };

            return scope;
        }

        /// <summary>
        /// Internal upsert scope info in a scope table
        /// </summary>
        internal async Task<IScopeInfo> InternalSaveScopeAsync(IScopeInfo scopeInfo, DbScopeType scopeType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var scopeId = scopeType switch
            {
                DbScopeType.Client => (scopeInfo as ScopeInfo).Id.ToString(),
                DbScopeType.Server => (scopeInfo as ServerScopeInfo).Name,
                DbScopeType.ServerHistory => (scopeInfo as ServerHistoryScopeInfo).Id.ToString(),
                _ => throw new NotImplementedException($"Can't set parameters to scope command type {scopeType}.")
            };

            var scopeExists = await InternalExistsScopeInfoAsync(scopeId, scopeInfo.Name, scopeType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            DbCommand command;

            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);


            if (scopeExists)
                command = scopeBuilder.GetCommandAsync(DbScopeCommandType.UpdateScope, scopeType, connection, transaction);
            else
                command = scopeBuilder.GetCommandAsync(DbScopeCommandType.InsertScope, scopeType, connection, transaction);

            if (command == null) return null;

            command = scopeType switch
            {
                DbScopeType.Client => SetSaveScopeParameters(scopeInfo as ScopeInfo, command),
                DbScopeType.Server => SetSaveScopeParameters(scopeInfo as ServerScopeInfo, command),
                DbScopeType.ServerHistory => SetSaveScopeParameters(scopeInfo as ServerHistoryScopeInfo, command),
                _ => throw new NotImplementedException($"Can't set parameters to scope command type {scopeType}.")
            };

            var ctx = this.GetContext(scopeInfo.Name);

            var action = new ScopeSavingArgs(ctx, scopeBuilder.ScopeInfoTableName.ToString(), scopeType, scopeInfo, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return default;

            await this.InterceptAsync(new DbCommandArgs(ctx, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            using DbDataReader reader = await action.Command.ExecuteReaderAsync().ConfigureAwait(false);

            reader.Read();

            IScopeInfo newScopeInfo = scopeType switch
            {
                DbScopeType.Client => ReadScopeInfo(reader),
                DbScopeType.Server => ReaderServerScopeInfo(reader),
                DbScopeType.ServerHistory => ReadServerHistoryScopeInfo(reader),
                _ => throw new NotImplementedException($"Can't get {scopeType} from the reader ")
            };

            reader.Close();

            await this.InterceptAsync(new ScopeSavedArgs(ctx, scopeBuilder.ScopeInfoTableName.ToString(), scopeType, newScopeInfo, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            return newScopeInfo;
        }

        private DbCommand SetSaveScopeParameters(ScopeInfo scopeInfo, DbCommand command)
        {
            DbSyncAdapter.SetParameterValue(command, "sync_scope_name", scopeInfo.Name);
            DbSyncAdapter.SetParameterValue(command, "sync_scope_schema", scopeInfo.Schema == null ? DBNull.Value : (object)JsonConvert.SerializeObject(scopeInfo.Schema));
            DbSyncAdapter.SetParameterValue(command, "sync_scope_setup", scopeInfo.Setup == null ? DBNull.Value : (object)JsonConvert.SerializeObject(scopeInfo.Setup));
            DbSyncAdapter.SetParameterValue(command, "sync_scope_version", scopeInfo.Version);
            DbSyncAdapter.SetParameterValue(command, "scope_last_sync", scopeInfo.LastSync.HasValue ? (object)scopeInfo.LastSync.Value : DBNull.Value);
            DbSyncAdapter.SetParameterValue(command, "scope_last_sync_timestamp", scopeInfo.LastSyncTimestamp);
            DbSyncAdapter.SetParameterValue(command, "scope_last_server_sync_timestamp", scopeInfo.LastServerSyncTimestamp);
            DbSyncAdapter.SetParameterValue(command, "scope_last_sync_duration", scopeInfo.LastSyncDuration);
            DbSyncAdapter.SetParameterValue(command, "sync_scope_id", scopeInfo.Id.ToString());

            return command;
        }
        private DbCommand SetSaveScopeParameters(ServerHistoryScopeInfo serverHistoryScopeInfo, DbCommand command)
        {
            DbSyncAdapter.SetParameterValue(command, "sync_scope_name", serverHistoryScopeInfo.Name);
            DbSyncAdapter.SetParameterValue(command, "scope_last_sync_timestamp", serverHistoryScopeInfo.LastSyncTimestamp);
            DbSyncAdapter.SetParameterValue(command, "scope_last_sync", serverHistoryScopeInfo.LastSync.HasValue ? (object)serverHistoryScopeInfo.LastSync.Value : DBNull.Value);
            DbSyncAdapter.SetParameterValue(command, "scope_last_sync_duration", serverHistoryScopeInfo.LastSyncDuration);
            DbSyncAdapter.SetParameterValue(command, "sync_scope_id", serverHistoryScopeInfo.Id.ToString());

            return command;
        }
        private DbCommand SetSaveScopeParameters(ServerScopeInfo serverScopeInfo, DbCommand command)
        {
            DbSyncAdapter.SetParameterValue(command, "sync_scope_name", serverScopeInfo.Name);
            DbSyncAdapter.SetParameterValue(command, "sync_scope_schema", serverScopeInfo.Schema == null ? DBNull.Value : (object)JsonConvert.SerializeObject(serverScopeInfo.Schema));
            DbSyncAdapter.SetParameterValue(command, "sync_scope_setup", serverScopeInfo.Setup == null ? DBNull.Value : (object)JsonConvert.SerializeObject(serverScopeInfo.Setup));
            DbSyncAdapter.SetParameterValue(command, "sync_scope_version", serverScopeInfo.Version);
            DbSyncAdapter.SetParameterValue(command, "sync_scope_last_clean_timestamp", serverScopeInfo.LastCleanupTimestamp);

            return command;
        }

        private ScopeInfo ReadScopeInfo(DbDataReader reader)
        {
            var scopeInfo = new ScopeInfo
            {
                Id = reader.GetGuid(reader.GetOrdinal("sync_scope_id")),
                Name = reader["sync_scope_name"] as string,
                Schema = reader["sync_scope_schema"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSet>((string)reader["sync_scope_schema"]),
                Setup = reader["sync_scope_setup"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSetup>((string)reader["sync_scope_setup"]),
                Version = reader["sync_scope_version"] as string,
                LastSync = reader["scope_last_sync"] != DBNull.Value ? reader.GetDateTime(reader.GetOrdinal("scope_last_sync")) : null,
                LastServerSyncTimestamp = reader["scope_last_server_sync_timestamp"] != DBNull.Value ? (long?)reader.GetInt64(reader.GetOrdinal("scope_last_server_sync_timestamp")) : null,
                LastSyncTimestamp = reader["scope_last_sync_timestamp"] != DBNull.Value ? (long?)reader.GetInt64(reader.GetOrdinal("scope_last_sync_timestamp")) : null,
                LastSyncDuration = reader["scope_last_sync_duration"] != DBNull.Value ? reader.GetInt64(reader.GetOrdinal("scope_last_sync_duration")) : 0L
            };

            scopeInfo.IsNewScope = scopeInfo.LastSync == null;

            return scopeInfo;
        }

        private ServerScopeInfo ReaderServerScopeInfo(DbDataReader reader)
        {
            var serverScopeInfo = new ServerScopeInfo
            {
                Name = reader["sync_scope_name"] as string,
                Schema = reader["sync_scope_schema"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSet>((string)reader["sync_scope_schema"]),
                Setup = reader["sync_scope_setup"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSetup>((string)reader["sync_scope_setup"]),
                Version = reader["sync_scope_version"] as string,
                LastCleanupTimestamp = reader["sync_scope_last_clean_timestamp"] != DBNull.Value ? reader.GetInt64(reader.GetOrdinal("sync_scope_last_clean_timestamp")) : 0L
            };

            return serverScopeInfo;
        }

        private ServerHistoryScopeInfo ReadServerHistoryScopeInfo(DbDataReader reader)
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
    }
}
