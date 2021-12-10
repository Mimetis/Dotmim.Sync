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
        public virtual async Task<bool> CreateScopeInfoTableAsync(DbScopeType scopeType, string scopeInfoTableName, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(SyncStage.Provisioning, connection, transaction, cancellationToken).ConfigureAwait(false);
                bool hasBeenCreated = false;

                // Get table builder
                var scopeBuilder = this.GetScopeBuilder(scopeInfoTableName);
                var exists = await InternalExistsScopeInfoTableAsync(this.GetContext(), scopeType, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // should create only if not exists OR if overwrite has been set
                var shouldCreate = !exists || overwrite;
                if (shouldCreate)
                {
                    // Drop trigger if already exists
                    if (exists && overwrite)
                        await InternalDropScopeInfoTableAsync(this.GetContext(), scopeType, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    hasBeenCreated = await InternalCreateScopeInfoTableAsync(this.GetContext(), scopeType, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                await runner.CommitAsync().ConfigureAwait(false);

                return hasBeenCreated;
            }
            catch (Exception ex)
            {
                throw GetSyncError(ex);
            }

        }


        /// <summary>
        /// Check if a scope info table exists
        /// </summary>
        public async Task<bool> ExistScopeInfoTableAsync(DbScopeType scopeType, string scopeInfoTableName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(SyncStage.None, connection, transaction, cancellationToken).ConfigureAwait(false);
                var scopeBuilder = this.GetScopeBuilder(scopeInfoTableName);
                var exists = await InternalExistsScopeInfoTableAsync(this.GetContext(), scopeType, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                await runner.CommitAsync().ConfigureAwait(false);
                return exists;
            }
            catch (Exception ex)
            {
                throw GetSyncError(ex);
            }
        }


        /// <summary>
        /// Internal exists scope table routine
        /// </summary>
        internal async Task<bool> InternalExistsScopeInfoTableAsync(SyncContext ctx, DbScopeType scopeType, DbScopeBuilder scopeBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // Get exists command
            var existsCommand = scopeBuilder.GetCommandAsync(DbScopeCommandType.ExistsScopeTable, scopeType, connection, transaction);

            if (existsCommand == null)
                return false;

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;
            return exists;
        }

        /// <summary>
        /// Internal exists scope
        /// </summary>
        internal async Task<bool> InternalExistsScopeInfoAsync(SyncContext ctx, DbScopeType scopeType, string scopeId, DbScopeBuilder scopeBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // Get exists command
            var existsCommand = scopeBuilder.GetCommandAsync(DbScopeCommandType.ExistScope, scopeType, connection, transaction);

            if (existsCommand == null) return false;

            // Just in case, in older version we may have sync_scope_name as primary key;
            DbSyncAdapter.SetParameterValue(existsCommand, "sync_scope_name", scopeId);
            // Set primary key value
            DbSyncAdapter.SetParameterValue(existsCommand, "sync_scope_id", scopeId);

            if (existsCommand == null)
                return false;

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;
            return exists;
        }

        /// <summary>
        /// Internal drop scope info table routine
        /// </summary>
        internal async Task<bool> InternalDropScopeInfoTableAsync(SyncContext ctx, DbScopeType scopeType, DbScopeBuilder scopeBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.DropScopeTable, scopeType, connection, transaction);

            if (command == null) return false;

            var action = new ScopeTableDroppingArgs(ctx, scopeBuilder.ScopeInfoTableName.ToString(), scopeType, command, connection, transaction);
            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

            await this.InterceptAsync(new ScopeTableDroppedArgs(ctx, scopeBuilder.ScopeInfoTableName.ToString(), scopeType, connection, transaction), cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal create scope info table routine
        /// </summary>
        internal async Task<bool> InternalCreateScopeInfoTableAsync(SyncContext ctx, DbScopeType scopeType, DbScopeBuilder scopeBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.CreateScopeTable, scopeType, connection, transaction);

            if (command == null)
                return false;

            var action = new ScopeTableCreatingArgs(ctx, scopeBuilder.ScopeInfoTableName.ToString(), scopeType, command, connection, transaction);
            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await action.Command.ExecuteNonQueryAsync();

            await this.InterceptAsync(new ScopeTableCreatedArgs(ctx, scopeBuilder.ScopeInfoTableName.ToString(), scopeType, connection, transaction), cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal load all scopes routine
        /// </summary>
        internal async Task<List<T>> InternalGetAllScopesAsync<T>(SyncContext ctx, DbScopeType scopeType, string scopeName, DbScopeBuilder scopeBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress) where T : class
        {
            var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetScopes, scopeType, connection, transaction);

            if (command == null) return null;

            DbSyncAdapter.SetParameterValue(command, "sync_scope_name", scopeName);

            var action = new ScopeLoadingArgs(ctx, scopeName, scopeType, command, connection, transaction);
            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return null;

            var scopes = new List<T>();

            using DbDataReader reader = await action.Command.ExecuteReaderAsync().ConfigureAwait(false);

            while (reader.Read())
            {
                T scopeInfo = scopeType switch
                {
                    DbScopeType.Server => ReaderServerScopeInfo(reader) as T,
                    DbScopeType.ServerHistory => ReadServerHistoryScopeInfo(reader) as T,
                    DbScopeType.Client => ReadScopeInfo(reader) as T,
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
        internal async Task<T> InternalGetScopeAsync<T>(SyncContext ctx, DbScopeType scopeType, string scopeName, DbScopeBuilder scopeBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress) where T : class
        {
            if (typeof(T) != typeof(ScopeInfo) && typeof(T) != typeof(ServerScopeInfo))
                throw new NotImplementedException($"Type {typeof(T).Name} is not implemented when trying to get a single instance");

            var scopes = await InternalGetAllScopesAsync<T>(ctx, scopeType, scopeName, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (scopes == null || scopes.Count <= 0)
            {
                scopes = new List<T>();

                // create a new scope id for the current owner (could be server or client as well)
                T scope = scopeType switch
                {
                    DbScopeType.Client => new ScopeInfo { Id = Guid.NewGuid(), Name = scopeName, IsNewScope = true, LastSync = null, LastServerSyncTimestamp = null, LastSyncTimestamp = null, Version = SyncVersion.Current.ToString() } as T,
                    DbScopeType.Server => new ServerScopeInfo { Name = scopeName, LastCleanupTimestamp = 0, Version = SyncVersion.Current.ToString() } as T,
                    _ => throw new NotImplementedException($"Type {typeof(T).Name} is not implemented when trying to get a single instance")
                };

                scope = await this.InternalSaveScopeAsync(ctx, scopeType, scope, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                scopes.Add(scope);
            }

            // get first scope
            var localScope = scopes.FirstOrDefault();

            if (typeof(T) == typeof(ScopeInfo))
            {
                //check if we have already a good last sync. if no, treat it as new
                scopes.ForEach(sc => (sc as ScopeInfo).IsNewScope = (sc as ScopeInfo).LastSync == null);

                var scopeInfo = localScope as ScopeInfo;

                if (scopeInfo?.Schema != null)
                    scopeInfo.Schema.EnsureSchema();

                var scopeLoadedArgs = new ScopeLoadedArgs<ScopeInfo>(ctx, scopeName, scopeType, scopeInfo, connection, transaction);
                await this.InterceptAsync(scopeLoadedArgs, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                var scopeInfo = localScope as ServerScopeInfo;

                if (scopeInfo?.Schema != null)
                    scopeInfo.Schema.EnsureSchema();

                var scopeLoadedArgs = new ScopeLoadedArgs<ServerScopeInfo>(ctx, scopeName, scopeType, scopeInfo, connection, transaction);
                await this.InterceptAsync(scopeLoadedArgs, cancellationToken).ConfigureAwait(false);
            }

            return localScope;
        }

        /// <summary>
        /// Internal upsert scope info in a scope table
        /// </summary>
        internal async Task<T> InternalSaveScopeAsync<T>(SyncContext ctx, DbScopeType scopeType, T scopeInfo, DbScopeBuilder scopeBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress) where T : class
        {

            var scopeId = scopeType switch
            {
                DbScopeType.Client => (scopeInfo as ScopeInfo).Id.ToString(),
                DbScopeType.Server => (scopeInfo as ServerScopeInfo).Name,
                DbScopeType.ServerHistory => (scopeInfo as ServerHistoryScopeInfo).Id.ToString(),
                _ => throw new NotImplementedException($"Can't set parameters to scope command type {scopeType}.")
            };

            var scopeExists = await InternalExistsScopeInfoAsync(ctx, scopeType, scopeId, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            DbCommand command;

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

            var action = new ScopeSavingArgs(ctx, scopeBuilder.ScopeInfoTableName.ToString(), scopeType, scopeInfo, command, connection, transaction);
            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return default;

            using DbDataReader reader = await action.Command.ExecuteReaderAsync().ConfigureAwait(false);

            reader.Read();

            T newScopeInfo = scopeType switch
            {
                DbScopeType.Client => ReadScopeInfo(reader) as T,
                DbScopeType.Server => ReaderServerScopeInfo(reader) as T,
                DbScopeType.ServerHistory => ReadServerHistoryScopeInfo(reader) as T,
                _ => throw new NotImplementedException($"Can't get {scopeType} from the reader ")
            };

            reader.Close();

            await this.InterceptAsync(new ScopeSavedArgs(ctx, scopeBuilder.ScopeInfoTableName.ToString(), scopeType, newScopeInfo, connection, transaction), cancellationToken).ConfigureAwait(false);

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
                LastSync = reader["scope_last_sync"] != DBNull.Value ? (DateTime?)reader.GetDateTime(reader.GetOrdinal("scope_last_sync")) : null,
                LastServerSyncTimestamp = reader["scope_last_server_sync_timestamp"] != DBNull.Value ? (long?)reader.GetInt64(reader.GetOrdinal("scope_last_server_sync_timestamp")) : null,
                LastSyncTimestamp = reader["scope_last_sync_timestamp"] != DBNull.Value ? (long?)reader.GetInt64(reader.GetOrdinal("scope_last_sync_timestamp")) : null,
                LastSyncDuration = reader["scope_last_sync_duration"] != DBNull.Value ? reader.GetInt64(reader.GetOrdinal("scope_last_sync_duration")) : 0L
            };

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
