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

        public virtual Task<ServerScopeInfo> GetServerScopeInfoAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => GetServerScopeInfoAsync(SyncOptions.DefaultScopeName, null, connection, transaction, cancellationToken, progress);

        public virtual Task<ServerScopeInfo> GetServerScopeInfoAsync(SyncSetup setup, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => GetServerScopeInfoAsync(SyncOptions.DefaultScopeName, setup, connection, transaction, cancellationToken, progress);

        /// <summary>
        /// Get the server scope info, ensures the scope is created.
        /// Provision is setup is defined (and scope does not exists in the database yet)
        /// </summary>
        /// <returns>Server scope info, containing scope name, version, setup and related schema infos</returns>
        public virtual async Task<ServerScopeInfo> GetServerScopeInfoAsync(string scopeName, SyncSetup setup = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Writing, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var serverScopeInfo = await this.InternalGetServerScopeInfoAsync(scopeName, setup , runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);    

                await runner.CommitAsync().ConfigureAwait(false);

                return serverScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeName, ex);
            }

        }

        /// <summary>
        /// Update or Insert a server scope row
        /// </summary>
        public virtual async Task<ServerScopeInfo> SaveServerScopeInfoAsync(ServerScopeInfo serverScopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(serverScopeInfo.Name, SyncMode.Writing, SyncStage.ScopeWriting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var exists = await this.InternalExistsScopeInfoTableAsync(serverScopeInfo.Name, DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(serverScopeInfo.Name, DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                 
                // Write scopes locally
                serverScopeInfo = await this.InternalSaveServerScopeInfoAsync(serverScopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return serverScopeInfo;
            } 
            catch (Exception ex)
            {
                throw GetSyncError(serverScopeInfo.Name, ex);
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
        /// Internal exists scope
        /// </summary>
        internal async Task<bool> InternalExistsServerScopeInfoAsync(string scopeName, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // Get exists command
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var existsCommand = scopeBuilder.GetCommandAsync(DbScopeCommandType.ExistServerScopeInfo, connection, transaction);

            if (existsCommand == null) return false;

            var ctx = this.GetContext(scopeName);

            DbSyncAdapter.SetParameterValue(existsCommand, "sync_scope_name", scopeName);

            if (existsCommand == null)
                return false;

            await this.InterceptAsync(new DbCommandArgs(ctx, existsCommand, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;
            return exists;
        }



        internal async Task<ServerScopeInfo> InternalGetServerScopeInfoAsync(string scopeName, SyncSetup setup, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var exists = await this.InternalExistsScopeInfoTableAsync(scopeName, DbScopeType.Server, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            if (!exists)
                await this.InternalCreateScopeInfoTableAsync(scopeName, DbScopeType.Server, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            exists = await this.InternalExistsScopeInfoTableAsync(scopeName, DbScopeType.ServerHistory, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            if (!exists)
                await this.InternalCreateScopeInfoTableAsync(scopeName, DbScopeType.ServerHistory, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            var serverScopeInfo = await this.InternalLoadServerScopeInfoAsync(scopeName, connection, transaction, cancellationToken, progress).ConfigureAwait(false) as ServerScopeInfo;


            if (serverScopeInfo == null)
            {
                serverScopeInfo = this.InternalCreateScopeInfo(scopeName, DbScopeType.Server) as ServerScopeInfo;

                serverScopeInfo = await this.InternalSaveServerScopeInfoAsync(serverScopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false) as ServerScopeInfo;
            }

            // Raise error only on server side, since we can't do nothing if we don't have any tables provisionned and no setup provided
            if ((serverScopeInfo.Setup == null || serverScopeInfo.Schema == null) && (setup == null || setup.Tables.Count <= 0))
                throw new MissingTablesException(scopeName);

            // if serverscopeinfo is a new, because we never run any sync before, grab schema and affect setup
            if (serverScopeInfo.Setup == null && serverScopeInfo.Schema == null && setup != null && setup.Tables.Count > 0)
            {
                var schema = await this.InternalGetSchemaAsync(scopeName, setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                serverScopeInfo.Setup = setup;
                serverScopeInfo.Schema = schema;

                // Checking if we have already some scopes
                // Then gets the first scope to get the id
                var allScopes = await this.InternalLoadAllServerScopesInfosAsync(scopeName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (allScopes.Count > 0)
                {
                    // Get the first scope with an existing setup
                    var firstScope = allScopes.FirstOrDefault(sc => sc.Setup != null);

                    if (firstScope != null)
                    {
                        if (serverScopeInfo.Setup.TrackingTablesPrefix != firstScope.Setup.TrackingTablesPrefix)
                            throw new Exception($"Can't add a new setup with different tracking table prefix. Please use same tracking table prefix as your first setup ([\"{firstScope.Setup.TrackingTablesPrefix}\"])");

                        if (serverScopeInfo.Setup.TrackingTablesSuffix != firstScope.Setup.TrackingTablesSuffix)
                            throw new Exception($"Can't add a new setup with different tracking table suffix. Please use same tracking table suffix as your first setup ([\"{firstScope.Setup.TrackingTablesSuffix}\"])");

                        if (serverScopeInfo.Setup.TriggersPrefix != firstScope.Setup.TriggersPrefix)
                            throw new Exception($"Can't add a new setup with different trigger prefix. Please use same trigger prefix as your first setup ([\"{firstScope.Setup.TriggersPrefix}\"])");

                        if (serverScopeInfo.Setup.TriggersSuffix != firstScope.Setup.TriggersSuffix)
                            throw new Exception($"Can't add a new setup with different trigger suffix. Please use same trigger suffix as your first setup ([\"{firstScope.Setup.TriggersSuffix}\"])");
                    }
                }

                // Write scopes locally
                serverScopeInfo = await this.InternalSaveServerScopeInfoAsync(serverScopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // override default value
                serverScopeInfo.IsNewScope = true;

                var scopeLoadedArgs = new ScopeLoadedArgs(this.GetContext(scopeName), scopeName, DbScopeType.Server, serverScopeInfo, connection, transaction);
                await this.InterceptAsync(scopeLoadedArgs, progress, cancellationToken).ConfigureAwait(false);
            }

            if (setup != null && setup.Tables.Count > 0 && !serverScopeInfo.Setup.EqualsByProperties(setup))
                throw new Exception("Seems you are trying another Setup tables that what is stored in your server scope database. Please make a migration or create a new scope");

            return serverScopeInfo;
        }

        /// <summary>
        /// Internal load a server scope by scope name
        /// </summary>
        internal async Task<ServerScopeInfo> InternalLoadServerScopeInfoAsync(string scopeName, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetServerScopeInfo, connection, transaction);

            if (command == null) return null;

            var ctx = this.GetContext(scopeName);

            DbSyncAdapter.SetParameterValue(command, "sync_scope_name", scopeName);

            var action = new ScopeLoadingArgs(ctx, scopeName, DbScopeType.Server, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return null;

            await this.InterceptAsync(new DbCommandArgs(ctx, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            using DbDataReader reader = await action.Command.ExecuteReaderAsync().ConfigureAwait(false);

            ServerScopeInfo serverScopeInfo = null;

            if (reader.Read())
                serverScopeInfo = InternalReadServerScopeInfo(reader);

            reader.Close();

            if (serverScopeInfo?.Schema != null)
                serverScopeInfo.Schema.EnsureSchema();

            var scopeLoadedArgs = new ScopeLoadedArgs(ctx, scopeName, DbScopeType.Server, serverScopeInfo, connection, transaction);
            await this.InterceptAsync(scopeLoadedArgs, progress, cancellationToken).ConfigureAwait(false);

            return serverScopeInfo;
        }

        /// <summary>
        /// Internal load all server scopes. scopeName arg is just here for getting context
        /// </summary>
        internal async Task<List<ServerScopeInfo>> InternalLoadAllServerScopesInfosAsync(string scopeName, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetAllServerScopesInfo, connection, transaction);

            if (command == null) return null;

            var ctx = this.GetContext(scopeName);

            var action = new ScopeLoadingArgs(ctx, scopeName, DbScopeType.Server, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return null;

            var serverScopes = new List<ServerScopeInfo>();

            await this.InterceptAsync(new DbCommandArgs(ctx, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            using DbDataReader reader = await action.Command.ExecuteReaderAsync().ConfigureAwait(false);

            while (reader.Read())
            {
                var scopeInfo = InternalReadServerScopeInfo(reader);

                if (scopeInfo.Schema != null)
                    scopeInfo.Schema.EnsureSchema();

                serverScopes.Add(scopeInfo);
            }

            reader.Close();

            foreach (var scopeInfo in serverScopes)
            {
                var scopeLoadedArgs = new ScopeLoadedArgs(ctx, scopeName, DbScopeType.Server, scopeInfo, connection, transaction);
                await this.InterceptAsync(scopeLoadedArgs, progress, cancellationToken).ConfigureAwait(false);

            }

            return serverScopes;
        }

        private DbCommand InternalSetSaveServerScopeInfoParameters(ServerScopeInfo serverScopeInfo, DbCommand command)
        {
            var serializedSchema = JsonConvert.SerializeObject(serverScopeInfo.Schema);
            var serializedSetup = JsonConvert.SerializeObject(serverScopeInfo.Setup);

            DbSyncAdapter.SetParameterValue(command, "sync_scope_name", serverScopeInfo.Name);
            DbSyncAdapter.SetParameterValue(command, "sync_scope_schema", serverScopeInfo.Schema == null ? DBNull.Value : serializedSchema);
            DbSyncAdapter.SetParameterValue(command, "sync_scope_setup", serverScopeInfo.Setup == null ? DBNull.Value : serializedSetup);
            DbSyncAdapter.SetParameterValue(command, "sync_scope_version", serverScopeInfo.Version);
            DbSyncAdapter.SetParameterValue(command, "sync_scope_last_clean_timestamp", serverScopeInfo.LastCleanupTimestamp);

            return command;
        }

        private ServerScopeInfo InternalReadServerScopeInfo(DbDataReader reader)
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

        /// <summary>
        /// Internal upsert server scope info in a server scope table
        /// </summary>
        internal async Task<ServerScopeInfo> InternalSaveServerScopeInfoAsync(ServerScopeInfo serverScopeInfo, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var scopeExists = await InternalExistsServerScopeInfoAsync(serverScopeInfo.Name, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            DbCommand command;
            if (scopeExists)
                command = scopeBuilder.GetCommandAsync(DbScopeCommandType.UpdateServerScopeInfo, connection, transaction);
            else
                command = scopeBuilder.GetCommandAsync(DbScopeCommandType.InsertServerScopeInfo, connection, transaction);

            if (command == null) return null;

            command = InternalSetSaveServerScopeInfoParameters(serverScopeInfo, command);

            var ctx = this.GetContext(serverScopeInfo.Name);

            var action = new ScopeSavingArgs(ctx, scopeBuilder.ScopeInfoTableName.ToString(), DbScopeType.Server, serverScopeInfo, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return default;

            await this.InterceptAsync(new DbCommandArgs(ctx, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            using DbDataReader reader = await action.Command.ExecuteReaderAsync().ConfigureAwait(false);

            reader.Read();

            var newScopeInfo = InternalReadServerScopeInfo(reader);

            reader.Close();

            await this.InterceptAsync(new ScopeSavedArgs(ctx, scopeBuilder.ScopeInfoTableName.ToString(), DbScopeType.Server, newScopeInfo, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            return newScopeInfo;
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