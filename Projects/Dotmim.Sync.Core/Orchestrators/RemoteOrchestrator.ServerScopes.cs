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
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                ServerScopeInfo serverScopeInfo;
                (context, serverScopeInfo) = await this.InternalGetServerScopeInfoAsync(context, setup, false, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return serverScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }


        /// <summary>
        /// Get all scopes. scopeName arg is just here for logging purpose and is not used
        /// </summary>
        public virtual async Task<List<ServerScopeInfo>>
            GetAllServerScopesInfoAsync(string scopeName = SyncOptions.DefaultScopeName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);

            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.Reading, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                List<ServerScopeInfo> localScopes;
                (context, localScopes) = await InternalLoadAllServerScopesInfosAsync(context,
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
        /// Update or Insert a server scope row
        /// </summary>
        public virtual async Task<ServerScopeInfo> SaveServerScopeInfoAsync(ServerScopeInfo serverScopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), serverScopeInfo.Name);
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.ScopeWriting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // Write scopes locally
                (context, serverScopeInfo) = await this.InternalSaveServerScopeInfoAsync(serverScopeInfo, context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return serverScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Delete a server scope row
        /// </summary>
        public virtual async Task<bool> DeleteServerScopeInfoAsync(ServerScopeInfo serverScopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), serverScopeInfo.Name);
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.ScopeWriting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                bool isDeleted;
                (context, isDeleted) = await this.InternalDeleteServerScopeInfoAsync(serverScopeInfo, context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return isDeleted;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Check 
        /// </summary>
        public virtual async Task<(SyncContext, bool, ServerScopeInfo)> IsConflictingSetupAsync(SyncContext context, SyncSetup inputSetup, ServerScopeInfo serverScopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (serverScopeInfo.IsNewScope || serverScopeInfo.Schema == null)
            {
                return (context, false, serverScopeInfo);

            }

            if (inputSetup != null && serverScopeInfo.Setup != null && !serverScopeInfo.Setup.EqualsByProperties(inputSetup))
            {

                var conflictingSetupArgs = new ConflictingSetupArgs(context, inputSetup, null, serverScopeInfo);
                await this.InterceptAsync(conflictingSetupArgs, progress, cancellationToken).ConfigureAwait(false);

                if (conflictingSetupArgs.Action == ConflictingSetupAction.Rollback)
                {
                    throw new Exception("Seems you are trying another Setup tables that what is stored in your server scope database. Please create a new scope or deprovision and provision again your server scope.");
                }
                if (conflictingSetupArgs.Action == ConflictingSetupAction.Abort)
                {
                    return (context, true, serverScopeInfo);
                }

                // re affect scope infos
                serverScopeInfo = conflictingSetupArgs.ServerScopeInfo;
            }


            // We gave 2 chances to user to edit the setup and fill correct values.
            // Final check, but if not valid, raise an error
            if (inputSetup != null && serverScopeInfo.Setup != null && !serverScopeInfo.Setup.EqualsByProperties(inputSetup))
                throw new Exception("Seems you are trying another Setup tables that what is stored in your server scope database. Please make a migration or create a new scope");

            return (context, false, serverScopeInfo);
        }


        /// <summary>
        /// Internal exists scope
        /// </summary>
        internal async Task<(SyncContext context, bool exists)> InternalExistsServerScopeInfoAsync(
            SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // Get exists command
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            using var existsCommand = scopeBuilder.GetCommandAsync(DbScopeCommandType.ExistServerScopeInfo, connection, transaction);

            if (existsCommand == null) return (context, false);

            SetParameterValue(existsCommand, "sync_scope_name", context.ScopeName);

            if (existsCommand == null)
                return (context, false);

            await this.InterceptAsync(new ExecuteCommandArgs(context, existsCommand, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;
            return (context, exists);
        }


        internal virtual async Task<(SyncContext context, ServerScopeInfo serverScopeInfo)>
            InternalGetServerScopeInfoAsync(SyncContext context, SyncSetup setup, bool overwrite, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.Server, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.Server, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                ServerScopeInfo serverScopeInfo;
                (context, serverScopeInfo) = await this.InternalLoadServerScopeInfoAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (serverScopeInfo == null)
                {
                    serverScopeInfo = this.InternalCreateScopeInfo(context.ScopeName, DbScopeType.Server) as ServerScopeInfo;

                    (context, serverScopeInfo) = await this.InternalSaveServerScopeInfoAsync(serverScopeInfo, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
                }

                // Raise error only on server side, since we can't do nothing if we don't have any tables provisionned and no setup provided
                if ((serverScopeInfo.Setup == null || serverScopeInfo.Schema == null) && (setup == null || setup.Tables.Count <= 0))
                    throw new MissingServerScopeTablesException(context.ScopeName);

                // if serverscopeinfo is a new, because we never run any sync before, grab schema and affect setup
                if (setup != null && setup.Tables.Count > 0)
                {
                    if ((serverScopeInfo.Setup == null && serverScopeInfo.Schema == null) || overwrite)
                    {
                        SyncSet schema;
                        (context, schema) = await this.InternalGetSchemaAsync(context, setup, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
                        serverScopeInfo.Setup = setup;
                        serverScopeInfo.Schema = schema;

                        // Checking if we have already some scopes
                        // Then gets the first scope to get the id
                        List<ServerScopeInfo> allScopes;
                        (context, allScopes) = await this.InternalLoadAllServerScopesInfosAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

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
                        (context, serverScopeInfo) = await this.InternalSaveServerScopeInfoAsync(serverScopeInfo, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        // override default value that is always false after saving
                        serverScopeInfo.IsNewScope = true;
                    }
                }
                await runner.CommitAsync().ConfigureAwait(false);

                return (context, serverScopeInfo);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Internal load a server scope by scope name
        /// </summary>
        internal async Task<(SyncContext context, ServerScopeInfo serverScopeInfo)>
            InternalLoadServerScopeInfoAsync(SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            using var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetServerScopeInfo, connection, transaction);

            if (command == null) return (context, null);

            SetParameterValue(command, "sync_scope_name", context.ScopeName);

            var action = new ServerScopeInfoLoadingArgs(context, context.ScopeName, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return (context, null);

            await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            using DbDataReader reader = await action.Command.ExecuteReaderAsync().ConfigureAwait(false);

            ServerScopeInfo serverScopeInfo = null;

            if (reader.Read())
                serverScopeInfo = InternalReadServerScopeInfo(reader);

            reader.Close();

            if (serverScopeInfo?.Schema != null)
                serverScopeInfo.Schema.EnsureSchema();

            var scopeLoadedArgs = new ServerScopeInfoLoadedArgs(context, context.ScopeName, serverScopeInfo, connection, transaction);
            await this.InterceptAsync(scopeLoadedArgs, progress, cancellationToken).ConfigureAwait(false);
            serverScopeInfo = scopeLoadedArgs.ServerScopeInfo;
            action.Command.Dispose();

            return (context, serverScopeInfo);
        }

        /// <summary>
        /// Internal load all server scopes. scopeName arg is just here for getting context
        /// </summary>
        internal async Task<(SyncContext context, List<ServerScopeInfo> serverScopeInfos)>
            InternalLoadAllServerScopesInfosAsync(SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            using var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetAllServerScopesInfo, connection, transaction);

            if (command == null) return (context, null);

            var serverScopes = new List<ServerScopeInfo>();

            await this.InterceptAsync(new ExecuteCommandArgs(context, command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            using DbDataReader reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

            while (reader.Read())
            {
                var scopeInfo = InternalReadServerScopeInfo(reader);

                if (scopeInfo.Schema != null)
                    scopeInfo.Schema.EnsureSchema();

                serverScopes.Add(scopeInfo);
            }

            reader.Close();
            command.Dispose();

            return (context, serverScopes);
        }

        private DbCommand InternalSetSaveServerScopeInfoParameters(ServerScopeInfo serverScopeInfo, DbCommand command)
        {
            var serializedSchema = JsonConvert.SerializeObject(serverScopeInfo.Schema);
            var serializedSetup = JsonConvert.SerializeObject(serverScopeInfo.Setup);

            SetParameterValue(command, "sync_scope_name", serverScopeInfo.Name);
            SetParameterValue(command, "sync_scope_schema", serverScopeInfo.Schema == null ? DBNull.Value : serializedSchema);
            SetParameterValue(command, "sync_scope_setup", serverScopeInfo.Setup == null ? DBNull.Value : serializedSetup);
            SetParameterValue(command, "sync_scope_version", serverScopeInfo.Version);
            SetParameterValue(command, "sync_scope_last_clean_timestamp", serverScopeInfo.LastCleanupTimestamp);

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
        internal async Task<(SyncContext context, ServerScopeInfo serverScopeInfo)>
            InternalSaveServerScopeInfoAsync(ServerScopeInfo serverScopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            bool scopeExists;
            (context, scopeExists) = await InternalExistsServerScopeInfoAsync(context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            DbCommand command;
            if (scopeExists)
                command = scopeBuilder.GetCommandAsync(DbScopeCommandType.UpdateServerScopeInfo, connection, transaction);
            else
                command = scopeBuilder.GetCommandAsync(DbScopeCommandType.InsertServerScopeInfo, connection, transaction);

            if (command == null) return (context, null);

            command = InternalSetSaveServerScopeInfoParameters(serverScopeInfo, command);

            var action = new ScopeSavingArgs(context, scopeBuilder.ScopeInfoTableName.ToString(), DbScopeType.Server, serverScopeInfo, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return default;

            await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            using DbDataReader reader = await action.Command.ExecuteReaderAsync().ConfigureAwait(false);

            reader.Read();

            var newScopeInfo = InternalReadServerScopeInfo(reader);

            reader.Close();

            await this.InterceptAsync(new ScopeSavedArgs(context, scopeBuilder.ScopeInfoTableName.ToString(), DbScopeType.Server, newScopeInfo, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            action.Command.Dispose();

            return (context, newScopeInfo);
        }

        /// Internal delete server scope info in a server scope table
        /// </summary>
        internal async Task<(SyncContext context, bool isDeleted)>
            InternalDeleteServerScopeInfoAsync(ServerScopeInfo serverScopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            bool scopeExists;
            (context, scopeExists) = await InternalExistsServerScopeInfoAsync(context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (!scopeExists)
                return (context, true);

            using var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.DeleteServerScopeInfo, connection, transaction);

            InternalSetSaveServerScopeInfoParameters(serverScopeInfo, command);

            var action = new ScopeSavingArgs(context, scopeBuilder.ScopeInfoTableName.ToString(), DbScopeType.Server, serverScopeInfo, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return default;

            await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

            await this.InterceptAsync(new ScopeSavedArgs(context, scopeBuilder.ScopeInfoTableName.ToString(), DbScopeType.Server, serverScopeInfo, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            action.Command.Dispose();

            return (context, true);
        }

    }
}