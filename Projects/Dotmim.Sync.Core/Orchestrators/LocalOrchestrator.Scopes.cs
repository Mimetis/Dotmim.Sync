using Dotmim.Sync.Args;
using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


namespace Dotmim.Sync
{
    public partial class LocalOrchestrator : BaseOrchestrator
    {

        public virtual Task<ClientScopeInfo> GetClientScopeInfoAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => GetClientScopeInfoAsync(SyncOptions.DefaultScopeName, connection, transaction, cancellationToken, progress);

        public virtual async Task<ClientScopeInfo>
            GetClientScopeInfoAsync(string scopeName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);

            await using var runner = await this.GetConnectionAsync(context, SyncMode.Reading, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            ClientScopeInfo localScope;
            (context, localScope) = await InternalGetClientScopeInfoAsync(context,
                runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            await runner.CommitAsync().ConfigureAwait(false);

            return localScope;
        }



        /// <summary>
        /// Internal load a ScopeInfo by scope name
        /// </summary>
        internal async Task<(SyncContext context, ClientScopeInfo clientScopeInfo)> InternalGetClientScopeInfoAsync(
            SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.Client, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (!exists)
                    (context, _) = await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.Client, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // Get scope from local client 
                ClientScopeInfo localScopeInfo;
                (context, localScopeInfo) = await this.InternalLoadClientScopeInfoAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                var shouldSave = false;

                // Get scopeId representing the client unique id
                if (localScopeInfo == null)
                {
                    shouldSave = true;

                    localScopeInfo = this.InternalCreateScopeInfo(context.ScopeName, DbScopeType.Client) as ClientScopeInfo;

                    // Checking if we have already some scopes
                    // Then gets the first scope to get the id
                    // This ID is identifying the client database
                    List<ClientScopeInfo> allClientScopeInfos;
                    (context, allClientScopeInfos) = await this.InternalLoadAllClientScopesInfoAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    if (allClientScopeInfos.Count > 0)
                        localScopeInfo.Id = allClientScopeInfos[0].Id;
                }

                if (shouldSave)
                {
                    (context, localScopeInfo) = await this.InternalSaveClientScopeInfoAsync(localScopeInfo, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    // if not shouldSave, that means we already raised this event before
                    var scopeLoadedArgs = new ScopeLoadedArgs(context, context.ScopeName, DbScopeType.Client, localScopeInfo, runner.Connection, runner.Transaction);
                    await this.InterceptAsync(scopeLoadedArgs, progress, cancellationToken).ConfigureAwait(false);

                }

                await runner.CommitAsync().ConfigureAwait(false);

                return (context, localScopeInfo);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Get all scopes. scopeName arg is just here for logging purpose and is not used
        /// </summary>
        public virtual async Task<List<ClientScopeInfo>>
            GetAllClientScopesInfoAsync(string scopeName = SyncOptions.DefaultScopeName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);

            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.Reading, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                List<ClientScopeInfo> localScopes;
                (context, localScopes) = await InternalLoadAllClientScopesInfoAsync(context,
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
        /// Write a Client Scope Info in a client database
        /// </summary> 
        public virtual async Task<ClientScopeInfo>
            SaveClientScopeInfoAsync(ClientScopeInfo clientScopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), clientScopeInfo.Name);
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.ScopeWriting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.Client, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.Client, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // Write scopes locally
                (context, clientScopeInfo) = await this.InternalSaveClientScopeInfoAsync(clientScopeInfo, context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return clientScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Internal exists scope
        /// </summary>
        internal async Task<(SyncContext context, bool exists)> InternalExistsClientScopeInfoAsync(string scopeName, SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // Get exists command
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var existsCommand = scopeBuilder.GetCommandAsync(DbScopeCommandType.ExistClientScopeInfo, connection, transaction);

            if (existsCommand == null) return (context, false);

            DbSyncAdapter.SetParameterValue(existsCommand, "sync_scope_name", scopeName);

            if (existsCommand == null)
                return (context, false);

            await this.InterceptAsync(new DbCommandArgs(context, existsCommand, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;
            return (context, exists);
        }

        /// <summary>
        /// Internal load a scope by scope name
        /// </summary>
        internal async Task<(SyncContext context, ClientScopeInfo clientScopeInfo)>
            InternalLoadClientScopeInfoAsync(SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetClientScopeInfo, connection, transaction);

            if (command == null) return (context, null);

            DbSyncAdapter.SetParameterValue(command, "sync_scope_name", context.ScopeName);

            var action = new ScopeLoadingArgs(context, context.ScopeName, DbScopeType.Client, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return (context, null);

            await this.InterceptAsync(new DbCommandArgs(context, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            using DbDataReader reader = await action.Command.ExecuteReaderAsync().ConfigureAwait(false);

            ClientScopeInfo clientScopeInfo = null;

            if (reader.Read())
                clientScopeInfo = InternalReadClientScopeInfo(reader);

            reader.Close();

            if (clientScopeInfo?.Schema != null)
                clientScopeInfo.Schema.EnsureSchema();

            var scopeLoadedArgs = new ScopeLoadedArgs(context, context.ScopeName, DbScopeType.Client, clientScopeInfo, connection, transaction);
            await this.InterceptAsync(scopeLoadedArgs, progress, cancellationToken).ConfigureAwait(false);

            return (context, clientScopeInfo);
        }

        /// <summary>
        /// Internal load all scopes. scopeName arg is just here for getting context
        /// </summary>
        internal async Task<(SyncContext context, List<ClientScopeInfo> clientScopeInfos)>
            InternalLoadAllClientScopesInfoAsync(SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetAllClientScopesInfo, connection, transaction);

            if (command == null) return (context, null);

            var action = new ScopeLoadingArgs(context, context.ScopeName, DbScopeType.Client, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return (context, null);

            var clientScopes = new List<ClientScopeInfo>();

            await this.InterceptAsync(new DbCommandArgs(context, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            using DbDataReader reader = await action.Command.ExecuteReaderAsync().ConfigureAwait(false);

            while (reader.Read())
            {
                var scopeInfo = InternalReadClientScopeInfo(reader);

                if (scopeInfo.Schema != null)
                    scopeInfo.Schema.EnsureSchema();

                clientScopes.Add(scopeInfo);
            }

            reader.Close();

            foreach (var scopeInfo in clientScopes)
            {
                var scopeLoadedArgs = new ScopeLoadedArgs(context, context.ScopeName, DbScopeType.Client, scopeInfo, connection, transaction);
                await this.InterceptAsync(scopeLoadedArgs, progress, cancellationToken).ConfigureAwait(false);

            }

            return (context, clientScopes);
        }

        /// <summary>
        /// Internal upsert scope info in a scope table
        /// </summary>
        internal async Task<(SyncContext context, ClientScopeInfo clientScopeInfo)>
            InternalSaveClientScopeInfoAsync(ClientScopeInfo clientScopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            bool scopeExists;
            (context, scopeExists) = await InternalExistsClientScopeInfoAsync(clientScopeInfo.Name, context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            DbCommand command;
            if (scopeExists)
                command = scopeBuilder.GetCommandAsync(DbScopeCommandType.UpdateClientScopeInfo, connection, transaction);
            else
                command = scopeBuilder.GetCommandAsync(DbScopeCommandType.InsertClientScopeInfo, connection, transaction);

            if (command == null) return (context, null);

            command = InternalSetSaveClientScopeInfoParameters(clientScopeInfo, command);

            var action = new ScopeSavingArgs(context, scopeBuilder.ScopeInfoTableName.ToString(), DbScopeType.Client, clientScopeInfo, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return (context, null);

            await this.InterceptAsync(new DbCommandArgs(context, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            using DbDataReader reader = await action.Command.ExecuteReaderAsync().ConfigureAwait(false);

            reader.Read();

            clientScopeInfo = InternalReadClientScopeInfo(reader);

            reader.Close();

            await this.InterceptAsync(new ScopeSavedArgs(context, scopeBuilder.ScopeInfoTableName.ToString(), DbScopeType.Client, clientScopeInfo, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            return (context, clientScopeInfo);
        }

        private DbCommand InternalSetSaveClientScopeInfoParameters(ClientScopeInfo clientScopeInfo, DbCommand command)
        {
            DbSyncAdapter.SetParameterValue(command, "sync_scope_id", clientScopeInfo.Id.ToString());
            DbSyncAdapter.SetParameterValue(command, "sync_scope_name", clientScopeInfo.Name);
            DbSyncAdapter.SetParameterValue(command, "sync_scope_schema", clientScopeInfo.Schema == null ? DBNull.Value : (object)JsonConvert.SerializeObject(clientScopeInfo.Schema));
            DbSyncAdapter.SetParameterValue(command, "sync_scope_setup", clientScopeInfo.Setup == null ? DBNull.Value : (object)JsonConvert.SerializeObject(clientScopeInfo.Setup));
            DbSyncAdapter.SetParameterValue(command, "sync_scope_version", clientScopeInfo.Version);
            DbSyncAdapter.SetParameterValue(command, "scope_last_sync", clientScopeInfo.LastSync.HasValue ? (object)clientScopeInfo.LastSync.Value : DBNull.Value);
            DbSyncAdapter.SetParameterValue(command, "scope_last_sync_timestamp", clientScopeInfo.LastSyncTimestamp);
            DbSyncAdapter.SetParameterValue(command, "scope_last_server_sync_timestamp", clientScopeInfo.LastServerSyncTimestamp);
            DbSyncAdapter.SetParameterValue(command, "scope_last_sync_duration", clientScopeInfo.LastSyncDuration);

            return command;
        }

        private ClientScopeInfo InternalReadClientScopeInfo(DbDataReader reader)
        {
            var clientScopeInfo = new ClientScopeInfo
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

            clientScopeInfo.IsNewScope = clientScopeInfo.LastSync == null;

            return clientScopeInfo;
        }

    }
}
