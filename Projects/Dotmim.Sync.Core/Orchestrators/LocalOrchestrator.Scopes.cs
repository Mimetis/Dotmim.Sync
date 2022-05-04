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

        public virtual Task<ScopeInfo> GetClientScopeInfoAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => GetClientScopeInfoAsync(SyncOptions.DefaultScopeName, default, connection, transaction, cancellationToken, progress);

        public virtual Task<ScopeInfo> GetClientScopeInfoAsync(SyncSetup setup, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => GetClientScopeInfoAsync(SyncOptions.DefaultScopeName, setup, connection, transaction, cancellationToken, progress);

        public virtual async Task<ScopeInfo> GetClientScopeInfoAsync(string scopeName, SyncSetup setup = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Reading, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var localScope = await InternalGetClientScopeInfo(scopeName, setup,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return localScope;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeName, ex);
            }
        }

        /// <summary>
        /// Get all scopes. scopeName arg is just here for logging purpose and is not used
        /// </summary>
        public virtual async Task<List<ScopeInfo>> GetAllClientScopesInfoAsync(string scopeName = SyncOptions.DefaultScopeName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Reading, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var localScopes = await InternalLoadAllClientScopesAsync(scopeName,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return localScopes;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeName, ex);
            }
        }

        /// <summary>
        /// Write a server scope 
        /// </summary> 
        public virtual async Task<ScopeInfo> SaveClientScopeAsync(ScopeInfo scopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(scopeInfo.Name, SyncMode.Writing, SyncStage.ScopeWriting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var exists = await this.InternalExistsScopeInfoTableAsync(scopeInfo.Name, DbScopeType.Client, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(scopeInfo.Name, DbScopeType.Client, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // Write scopes locally
                scopeInfo = await this.InternalSaveClientScopeAsync(scopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return scopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeInfo.Name, ex);
            }
        }

        /// <summary>
        /// Internal load a ScopeInfo by scope name
        /// </summary>
        internal async Task<ScopeInfo> InternalGetClientScopeInfo(string scopeName, SyncSetup setup, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var exists = await this.InternalExistsScopeInfoTableAsync(scopeName, DbScopeType.Client, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (!exists)
                await this.InternalCreateScopeInfoTableAsync(scopeName, DbScopeType.Client, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            // Get scope from local client 
            var localScope = await this.InternalLoadClientScopeAsync(scopeName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            var shouldSave = false;

            // Get scopeId representing the client unique id
            if (localScope == null)
            {
                shouldSave = true;

                localScope = this.InternalCreateScopeInfo(scopeName, DbScopeType.Client) as ScopeInfo;

                // Checking if we have already some scopes
                // Then gets the first scope to get the id
                var allScopes = await this.InternalLoadAllClientScopesAsync(scopeName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (allScopes.Count > 0)
                {
                    if (setup != null)
                    {
                        // Get the first scope with an existing setup
                        var firstScope = allScopes.FirstOrDefault(sc => sc.Setup != null);

                        if (firstScope != null)
                        {
                            localScope.Id = firstScope.Id;

                            if (setup.TrackingTablesPrefix != firstScope.Setup.TrackingTablesPrefix)
                                throw new Exception($"Can't add a new setup with different tracking table prefix. Please use same tracking table prefix as your first setup ([\"{firstScope.Setup.TrackingTablesPrefix}\"])");

                            if (setup.TrackingTablesSuffix != firstScope.Setup.TrackingTablesSuffix)
                                throw new Exception($"Can't add a new setup with different tracking table suffix. Please use same tracking table suffix as your first setup ([\"{firstScope.Setup.TrackingTablesSuffix}\"])");

                            if (setup.TriggersPrefix != firstScope.Setup.TriggersPrefix)
                                throw new Exception($"Can't add a new setup with different trigger prefix. Please use same trigger prefix as your first setup ([\"{firstScope.Setup.TriggersPrefix}\"])");

                            if (setup.TriggersSuffix != firstScope.Setup.TriggersSuffix)
                                throw new Exception($"Can't add a new setup with different trigger suffix. Please use same trigger suffix as your first setup ([\"{firstScope.Setup.TriggersSuffix}\"])");
                        }
                    }
                    else
                    {
                        localScope.Id = allScopes[0].Id;
                    }
                }
            }

            // if localScope is empty, grab the schema locally from setup 
            if (localScope.Setup == null && localScope.Schema == null && setup != null && setup.Tables.Count > 0)
            {
                var schema = await this.InternalGetSchemaAsync(scopeName, setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                localScope.Setup = setup;
                localScope.Schema = schema;
                shouldSave = true;
            }

            if (shouldSave)
            {
                localScope = await this.InternalSaveClientScopeAsync(localScope, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // if not shouldSave, that means we already raised this event before
                var scopeLoadedArgs = new ScopeLoadedArgs(this.GetContext(scopeName), scopeName, DbScopeType.Client, localScope, connection, transaction);
                await this.InterceptAsync(scopeLoadedArgs, progress, cancellationToken).ConfigureAwait(false);

            }

            return localScope;
        }

        /// <summary>
        /// Internal exists scope
        /// </summary>
        internal async Task<bool> InternalExistsClientScopeInfoAsync(string scopeId, string scopeName, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // Get exists command
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var existsCommand = scopeBuilder.GetCommandAsync(DbScopeCommandType.ExistClientScopeInfo, connection, transaction);

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
        /// Internal load a scope by scope name
        /// </summary>
        internal async Task<ScopeInfo> InternalLoadClientScopeAsync(string scopeName, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetClientScopeInfo, connection, transaction);

            if (command == null) return null;

            var ctx = this.GetContext(scopeName);

            DbSyncAdapter.SetParameterValue(command, "sync_scope_name", scopeName);

            var action = new ScopeLoadingArgs(ctx, scopeName, DbScopeType.Client, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return null;

            await this.InterceptAsync(new DbCommandArgs(ctx, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            using DbDataReader reader = await action.Command.ExecuteReaderAsync().ConfigureAwait(false);

            ScopeInfo scopeInfo = null;

            if (reader.Read())
                scopeInfo = InternalReadClientScopeInfo(reader);

            reader.Close();

            if (scopeInfo?.Schema != null)
                scopeInfo.Schema.EnsureSchema();

            var scopeLoadedArgs = new ScopeLoadedArgs(ctx, scopeName, DbScopeType.Client, scopeInfo, connection, transaction);
            await this.InterceptAsync(scopeLoadedArgs, progress, cancellationToken).ConfigureAwait(false);

            return scopeInfo;
        }

        /// <summary>
        /// Internal load all scopes. scopeName arg is just here for getting context
        /// </summary>
        internal async Task<List<ScopeInfo>> InternalLoadAllClientScopesAsync(string scopeName, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetAllClientScopesInfo, connection, transaction);

            if (command == null) return null;

            var ctx = this.GetContext(scopeName);

            var action = new ScopeLoadingArgs(ctx, scopeName, DbScopeType.Client, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return null;

            var scopes = new List<ScopeInfo>();

            await this.InterceptAsync(new DbCommandArgs(ctx, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            using DbDataReader reader = await action.Command.ExecuteReaderAsync().ConfigureAwait(false);

            while (reader.Read())
            {
                var scopeInfo = InternalReadClientScopeInfo(reader);

                if (scopeInfo.Schema != null)
                    scopeInfo.Schema.EnsureSchema();

                scopes.Add(scopeInfo);
            }

            reader.Close();

            foreach (var scopeInfo in scopes)
            {
                var scopeLoadedArgs = new ScopeLoadedArgs(ctx, scopeName, DbScopeType.Client, scopeInfo, connection, transaction);
                await this.InterceptAsync(scopeLoadedArgs, progress, cancellationToken).ConfigureAwait(false);

            }

            return scopes;
        }

        /// <summary>
        /// Internal upsert scope info in a scope table
        /// </summary>
        internal async Task<ScopeInfo> InternalSaveClientScopeAsync(ScopeInfo scopeInfo, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var scopeExists = await InternalExistsClientScopeInfoAsync(scopeInfo.Id.ToString(), scopeInfo.Name, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            DbCommand command;
            if (scopeExists)
                command = scopeBuilder.GetCommandAsync(DbScopeCommandType.UpdateClientScopeInfo, connection, transaction);
            else
                command = scopeBuilder.GetCommandAsync(DbScopeCommandType.InsertClientScopeInfo, connection, transaction);

            if (command == null) return null;

            command = InternalSetSaveClientScopeInfoParameters(scopeInfo, command);

            var ctx = this.GetContext(scopeInfo.Name);

            var action = new ScopeSavingArgs(ctx, scopeBuilder.ScopeInfoTableName.ToString(), DbScopeType.Client, scopeInfo, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return default;

            await this.InterceptAsync(new DbCommandArgs(ctx, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            using DbDataReader reader = await action.Command.ExecuteReaderAsync().ConfigureAwait(false);

            reader.Read();

            scopeInfo = InternalReadClientScopeInfo(reader);

            reader.Close();

            await this.InterceptAsync(new ScopeSavedArgs(ctx, scopeBuilder.ScopeInfoTableName.ToString(), DbScopeType.Client, scopeInfo, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            return scopeInfo;
        }

        private DbCommand InternalSetSaveClientScopeInfoParameters(ScopeInfo scopeInfo, DbCommand command)
        {
            DbSyncAdapter.SetParameterValue(command, "sync_scope_id", scopeInfo.Id.ToString());
            DbSyncAdapter.SetParameterValue(command, "sync_scope_name", scopeInfo.Name);
            DbSyncAdapter.SetParameterValue(command, "sync_scope_schema", scopeInfo.Schema == null ? DBNull.Value : (object)JsonConvert.SerializeObject(scopeInfo.Schema));
            DbSyncAdapter.SetParameterValue(command, "sync_scope_setup", scopeInfo.Setup == null ? DBNull.Value : (object)JsonConvert.SerializeObject(scopeInfo.Setup));
            DbSyncAdapter.SetParameterValue(command, "sync_scope_version", scopeInfo.Version);
            DbSyncAdapter.SetParameterValue(command, "scope_last_sync", scopeInfo.LastSync.HasValue ? (object)scopeInfo.LastSync.Value : DBNull.Value);
            DbSyncAdapter.SetParameterValue(command, "scope_last_sync_timestamp", scopeInfo.LastSyncTimestamp);
            DbSyncAdapter.SetParameterValue(command, "scope_last_server_sync_timestamp", scopeInfo.LastServerSyncTimestamp);
            DbSyncAdapter.SetParameterValue(command, "scope_last_sync_duration", scopeInfo.LastSyncDuration);

            return command;
        }

        private ScopeInfo InternalReadClientScopeInfo(DbDataReader reader)
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

    }
}
