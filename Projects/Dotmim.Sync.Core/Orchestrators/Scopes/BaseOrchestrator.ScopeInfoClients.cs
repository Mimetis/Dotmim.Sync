﻿using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Extensions;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public partial class BaseOrchestrator
    {
        /// <summary>
        /// Get all scopes info clients instances
        /// <example>
        /// This code gets the min last sync timestamp
        /// <code>
        /// var cAllScopeInfoClients = await agent.LocalOrchestrator.GetAllScopeInfoClientsAsync();
        /// 
        /// var minServerTimeStamp = cAllScopeInfoClients.Min(sic => sic.LastServerSyncTimestamp);
        /// var minClientTimeStamp = cAllScopeInfoClients.Min(sic => sic.LastSyncTimestamp);
        /// var minLastSync = cAllScopeInfoClients.Min(sic => sic.LastSync);
        /// </code>
        /// </example>
        /// </summary>
        public virtual async Task<List<ScopeInfoClient>> GetAllScopeInfoClientsAsync(DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);

            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ScopeLoading, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    bool existsCScopeInfoClient;
                    (context, existsCScopeInfoClient) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    if (!existsCScopeInfoClient)
                        (context, _) = await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                            runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    var scopeInfoClients = await InternalLoadAllScopeInfoClientsAsync(context,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    await runner.CommitAsync().ConfigureAwait(false);

                    return scopeInfoClients;
                }
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Save a <see cref="ScopeInfoClient"/> instance to the local data source.
        /// <example>
        /// <code>
        ///  var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync();
        ///  
        ///  if (cScopeInfoClient.IsNewScope)
        ///  {
        ///    cScopeInfoClient.IsNewScope = false;
        ///    cScopeInfoClient.LastSync = DateTime.Now;
        ///    cScopeInfoClient.LastSyncTimestamp = 0;
        ///    cScopeInfoClient.LastServerSyncTimestamp = 0;
        ///  
        ///    await agent.LocalOrchestrator.SaveScopeInfoClientAsync(cScopeInfoClient);
        ///  }
        /// </code>
        /// </example>
        /// </summary>
        /// <returns><see cref="ScopeInfoClient"/> instance.</returns>
        public virtual async Task<ScopeInfoClient> SaveScopeInfoClientAsync(ScopeInfoClient scopeInfoClient, DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfoClient);
            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.ScopeWriting, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    bool exists;
                    (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    if (!exists)
                        await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                            runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    (context, scopeInfoClient) = await this.InternalSaveScopeInfoClientAsync(scopeInfoClient, context,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    await runner.CommitAsync().ConfigureAwait(false);

                    return scopeInfoClient;
                }
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Internal Sxists Scope Info Client
        /// </summary>
        internal virtual async Task<(SyncContext context, bool exists)>
            InternalExistsScopeInfoClientAsync(SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            try
            {

                if (string.IsNullOrEmpty(context.ScopeName) || !context.ClientId.HasValue)
                    return (context, false);

                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ScopeLoading, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {

                    // Get exists command
                    var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                    using var existsCommand = scopeBuilder.GetCommandAsync(DbScopeCommandType.ExistScopeInfoClient, runner.Connection, runner.Transaction);

                    if (existsCommand == null) return (context, false);

                    InternalSetParameterValue(existsCommand, "sync_scope_name", context.ScopeName);
                    InternalSetParameterValue(existsCommand, "sync_scope_id", context.ClientId);
                    InternalSetParameterValue(existsCommand, "sync_scope_hash", context.Hash);

                    await this.InterceptAsync(new ExecuteCommandArgs(context, existsCommand, default, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
                    var exists = Convert.ToInt32(existsResultObject) > 0;
                    return (context, exists);
                }
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Internal load a scope info client
        /// </summary>
        internal virtual async Task<(SyncContext context, ScopeInfoClient scopeInfoClient)>
            InternalLoadScopeInfoClientAsync(SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            try
            {
                var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ScopeLoading, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    using var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetScopeInfoClient, runner.Connection, runner.Transaction);

                    if (command == null) return (context, null);

                    InternalSetParameterValue(command, "sync_scope_name", context.ScopeName);
                    InternalSetParameterValue(command, "sync_scope_id", context.ClientId);
                    InternalSetParameterValue(command, "sync_scope_hash", context.Hash);

                    await this.InterceptAsync(new ExecuteCommandArgs(context, command, default, runner.Connection, runner.Transaction), progress, cancellationToken).ConfigureAwait(false);

                    using DbDataReader reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

                    ScopeInfoClient scopeInfoClient = null;

                    if (await reader.ReadAsync().ConfigureAwait(false))
                        scopeInfoClient = InternalReadScopeInfoClient(reader);

                    reader.Close();

                    command.Dispose();

                    return (context, scopeInfoClient);
                }
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Internal load all client histories scopes
        /// </summary>
        internal virtual async Task<List<ScopeInfoClient>> InternalLoadAllScopeInfoClientsAsync(SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            try
            {
                var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ScopeLoading, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    using var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetAllScopeInfoClients, runner.Connection, runner.Transaction);

                    if (command == null) return default;

                    var scopeInfoClients = new List<ScopeInfoClient>();

                    await this.InterceptAsync(new ExecuteCommandArgs(context, command, default, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    using DbDataReader reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        var scopeInfoClient = InternalReadScopeInfoClient(reader);

                        scopeInfoClients.Add(scopeInfoClient);
                    }

                    reader.Close();
                    command.Dispose();
                    return scopeInfoClients;
                }
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Internal upsert scope info client
        /// </summary>
        internal async Task<(SyncContext context, ScopeInfoClient scopeInfoClient)>
            InternalSaveScopeInfoClientAsync(ScopeInfoClient scopeInfoClient, SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            try
            {
                var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ScopeLoading, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    bool scopeExists;
                    (context, scopeExists) = await InternalExistsScopeInfoClientAsync(context,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    DbCommand command;
                    if (scopeExists)
                        command = scopeBuilder.GetCommandAsync(DbScopeCommandType.UpdateScopeInfoClient, runner.Connection, runner.Transaction);
                    else
                        command = scopeBuilder.GetCommandAsync(DbScopeCommandType.InsertScopeInfoClient, runner.Connection, runner.Transaction);

                    if (command == null) return (context, null);

                    InternalSetSaveScopeInfoClientParameters(scopeInfoClient, command);

                    //var action = new ScopeSavingArgs(context, scopeBuilder.ScopeInfoTableName.ToString(), scopeInfoClient, command, runner.Connection, runner.Transaction);
                    //await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

                    //if (action.Cancel || action.Command == null)
                    //    return default;

                    await this.InterceptAsync(new ExecuteCommandArgs(context, command, default, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    using DbDataReader reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

                    await reader.ReadAsync().ConfigureAwait(false);

                    var newScopeInfoClient = InternalReadScopeInfoClient(reader);

                    reader.Close();

                    //await this.InterceptAsync(new ScopeSavedArgs(context, scopeBuilder.ScopeInfoTableName.ToString(), newScopeInfoClient, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
                    //command.Dispose();

                    return (context, newScopeInfoClient);
                }
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Create an instance of scope info client
        /// </summary>
        internal ScopeInfoClient InternalCreateScopeInfoClient(string scopeName, SyncParameters syncParameters = null)
        {
            var scopeInfoClient = new ScopeInfoClient
            {
                Id = Guid.NewGuid(),
                Name = scopeName,
                IsNewScope = true,
                LastSync = null,
                Hash = syncParameters == null || syncParameters.Count <= 0 ? SyncParameters.DefaultScopeHash : syncParameters.GetHash(),
                Parameters = syncParameters,
            };

            return scopeInfoClient;
        }

        private DbCommand InternalSetSaveScopeInfoClientParameters(ScopeInfoClient scopeInfoClient, DbCommand command)
        {
            InternalSetParameterValue(command, "sync_scope_id", scopeInfoClient.Id.ToString());
            InternalSetParameterValue(command, "sync_scope_name", scopeInfoClient.Name);
            InternalSetParameterValue(command, "sync_scope_hash", scopeInfoClient.Hash);
            InternalSetParameterValue(command, "scope_last_sync_timestamp", scopeInfoClient.LastSyncTimestamp);
            InternalSetParameterValue(command, "scope_last_server_sync_timestamp", scopeInfoClient.LastServerSyncTimestamp);
            InternalSetParameterValue(command, "scope_last_sync", scopeInfoClient.LastSync.HasValue ? (object)scopeInfoClient.LastSync.Value : DBNull.Value);
            InternalSetParameterValue(command, "scope_last_sync_duration", scopeInfoClient.LastSyncDuration);
            InternalSetParameterValue(command, "sync_scope_properties", scopeInfoClient.Properties);
            InternalSetParameterValue(command, "sync_scope_errors", scopeInfoClient.Errors);
            InternalSetParameterValue(command, "sync_scope_parameters", scopeInfoClient.Parameters != null ? serializer.Serialize(scopeInfoClient.Parameters).ToUtf8String() : DBNull.Value);

            return command;
        }

        private ScopeInfoClient InternalReadScopeInfoClient(DbDataReader reader)
        {
            var scopeInfoClient = new ScopeInfoClient
            {
                Id = reader.GetGuid(reader.GetOrdinal("sync_scope_id")),
                Name = reader["sync_scope_name"] as string,
                Hash = reader["sync_scope_hash"] as string,
                LastSync = reader["scope_last_sync"] != DBNull.Value ? reader.GetDateTime(reader.GetOrdinal("scope_last_sync")) : null,
                LastSyncDuration = reader["scope_last_sync_duration"] != DBNull.Value ? reader.GetInt64(reader.GetOrdinal("scope_last_sync_duration")) : 0L,
                LastSyncTimestamp = reader["scope_last_sync_timestamp"] != DBNull.Value ? reader.GetInt64(reader.GetOrdinal("scope_last_sync_timestamp")) : null,
                LastServerSyncTimestamp = reader["scope_last_server_sync_timestamp"] != DBNull.Value ? reader.GetInt64(reader.GetOrdinal("scope_last_server_sync_timestamp")) : null,
                Properties = reader["sync_scope_properties"] as string,
                Errors = reader["sync_scope_errors"] as string,
                Parameters = reader["sync_scope_parameters"] != DBNull.Value ? serializer.Deserialize<SyncParameters>((string)reader["sync_scope_parameters"]) : null

            };
            scopeInfoClient.IsNewScope = scopeInfoClient.LastSync == null;

            return scopeInfoClient;
        }

    }
}