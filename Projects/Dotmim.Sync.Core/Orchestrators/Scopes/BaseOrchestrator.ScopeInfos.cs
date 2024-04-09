using Dotmim.Sync.Builders;
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
        /// Get all scope infos from a data source. A scope contains the <see cref="SyncSetup"/> setup and the <see cref="SyncSet"/> schema.
        /// <para>
        /// If the <strong>scope_info</strong> table is not existing, it will be created.
        /// </para>
        /// <example>
        /// <code>
        ///  var localOrchestrator = new LocalOrchestrator(clientProvider);
        ///  var scopeInfo = await localOrchestrator.GetAllScopeInfosAsync();
        /// </code>
        /// </example>
        /// </summary>
        /// <returns><see cref="ScopeInfo"/> instance.</returns>
        public virtual async Task<List<ScopeInfo>> GetAllScopeInfosAsync(DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);

            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.ScopeLoading, connection, transaction).ConfigureAwait(false);

                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                List<ScopeInfo> localScopes;
                (context, localScopes) = await InternalLoadAllScopeInfosAsync(context,
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
        /// Save a <see cref="ScopeInfo"/> instance to the local data source.
        /// <example>
        /// <code>
        ///  var scopeInfo = await localOrchestrator.GetScopeInfoAsync();
        ///  scopeInfo.Setup = setup;
        ///  scopeInfo.Schema = schema;
        ///  scopeInfo.ScopeName = "v1";
        ///  await localOrchestrator.SaveScopeInfoAsync(scopeInfo);
        /// </code>
        /// </example>
        /// </summary>
        /// <returns><see cref="ScopeInfo"/> instance.</returns>
        public virtual async Task<ScopeInfo> SaveScopeInfoAsync(ScopeInfo scopeInfo, DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.ScopeWriting, connection, transaction).ConfigureAwait(false);

                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                (context, scopeInfo) = await this.InternalSaveScopeInfoAsync(scopeInfo, context,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return scopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }


        /// <summary>
        /// Delete a <see cref="ScopeInfo"/> instance to the local data source.
        /// <example>
        /// <code>
        ///  var scopeInfo = await localOrchestrator.GetScopeInfoAsync("v0");
        ///  await localOrchestrator.DeleteScopeInfoAsync(scopeInfo);
        /// </code>
        /// </example>
        /// </summary>
        /// <returns><see cref="ScopeInfo"/> instance.</returns> 
        public virtual async Task<bool> DeleteScopeInfoAsync(ScopeInfo scopeInfo, DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.ScopeWriting, connection, transaction).ConfigureAwait(false);

                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                bool isDeleted;
                (context, isDeleted) = await this.InternalDeleteScopeInfoAsync(scopeInfo, context,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return isDeleted;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }


        /// <summary>
        /// Internal load a ScopeInfo by scope name
        /// </summary>
        internal async Task<(SyncContext context, ScopeInfo scopeInfo)> InternalGetScopeInfoAsync(SyncContext context,
            DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (!exists)
                    (context, _) = await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfo, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                ScopeInfo localScopeInfo;
                (context, localScopeInfo) = await this.InternalLoadScopeInfoAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return (context, localScopeInfo);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }


        /// <summary>
        /// Internal load a scope by scope name
        /// </summary>
        internal virtual async Task<(SyncContext context, ScopeInfo scopeInfo)> InternalLoadScopeInfoAsync(SyncContext context,
            DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            try
            {
                var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                using var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetScopeInfo, runner.Connection, runner.Transaction);

                if (command == null) return (context, null);

                InternalSetParameterValue(command, "sync_scope_name", context.ScopeName);

                var action = new ScopeInfoLoadingArgs(context, context.ScopeName, command, runner.Connection, runner.Transaction);
                await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

                if (action.Cancel || action.Command == null)
                    return (context, null);

                await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                using DbDataReader reader = await action.Command.ExecuteReaderAsync().ConfigureAwait(false);

                ScopeInfo scopeInfo = null;

                if (reader.Read())
                    scopeInfo = await InternalReadScopeInfo(reader);

                reader.Close();

                if (scopeInfo?.Schema != null)
                    scopeInfo.Schema.EnsureSchema();

                var scopeLoadedArgs = new ScopeInfoLoadedArgs(context, scopeInfo, runner.Connection, runner.Transaction);
                await this.InterceptAsync(scopeLoadedArgs, progress, cancellationToken).ConfigureAwait(false);
                scopeInfo = scopeLoadedArgs.ScopeInfo;

                action.Command.Dispose();

                return (context, scopeInfo);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }


        /// <summary>
        /// Internal exists scope
        /// </summary>
        internal async Task<(SyncContext context, bool exists)> InternalExistsScopeInfoAsync(string scopeName, SyncContext context,
            DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            try
            {
                // Get exists command
                var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                using var existsCommand = scopeBuilder.GetCommandAsync(DbScopeCommandType.ExistScopeInfo, runner.Connection, runner.Transaction);

                if (existsCommand == null) return (context, false);

                InternalSetParameterValue(existsCommand, "sync_scope_name", scopeName);

                if (existsCommand == null)
                    return (context, false);

                await this.InterceptAsync(new ExecuteCommandArgs(context, existsCommand, default, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
                var exists = Convert.ToInt32(existsResultObject) > 0;
                return (context, exists);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Internal load all scopes. scopeName arg is just here for getting context
        /// </summary>
        internal async Task<(SyncContext context, List<ScopeInfo> scopeInfos)> InternalLoadAllScopeInfosAsync(SyncContext context,
            DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            try
            {
                var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                using var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetAllScopeInfos, runner.Connection, runner.Transaction);

                if (command == null) return (context, null);

                var clientScopes = new List<ScopeInfo>();

                await this.InterceptAsync(new ExecuteCommandArgs(context, command, default, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                using DbDataReader reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

                while (reader.Read())
                {
                    var scopeInfo = await InternalReadScopeInfo(reader);

                    if (scopeInfo.Schema != null)
                        scopeInfo.Schema.EnsureSchema();

                    clientScopes.Add(scopeInfo);
                }

                reader.Close();

                command.Dispose();

                return (context, clientScopes);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Internal upsert scope info in a scope table
        /// </summary>
        internal async Task<(SyncContext context, ScopeInfo clientScopeInfo)> InternalSaveScopeInfoAsync(ScopeInfo scopeInfo, SyncContext context,
            DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            try
            {
                var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ScopeWriting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                bool scopeExists;
                (context, scopeExists) = await InternalExistsScopeInfoAsync(scopeInfo.Name, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                DbCommand command;
                if (scopeExists)
                    command = scopeBuilder.GetCommandAsync(DbScopeCommandType.UpdateScopeInfo, runner.Connection, runner.Transaction);
                else
                    command = scopeBuilder.GetCommandAsync(DbScopeCommandType.InsertScopeInfo, runner.Connection, runner.Transaction);

                if (command == null) return (context, null);

                command = InternalSetSaveScopeInfoParameters(scopeInfo, command);

                var action = new ScopeInfoSavingArgs(context, scopeInfo, command, runner.Connection, runner.Transaction);
                await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

                if (action.Cancel || action.Command == null)
                    return (context, null);

                await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                using DbDataReader reader = await action.Command.ExecuteReaderAsync().ConfigureAwait(false);

                reader.Read();

                scopeInfo = await InternalReadScopeInfo(reader);

                reader.Close();

                // ensure schema on tables
                if (scopeInfo.Schema != null)
                    scopeInfo.Schema.EnsureSchema();

                await this.InterceptAsync(new ScopeInfoSavedArgs(context, scopeInfo, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                action.Command.Dispose();

                return (context, scopeInfo);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Internal delete scope info in a scope table
        /// </summary>
        internal async Task<(SyncContext context, bool deleted)> InternalDeleteScopeInfoAsync(ScopeInfo scopeInfo, SyncContext context,
            DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            try
            {
                var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ScopeWriting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                bool scopeExists;
                (context, scopeExists) = await InternalExistsScopeInfoAsync(scopeInfo.Name, context,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (!scopeExists)
                    return (context, true);

                using var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.DeleteScopeInfo, runner.Connection, runner.Transaction);

                InternalSetDeleteScopeInfoParameters(scopeInfo, command);

                var action = new ScopeInfoSavingArgs(context, scopeInfo, command, runner.Connection, runner.Transaction);
                await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

                if (action.Cancel || action.Command == null)
                    return (context, false);

                await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

                await this.InterceptAsync(new ScopeInfoSavedArgs(context, scopeInfo, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                action.Command.Dispose();

                return (context, true);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }


        internal ScopeInfo InternalCreateScopeInfo(string scopeName) => new ScopeInfo
        {
            Name = scopeName,
            Version = SyncVersion.Current.ToString(),
        };


        private DbCommand InternalSetSaveScopeInfoParameters(ScopeInfo scopeInfo, DbCommand command)
        {
            InternalSetParameterValue(command, "sync_scope_name", scopeInfo.Name);
            InternalSetParameterValue(command, "sync_scope_schema", scopeInfo.Schema == null ? DBNull.Value : serializer.Serialize(scopeInfo.Schema).ToUtf8String());
            InternalSetParameterValue(command, "sync_scope_setup", scopeInfo.Setup == null ? DBNull.Value : serializer.Serialize(scopeInfo.Setup).ToUtf8String());
            InternalSetParameterValue(command, "sync_scope_version", scopeInfo.Version);
            InternalSetParameterValue(command, "sync_scope_last_clean_timestamp", !scopeInfo.LastCleanupTimestamp.HasValue ? DBNull.Value : scopeInfo.LastCleanupTimestamp);
            InternalSetParameterValue(command, "sync_scope_properties", scopeInfo.Properties == null ? DBNull.Value : scopeInfo.Properties);

            return command;
        }

        private DbCommand InternalSetDeleteScopeInfoParameters(ScopeInfo scopeInfo, DbCommand command)
        {
            InternalSetParameterValue(command, "sync_scope_name", scopeInfo.Name);

            return command;
        }

        public static void InternalSetParameterValue(DbCommand command, string parameterName, object value)
        {
            var parameter = DbSyncAdapter.InternalGetParameter(command, parameterName);
            if (parameter == null)
                return;

            if (value == null || value == DBNull.Value)
                parameter.Value = DBNull.Value;
            else
                parameter.Value = SyncTypeConverter.TryConvertFromDbType(value, parameter.DbType);
        }



        private async Task<ScopeInfo> InternalReadScopeInfo(DbDataReader reader)
        {
            var clientScopeInfo = new ScopeInfo
            {
                Name = reader["sync_scope_name"] as string,
                Schema = reader["sync_scope_schema"] == DBNull.Value ? null : serializer.Deserialize<SyncSet>((string)reader["sync_scope_schema"]),
                Setup = reader["sync_scope_setup"] == DBNull.Value ? null : serializer.Deserialize<SyncSetup>((string)reader["sync_scope_setup"]),
                Version = reader["sync_scope_version"] as string,
                LastCleanupTimestamp = reader["sync_scope_last_clean_timestamp"] != DBNull.Value ? reader.GetInt64(reader.GetOrdinal("sync_scope_last_clean_timestamp")) : null,
                Properties = reader["sync_scope_properties"] as string,

            };
            return clientScopeInfo;
        }
    }
}
