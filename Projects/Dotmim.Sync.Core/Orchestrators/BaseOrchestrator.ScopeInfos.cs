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
    public partial class BaseOrchestrator
    {


        /// <summary>
        /// Internal load a ScopeInfo by scope name
        /// </summary>
        internal async Task<(SyncContext context, ScopeInfo scopeInfo)> InternalGetScopeInfoAsync(
            SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
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
        internal async Task<(SyncContext context, ScopeInfo scopeInfo)>
            InternalLoadScopeInfoAsync(SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            using var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetScopeInfo, connection, transaction);

            if (command == null) return (context, null);

            SetParameterValue(command, "sync_scope_name", context.ScopeName);

            var action = new ScopeInfoLoadingArgs(context, context.ScopeName, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return (context, null);

            await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            using DbDataReader reader = await action.Command.ExecuteReaderAsync().ConfigureAwait(false);

            ScopeInfo scopeInfo = null;

            if (reader.Read())
                scopeInfo = InternalReadScopeInfo(reader);

            reader.Close();

            if (scopeInfo?.Schema != null)
                scopeInfo.Schema.EnsureSchema();

            if (scopeInfo != null)
            {
                var scopeLoadedArgs = new ScopeInfoLoadedArgs(context, scopeInfo, connection, transaction);
                await this.InterceptAsync(scopeLoadedArgs, progress, cancellationToken).ConfigureAwait(false);
                scopeInfo = scopeLoadedArgs.ScopeInfo;
            }

            action.Command.Dispose();

            return (context, scopeInfo);
        }

        /// <summary>
        /// Get all scopes. scopeName arg is just here for logging purpose and is not used
        /// </summary>
        public virtual async Task<List<ScopeInfo>>
            GetAllScopeInfosAsync(string scopeName = SyncOptions.DefaultScopeName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);

            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

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
        /// Write a Client Scope Info in a client database
        /// </summary> 
        public virtual async Task<ScopeInfo>
            SaveScopeInfoAsync(ScopeInfo scopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.ScopeWriting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // Write scopes locally
                (context, scopeInfo) = await this.InternalSaveScopeInfoAsync(scopeInfo, context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return scopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }


        /// <summary>
        /// Delete a Client Scope Info from a client database
        /// </summary> 
        public virtual async Task<bool>
            DeleteScopeInfoAsync(ScopeInfo scopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.ScopeWriting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                bool isDeleted;
                // Write scopes locally
                (context, isDeleted) = await this.InternalDeleteScopeInfoAsync(scopeInfo, context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return isDeleted;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Internal exists scope
        /// </summary>
        internal async Task<(SyncContext context, bool exists)> InternalExistsScopeInfoAsync(string scopeName, SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // Get exists command
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            using var existsCommand = scopeBuilder.GetCommandAsync(DbScopeCommandType.ExistScopeInfo, connection, transaction);

            if (existsCommand == null) return (context, false);

            SetParameterValue(existsCommand, "sync_scope_name", scopeName);

            if (existsCommand == null)
                return (context, false);

            await this.InterceptAsync(new ExecuteCommandArgs(context, existsCommand, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;
            return (context, exists);
        }

        /// <summary>
        /// Internal load all scopes. scopeName arg is just here for getting context
        /// </summary>
        internal async Task<(SyncContext context, List<ScopeInfo> scopeInfos)>
            InternalLoadAllScopeInfosAsync(SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            using var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.GetAllScopeInfos, connection, transaction);

            if (command == null) return (context, null);

            var clientScopes = new List<ScopeInfo>();

            await this.InterceptAsync(new ExecuteCommandArgs(context, command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            using DbDataReader reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

            while (reader.Read())
            {
                var scopeInfo = InternalReadScopeInfo(reader);

                if (scopeInfo.Schema != null)
                    scopeInfo.Schema.EnsureSchema();

                clientScopes.Add(scopeInfo);
            }

            reader.Close();

            command.Dispose();

            return (context, clientScopes);
        }

        /// <summary>
        /// Internal upsert scope info in a scope table
        /// </summary>
        internal async Task<(SyncContext context, ScopeInfo clientScopeInfo)>
            InternalSaveScopeInfoAsync(ScopeInfo scopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            bool scopeExists;
            (context, scopeExists) = await InternalExistsScopeInfoAsync(scopeInfo.Name, context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            DbCommand command;
            if (scopeExists)
                command = scopeBuilder.GetCommandAsync(DbScopeCommandType.UpdateScopeInfo, connection, transaction);
            else
                command = scopeBuilder.GetCommandAsync(DbScopeCommandType.InsertScopeInfo, connection, transaction);

            if (command == null) return (context, null);

            command = InternalSetSaveScopeInfoParameters(scopeInfo, command);

            var action = new ScopeInfoSavingArgs(context, scopeInfo, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return (context, null);

            await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            using DbDataReader reader = await action.Command.ExecuteReaderAsync().ConfigureAwait(false);

            reader.Read();

            scopeInfo = InternalReadScopeInfo(reader);

            reader.Close();

            await this.InterceptAsync(new ScopeInfoSavedArgs(context, scopeInfo, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            action.Command.Dispose();

            return (context, scopeInfo);
        }

        /// <summary>
        /// Internal delete scope info in a scope table
        /// </summary>
        internal async Task<(SyncContext context, bool deleted)>
            InternalDeleteScopeInfoAsync(ScopeInfo scopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            bool scopeExists;
            (context, scopeExists) = await InternalExistsScopeInfoAsync(scopeInfo.Name, context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (!scopeExists)
                return (context, true);

            using var command = scopeBuilder.GetCommandAsync(DbScopeCommandType.DeleteScopeInfo, connection, transaction);

            InternalSetDeleteScopeInfoParameters(scopeInfo, command);

            var action = new ScopeInfoSavingArgs(context, scopeInfo, command, connection, transaction);
            await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return (context, false);

            await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

            await this.InterceptAsync(new ScopeInfoSavedArgs(context, scopeInfo, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            action.Command.Dispose();

            return (context, true);
        }


        internal ScopeInfo InternalCreateScopeInfo(string scopeName)
        {
            return new ScopeInfo
            {
                Name = scopeName,
                Version = SyncVersion.Current.ToString()
            };
        }


        private DbCommand InternalSetSaveScopeInfoParameters(ScopeInfo scopeInfo, DbCommand command)
        {
            SetParameterValue(command, "sync_scope_name", scopeInfo.Name);
            SetParameterValue(command, "sync_scope_schema", scopeInfo.Schema == null ? DBNull.Value : JsonConvert.SerializeObject(scopeInfo.Schema));
            SetParameterValue(command, "sync_scope_setup", scopeInfo.Setup == null ? DBNull.Value : JsonConvert.SerializeObject(scopeInfo.Setup));
            SetParameterValue(command, "sync_scope_version", scopeInfo.Version);
            SetParameterValue(command, "sync_scope_last_clean_timestamp", scopeInfo.LastCleanupTimestamp);
            SetParameterValue(command, "sync_scope_properties", scopeInfo.Properties == null ? DBNull.Value : scopeInfo.Properties);


            return command;
        }

        private DbCommand InternalSetDeleteScopeInfoParameters(ScopeInfo scopeInfo, DbCommand command)
        {
            SetParameterValue(command, "sync_scope_name", scopeInfo.Name);

            return command;
        }

        private ScopeInfo InternalReadScopeInfo(DbDataReader reader)
        {
            var clientScopeInfo = new ScopeInfo
            {
                Name = reader["sync_scope_name"] as string,
                Schema = reader["sync_scope_schema"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSet>((string)reader["sync_scope_schema"]),
                Setup = reader["sync_scope_setup"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSetup>((string)reader["sync_scope_setup"]),
                Version = reader["sync_scope_version"] as string,
                LastCleanupTimestamp = reader["sync_scope_last_clean_timestamp"] != DBNull.Value ? reader.GetInt64(reader.GetOrdinal("sync_scope_last_clean_timestamp")) : 0L,
                Properties = reader["sync_scope_properties"] as string,

            };
            return clientScopeInfo;
        }

    }
}
