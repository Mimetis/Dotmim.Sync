using Dotmim.Sync.Args;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {
        /// <summary>
        /// Reset a table, deleting rows from table and tracking_table
        /// </summary>
        public async Task<bool> ResetTableAsync(string scopeName, string tableName, string schemaName = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Writing, SyncStage.None, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var scopeInfo = await this.InternalGetScopeAsync(scopeName, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                var syncAdapter = this.GetSyncAdapter(schemaTable, scopeInfo);
                await this.InternalResetTableAsync(scopeInfo, syncAdapter, runner.Connection, runner.Transaction).ConfigureAwait(false);
                await runner.CommitAsync().ConfigureAwait(false);
                
                return true;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeName, ex);
            }

        }

        /// <summary>
        /// Disabling constraints on one table
        /// </summary>
        public async Task<bool> DisableConstraintsAsync(string scopeName, string tableName, string schemaName = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Writing, SyncStage.None, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var scopeInfo = await this.InternalGetScopeAsync(scopeName, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                var syncAdapter = this.GetSyncAdapter(schemaTable, scopeInfo);
                await this.InternalDisableConstraintsAsync(scopeInfo, syncAdapter, runner.Connection, runner.Transaction).ConfigureAwait(false);
                await runner.CommitAsync().ConfigureAwait(false);
                return true;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeName, ex);
            }

        }

        /// <summary>
        /// Enabling constraints on one table
        /// </summary>
        public async Task<bool> EnableConstraintsAsync(string scopeName, string tableName, string schemaName = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Writing, SyncStage.None, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var scopeInfo = await this.InternalGetScopeAsync(scopeName, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                var syncAdapter = this.GetSyncAdapter(schemaTable, scopeInfo);
                await this.InternalEnableConstraintsAsync(scopeInfo, syncAdapter, runner.Connection, runner.Transaction).ConfigureAwait(false);
                await runner.CommitAsync().ConfigureAwait(false);
                return true;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeName, ex);
            }
        }

        /// <summary>
        /// Disabling all constraints on synced tables
        /// </summary>
        internal async Task InternalDisableConstraintsAsync(IScopeInfo scopeInfo, DbSyncAdapter syncAdapter, DbConnection connection, DbTransaction transaction = null)
        {
            var (command, _) = await syncAdapter.GetCommandAsync(DbCommandType.DisableConstraints, connection, transaction).ConfigureAwait(false);

            if (command == null) return;
            var context = this.GetContext(scopeInfo.Name);

            await this.InterceptAsync(new DbCommandArgs(context, command, connection, transaction)).ConfigureAwait(false);

            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Enabling all constraints on synced tables
        /// </summary>
        internal async Task InternalEnableConstraintsAsync(IScopeInfo scopeInfo, DbSyncAdapter syncAdapter, DbConnection connection, DbTransaction transaction)
        {
            var (command, _) = await syncAdapter.GetCommandAsync(DbCommandType.EnableConstraints, connection, transaction).ConfigureAwait(false);

            if (command == null) return;
            var context = this.GetContext(scopeInfo.Name);
            await this.InterceptAsync(new DbCommandArgs(context, command, connection, transaction)).ConfigureAwait(false);

            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Reset a table, deleting rows from table and tracking_table
        /// </summary>
        internal async Task<bool> InternalResetTableAsync(IScopeInfo scopeInfo, DbSyncAdapter syncAdapter, DbConnection connection, DbTransaction transaction)
        {
            var (command, _) = await syncAdapter.GetCommandAsync(DbCommandType.Reset, connection, transaction);

            if (command != null)
            {
                var context = this.GetContext(scopeInfo.Name);
                await this.InterceptAsync(new DbCommandArgs(context, command, connection, transaction)).ConfigureAwait(false);
                var rowCount = await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                return rowCount > 0;
            }

            return true;
        }

    }
}
