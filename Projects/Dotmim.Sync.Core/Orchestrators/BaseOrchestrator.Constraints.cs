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
        public virtual Task<SyncContext> ResetTableAsync(IScopeInfo scopeInfo, string tableName, string schemaName = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            return ResetTableAsync(scopeInfo, context, tableName, schemaName, connection, transaction, cancellationToken, progress);
        }


        /// <summary>
        /// Reset a table, deleting rows from table and tracking_table
        /// </summary>
        public virtual async Task<SyncContext> ResetTableAsync(IScopeInfo scopeInfo, SyncContext context, string tableName, string schemaName = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return context;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return context;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.None, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var syncAdapter = this.GetSyncAdapter(schemaTable, scopeInfo);
                context = await this.InternalResetTableAsync(scopeInfo, context, syncAdapter, runner.Connection, runner.Transaction).ConfigureAwait(false);
                await runner.CommitAsync().ConfigureAwait(false);

                return context;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }

        /// <summary>
        /// Disabling constraints on one table
        /// </summary>
        public virtual async Task<SyncContext> DisableConstraintsAsync(IScopeInfo scopeInfo, string tableName, string schemaName = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {

                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return context;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return context;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.None, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var syncAdapter = this.GetSyncAdapter(schemaTable, scopeInfo);
                context = await this.InternalDisableConstraintsAsync(scopeInfo, context, syncAdapter, runner.Connection, runner.Transaction).ConfigureAwait(false);
                await runner.CommitAsync().ConfigureAwait(false);
                return context;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }

        /// <summary>
        /// Enabling constraints on one table
        /// </summary>
        public virtual async Task<SyncContext> EnableConstraintsAsync(IScopeInfo scopeInfo, string tableName, string schemaName = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {

                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return context;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return context;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.None, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var syncAdapter = this.GetSyncAdapter(schemaTable, scopeInfo);
                context = await this.InternalEnableConstraintsAsync(scopeInfo, context, syncAdapter, runner.Connection, runner.Transaction).ConfigureAwait(false);
                await runner.CommitAsync().ConfigureAwait(false);
                return context;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Disabling all constraints on synced tables
        /// </summary>
        internal async Task<SyncContext> InternalDisableConstraintsAsync(IScopeInfo scopeInfo, SyncContext context, DbSyncAdapter syncAdapter, DbConnection connection, DbTransaction transaction = null)
        {
            var (command, _) = await syncAdapter.GetCommandAsync(DbCommandType.DisableConstraints, connection, transaction).ConfigureAwait(false);

            if (command == null) return context;

            // Parametrized command timeout established if exist
            if (Options.DbCommandTimeout.HasValue)
            {
                command.CommandTimeout = Options.DbCommandTimeout.Value;
            }

            await this.InterceptAsync(new DbCommandArgs(context, command, connection, transaction)).ConfigureAwait(false);

            await command.ExecuteNonQueryAsync().ConfigureAwait(false);

            command.Dispose();
            return context;
        }

        /// <summary>
        /// Enabling all constraints on synced tables
        /// </summary>
        internal async Task<SyncContext> InternalEnableConstraintsAsync(IScopeInfo scopeInfo, SyncContext context, DbSyncAdapter syncAdapter, DbConnection connection, DbTransaction transaction)
        {
            var (command, _) = await syncAdapter.GetCommandAsync(DbCommandType.EnableConstraints, connection, transaction).ConfigureAwait(false);

            if (command == null) return context;

            // Parametrized command timeout established if exist
            if (Options.DbCommandTimeout.HasValue)
            {
                command.CommandTimeout = Options.DbCommandTimeout.Value;
            }

            await this.InterceptAsync(new DbCommandArgs(context, command, connection, transaction)).ConfigureAwait(false);

            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            command.Dispose();

            return context;
        }

        /// <summary>
        /// Reset a table, deleting rows from table and tracking_table
        /// </summary>
        internal async Task<SyncContext> InternalResetTableAsync(IScopeInfo scopeInfo, SyncContext context, DbSyncAdapter syncAdapter, DbConnection connection, DbTransaction transaction)
        {
            var (command, _) = await syncAdapter.GetCommandAsync(DbCommandType.Reset, connection, transaction);

            if (command != null)
            {
                // Parametrized command timeout established if exist
                if (Options.DbCommandTimeout.HasValue)
                {
                    command.CommandTimeout = Options.DbCommandTimeout.Value;
                }

                await this.InterceptAsync(new DbCommandArgs(context, command, connection, transaction)).ConfigureAwait(false);
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                command.Dispose();
            }

            return context;
        }

    }
}
