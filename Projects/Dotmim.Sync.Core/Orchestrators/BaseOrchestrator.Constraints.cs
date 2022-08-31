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
        public virtual Task<SyncContext> ResetTableAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            return ResetTableAsync(scopeInfo, context, tableName, schemaName);
        }


        /// <summary>
        /// Reset a table, deleting rows from table and tracking_table
        /// </summary>
        public virtual async Task<SyncContext> ResetTableAsync(ScopeInfo scopeInfo, SyncContext context, string tableName, string schemaName = null)
        {
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return context;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return context;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.None).ConfigureAwait(false);

                context = await this.InternalResetTableAsync(scopeInfo, context, schemaTable, 
                    runner.Connection, runner.Transaction).ConfigureAwait(false);

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
        public virtual async Task<SyncContext> DisableConstraintsAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {

                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return context;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return context;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.None).ConfigureAwait(false);
                
                context = await this.InternalDisableConstraintsAsync(scopeInfo, context, schemaTable, 
                    runner.Connection, runner.Transaction).ConfigureAwait(false);
                
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
        public virtual async Task<SyncContext> EnableConstraintsAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {

                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return context;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return context;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.None).ConfigureAwait(false);
                
                context = await this.InternalEnableConstraintsAsync(scopeInfo, context, schemaTable,
                    runner.Connection, runner.Transaction).ConfigureAwait(false);
                
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
        internal async Task<SyncContext> InternalDisableConstraintsAsync(ScopeInfo scopeInfo, SyncContext context,
            SyncTable schemaTable, DbConnection connection, DbTransaction transaction = null)
        {
            await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesApplying, connection, transaction).ConfigureAwait(false);

            var syncAdapter = this.GetSyncAdapter(scopeInfo.Name, schemaTable, scopeInfo.Setup);

            var (command, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, DbCommandType.DisableConstraints, null,
            runner.Connection, runner.Transaction, default, default).ConfigureAwait(false);

            if (command == null) return context;

            // Parametrized command timeout established if exist
            if (this.Options.DbCommandTimeout.HasValue)
                command.CommandTimeout = this.Options.DbCommandTimeout.Value;

            await this.InterceptAsync(new ExecuteCommandArgs(context, command, DbCommandType.DisableConstraints, runner.Connection, runner.Transaction)).ConfigureAwait(false);
            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            command.Dispose();

            return context;
        }

        /// <summary>
        /// Enabling all constraints on synced tables
        /// </summary>
        internal async Task<SyncContext> InternalEnableConstraintsAsync(ScopeInfo scopeInfo, SyncContext context,
            SyncTable schemaTable, DbConnection connection, DbTransaction transaction)
        {
            await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesApplying, connection, transaction).ConfigureAwait(false);

            var syncAdapter = this.GetSyncAdapter(scopeInfo.Name, schemaTable, scopeInfo.Setup);

            var (command, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, DbCommandType.EnableConstraints, null,
                runner.Connection, runner.Transaction, default, default).ConfigureAwait(false);

            if (command == null) return context;

            // Parametrized command timeout established if exist
            if (this.Options.DbCommandTimeout.HasValue)
                command.CommandTimeout = this.Options.DbCommandTimeout.Value;

            await this.InterceptAsync(new ExecuteCommandArgs(context, command, DbCommandType.EnableConstraints, runner.Connection, runner.Transaction)).ConfigureAwait(false);

            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            command.Dispose();

            return context;
        }

        /// <summary>
        /// Reset a table, deleting rows from table and tracking_table
        /// </summary>
        internal async Task<SyncContext> InternalResetTableAsync(ScopeInfo scopeInfo, SyncContext context,
            SyncTable schemaTable, DbConnection connection, DbTransaction transaction)
        {
            await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesApplying, connection, transaction).ConfigureAwait(false);

            var syncAdapter = this.GetSyncAdapter(scopeInfo.Name, schemaTable, scopeInfo.Setup);

            var (command, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, DbCommandType.Reset, null,
                runner.Connection, runner.Transaction, default, default).ConfigureAwait(false);

            if (command != null)
            {
                // Parametrized command timeout established if exist
                if (this.Options.DbCommandTimeout.HasValue)
                    command.CommandTimeout = this.Options.DbCommandTimeout.Value;

                await this.InterceptAsync(new ExecuteCommandArgs(context, command, DbCommandType.Reset, runner.Connection, runner.Transaction)).ConfigureAwait(false);
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                command.Dispose();
            }

            return context;
        }

    }
}
