
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {

        /// <summary>
        /// Reset a table, deleting all rows from table and tracking_table. This method is used when you want to Reinitialize your database
        /// </summary>
        public virtual Task ResetTableAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null, DbConnection connection = null, DbTransaction transaction = null,
            CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            return ResetTableAsync(scopeInfo, context, tableName, schemaName, connection, transaction, cancellationToken, progress);
        }

        /// <summary>
        /// Reset a table, deleting rows from table and tracking_table
        /// </summary>
        internal virtual async Task ResetTableAsync(ScopeInfo scopeInfo, SyncContext context, string tableName, string schemaName = null, DbConnection connection = null, DbTransaction transaction = null,
            CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.None, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                context = await this.InternalResetTableAsync(scopeInfo, context, schemaTable,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                string message = null;

                var tableFullName = string.IsNullOrEmpty(schemaName) ? tableName : $"{schemaName}.{tableName}";
                message += $"Table:{tableFullName}.";

                throw GetSyncError(context, ex, message);
            }
        }


        public virtual async Task ResetTablesAsync(ScopeInfo scopeInfo, DbConnection connection = null, DbTransaction transaction = null,
            CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            var schemaTables = scopeInfo.Schema.Tables.SortByDependencies(tab => tab.GetRelations().Select(r => r.GetParentTable()));

            foreach (var schemaTable in schemaTables.Reverse())
            {
                context = await this.InternalResetTableAsync(scopeInfo, context, schemaTable, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            }
        }


        /// <summary>
        /// Disable a table's constraints
        /// <para>
        /// Usually this method is surrounded by a connection / transaction
        /// </para>
        /// <example>
        /// <code>
        /// var localOrchestrator = new LocalOrchestrator(clientProvider);
        /// 
        /// using var sqlConnection = new SqlConnection(clientProvider.ConnectionString);
        /// 
        /// sqlConnection.Open();
        /// using var sqlTransaction = sqlConnection.BeginTransaction();
        /// 
        /// var scopeInfo = await localOrchestrator.GetScopeInfoAsync(sqlConnection, sqlTransaction);
        /// await localOrchestrator.DisableConstraintsAsync(scopeInfo, "ProductCategory", default,
        ///     sqlConnection, sqlTransaction);
        /// 
        /// // .. Do some random insert in the ProductCategory table
        /// await DoSomeRandomInsertInProductCategoryTableAsync(sqlConnection, sqlTransaction);
        /// 
        /// await localOrchestrator.EnableConstraintsAsync(scopeInfo, "ProductCategory", default,
        ///     sqlConnection, sqlTransaction);
        /// 
        /// sqlTransaction.Commit();
        /// sqlConnection.Close();
        /// </code>
        /// </example>
        /// </summary>
        public virtual async Task DisableConstraintsAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null,
            DbConnection connection = null, DbTransaction transaction = null, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                context = await this.InternalDisableConstraintsAsync(scopeInfo, context, schemaTable,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                return;
            }
            catch (Exception ex)
            {
                string message = null;

                var tableFullName = string.IsNullOrEmpty(schemaName) ? tableName : $"{schemaName}.{tableName}";
                message += $"Table:{tableFullName}.";

                throw GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Enable a table's constraints
        /// <para>
        /// Usually this method is surrounded by a connection / transaction
        /// </para>
        /// <example>
        /// <code>
        /// var localOrchestrator = new LocalOrchestrator(clientProvider);
        /// 
        /// using var sqlConnection = new SqlConnection(clientProvider.ConnectionString);
        /// 
        /// sqlConnection.Open();
        /// using var sqlTransaction = sqlConnection.BeginTransaction();
        /// 
        /// var scopeInfo = await localOrchestrator.GetScopeInfoAsync(sqlConnection, sqlTransaction);
        /// await localOrchestrator.DisableConstraintsAsync(scopeInfo, "ProductCategory", default,
        ///     sqlConnection, sqlTransaction);
        /// 
        /// // .. Do some random insert in the ProductCategory table
        /// await DoSomeRandomInsertInProductCategoryTableAsync(sqlConnection, sqlTransaction);
        /// 
        /// await localOrchestrator.EnableConstraintsAsync(scopeInfo, "ProductCategory", default,
        ///     sqlConnection, sqlTransaction);
        /// 
        /// sqlTransaction.Commit();
        /// sqlConnection.Close();
        /// </code>
        /// </example>
        /// </summary>
        public virtual async Task EnableConstraintsAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null,
            DbConnection connection = null, DbTransaction transaction = null, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                context = await this.InternalEnableConstraintsAsync(scopeInfo, context, schemaTable,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                return;
            }
            catch (Exception ex)
            {
                string message = null;

                var tableFullName = string.IsNullOrEmpty(schemaName) ? tableName : $"{schemaName}.{tableName}";
                message += $"Table:{tableFullName}.";

                throw GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Disabling all constraints on synced tables
        /// </summary>
        internal async Task<SyncContext> InternalDisableConstraintsAsync(ScopeInfo scopeInfo, SyncContext context,
            SyncTable schemaTable, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            try
            {
                Debug.WriteLine($"Disabling constraints on table {schemaTable.GetFullName()}");
                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var syncAdapter = this.GetSyncAdapter(scopeInfo.Name, schemaTable, scopeInfo.Setup);

                var (command, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, DbCommandType.DisableConstraints,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (command == null) return context;

                await this.InterceptAsync(new ExecuteCommandArgs(context, command, DbCommandType.DisableConstraints, runner.Connection, runner.Transaction)).ConfigureAwait(false);
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                command.Dispose();

                return context;
            }
            catch (Exception ex)
            {
                string message = null;

                if (schemaTable != null)
                    message += $"Table:{schemaTable.GetFullName()}.";

                throw GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Enabling all constraints on synced tables
        /// </summary>
        internal async Task<SyncContext> InternalEnableConstraintsAsync(ScopeInfo scopeInfo, SyncContext context,
            SyncTable schemaTable, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            try
            {
                Debug.WriteLine($"Enabling constraints on table {schemaTable.GetFullName()}");
                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var syncAdapter = this.GetSyncAdapter(scopeInfo.Name, schemaTable, scopeInfo.Setup);

                var (command, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, DbCommandType.EnableConstraints,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (command == null) return context;

                await this.InterceptAsync(new ExecuteCommandArgs(context, command, DbCommandType.EnableConstraints, runner.Connection, runner.Transaction)).ConfigureAwait(false);
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                command.Dispose();

                return context;
            }
            catch (Exception ex)
            {
                string message = null;

                if (schemaTable != null)
                    message += $"Table:{schemaTable.GetFullName()}.";

                throw GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Reset a table, deleting rows from table and tracking_table
        /// </summary>
        internal async Task<SyncContext> InternalResetTableAsync(ScopeInfo scopeInfo, SyncContext context,
            SyncTable schemaTable, DbConnection connection, DbTransaction transaction,
            CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Disable check constraints for provider supporting only at table level
                if (this.Options.DisableConstraintsOnApplyChanges && this.Provider.ConstraintsLevelAction == ConstraintsLevelAction.OnTableLevel)
                    await this.InternalDisableConstraintsAsync(scopeInfo, context, schemaTable, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                var syncAdapter = this.GetSyncAdapter(scopeInfo.Name, schemaTable, scopeInfo.Setup);

                var (command, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, DbCommandType.Reset,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (command != null)
                {
                    await this.InterceptAsync(new ExecuteCommandArgs(context, command, DbCommandType.Reset, runner.Connection, runner.Transaction)).ConfigureAwait(false);
                    // Check if we have a return value instead
                    var rowDeletedCount  = await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                    var syncRowCountParam = syncAdapter.GetParameter(context, command, "sync_row_count");

                    if (syncRowCountParam != null && syncRowCountParam.Value != null && syncRowCountParam.Value != DBNull.Value)
                        rowDeletedCount = (int)syncRowCountParam.Value;
                    command.Dispose();
                }

                // Enable check constraints for provider supporting only at table level
                if (this.Options.DisableConstraintsOnApplyChanges && this.Provider.ConstraintsLevelAction == ConstraintsLevelAction.OnTableLevel)
                    await this.InternalEnableConstraintsAsync(scopeInfo, context, schemaTable, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                var args = new TableResetAppliedArgs(context, schemaTable, connection, transaction);
                await this.InterceptAsync(args, progress, cancellationToken).ConfigureAwait(false);


                return context;
            }
            catch (Exception ex)
            {
                string message = null;

                if (schemaTable != null)
                    message += $"Table:{schemaTable.GetFullName()}.";

                throw GetSyncError(context, ex, message);
            }
        }

    }
}
