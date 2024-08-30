using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains methods to reset a table, disable and enable constraints.
    /// </summary>
    public abstract partial class BaseOrchestrator
    {

        /// <summary>
        /// Reset a table designed from objectName and optionally ownerName, deleting all rows from this table and corresponding tracking_table. This method is used when you want to Reinitialize your database.
        /// </summary>
        public virtual Task ResetTableAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null, DbConnection connection = null, DbTransaction transaction = null,
            IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            Guard.ThrowIfNull(scopeInfo);

            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            return this.ResetTableAsync(scopeInfo, context, tableName, schemaName, connection, transaction, progress, cancellationToken);
        }

        /// <summary>
        /// Reset all tables, deleting all rows from table and tracking_table. This method is used when you want to Reinitialize your database.
        /// </summary>
        public virtual async Task ResetTablesAsync(ScopeInfo scopeInfo, DbConnection connection = null, DbTransaction transaction = null,
            IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            Guard.ThrowIfNull(scopeInfo);

            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            var schemaTables = scopeInfo.Schema.Tables.SortByDependencies(tab => tab.GetRelations().Select(r => r.GetParentTable()));

            foreach (var schemaTable in schemaTables.Reverse())
                await this.InternalResetTableAsync(scopeInfo, context, schemaTable, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Disable a table's constraints.
        /// <para>
        /// Usually this method is surrounded by a connection / transaction.
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
            DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            Guard.ThrowIfNull(scopeInfo);

            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return;

                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    context = await this.InternalDisableConstraintsAsync(scopeInfo, context, schemaTable,
                    runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                string message = null;

                var tableFullName = string.IsNullOrEmpty(schemaName) ? tableName : $"{schemaName}.{tableName}";
                message += $"Table:{tableFullName}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Enable a table's constraints.
        /// <para>
        /// Usually this method is surrounded by a connection / transaction.
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
            DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            Guard.ThrowIfNull(scopeInfo);

            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return;

                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    context = await this.InternalEnableConstraintsAsync(scopeInfo, context, schemaTable,
                    runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                string message = null;

                var tableFullName = string.IsNullOrEmpty(schemaName) ? tableName : $"{schemaName}.{tableName}";
                message += $"Table:{tableFullName}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Reset a table, deleting rows from table and tracking_table.
        /// </summary>
        internal virtual async Task ResetTableAsync(ScopeInfo scopeInfo, SyncContext context, string tableName, string schemaName = null, DbConnection connection = null, DbTransaction transaction = null,
            IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            try
            {
                Guard.ThrowIfNull(scopeInfo);

                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return;

                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.None, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    context = await this.InternalResetTableAsync(scopeInfo, context, schemaTable,
                    runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    await runner.CommitAsync().ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                string message = null;

                var tableFullName = string.IsNullOrEmpty(schemaName) ? tableName : $"{schemaName}.{tableName}";
                message += $"Table:{tableFullName}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Disabling all constraints on synced tables.
        /// </summary>
        internal async Task<SyncContext> InternalDisableConstraintsAsync(ScopeInfo scopeInfo, SyncContext context,
            SyncTable schemaTable, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                Debug.WriteLine($"Disabling constraints on table {schemaTable.GetFullName()}");
                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesApplying, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    var syncAdapter = this.GetSyncAdapter(schemaTable, scopeInfo);

                    var (command, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, DbCommandType.DisableConstraints,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    try
                    {
                        if (command == null)
                            return context;

                        await this.InterceptAsync(new ExecuteCommandArgs(context, command, DbCommandType.DisableConstraints, runner.Connection, runner.Transaction), cancellationToken: cancellationToken).ConfigureAwait(false);
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }
                    finally
                    {
                        command.Dispose();
                    }

                    return context;
                }
            }
            catch (Exception ex)
            {
                string message = null;

                if (schemaTable != null)
                    message += $"Table:{schemaTable.GetFullName()}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Enabling all constraints on synced tables.
        /// </summary>
        internal async Task<SyncContext> InternalEnableConstraintsAsync(ScopeInfo scopeInfo, SyncContext context,
            SyncTable schemaTable, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                Debug.WriteLine($"Enabling constraints on table {schemaTable.GetFullName()}");
                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesApplying, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    var syncAdapter = this.GetSyncAdapter(schemaTable, scopeInfo);

                    var (command, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, DbCommandType.EnableConstraints,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (command == null)
                        return context;

                    await this.InterceptAsync(new ExecuteCommandArgs(context, command, DbCommandType.EnableConstraints, runner.Connection, runner.Transaction), cancellationToken: cancellationToken).ConfigureAwait(false);
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    command.Dispose();

                    return context;
                }
            }
            catch (Exception ex)
            {
                string message = null;

                if (schemaTable != null)
                    message += $"Table:{schemaTable.GetFullName()}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Reset a table, deleting rows from table and tracking_table.
        /// </summary>
        internal async Task<SyncContext> InternalResetTableAsync(ScopeInfo scopeInfo, SyncContext context,
            SyncTable schemaTable, DbConnection connection, DbTransaction transaction,
            IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesApplying, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    // Disable check constraints for provider supporting only at table level
                    if (this.Options.DisableConstraintsOnApplyChanges && this.Provider.ConstraintsLevelAction == ConstraintsLevelAction.OnTableLevel)
                        await this.InternalDisableConstraintsAsync(scopeInfo, context, schemaTable, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    var syncAdapter = this.GetSyncAdapter(schemaTable, scopeInfo);

                    (var command, var _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, DbCommandType.Reset,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (command != null)
                    {
                        try
                        {
                            await this.InterceptAsync(new ExecuteCommandArgs(context, command, DbCommandType.Reset, runner.Connection, runner.Transaction), progress, cancellationToken).ConfigureAwait(false);

                            // Check if we have a return value instead
                            var rowDeletedCount = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                            var syncRowCountParam = syncAdapter.GetParameter(context, command, "sync_row_count");

                            if (syncRowCountParam != null && syncRowCountParam.Value != null && syncRowCountParam.Value != DBNull.Value)
                                rowDeletedCount = (int)syncRowCountParam.Value;
                        }
                        finally
                        {
                            command.Dispose();
                        }
                    }

                    // Enable check constraints for provider supporting only at table level
                    if (this.Options.DisableConstraintsOnApplyChanges && this.Provider.ConstraintsLevelAction == ConstraintsLevelAction.OnTableLevel)
                        await this.InternalEnableConstraintsAsync(scopeInfo, context, schemaTable, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    var args = new TableResetAppliedArgs(context, schemaTable, connection, transaction);
                    await this.InterceptAsync(args, progress, cancellationToken).ConfigureAwait(false);

                    return context;
                }
            }
            catch (Exception ex)
            {
                string message = null;

                if (schemaTable != null)
                    message += $"Table:{schemaTable.GetFullName()}.";

                throw this.GetSyncError(context, ex, message);
            }
        }
    }
}