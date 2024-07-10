using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains internals methods to manage tables.
    /// </summary>
    public abstract partial class BaseOrchestrator
    {
        /// <summary>
        /// Create a <strong>Table</strong> for a given table present in an existing scopeInfo.
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
        /// var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        /// await remoteOrchestrator.CreateTableAsync(scopeInfo, "Employee");
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="scopeInfo">ScopeInfo instance used to defines table generation (name, columns....).</param>
        /// <param name="tableName"><strong>Table Name</strong>. Should exists in ScopeInfo instance.</param>
        /// <param name="schemaName">Optional <strong>Schema Name</strong>. Only available for <strong>Sql Server</strong>.</param>
        /// <param name="overwrite">If specified the table is dropped, if exists, then created.</param>
        public async Task<bool> CreateTableAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null, bool overwrite = false)
        {
            Guard.ThrowIfNull(scopeInfo);

            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);

            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    var hasBeenCreated = false;

                    // Get table builder
                    var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                    bool schemaExists;
                    (context, schemaExists) = await this.InternalExistsSchemaAsync(scopeInfo, context, tableBuilder,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (!schemaExists)
                        (context, _) = await this.InternalCreateSchemaAsync(scopeInfo, context, tableBuilder,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    bool exists;
                    (context, exists) = await this.InternalExistsTableAsync(scopeInfo, context, tableBuilder,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    // should create only if not exists OR if overwrite has been set
                    var shouldCreate = !exists || overwrite;

                    if (shouldCreate)
                    {
                        // Drop if already exists and we need to overwrite
                        if (exists && overwrite)
                            (context, _) = await this.InternalDropTableAsync(scopeInfo, context, tableBuilder,
                                runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                        (context, hasBeenCreated) = await this.InternalCreateTableAsync(scopeInfo, context, tableBuilder,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                    }

                    await runner.CommitAsync().ConfigureAwait(false);

                    return hasBeenCreated;
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Create all <strong>Tables</strong> present in an existing scopeInfo.
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
        /// var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        /// await remoteOrchestrator.CreateTablesAsync(scopeInfo);
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="scopeInfo">ScopeInfo instance used to defines table generation (name, columns....).</param>
        /// <param name="overwrite">If specified all tables are dropped, if exists, then created.</param>
        public async Task<bool> CreateTablesAsync(ScopeInfo scopeInfo, bool overwrite = false)
        {
            Guard.ThrowIfNull(scopeInfo);
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);

            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    var atLeastOneHasBeenCreated = false;

                    // Sorting tables based on dependencies between them
                    var schemaTables = scopeInfo.Schema.Tables.SortByDependencies(tab => tab.GetRelations().Select(r => r.GetParentTable())).ToList();
                    var reverseSchemaTables = scopeInfo.Schema.Tables.SortByDependencies(tab => tab.GetRelations().Select(r => r.GetParentTable())).Reverse().ToList();

                    // if we overwritten all tables, we need to delete all of them, before recreating them
                    if (overwrite)
                    {
                        foreach (var schemaTable in reverseSchemaTables)
                        {
                            var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);
                            bool exists;
                            (context, exists) = await this.InternalExistsTableAsync(scopeInfo, context, tableBuilder,
                                runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                            if (exists)
                                (context, _) = await this.InternalDropTableAsync(scopeInfo, context, tableBuilder,
                                    runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                        }
                    }

                    // Then create them
                    foreach (var schemaTable in schemaTables)
                    {
                        // Get table builder
                        var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                        bool schemaExists;
                        (context, schemaExists) = await this.InternalExistsSchemaAsync(scopeInfo, context, tableBuilder,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                        if (!schemaExists)
                            (context, _) = await this.InternalCreateSchemaAsync(scopeInfo, context, tableBuilder,
                                runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                        bool exists;
                        (context, exists) = await this.InternalExistsTableAsync(scopeInfo, context, tableBuilder,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                        // should create only if not exists OR if overwrite has been set
                        var shouldCreate = !exists || overwrite;

                        if (shouldCreate)
                        {
                            // Drop if already exists and we need to overwrite
                            if (exists && overwrite)
                                (context, _) = await this.InternalDropTableAsync(scopeInfo, context, tableBuilder,
                                    runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                            bool hasBeenCreated;
                            (context, hasBeenCreated) = await this.InternalCreateTableAsync(scopeInfo, context, tableBuilder,
                                runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                            if (hasBeenCreated)
                                atLeastOneHasBeenCreated = true;
                        }
                    }

                    await runner.CommitAsync().ConfigureAwait(false);

                    return atLeastOneHasBeenCreated;
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Check if <strong>Table</strong> exists, for a given table present in an existing scopeInfo.
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
        /// var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        /// var exists = await remoteOrchestrator.ExistTableAsync(scopeInfo, "Employee");
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="scopeInfo">ScopeInfo instance used to defines table generation (name, columns....).</param>
        /// <param name="tableName"><strong>Table Name</strong>. Should exists in ScopeInfo instance.</param>
        /// <param name="schemaName">Optional <strong>Schema Name</strong>. Only available for <strong>Sql Server</strong>.</param>
        public async Task<bool> ExistTableAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null)
        {
            Guard.ThrowIfNull(scopeInfo);
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);
                    bool exists;
                    (context, exists) = await this.InternalExistsTableAsync(scopeInfo, context, tableBuilder,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    await runner.CommitAsync().ConfigureAwait(false);
                    return exists;
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
        /// Drop a <strong>Table</strong> for a given table present in an existing scopeInfo.
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
        /// var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        /// var exists = await remoteOrchestrator.DropTableAsync(scopeInfo, "Employee");
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="scopeInfo">ScopeInfo instance used to defines table generation (name, columns....).</param>
        /// <param name="tableName"><strong>Table Name</strong>. Should exists in ScopeInfo instance.</param>
        /// <param name="schemaName">Optional <strong>Schema Name</strong>. Only available for <strong>Sql Server</strong>.</param>
        public async Task<bool> DropTableAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null)
        {
            Guard.ThrowIfNull(scopeInfo);
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Deprovisioning).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    var hasBeenDropped = false;

                    // Get table builder
                    var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                    bool exists;
                    (context, exists) = await this.InternalExistsTableAsync(scopeInfo, context, tableBuilder,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (exists)
                        (context, hasBeenDropped) = await this.InternalDropTableAsync(scopeInfo, context, tableBuilder,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    await runner.CommitAsync().ConfigureAwait(false);

                    return hasBeenDropped;
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
        /// Drop all <strong>Tables</strong> present in an existing scopeInfo.
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
        /// var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        /// await remoteOrchestrator.DropTablesAsync(scopeInfo);
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="scopeInfo">ScopeInfo instance used to defines table generation (name, columns....).</param>
        public async Task<bool> DropTablesAsync(ScopeInfo scopeInfo)
        {
            Guard.ThrowIfNull(scopeInfo);
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Deprovisioning).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    var atLeastOneTableHasBeenDropped = false;

                    // Sorting tables based on dependencies between them
                    var schemaTables = scopeInfo.Schema.Tables.SortByDependencies(tab => tab.GetRelations().Select(r => r.GetParentTable()));

                    foreach (var schemaTable in schemaTables.Reverse())
                    {
                        // Get table builder
                        var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                        bool exists;
                        (context, exists) = await this.InternalExistsTableAsync(scopeInfo, context, tableBuilder,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                        if (exists)
                            (context, atLeastOneTableHasBeenDropped) = await this.InternalDropTableAsync(scopeInfo, context, tableBuilder,
                                runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                    }

                    await runner.CommitAsync().ConfigureAwait(false);

                    return atLeastOneTableHasBeenDropped;
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Internal add column routine.
        /// </summary>
        internal async Task<(SyncContext Context, bool IsAdded)> InternalAddColumnAsync(ScopeInfo scopeInfo, SyncContext context, string addedColumnName, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            Guard.ThrowIfNull(scopeInfo);
            try
            {
                if (this.Provider == null)
                    throw new MissingProviderException(nameof(this.InternalAddColumnAsync));

                if (tableBuilder.TableDescription.Columns.Count <= 0)
                    throw new MissingsColumnException(tableBuilder.TableDescription.GetFullName());

                if (tableBuilder.TableDescription.PrimaryKeys.Count <= 0)
                    throw new MissingPrimaryKeyException(tableBuilder.TableDescription.GetFullName());

                using var command = await tableBuilder.GetAddColumnCommandAsync(addedColumnName, connection, transaction).ConfigureAwait(false);

                if (command == null)
                    return (context, false);

                var (tableName, _) = this.Provider.GetParsers(tableBuilder.TableDescription, scopeInfo.Setup);

                var action = new ColumnCreatingArgs(context, addedColumnName, tableBuilder.TableDescription, tableName, command, connection, transaction);

                await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

                if (action.Cancel || action.Command == null)
                    return (context, false);

                await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

                await this.InterceptAsync(new ColumnCreatedArgs(context, addedColumnName, tableBuilder.TableDescription, tableName, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                action.Command.Dispose();

                return (context, true);
            }
            catch (Exception ex)
            {
                string message = null;

                if (tableBuilder != null && tableBuilder.TableDescription != null)
                    message += $"Table:{tableBuilder.TableDescription.GetFullName()}.";

                if (!string.IsNullOrEmpty(addedColumnName))
                    message += $"Column:{addedColumnName}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Internal add column routine.
        /// </summary>
        internal async Task<(SyncContext Context, bool IsDropped)> InternalDropColumnAsync(ScopeInfo scopeInfo, SyncContext context, string droppedColumnName, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                if (tableBuilder.TableDescription.Columns.Count <= 0)
                    throw new MissingsColumnException(tableBuilder.TableDescription.GetFullName());

                if (tableBuilder.TableDescription.PrimaryKeys.Count <= 0)
                    throw new MissingPrimaryKeyException(tableBuilder.TableDescription.GetFullName());

                using var command = await tableBuilder.GetDropColumnCommandAsync(droppedColumnName, connection, transaction).ConfigureAwait(false);

                if (command == null)
                    return (context, false);

                var (tableName, _) = this.Provider.GetParsers(tableBuilder.TableDescription, scopeInfo.Setup);

                var action = await this.InterceptAsync(new ColumnDroppingArgs(context, droppedColumnName, tableBuilder.TableDescription, tableName, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                if (action.Cancel || action.Command == null)
                    return (context, false);

                await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

                await this.InterceptAsync(new ColumnDroppedArgs(context, droppedColumnName, tableBuilder.TableDescription, tableName, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                action.Command.Dispose();

                return (context, true);
            }
            catch (Exception ex)
            {
                string message = null;

                if (tableBuilder != null && tableBuilder.TableDescription != null)
                    message += $"Table:{tableBuilder.TableDescription.GetFullName()}.";

                if (!string.IsNullOrEmpty(droppedColumnName))
                    message += $"Column:{droppedColumnName}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Internal create table routine.
        /// </summary>
        internal async Task<(SyncContext Context, bool IsCreated)> InternalCreateTableAsync(ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                if (tableBuilder.TableDescription.Columns.Count <= 0)
                    throw new MissingsColumnException(tableBuilder.TableDescription.GetFullName());

                if (tableBuilder.TableDescription.PrimaryKeys.Count <= 0)
                    throw new MissingPrimaryKeyException(tableBuilder.TableDescription.GetFullName());

                using var command = await tableBuilder.GetCreateTableCommandAsync(connection, transaction).ConfigureAwait(false);

                if (command == null)
                    return (context, false);

                var (tableName, _) = this.Provider.GetParsers(tableBuilder.TableDescription, scopeInfo.Setup);

                var action = new TableCreatingArgs(context, tableBuilder.TableDescription, tableName, command, connection, transaction);

                await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

                if (action.Cancel || action.Command == null)
                    return (context, false);

                await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

                await this.InterceptAsync(new TableCreatedArgs(context, tableBuilder.TableDescription, tableName, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                action.Command.Dispose();

                return (context, true);
            }
            catch (Exception ex)
            {
                string message = null;

                if (tableBuilder != null && tableBuilder.TableDescription != null)
                    message += $"Table:{tableBuilder.TableDescription.GetFullName()}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Internal create table routine.
        /// </summary>
        internal async Task<(SyncContext Context, bool IsCreated)> InternalCreateSchemaAsync(
            ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                using var command = await tableBuilder.GetCreateSchemaCommandAsync(connection, transaction).ConfigureAwait(false);

                if (command == null)
                    return (context, false);

                var action = await this.InterceptAsync(new SchemaNameCreatingArgs(context, tableBuilder.TableDescription, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                if (action.Cancel || action.Command == null)
                    return (context, false);

                await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

                await this.InterceptAsync(new SchemaNameCreatedArgs(context, tableBuilder.TableDescription, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                action.Command.Dispose();

                return (context, true);
            }
            catch (Exception ex)
            {
                string message = null;

                if (tableBuilder != null && tableBuilder.TableDescription != null)
                    message += $"Schema:{tableBuilder.TableDescription.SchemaName}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Internal drop table routine.
        /// </summary>
        internal async Task<(SyncContext Context, bool IsDropped)> InternalDropTableAsync(ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {

                using var command = await tableBuilder.GetDropTableCommandAsync(connection, transaction).ConfigureAwait(false);

                if (command == null)
                    return (context, false);

                var (tableName, _) = this.Provider.GetParsers(tableBuilder.TableDescription, scopeInfo.Setup);

                var action = await this.InterceptAsync(new TableDroppingArgs(context, tableBuilder.TableDescription, tableName, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                if (action.Cancel || action.Command == null)
                    return (context, false);

                await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

                await this.InterceptAsync(new TableDroppedArgs(context, tableBuilder.TableDescription, tableName, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                action.Command.Dispose();

                return (context, true);
            }
            catch (Exception ex)
            {
                string message = null;

                if (tableBuilder != null && tableBuilder.TableDescription != null)
                    message += $"Table:{tableBuilder.TableDescription.GetFullName()}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Internal exists table procedure routine.
        /// </summary>
        internal async Task<(SyncContext Context, bool IsExisting)> InternalExistsTableAsync(ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {

                // Get exists command
                using var existsCommand = await tableBuilder.GetExistsTableCommandAsync(connection, transaction).ConfigureAwait(false);

                if (existsCommand == null)
                    return (context, false);

                await this.InterceptAsync(new ExecuteCommandArgs(context, existsCommand, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
                var exists = SyncTypeConverter.TryConvertTo<int>(existsResultObject) > 0;
                return (context, exists);
            }
            catch (Exception ex)
            {
                string message = null;

                if (tableBuilder != null && tableBuilder.TableDescription != null)
                    message += $"Table:{tableBuilder.TableDescription.GetFullName()}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Internal exists schema procedure routine.
        /// </summary>
        internal async Task<(SyncContext Context, bool IsExisting)> InternalExistsSchemaAsync(ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {

                if (string.IsNullOrEmpty(tableBuilder.TableDescription.SchemaName) || tableBuilder.TableDescription.SchemaName == this.Provider.DefaultSchemaName)
                    return (context, true);

                // Get exists command
                using var existsCommand = await tableBuilder.GetExistsSchemaCommandAsync(connection, transaction).ConfigureAwait(false);

                if (existsCommand == null)
                    return (context, false);

                await this.InterceptAsync(new ExecuteCommandArgs(context, existsCommand, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
                var exists = SyncTypeConverter.TryConvertTo<int>(existsResultObject) > 0;
                return (context, exists);
            }
            catch (Exception ex)
            {
                string message = null;

                if (tableBuilder != null && tableBuilder.TableDescription != null)
                    message += $"Table:{tableBuilder.TableDescription.GetFullName()}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Internal exists column procedure routine.
        /// </summary>
        internal async Task<(SyncContext Context, bool IsExisting)> InternalExistsColumnAsync(ScopeInfo scopeInfo, SyncContext context, string columnName, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {

                // Get exists command
                using var existsCommand = await tableBuilder.GetExistsColumnCommandAsync(columnName, connection, transaction).ConfigureAwait(false);

                if (existsCommand == null)
                    return (context, false);

                await this.InterceptAsync(new ExecuteCommandArgs(context, existsCommand, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
                var exists = SyncTypeConverter.TryConvertTo<int>(existsResultObject) > 0;

                return (context, exists);
            }
            catch (Exception ex)
            {
                string message = null;

                if (tableBuilder != null && tableBuilder.TableDescription != null)
                    message += $"Table:{tableBuilder.TableDescription.GetFullName()}.";

                if (!string.IsNullOrEmpty(columnName))
                    message += $"Column:{columnName}.";

                throw this.GetSyncError(context, ex, message);
            }
        }
    }
}