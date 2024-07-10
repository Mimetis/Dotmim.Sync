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
    /// Contains internal methods to create, drop, check stored procedures.
    /// </summary>
    public abstract partial class BaseOrchestrator
    {
        /// <summary>
        /// Create a <strong>Stored Procedure</strong> for a given table present in an existing scopeInfo.
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
        /// var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        /// await remoteOrchestrator.CreateStoredProcedureAsync(scopeInfo, "Employee", null, DbStoredProcedureType.SelectChanges);
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="scopeInfo">ScopeInfo instance used to defines stored procedure generation (name, prefix, suffix, filters ....).</param>
        /// <param name="tableName"><strong>Table Name</strong>. Should exists in ScopeInfo instance.</param>
        /// <param name="schemaName">Optional <strong>Schema Name</strong>. Only available for <strong>Sql Server</strong>.</param>
        /// <param name="storedProcedureType">Stored Procedure type. See <see cref="DbStoredProcedureType"/> enumeration.</param>
        /// <param name="overwrite">If specified the stored procedure is generated again, even if already exists.</param>
        public virtual async Task<bool> CreateStoredProcedureAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null, DbStoredProcedureType storedProcedureType = default, bool overwrite = false)
        {
            Guard.ThrowIfNull(scopeInfo);
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                var syncTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (syncTable == null)
                    return false;

                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    var hasBeenCreated = false;

                    // Get table builder
                    var tableBuilder = this.GetTableBuilder(syncTable, scopeInfo);

                    bool exists;
                    (context, exists) = await this.InternalExistsStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    // should create only if not exists OR if overwrite has been set
                    var shouldCreate = !exists || overwrite;

                    if (shouldCreate)
                    {
                        // Drop storedProcedure if already exists
                        if (exists && overwrite)
                            (context, _) = await this.InternalDropStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                        (context, hasBeenCreated) = await this.InternalCreateStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                    }

                    await runner.CommitAsync().ConfigureAwait(false);

                    return hasBeenCreated;
                }
            }
            catch (Exception ex)
            {
                string message = null;

                var tableFullName = string.IsNullOrEmpty(schemaName) ? tableName : $"{schemaName}.{tableName}";
                message += $"Table:{tableFullName}.";

                message += $"StoredProcedure:{storedProcedureType}.";
                message += $"Overwrite:{overwrite}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Create all <strong>Stored Procedures</strong> for a given table present in an existing scopeInfo.
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
        /// var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        /// await remoteOrchestrator.CreateStoredProceduresAsync(scopeInfo, "Employee");
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="scopeInfo">ScopeInfo instance used to defines stored procedure generation (name, prefix, suffix, filters ....).</param>
        /// <param name="tableName"><strong>Table Name</strong>. Should exists in ScopeInfo instance.</param>
        /// <param name="schemaName">Optional <strong>Schema Name</strong>. Only available for <strong>Sql Server</strong>.</param>
        /// <param name="overwrite">If specified all the stored procedures are generated again, even if they already exist.</param>
        public virtual async Task<bool> CreateStoredProceduresAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null, bool overwrite = false)
        {
            Guard.ThrowIfNull(scopeInfo);
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);

            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                var syncTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (syncTable == null)
                    return false;

                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    // Get table builder
                    var tableBuilder = this.GetTableBuilder(syncTable, scopeInfo);
                    bool isCreated;
                    (context, isCreated) = await this.InternalCreateStoredProceduresAsync(scopeInfo, context, overwrite, tableBuilder,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    await runner.CommitAsync().ConfigureAwait(false);

                    return isCreated;
                }
            }
            catch (Exception ex)
            {
                string message = null;

                var tableFullName = string.IsNullOrEmpty(schemaName) ? tableName : $"{schemaName}.{tableName}";
                message += $"Table:{tableFullName}.";
                message += $"Overwrite:{overwrite}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Check if a <strong>Stored Procedure</strong>, for a given table, exists.
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
        /// var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        /// var exists = await remoteOrchestrator.ExistStoredProcedureAsync(scopeInfo, "Employee", null, DbStoredProcedureType.SelectChanges);
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="scopeInfo">ScopeInfo instance used to defines stored procedure generation (name, prefix, suffix, filters ....).</param>
        /// <param name="tableName"><strong>Table Name</strong>. Should exists in ScopeInfo instance.</param>
        /// <param name="schemaName">Optional <strong>Schema Name</strong>. Only available for <strong>Sql Server</strong>.</param>
        /// <param name="storedProcedureType">Stored Procedure type. See <see cref="DbStoredProcedureType"/> enumeration.</param>
        public virtual async Task<bool> ExistStoredProcedureAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null, DbStoredProcedureType storedProcedureType = default)
        {
            Guard.ThrowIfNull(scopeInfo);
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || scopeInfo.Schema.Tables.Count <= 0 || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                    bool exists;
                    (context, exists) = await this.InternalExistsStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    return exists;
                }
            }
            catch (Exception ex)
            {
                string message = null;

                var tableFullName = string.IsNullOrEmpty(schemaName) ? tableName : $"{schemaName}.{tableName}";
                message += $"Table:{tableFullName}.";

                message += $"StoredProcedure:{storedProcedureType}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Drop a <strong>Stored Procedure</strong> for a given table present in an existing scopeInfo.
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
        /// var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        /// await remoteOrchestrator.DropStoredProcedureAsync(scopeInfo, "Employee", null, DbStoredProcedureType.SelectChanges);
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="scopeInfo">ScopeInfo instance used to defines stored procedure generation (name, prefix, suffix, filters ....).</param>
        /// <param name="tableName"><strong>Table Name</strong>. Should exists in ScopeInfo instance.</param>
        /// <param name="schemaName">Optional <strong>Schema Name</strong>. Only available for <strong>Sql Server</strong>.</param>
        /// <param name="storedProcedureType">Stored Procedure type. See <see cref="DbStoredProcedureType"/> enumeration.</param>
        public virtual async Task<bool> DropStoredProcedureAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null, DbStoredProcedureType storedProcedureType = default)
        {
            Guard.ThrowIfNull(scopeInfo);
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || scopeInfo.Schema.Tables.Count <= 0 || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Deprovisioning).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    var hasBeenDropped = false;

                    var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                    bool existsAndCanBeDeleted;
                    (context, existsAndCanBeDeleted) = await this.InternalExistsStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (existsAndCanBeDeleted)
                        (context, hasBeenDropped) = await this.InternalDropStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    // Removing cached commands
                    this.RemoveCommands();

                    await runner.CommitAsync().ConfigureAwait(false);

                    return hasBeenDropped;
                }
            }
            catch (Exception ex)
            {
                string message = null;

                var tableFullName = string.IsNullOrEmpty(schemaName) ? tableName : $"{schemaName}.{tableName}";
                message += $"Table:{tableFullName}.";

                message += $"StoredProcedure:{storedProcedureType}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Drop all <strong>Stored Procedures</strong> for a given table present in an existing scopeInfo.
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
        /// var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        /// await remoteOrchestrator.DropStoredProceduresAsync(scopeInfo, "Employee");
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="scopeInfo">ScopeInfo instance used to defines stored procedure generation (name, prefix, suffix, filters ....).</param>
        /// <param name="tableName"><strong>Table Name</strong>. Should exists in ScopeInfo instance.</param>
        /// <param name="schemaName">Optional <strong>Schema Name</strong>. Only available for <strong>Sql Server</strong>.</param>
        public virtual async Task<bool> DropStoredProceduresAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null)
        {
            Guard.ThrowIfNull(scopeInfo);
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || scopeInfo.Schema.Tables.Count <= 0 || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Deprovisioning).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    var hasDroppedAtLeastOneStoredProcedure = false;

                    // using a fake SyncTable based on SetupTable, since we don't need columns
                    var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                    // check bulk before
                    (context, hasDroppedAtLeastOneStoredProcedure) = await this.InternalDropStoredProceduresAsync(scopeInfo, context, tableBuilder,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    // Removing cached commands
                    this.RemoveCommands();

                    await runner.CommitAsync().ConfigureAwait(false);

                    return hasDroppedAtLeastOneStoredProcedure;
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
        /// Internal create Stored Procedure routine.
        /// </summary>
        internal async Task<(SyncContext Context, bool IsCreated)> InternalCreateStoredProcedureAsync(ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbStoredProcedureType storedProcedureType,
            DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                if (tableBuilder.TableDescription.Columns.Count <= 0)
                    throw new MissingsColumnException(tableBuilder.TableDescription.GetFullName());

                if (tableBuilder.TableDescription.PrimaryKeys.Count <= 0)
                    throw new MissingPrimaryKeyException(tableBuilder.TableDescription.GetFullName());

                var filter = tableBuilder.TableDescription.GetFilter();

                var command = await tableBuilder.GetCreateStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction).ConfigureAwait(false);

                if (command == null)
                    return (context, false);

                var action = new StoredProcedureCreatingArgs(context, tableBuilder.TableDescription, storedProcedureType, command, connection, transaction);
                await this.InterceptAsync(action, progress, cancellationToken).ConfigureAwait(false);

                if (action.Cancel || action.Command == null)
                    return (context, false);

                await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);
                await this.InterceptAsync(new StoredProcedureCreatedArgs(context, tableBuilder.TableDescription, storedProcedureType, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                return (context, true);
            }
            catch (Exception ex)
            {
                string message = null;

                if (tableBuilder != null && tableBuilder.TableDescription != null)
                    message += $"Table:{tableBuilder.TableDescription.GetFullName()}.";

                message += $"StoredProcedure:{storedProcedureType}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Internal drop storedProcedure routine.
        /// </summary>
        internal async Task<(SyncContext Context, bool IsDropped)> InternalDropStoredProcedureAsync(ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbStoredProcedureType storedProcedureType,
            DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                var filter = tableBuilder.TableDescription.GetFilter();

                var command = await tableBuilder.GetDropStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction).ConfigureAwait(false);

                if (command == null)
                    return (context, false);

                var action = await this.InterceptAsync(new StoredProcedureDroppingArgs(context, tableBuilder.TableDescription, storedProcedureType, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                if (action.Cancel || action.Command == null)
                    return (context, false);

                await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

                await this.InterceptAsync(new StoredProcedureDroppedArgs(context, tableBuilder.TableDescription, storedProcedureType, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                return (context, true);
            }
            catch (Exception ex)
            {
                string message = null;

                if (tableBuilder != null && tableBuilder.TableDescription != null)
                    message += $"Table:{tableBuilder.TableDescription.GetFullName()}.";

                message += $"StoredProcedure:{storedProcedureType}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Internal exists storedProcedure procedure routine.
        /// </summary>
        internal async Task<(SyncContext Context, bool IsExisting)> InternalExistsStoredProcedureAsync(ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbStoredProcedureType storedProcedureType,
            DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                var filter = tableBuilder.TableDescription.GetFilter();

                var existsCommand = await tableBuilder.GetExistsStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction).ConfigureAwait(false);
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

                message += $"StoredProcedure:{storedProcedureType}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Internal drop storedProcedures routine.
        /// </summary>
        internal async Task<(SyncContext Context, bool IsDropped)> InternalDropStoredProceduresAsync(
            ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                // check bulk before
                var hasDroppedAtLeastOneStoredProcedure = false;

                var listStoredProcedureType = Enum.GetValues(typeof(DbStoredProcedureType)).Cast<DbStoredProcedureType>().OrderBy(sp => (int)sp);

                foreach (DbStoredProcedureType storedProcedureType in Enum.GetValues(typeof(DbStoredProcedureType)))
                {
                    bool exists;
                    (context, exists) = await this.InternalExistsStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                    if (exists)
                    {
                        bool dropped;
                        (context, dropped) = await this.InternalDropStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                        // If at least one stored proc has been dropped, we're good to return true;
                        if (dropped && !hasDroppedAtLeastOneStoredProcedure)
                            hasDroppedAtLeastOneStoredProcedure = true;
                    }
                }

                return (context, hasDroppedAtLeastOneStoredProcedure);
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
        /// Internal create storedProcedures routine.
        /// </summary>
        internal async Task<(SyncContext Context, bool IsCreated)> InternalCreateStoredProceduresAsync(
            ScopeInfo scopeInfo, SyncContext context, bool overwrite, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                var hasCreatedAtLeastOneStoredProcedure = false;

                // Order Asc is the correct order to Delete
                var listStoredProcedureType = Enum.GetValues(typeof(DbStoredProcedureType)).Cast<DbStoredProcedureType>().OrderBy(sp => (int)sp);

                // we need to drop bulk in order to be sure bulk type is delete after all
                if (overwrite)
                {
                    foreach (var storedProcedureType in listStoredProcedureType)
                    {
                        bool exists;
                        (context, exists) = await this.InternalExistsStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                        if (exists)
                            (context, _) = await this.InternalDropStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                    }
                }

                // Order Desc is the correct order to Create
                listStoredProcedureType = Enum.GetValues(typeof(DbStoredProcedureType)).Cast<DbStoredProcedureType>().OrderByDescending(sp => (int)sp);

                foreach (var storedProcedureType in listStoredProcedureType)
                {
                    // check with filter
                    if ((storedProcedureType is DbStoredProcedureType.SelectChangesWithFilters || storedProcedureType is DbStoredProcedureType.SelectInitializedChangesWithFilters)
                        && tableBuilder.TableDescription.GetFilter() == null)
                        continue;

                    bool exists;
                    (context, exists) = await this.InternalExistsStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                    // Drop storedProcedure if already exists
                    if (exists && overwrite)
                        (context, _) = await this.InternalDropStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                    var shouldCreate = !exists || overwrite;

                    if (!shouldCreate)
                        continue;

                    bool created;
                    (context, created) = await this.InternalCreateStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                    // If at least one stored proc has been created, we're good to return true;
                    if (created && !hasCreatedAtLeastOneStoredProcedure)
                        hasCreatedAtLeastOneStoredProcedure = true;
                }

                return (context, hasCreatedAtLeastOneStoredProcedure);
            }
            catch (Exception ex)
            {
                string message = null;

                if (tableBuilder != null && tableBuilder.TableDescription != null)
                    message += $"Table:{tableBuilder.TableDescription.GetFullName()}.";

                message += $"Overwrite:{overwrite}.";

                throw this.GetSyncError(context, ex, message);
            }
        }
    }
}