using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains internal methods to create triggers.
    /// </summary>
    public abstract partial class BaseOrchestrator
    {
        /// <summary>
        /// Create a <strong>Trigger</strong> for a given table present in an existing scopeInfo.
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
        /// var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        /// await remoteOrchestrator.CreateTriggerAsync(scopeInfo, "Employee", DbTriggerType.Insert);
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="scopeInfo">ScopeInfo instance used to defines trigger generation (name, prefix, suffix...).</param>
        /// <param name="tableName"><strong>Table Name</strong>. Should exists in ScopeInfo instance.</param>
        /// <param name="schemaName">Optional <strong>Schema Name</strong>. Only available for <strong>Sql Server</strong>.</param>
        /// <param name="triggerType">Trigger type to create.</param>
        /// <param name="overwrite">If specified the trigger is dropped, if exists, then created again.</param>
        /// <param name="connection">Optional Connection.</param>
        /// <param name="transaction">Optional Transaction.</param>
        public async Task<bool> CreateTriggerAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null, DbTriggerType triggerType = DbTriggerType.Insert, bool overwrite = false,
            DbConnection connection = null, DbTransaction transaction = null)
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

                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    var hasBeenCreated = false;

                    // Get table builder
                    var tableBuilder = this.GetSyncAdapter(schemaTable, scopeInfo).GetTableBuilder();

                    bool exists;
                    (context, exists) = await this.InternalExistsTriggerAsync(scopeInfo, context, tableBuilder, triggerType,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    // should create only if not exists OR if overwrite has been set
                    var shouldCreate = !exists || overwrite;

                    if (shouldCreate)
                    {
                        // Drop trigger if already exists
                        if (exists && overwrite)
                        {

                            (context, _) = await this.InternalDropTriggerAsync(scopeInfo, context, tableBuilder, triggerType,
                                runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                        }

                        (context, hasBeenCreated) = await this.InternalCreateTriggerAsync(scopeInfo, context, tableBuilder, triggerType,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
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
                message += $"Trigger:{triggerType}.";
                message += $"Overwrite:{overwrite}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Create all <strong>Triggers</strong> for a given table present in an existing scopeInfo.
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
        /// var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        /// await remoteOrchestrator.CreateTriggersAsync(scopeInfo, "Employee");
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="scopeInfo">ScopeInfo instance used to defines trigger generation (name, prefix, suffix...).</param>
        /// <param name="tableName"><strong>Table Name</strong>. Should exists in ScopeInfo instance.</param>
        /// <param name="schemaName">Optional <strong>Schema Name</strong>. Only available for <strong>Sql Server</strong>.</param>
        /// <param name="overwrite">If specified the triggers are dropped, if exists, then created again.</param>
        /// <param name="connection">Optional Connection.</param>
        /// <param name="transaction">Optional Transaction.</param>
        public async Task<bool> CreateTriggersAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null, bool overwrite = false,
            DbConnection connection = null, DbTransaction transaction = null)
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

                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {

                    // Get table builder
                    var tableBuilder = this.GetSyncAdapter(schemaTable, scopeInfo).GetTableBuilder();

                    bool created;
                    (context, created) = await this.InternalCreateTriggersAsync(scopeInfo, context, overwrite, tableBuilder,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    await runner.CommitAsync().ConfigureAwait(false);

                    return created;
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
        /// Check if a <strong>Trigger</strong>exists, for a given table present in an existing scopeInfo.
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
        /// var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        /// await remoteOrchestrator.ExistTriggerAsync(scopeInfo, "Employee");
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="scopeInfo">ScopeInfo instance used to defines trigger generation (name, prefix, suffix...).</param>
        /// <param name="tableName"><strong>Table Name</strong>. Should exists in ScopeInfo instance.</param>
        /// <param name="schemaName">Optional <strong>Schema Name</strong>. Only available for <strong>Sql Server</strong>.</param>
        /// <param name="triggerType">Trigger type to check if exist.</param>
        /// <param name="connection">Optional Connection.</param>
        /// <param name="transaction">Optional Transaction.</param>
        public async Task<bool> ExistTriggerAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null, DbTriggerType triggerType = DbTriggerType.Insert,
            DbConnection connection = null, DbTransaction transaction = null)
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

                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    // Get table builder
                    var tableBuilder = this.GetSyncAdapter(schemaTable, scopeInfo).GetTableBuilder();

                    bool exists;
                    (context, exists) = await this.InternalExistsTriggerAsync(scopeInfo, context, tableBuilder, triggerType,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    return exists;
                }
            }
            catch (Exception ex)
            {
                string message = null;

                var tableFullName = string.IsNullOrEmpty(schemaName) ? tableName : $"{schemaName}.{tableName}";
                message += $"Table:{tableFullName}.";
                message += $"Trigger:{triggerType}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Drop a <strong>Trigger</strong>, for a given table present in an existing scopeInfo.
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
        /// var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        /// await remoteOrchestrator.DropTriggerAsync(scopeInfo, "Employee", null, DbTriggerType.Insert);
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="scopeInfo">ScopeInfo instance used to defines trigger generation (name, prefix, suffix...).</param>
        /// <param name="tableName"><strong>Table Name</strong>. Should exists in ScopeInfo instance.</param>
        /// <param name="schemaName">Optional <strong>Schema Name</strong>. Only available for <strong>Sql Server</strong>.</param>
        /// <param name="triggerType">Trigger type to drop.</param>
        /// <param name="connection">Optional Connection.</param>
        /// <param name="transaction">Optional Transaction.</param>
        public async Task<bool> DropTriggerAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null, DbTriggerType triggerType = DbTriggerType.Insert, DbConnection connection = null, DbTransaction transaction = null)
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

                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.Deprovisioning, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    var hasBeenDropped = false;

                    // Get table builder
                    var tableBuilder = this.GetSyncAdapter(schemaTable, scopeInfo).GetTableBuilder();

                    bool existsAndCanBeDeleted;
                    (context, existsAndCanBeDeleted) = await this.InternalExistsTriggerAsync(scopeInfo, context, tableBuilder, triggerType,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (existsAndCanBeDeleted)
                    {

                        (context, hasBeenDropped) = await this.InternalDropTriggerAsync(scopeInfo, context, tableBuilder, triggerType,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                    }

                    return hasBeenDropped;
                }
            }
            catch (Exception ex)
            {
                string message = null;

                var tableFullName = string.IsNullOrEmpty(schemaName) ? tableName : $"{schemaName}.{tableName}";
                message += $"Table:{tableFullName}.";
                message += $"Trigger:{triggerType}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Drop all <strong>Triggers</strong>, for a given table present in an existing scopeInfo.
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
        /// var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        /// await remoteOrchestrator.DropTriggersAsync(scopeInfo, "Employee");
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="scopeInfo">ScopeInfo instance used to defines trigger generation (name, prefix, suffix...).</param>
        /// <param name="tableName"><strong>Table Name</strong>. Should exists in ScopeInfo instance.</param>
        /// <param name="schemaName">Optional <strong>Schema Name</strong>. Only available for <strong>Sql Server</strong>.</param>
        /// <param name="connection">Optional Connection.</param>
        /// <param name="transaction">Optional Transaction.</param>
        public async Task<bool> DropTriggersAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null,
            DbConnection connection = null, DbTransaction transaction = null)
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

                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Deprovisioning, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    // Get table builder
                    var tableBuilder = this.GetSyncAdapter(schemaTable, scopeInfo).GetTableBuilder();

                    bool dropped;
                    (context, dropped) = await this.InternalDropTriggersAsync(scopeInfo, context, tableBuilder,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    await runner.CommitAsync().ConfigureAwait(false);

                    return dropped;
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
        /// Internal create trigger routine.
        /// </summary>
        internal virtual async Task<(SyncContext Context, bool IsCreated)> InternalCreateTriggerAsync(
            ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbTriggerType triggerType, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {

                if (tableBuilder.TableDescription.Columns.Count <= 0)
                    throw new MissingsColumnException(tableBuilder.TableDescription.GetFullName());

                if (tableBuilder.TableDescription.PrimaryKeys.Count <= 0)
                    throw new MissingPrimaryKeyException(tableBuilder.TableDescription.GetFullName());

                using var command = await tableBuilder.GetCreateTriggerCommandAsync(triggerType, connection, transaction).ConfigureAwait(false);

                if (command == null)
                    return (context, false);

                var action = await this.InterceptAsync(new TriggerCreatingArgs(context, scopeInfo, tableBuilder.TableDescription, triggerType, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                if (action.Cancel || action.Command == null)
                    return (context, false);

                await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                await action.Command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                await this.InterceptAsync(new TriggerCreatedArgs(context, scopeInfo, tableBuilder.TableDescription, triggerType, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
                action.Command.Dispose();

                return (context, true);
            }
            catch (Exception ex)
            {
                string message = null;

                if (tableBuilder != null && tableBuilder.TableDescription != null)
                    message += $"Table:{tableBuilder.TableDescription.GetFullName()}.";

                message += $"Trigger:{triggerType}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Internal create triggers routine.
        /// </summary>
        internal virtual async Task<(SyncContext Context, bool IsCreated)> InternalCreateTriggersAsync(
            ScopeInfo scopeInfo, SyncContext context, bool overwrite, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            var hasCreatedAtLeastOneTrigger = false;

            var listTriggerType = Enum.GetValues(typeof(DbTriggerType));

            try
            {

                foreach (DbTriggerType triggerType in listTriggerType)
                {
                    bool exists;
                    (context, exists) = await this.InternalExistsTriggerAsync(scopeInfo, context, tableBuilder, triggerType, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                    // Drop trigger if already exists
                    if (exists && overwrite)
                        (context, _) = await this.InternalDropTriggerAsync(scopeInfo, context, tableBuilder, triggerType, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                    var shouldCreate = !exists || overwrite;

                    if (!shouldCreate)
                        continue;

                    bool hasBeenCreated;
                    (context, hasBeenCreated) = await this.InternalCreateTriggerAsync(scopeInfo, context, tableBuilder, triggerType, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                    if (hasBeenCreated && !hasCreatedAtLeastOneTrigger)
                        hasCreatedAtLeastOneTrigger = true;
                }

                return (context, hasCreatedAtLeastOneTrigger);
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

        /// <summary>
        /// Internal drop trigger routine.
        /// </summary>
        internal virtual async Task<(SyncContext Context, bool IsDropped)> InternalDropTriggerAsync(
            ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbTriggerType triggerType, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                using var command = await tableBuilder.GetDropTriggerCommandAsync(triggerType, connection, transaction).ConfigureAwait(false);

                if (command == null)
                    return (context, false);

                var action = await this.InterceptAsync(new TriggerDroppingArgs(context, tableBuilder.TableDescription, triggerType, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                if (action.Cancel || action.Command == null)
                    return (context, false);

                await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                await action.Command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                await this.InterceptAsync(new TriggerDroppedArgs(context, tableBuilder.TableDescription, triggerType, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
                action.Command.Dispose();

                return (context, true);
            }
            catch (Exception ex)
            {
                string message = null;

                if (tableBuilder != null && tableBuilder.TableDescription != null)
                    message += $"Table:{tableBuilder.TableDescription.GetFullName()}.";

                message += $"Trigger:{triggerType}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Internal drop triggers routine.
        /// </summary>
        internal virtual async Task<(SyncContext Context, bool IsDropped)> InternalDropTriggersAsync(
            ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            var hasDroppeAtLeastOneTrigger = false;

            var listTriggerType = Enum.GetValues(typeof(DbTriggerType));

            try
            {
                foreach (DbTriggerType triggerType in listTriggerType)
                {
                    bool exists;
                    (context, exists) = await this.InternalExistsTriggerAsync(scopeInfo, context, tableBuilder, triggerType, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                    if (!exists)
                        continue;
                    bool dropped;
                    (context, dropped) = await this.InternalDropTriggerAsync(scopeInfo, context, tableBuilder, triggerType, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                    if (dropped)
                        hasDroppeAtLeastOneTrigger = true;
                }

                return (context, hasDroppeAtLeastOneTrigger);
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
        /// Internal exists trigger procedure routine.
        /// </summary>
        internal virtual async Task<(SyncContext Context, bool IsExisting)> InternalExistsTriggerAsync(
            ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbTriggerType triggerType, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                // Get exists command
                using var existsCommand = await tableBuilder.GetExistsTriggerCommandAsync(triggerType, connection, transaction).ConfigureAwait(false);

                if (existsCommand == null)
                    return (context, false);

                await this.InterceptAsync(new ExecuteCommandArgs(context, existsCommand, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                var existsResultObject = await existsCommand.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                var exists = SyncTypeConverter.TryConvertTo<int>(existsResultObject) > 0;
                return (context, exists);
            }
            catch (Exception ex)
            {
                string message = null;

                if (tableBuilder != null && tableBuilder.TableDescription != null)
                    message += $"Table:{tableBuilder.TableDescription.GetFullName()}.";

                message += $"Trigger:{triggerType}.";

                throw this.GetSyncError(context, ex, message);
            }
        }
    }
}