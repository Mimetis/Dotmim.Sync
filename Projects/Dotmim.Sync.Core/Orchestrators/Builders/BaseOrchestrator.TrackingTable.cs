using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains internals methods to create tracking tables.
    /// </summary>
    public abstract partial class BaseOrchestrator
    {
        /// <summary>
        /// Create a <strong>Tracking Table</strong> for a given table present in an existing scopeInfo.
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
        /// var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        /// await remoteOrchestrator.CreateTrackingTableAsync(scopeInfo, "Employee");
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="scopeInfo">ScopeInfo instance used to defines tracking table generation (name, prefix, suffix...).</param>
        /// <param name="tableName"><strong>Table Name</strong>. Should exists in ScopeInfo instance.</param>
        /// <param name="schemaName">Optional <strong>Schema Name</strong>. Only available for <strong>Sql Server</strong>.</param>
        /// <param name="overwrite">If specified the tracking table is dropped, if exists, then created.</param>
        /// <param name="connection">Optional Connection.</param>
        /// <param name="transaction">Optional Transaction.</param>
        public async Task<bool> CreateTrackingTableAsync(ScopeInfo scopeInfo, string tableName, string schemaName = default, bool overwrite = false,
            DbConnection connection = null, DbTransaction transaction = null)
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

                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    var hasBeenCreated = false;

                    // Get table builder
                    var tableBuilder = this.GetSyncAdapter(schemaTable, scopeInfo).GetTableBuilder();

                    bool schemaExists;
                    (context, schemaExists) = await this.InternalExistsSchemaAsync(scopeInfo, context, tableBuilder,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (!schemaExists)
                    {
                        (context, _) = await this.InternalCreateSchemaAsync(scopeInfo, context, tableBuilder,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                    }

                    bool exists;
                    (context, exists) = await this.InternalExistsTrackingTableAsync(scopeInfo, context, tableBuilder,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    // should create only if not exists OR if overwrite has been set
                    var shouldCreate = !exists || overwrite;

                    if (shouldCreate)
                    {
                        // Drop if already exists and we need to overwrite
                        if (exists && overwrite)
                        {
                            (context, _) = await this.InternalDropTrackingTableAsync(scopeInfo, context, tableBuilder,
                                runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                        }

                        (context, hasBeenCreated) = await this.InternalCreateTrackingTableAsync(scopeInfo, context, tableBuilder,
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

                message += $"Overwrite:{overwrite}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Check if a <strong>Tracking Table</strong> exists, for a given table present in an existing scopeInfo.
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
        /// var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        /// var exists = await remoteOrchestrator.ExistTrackingTableAsync(scopeInfo, "Employee");
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="scopeInfo">ScopeInfo instance used to defines tracking table generation (name, prefix, suffix...).</param>
        /// <param name="tableName"><strong>Table Name</strong>. Should exists in ScopeInfo instance.</param>
        /// <param name="schemaName">Optional <strong>Schema Name</strong>. Only available for <strong>Sql Server</strong>.</param>
        /// <param name="connection">Optional Connection.</param>
        /// <param name="transaction">Optional Transaction.</param>
        public async Task<bool> ExistTrackingTableAsync(ScopeInfo scopeInfo, string tableName, string schemaName = default,
            DbConnection connection = null, DbTransaction transaction = null)
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

                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {

                    // Get table builder
                    var tableBuilder = this.GetSyncAdapter(schemaTable, scopeInfo).GetTableBuilder();

                    bool exists;
                    (context, exists) = await this.InternalExistsTrackingTableAsync(scopeInfo, context, tableBuilder,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

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
        /// Create ALL <strong>Tracking Tables</strong> for a given table present in an existing scopeInfo.
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
        /// var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        /// await remoteOrchestrator.CreateTrackingTablesAsync(scopeInfo);
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="scopeInfo">ScopeInfo instance used to defines tracking table generation (name, prefix, suffix...).</param>
        /// <param name="overwrite">If specified the tracking table is dropped, if exists, then created.</param>
        /// <param name="connection">Optional Connection.</param>
        /// <param name="transaction">Optional Transaction.</param>
        public async Task<bool> CreateTrackingTablesAsync(ScopeInfo scopeInfo, bool overwrite = false,
            DbConnection connection = null, DbTransaction transaction = null)
        {
            Guard.ThrowIfNull(scopeInfo);
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    var atLeastOneHasBeenCreated = false;

                    foreach (var schemaTable in scopeInfo.Schema.Tables)
                    {
                        // Get table builder
                        var tableBuilder = this.GetSyncAdapter(schemaTable, scopeInfo).GetTableBuilder();

                        bool schemaExists;
                        (context, schemaExists) = await this.InternalExistsSchemaAsync(scopeInfo, context, tableBuilder,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                        if (!schemaExists)
                        {
                            (context, _) = await this.InternalCreateSchemaAsync(scopeInfo, context, tableBuilder,
                                runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                        }

                        bool exists;
                        (context, exists) = await this.InternalExistsTrackingTableAsync(scopeInfo, context, tableBuilder,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                        // should create only if not exists OR if overwrite has been set
                        var shouldCreate = !exists || overwrite;

                        if (shouldCreate)
                        {
                            // Drop if already exists and we need to overwrite
                            if (exists && overwrite)
                            {
                                (context, _) = await this.InternalDropTrackingTableAsync(scopeInfo, context, tableBuilder,
                                    runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                            }

                            bool hasBeenCreated;
                            (context, hasBeenCreated) = await this.InternalCreateTrackingTableAsync(scopeInfo, context, tableBuilder,
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
                string message = null;

                message += $"Overwrite:{overwrite}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Drop a tracking table.
        /// </summary>
        public async Task<bool> DropTrackingTableAsync(ScopeInfo scopeInfo, string tableName, string schemaName = default,
            DbConnection connection = null, DbTransaction transaction = null)
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

                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.Deprovisioning, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    var hasBeenDropped = false;

                    var tableBuilder = this.GetSyncAdapter(schemaTable, scopeInfo).GetTableBuilder();

                    bool exists;
                    (context, exists) = await this.InternalExistsTrackingTableAsync(scopeInfo, context, tableBuilder,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (exists)
                    {
                        (context, hasBeenDropped) = await this.InternalDropTrackingTableAsync(scopeInfo, context, tableBuilder,
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

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Drop all <strong>Tracking Tables</strong> for a given table present in an existing scopeInfo.
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
        /// var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        /// await remoteOrchestrator.DropTrackingTablesAsync(scopeInfo);
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="scopeInfo">ScopeInfo instance used to defines tracking table generation (name, prefix, suffix...).</param>
        /// <param name="connection">Optional Connection.</param>
        /// <param name="transaction">Optional Transaction.</param>
        public async Task<bool> DropTrackingTablesAsync(ScopeInfo scopeInfo, DbConnection connection = null, DbTransaction transaction = null)
        {
            Guard.ThrowIfNull(scopeInfo);
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Deprovisioning, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    var atLeastOneTrackingTableHasBeenDropped = false;

                    foreach (var schemaTable in scopeInfo.Schema.Tables.Reverse())
                    {
                        // Get table builder
                        var tableBuilder = this.GetSyncAdapter(schemaTable, scopeInfo).GetTableBuilder();

                        bool exists;
                        (context, exists) = await this.InternalExistsTrackingTableAsync(scopeInfo, context, tableBuilder,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                        if (exists)
                        {
                            (context, atLeastOneTrackingTableHasBeenDropped) = await this.InternalDropTrackingTableAsync(scopeInfo, context, tableBuilder,
                                runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                        }
                    }

                    await runner.CommitAsync().ConfigureAwait(false);

                    return atLeastOneTrackingTableHasBeenDropped;
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Internal create tracking table routine.
        /// </summary>
        internal virtual async Task<(SyncContext Context, bool IsCreated)> InternalCreateTrackingTableAsync(
            ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {

                if (this.Provider == null)
                    throw new MissingProviderException(nameof(this.InternalCreateTrackingTableAsync));

                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.Provisioning, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    if (tableBuilder.TableDescription.Columns.Count <= 0)
                        throw new MissingsColumnException(tableBuilder.TableDescription.GetFullName());

                    if (tableBuilder.TableDescription.PrimaryKeys.Count <= 0)
                        throw new MissingPrimaryKeyException(tableBuilder.TableDescription.GetFullName());

                    using var command = await tableBuilder.GetCreateTrackingTableCommandAsync(runner.Connection, runner.Transaction).ConfigureAwait(false);

                    if (command == null)
                        return (context, false);

                    var trackingTableNames = tableBuilder.GetParsedTrackingTableNames();

                    var action = await this.InterceptAsync(
                        new TrackingTableCreatingArgs(context, scopeInfo, tableBuilder.TableDescription, trackingTableNames.QuotedFullName, command, runner.Connection, runner.Transaction),
                        runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (action.Cancel || action.Command == null)
                        return (context, false);

                    await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    await action.Command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

                    await this.InterceptAsync(new TrackingTableCreatedArgs(context, scopeInfo, tableBuilder.TableDescription, trackingTableNames.QuotedFullName, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    action.Command.Dispose();

                    return (context, true);
                }
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
        /// Internal drop tracking table routine.
        /// </summary>
        internal virtual async Task<(SyncContext Context, bool IsDropped)> InternalDropTrackingTableAsync(
            ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.Deprovisioning, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    using var command = await tableBuilder.GetDropTrackingTableCommandAsync(connection, transaction).ConfigureAwait(false);

                    if (command == null)
                        return (context, false);

                    var trackingTableName = tableBuilder.GetParsedTrackingTableNames().QuotedFullName;

                    var action = await this.InterceptAsync(new TrackingTableDroppingArgs(context, scopeInfo, tableBuilder.TableDescription, trackingTableName, command, runner.Connection, runner.Transaction), progress, cancellationToken).ConfigureAwait(false);

                    if (action.Cancel || action.Command == null)
                        return (context, false);

                    await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, runner.Connection, runner.Transaction), progress, cancellationToken).ConfigureAwait(false);

                    await action.Command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    await this.InterceptAsync(new TrackingTableDroppedArgs(context, scopeInfo, tableBuilder.TableDescription, trackingTableName, runner.Connection, runner.Transaction), progress, cancellationToken).ConfigureAwait(false);

                    action.Command.Dispose();

                    return (context, true);
                }
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
        /// Internal exists tracking table procedure routine.
        /// </summary>
        internal virtual async Task<(SyncContext Context, bool IsExisting)> InternalExistsTrackingTableAsync(ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {// Get exists command
                    using var existsCommand = await tableBuilder.GetExistsTrackingTableCommandAsync(runner.Connection, runner.Transaction).ConfigureAwait(false);

                    if (existsCommand == null)
                        return (context, false);

                    await this.InterceptAsync(new ExecuteCommandArgs(context, existsCommand, default, runner.Connection, runner.Transaction), progress, cancellationToken).ConfigureAwait(false);

                    var existsResultObject = await existsCommand.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                    var exists = SyncTypeConverter.TryConvertTo<int>(existsResultObject) > 0;
                    await runner.CommitAsync().ConfigureAwait(false);
                    return (context, exists);
                }
            }
            catch (Exception ex)
            {
                string message = null;

                if (tableBuilder != null && tableBuilder.TableDescription != null)
                    message += $"Table:{tableBuilder.TableDescription.GetFullName()}.";

                throw this.GetSyncError(context, ex, message);
            }
        }
    }
}