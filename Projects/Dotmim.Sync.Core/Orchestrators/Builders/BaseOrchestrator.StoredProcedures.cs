using Dotmim.Sync.Args;
using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
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

            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                var syncTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (syncTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning).ConfigureAwait(false);

                bool hasBeenCreated = false;

                // Get table builder
                var tableBuilder = this.GetTableBuilder(syncTable, scopeInfo);

                bool exists;
                (context, exists) = await InternalExistsStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType, 
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // should create only if not exists OR if overwrite has been set
                var shouldCreate = !exists || overwrite;

                if (shouldCreate)
                {
                    // Drop storedProcedure if already exists
                    if (exists && overwrite)
                        (context, _) = await InternalDropStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    (context, hasBeenCreated) = await InternalCreateStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
                }

                await runner.CommitAsync().ConfigureAwait(false);

                return hasBeenCreated;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
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
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);

            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                var syncTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (syncTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning).ConfigureAwait(false);

                // Get table builder
                var tableBuilder = this.GetTableBuilder(syncTable, scopeInfo);
                bool isCreated;
                (context, isCreated) = await InternalCreateStoredProceduresAsync(scopeInfo, context, overwrite, tableBuilder,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return isCreated;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
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
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || scopeInfo.Schema.Tables.Count <= 0 || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None).ConfigureAwait(false);

                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                bool exists;
                (context, exists) = await InternalExistsStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType, 
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                return exists;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
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
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || scopeInfo.Schema.Tables.Count <= 0 || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Deprovisioning).ConfigureAwait(false);
                bool hasBeenDropped = false;

                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                bool existsAndCanBeDeleted;
                (context, existsAndCanBeDeleted) = await InternalExistsStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType, 
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (existsAndCanBeDeleted)
                    (context, hasBeenDropped) = await InternalDropStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType, 
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // Removing cached commands
                this.RemoveCommands();

                await runner.CommitAsync().ConfigureAwait(false);

                return hasBeenDropped;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
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
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || scopeInfo.Schema.Tables.Count <= 0 || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Deprovisioning).ConfigureAwait(false);

                var hasDroppedAtLeastOneStoredProcedure = false;

                // using a fake SyncTable based on SetupTable, since we don't need columns
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                // check bulk before
                (context, hasDroppedAtLeastOneStoredProcedure) = await InternalDropStoredProceduresAsync(scopeInfo, context, tableBuilder, 
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // Removing cached commands
                this.RemoveCommands();

                await runner.CommitAsync().ConfigureAwait(false);

                return hasDroppedAtLeastOneStoredProcedure;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }

        /// <summary>
        /// Internal create Stored Procedure routine
        /// </summary>
        internal async Task<(SyncContext context, bool created)> InternalCreateStoredProcedureAsync(ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbStoredProcedureType storedProcedureType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
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

            // Parametrized command timeout established if exist
            if (this.Options.DbCommandTimeout.HasValue)
                action.Command.CommandTimeout = this.Options.DbCommandTimeout.Value;

            await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);
            await this.InterceptAsync(new StoredProcedureCreatedArgs(context, tableBuilder.TableDescription, storedProcedureType, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            return (context, true);
        }

        /// <summary>
        /// Internal drop storedProcedure routine
        /// </summary>
        internal async Task<(SyncContext context, bool dropped)> InternalDropStoredProcedureAsync(ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbStoredProcedureType storedProcedureType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var filter = tableBuilder.TableDescription.GetFilter();

            var command = await tableBuilder.GetDropStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction).ConfigureAwait(false);

            if (command == null)
                return (context, false);

            var action = await this.InterceptAsync(new StoredProcedureDroppingArgs(context, tableBuilder.TableDescription, storedProcedureType, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return (context, false);

            // Parametrized command timeout established if exist
            if (this.Options.DbCommandTimeout.HasValue)
                action.Command.CommandTimeout = this.Options.DbCommandTimeout.Value;

            await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

            await this.InterceptAsync(new StoredProcedureDroppedArgs(context, tableBuilder.TableDescription, storedProcedureType, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            return (context, true);
        }

        /// <summary>
        /// Internal exists storedProcedure procedure routine
        /// </summary>
        internal async Task<(SyncContext context, bool exists)> InternalExistsStoredProcedureAsync(ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbStoredProcedureType storedProcedureType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var filter = tableBuilder.TableDescription.GetFilter();

            var existsCommand = await tableBuilder.GetExistsStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction).ConfigureAwait(false);
            if (existsCommand == null)
                return (context, false);

            // Parametrized command timeout established if exist
            if (this.Options.DbCommandTimeout.HasValue)
                existsCommand.CommandTimeout = this.Options.DbCommandTimeout.Value;

            await this.InterceptAsync(new ExecuteCommandArgs(context, existsCommand, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;

            return (context, exists);
        }

        /// <summary>
        /// Internal drop storedProcedures routine
        /// </summary>
        internal async Task<(SyncContext context, bool dropped)> InternalDropStoredProceduresAsync(
            ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // check bulk before
            var hasDroppedAtLeastOneStoredProcedure = false;

            var listStoredProcedureType = Enum.GetValues(typeof(DbStoredProcedureType)).Cast<DbStoredProcedureType>().OrderBy(sp => (int)sp);

            foreach (DbStoredProcedureType storedProcedureType in Enum.GetValues(typeof(DbStoredProcedureType)))
            {
                bool exists;
                (context, exists) = await InternalExistsStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                if (exists)
                {
                    bool dropped;
                    (context, dropped) = await InternalDropStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    // If at least one stored proc has been dropped, we're good to return true;
                    if (dropped)
                        hasDroppedAtLeastOneStoredProcedure = true;
                }
            }

            return (context, hasDroppedAtLeastOneStoredProcedure);
        }

        /// <summary>
        /// Internal create storedProcedures routine
        /// </summary>
        internal async Task<(SyncContext context, bool created)> InternalCreateStoredProceduresAsync(
            ScopeInfo scopeInfo, SyncContext context, bool overwrite, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var hasCreatedAtLeastOneStoredProcedure = false;

            // Order Asc is the correct order to Delete
            var listStoredProcedureType = Enum.GetValues(typeof(DbStoredProcedureType)).Cast<DbStoredProcedureType>().OrderBy(sp => (int)sp);

            // we need to drop bulk in order to be sure bulk type is delete after all
            if (overwrite)
            {
                foreach (DbStoredProcedureType storedProcedureType in listStoredProcedureType)
                {
                    bool exists;
                    (context, exists) = await InternalExistsStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (exists)
                        (context, _) = await InternalDropStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                }

            }

            // Order Desc is the correct order to Create
            listStoredProcedureType = Enum.GetValues(typeof(DbStoredProcedureType)).Cast<DbStoredProcedureType>().OrderByDescending(sp => (int)sp);

            foreach (DbStoredProcedureType storedProcedureType in listStoredProcedureType)
            {
                // check with filter
                if ((storedProcedureType is DbStoredProcedureType.SelectChangesWithFilters || storedProcedureType is DbStoredProcedureType.SelectInitializedChangesWithFilters)
                    && tableBuilder.TableDescription.GetFilter() == null)
                    continue;

                bool exists;
                (context, exists) = await InternalExistsStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Drop storedProcedure if already exists
                if (exists && overwrite)
                    (context, _) = await InternalDropStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var shouldCreate = !exists || overwrite;

                if (!shouldCreate)
                    continue;

                bool created;
                (context, created) = await InternalCreateStoredProcedureAsync(scopeInfo, context, tableBuilder, storedProcedureType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // If at least one stored proc has been created, we're good to return true;
                if (created)
                    hasCreatedAtLeastOneStoredProcedure = true;
            }

            return (context, hasCreatedAtLeastOneStoredProcedure);
        }

    }
}
