
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
    /// <summary>
    /// Based Orchestrator class. Don't use it as is. Prefer use <see cref="LocalOrchestrator"/>, <see cref="RemoteOrchestrator"/> or <c>WebRemoteOrchestrator</c> 
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
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);

            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning).ConfigureAwait(false);
                var hasBeenCreated = false;

                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                bool schemaExists;
                (context, schemaExists) = await InternalExistsSchemaAsync(scopeInfo, context, tableBuilder, 
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (!schemaExists)
                    (context, _) = await InternalCreateSchemaAsync(scopeInfo, context, tableBuilder, 
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                bool exists;
                (context, exists) = await InternalExistsTableAsync(scopeInfo, context, tableBuilder, 
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // should create only if not exists OR if overwrite has been set
                var shouldCreate = !exists || overwrite;

                if (shouldCreate)
                {
                    // Drop if already exists and we need to overwrite
                    if (exists && overwrite)
                        (context, _) = await InternalDropTableAsync(scopeInfo, context, tableBuilder, 
                            runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    (context, hasBeenCreated) = await InternalCreateTableAsync(scopeInfo, context, tableBuilder,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
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
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);

            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning).ConfigureAwait(false);

                var atLeastOneHasBeenCreated = false;

                // Sorting tables based on dependencies between them
                var schemaTables = scopeInfo.Schema.Tables.SortByDependencies(tab => tab.GetRelations().Select(r => r.GetParentTable()));

                // if we overwritten all tables, we need to delete all of them, before recreating them
                if (overwrite)
                {
                    foreach (var schemaTable in schemaTables.Reverse())
                    {
                        var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);
                        bool exists;
                        (context, exists) = await InternalExistsTableAsync(scopeInfo, context, tableBuilder, 
                            runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        if (exists)
                            (context, _) = await InternalDropTableAsync(scopeInfo, context, tableBuilder, 
                                runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
                    }
                }
                // Then create them
                foreach (var schemaTable in schemaTables)
                {
                    // Get table builder
                    var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                    bool schemaExists;
                    (context, schemaExists) = await InternalExistsSchemaAsync(scopeInfo, context, tableBuilder, 
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    if (!schemaExists)
                        (context, _) = await InternalCreateSchemaAsync(scopeInfo, context, tableBuilder, 
                            runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    bool exists;
                    (context, exists) = await InternalExistsTableAsync(scopeInfo, context, tableBuilder, 
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    // should create only if not exists OR if overwrite has been set
                    var shouldCreate = !exists || overwrite;

                    if (shouldCreate)
                    {
                        // Drop if already exists and we need to overwrite
                        if (exists && overwrite)
                            (context, _) = await InternalDropTableAsync(scopeInfo, context, tableBuilder, 
                                runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        bool hasBeenCreated;
                        (context, hasBeenCreated) = await InternalCreateTableAsync(scopeInfo, context, tableBuilder, 
                            runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        if (hasBeenCreated)
                            atLeastOneHasBeenCreated = true;
                    }
                }

                await runner.CommitAsync().ConfigureAwait(false);

                return atLeastOneHasBeenCreated;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
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
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None).ConfigureAwait(false);

                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);
                bool exists;
                (context, exists) = await InternalExistsTableAsync(scopeInfo, context, tableBuilder, 
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);
                return exists;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
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
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Deprovisioning).ConfigureAwait(false);

                var hasBeenDropped = false;

                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                bool exists;
                (context, exists) = await InternalExistsTableAsync(scopeInfo, context, tableBuilder, 
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (exists)
                    (context, hasBeenDropped) = await InternalDropTableAsync(scopeInfo, context, tableBuilder, 
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return hasBeenDropped;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
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
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Deprovisioning).ConfigureAwait(false);
                bool atLeastOneTableHasBeenDropped = false;

                // Sorting tables based on dependencies between them
                var schemaTables = scopeInfo.Schema.Tables.SortByDependencies(tab => tab.GetRelations().Select(r => r.GetParentTable()));

                foreach (var schemaTable in schemaTables.Reverse())
                {
                    // Get table builder
                    var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                    bool exists;
                    (context, exists) = await InternalExistsTableAsync(scopeInfo, context, tableBuilder,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    if (exists)
                        (context, atLeastOneTableHasBeenDropped) = await InternalDropTableAsync(scopeInfo, context, tableBuilder, 
                            runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
                }

                await runner.CommitAsync().ConfigureAwait(false);

                return atLeastOneTableHasBeenDropped;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Internal add column routine
        /// </summary>
        internal async Task<(SyncContext contet, bool added)> InternalAddColumnAsync(ScopeInfo scopeInfo, SyncContext context, string addedColumnName, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (Provider == null)
                throw new MissingProviderException(nameof(InternalAddColumnAsync));

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

            // Parametrized command timeout established if exist
            if (this.Options.DbCommandTimeout.HasValue)
                action.Command.CommandTimeout = this.Options.DbCommandTimeout.Value;

            await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            
            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

            await this.InterceptAsync(new ColumnCreatedArgs(context, addedColumnName, tableBuilder.TableDescription, tableName, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            action.Command.Dispose();

            return (context, true);
        }


        /// <summary>
        /// Internal add column routine
        /// </summary>
        internal async Task<(SyncContext context, bool dropped)> InternalDropColumnAsync(ScopeInfo scopeInfo, SyncContext context, string droppedColumnName, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
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

            // Parametrized command timeout established if exist
            if (this.Options.DbCommandTimeout.HasValue)
                action.Command.CommandTimeout = this.Options.DbCommandTimeout.Value;

            await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

            await this.InterceptAsync(new ColumnDroppedArgs(context, droppedColumnName, tableBuilder.TableDescription, tableName, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            action.Command.Dispose();

            return (context, true);
        }

        /// <summary>
        /// Internal create table routine
        /// </summary>
        internal async Task<(SyncContext context, bool created)> InternalCreateTableAsync(ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
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

            // Parametrized command timeout established if exist
            if (this.Options.DbCommandTimeout.HasValue)
                action.Command.CommandTimeout = this.Options.DbCommandTimeout.Value;

            await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

            await this.InterceptAsync(new TableCreatedArgs(context, tableBuilder.TableDescription, tableName, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            action.Command.Dispose();
            
            return (context, true);
        }

        /// <summary>
        /// Internal create table routine
        /// </summary>
        internal async Task<(SyncContext context, bool created)> InternalCreateSchemaAsync(
            ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            using var command = await tableBuilder.GetCreateSchemaCommandAsync(connection, transaction).ConfigureAwait(false);

            if (command == null)
                return (context, false);

            var action = await this.InterceptAsync(new SchemaNameCreatingArgs(context, tableBuilder.TableDescription, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return (context, false);

            // Parametrized command timeout established if exist
            if (this.Options.DbCommandTimeout.HasValue)
                action.Command.CommandTimeout = this.Options.DbCommandTimeout.Value;

            await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

            await this.InterceptAsync(new SchemaNameCreatedArgs(context, tableBuilder.TableDescription, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            action.Command.Dispose();

            return (context, true);
        }

        /// <summary>
        /// Internal drop table routine
        /// </summary>
        internal async Task<(SyncContext context, bool dropped)> InternalDropTableAsync(ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            using var command = await tableBuilder.GetDropTableCommandAsync(connection, transaction).ConfigureAwait(false);

            if (command == null)
                return (context, false);

            var (tableName, _) = this.Provider.GetParsers(tableBuilder.TableDescription, scopeInfo.Setup);

            var action = await this.InterceptAsync(new TableDroppingArgs(context, tableBuilder.TableDescription, tableName, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return (context, false);

            // Parametrized command timeout established if exist
            if (this.Options.DbCommandTimeout.HasValue)
                action.Command.CommandTimeout = this.Options.DbCommandTimeout.Value;

            await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

            await this.InterceptAsync(new TableDroppedArgs(context, tableBuilder.TableDescription, tableName, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            action.Command.Dispose();

            return (context, true);

        }

        /// <summary>
        /// Internal exists table procedure routine
        /// </summary>
        internal async Task<(SyncContext context, bool exists)> InternalExistsTableAsync(ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // Get exists command
            using var existsCommand = await tableBuilder.GetExistsTableCommandAsync(connection, transaction).ConfigureAwait(false);

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
        /// Internal exists schema procedure routine
        /// </summary>
        internal async Task<(SyncContext context, bool exists)> InternalExistsSchemaAsync(ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (string.IsNullOrEmpty(tableBuilder.TableDescription.SchemaName) || tableBuilder.TableDescription.SchemaName == "dbo")
                return (context, true);

            // Get exists command
            using var existsCommand = await tableBuilder.GetExistsSchemaCommandAsync(connection, transaction).ConfigureAwait(false);

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
        /// Internal exists column procedure routine
        /// </summary>
        internal async Task<(SyncContext context, bool exists)> InternalExistsColumnAsync(ScopeInfo scopeInfo, SyncContext context, string columnName, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // Get exists command
            using var existsCommand = await tableBuilder.GetExistsColumnCommandAsync(columnName, connection, transaction).ConfigureAwait(false);

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

    }
}
