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
        /// Create a table
        /// </summary>
        /// <param name="table">A table from your Setup instance you want to create</param>
        public async Task<bool> CreateTableAsync(IScopeInfo scopeInfo, string tableName, string schemaName = null, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);

            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                var hasBeenCreated = false;

                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                bool schemaExists;
                (context, schemaExists) = await InternalExistsSchemaAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!schemaExists)
                    (context, _) = await InternalCreateSchemaAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                bool exists;
                (context, exists) = await InternalExistsTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // should create only if not exists OR if overwrite has been set
                var shouldCreate = !exists || overwrite;

                if (shouldCreate)
                {
                    // Drop if already exists and we need to overwrite
                    if (exists && overwrite)
                        (context, _) = await InternalDropTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    (context, hasBeenCreated) = await InternalCreateTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
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
        /// Create all tables
        /// </summary>
        /// <param name="schema">A complete schema you want to create, containing table, primary keys and relations</param>
        public async Task<bool> CreateTablesAsync(IScopeInfo scopeInfo, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);

            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

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
                        (context, exists) = await InternalExistsTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (exists)
                            (context, _) = await InternalDropTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                    }
                }
                // Then create them
                foreach (var schemaTable in schemaTables)
                {
                    // Get table builder
                    var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                    bool schemaExists;
                    (context, schemaExists) = await InternalExistsSchemaAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (!schemaExists)
                        (context, _) = await InternalCreateSchemaAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    bool exists;
                    (context, exists) = await InternalExistsTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    // should create only if not exists OR if overwrite has been set
                    var shouldCreate = !exists || overwrite;

                    if (shouldCreate)
                    {
                        // Drop if already exists and we need to overwrite
                        if (exists && overwrite)
                            (context, _) = await InternalDropTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                        bool hasBeenCreated;
                        (context, hasBeenCreated) = await InternalCreateTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

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
        /// Check if a table exists
        /// </summary>
        /// <param name="table">A table from your Setup instance, you want to check if it exists</param>
        public async Task<bool> ExistTableAsync(IScopeInfo scopeInfo, string tableName, string schemaName = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Reading, SyncStage.None, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);
                bool exists;
                (context, exists) = await InternalExistsTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);
                return exists;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Drop a table
        /// </summary>
        /// <param name="table">A table from your Setup instance you want to drop</param>
        public async Task<bool> DropTableAsync(IScopeInfo scopeInfo, string tableName, string schemaName = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Deprovisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var hasBeenDropped = false;

                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                bool exists;
                (context, exists) = await InternalExistsTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (exists)
                    (context, hasBeenDropped) = await InternalDropTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return hasBeenDropped;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Drop all tables, declared in the Setup instance
        /// </summary>
        public async Task<bool> DropTablesAsync(IScopeInfo scopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Deprovisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                bool atLeastOneTableHasBeenDropped = false;

                // Sorting tables based on dependencies between them
                var schemaTables = scopeInfo.Schema.Tables.SortByDependencies(tab => tab.GetRelations().Select(r => r.GetParentTable()));

                foreach (var schemaTable in schemaTables.Reverse())
                {
                    // Get table builder
                    var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                    bool exists;
                    (context, exists) = await InternalExistsTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (exists)
                        (context, atLeastOneTableHasBeenDropped) = await InternalDropTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
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
        internal async Task<(SyncContext contet, bool added)> InternalAddColumnAsync(IScopeInfo scopeInfo, SyncContext context, string addedColumnName, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
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

            await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

            await this.InterceptAsync(new ColumnCreatedArgs(context, addedColumnName, tableBuilder.TableDescription, tableName, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            action.Command.Dispose();

            return (context, true);
        }


        /// <summary>
        /// Internal add column routine
        /// </summary>
        internal async Task<(SyncContext context, bool dropped)> InternalDropColumnAsync(IScopeInfo scopeInfo, SyncContext context, string droppedColumnName, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
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

        /// <summary>
        /// Internal create table routine
        /// </summary>
        internal async Task<(SyncContext context, bool created)> InternalCreateTableAsync(IScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
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

        /// <summary>
        /// Internal create table routine
        /// </summary>
        internal async Task<(SyncContext context, bool created)> InternalCreateSchemaAsync(
            IScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
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

        /// <summary>
        /// Internal drop table routine
        /// </summary>
        internal async Task<(SyncContext context, bool dropped)> InternalDropTableAsync(IScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
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

        /// <summary>
        /// Internal exists table procedure routine
        /// </summary>
        internal async Task<(SyncContext context, bool exists)> InternalExistsTableAsync(IScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // Get exists command
            using var existsCommand = await tableBuilder.GetExistsTableCommandAsync(connection, transaction).ConfigureAwait(false);

            if (existsCommand == null)
                return (context, false);

            await this.InterceptAsync(new ExecuteCommandArgs(context, existsCommand, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;
            return (context, exists);

        }

        /// <summary>
        /// Internal exists schema procedure routine
        /// </summary>
        internal async Task<(SyncContext context, bool exists)> InternalExistsSchemaAsync(IScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (string.IsNullOrEmpty(tableBuilder.TableDescription.SchemaName) || tableBuilder.TableDescription.SchemaName == "dbo")
                return (context, true);

            // Get exists command
            using var existsCommand = await tableBuilder.GetExistsSchemaCommandAsync(connection, transaction).ConfigureAwait(false);

            if (existsCommand == null)
                return (context, false);

            await this.InterceptAsync(new ExecuteCommandArgs(context, existsCommand, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;
            return (context, exists);

        }

        /// <summary>
        /// Internal exists column procedure routine
        /// </summary>
        internal async Task<(SyncContext context, bool exists)> InternalExistsColumnAsync(IScopeInfo scopeInfo, SyncContext context, string columnName, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // Get exists command
            using var existsCommand = await tableBuilder.GetExistsColumnCommandAsync(columnName, connection, transaction).ConfigureAwait(false);

            if (existsCommand == null)
                return (context, false);

            await this.InterceptAsync(new ExecuteCommandArgs(context, existsCommand, default, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;

            return (context, exists);
        }



    }
}
