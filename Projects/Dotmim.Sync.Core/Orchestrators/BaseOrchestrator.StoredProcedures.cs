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
        /// Create a Stored Procedure
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to create the Stored Procedure</param>
        /// <param name="storedProcedureType">StoredProcedure type</param>
        /// <param name="overwrite">If true, drop the existing stored procedure then create again</param>
        public async Task<bool> CreateStoredProcedureAsync(SetupTable table, DbStoredProcedureType storedProcedureType, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(SyncStage.Provisioning, connection, transaction, cancellationToken).ConfigureAwait(false);
                bool hasBeenCreated = false;
                var (schemaTable, _) = await this.InternalGetTableSchemaAsync(this.GetContext(), this.Setup, table, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (schemaTable == null)
                    throw new MissingTableException(table.GetFullName());

                // Create a temporary SyncSet for attaching to the schemaTable
                var schema = new SyncSet();
                schema.Tables.Add(schemaTable);
                schema.EnsureSchema();

                // copy filters from setup
                foreach (var filter in this.Setup.Filters)
                    schema.Filters.Add(filter);

                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, this.Setup);

                var exists = await InternalExistsStoredProcedureAsync(this.GetContext(), tableBuilder, storedProcedureType, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // should create only if not exists OR if overwrite has been set
                var shouldCreate = !exists || overwrite;

                if (shouldCreate)
                {
                    // Drop storedProcedure if already exists
                    if (exists && overwrite)
                        await InternalDropStoredProcedureAsync(this.GetContext(), tableBuilder, storedProcedureType, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    hasBeenCreated = await InternalCreateStoredProcedureAsync(this.GetContext(), tableBuilder, storedProcedureType, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                await runner.CommitAsync().ConfigureAwait(false);

                return hasBeenCreated;
            }
            catch (Exception ex)
            {
                throw GetSyncError(ex);
            }
        }

        /// <summary>
        /// Create a Stored Procedure
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to create the Stored Procedures</param>
        /// <param name="overwrite">If true, drop the existing Stored Procedures then create them all, again</param>
        public async Task<bool> CreateStoredProceduresAsync(SetupTable table, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(SyncStage.Provisioning, connection, transaction, cancellationToken).ConfigureAwait(false);
                var schema = await this.InternalGetSchemaAsync(this.GetContext(), this.Setup, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                var schemaTable = schema.Tables[table.TableName, table.SchemaName];

                if (schemaTable == null)
                    throw new MissingTableException(table.GetFullName());

                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, this.Setup);
                var r = await InternalCreateStoredProceduresAsync(this.GetContext(), overwrite, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return r;
            }
            catch (Exception ex)
            {
                throw GetSyncError(ex);
            }

        }

        /// <summary>
        /// Check if a Stored Procedure exists
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to check if the Stored Procedure exists</param>
        /// <param name="storedProcedureType">StoredProcedure type</param>
        public async Task<bool> ExistStoredProcedureAsync(SetupTable table, DbStoredProcedureType storedProcedureType, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(SyncStage.None, connection, transaction, cancellationToken).ConfigureAwait(false);
                // using a fake SyncTable based on SetupTable, since we don't need columns
                var schemaTable = new SyncTable(table.TableName, table.SchemaName);

                // Create a temporary SyncSet for attaching to the schemaTable
                var schema = new SyncSet();
                schema.Tables.Add(schemaTable);
                schema.EnsureSchema();

                // copy filters from setup
                foreach (var filter in this.Setup.Filters)
                    schema.Filters.Add(filter);

                var tableBuilder = this.GetTableBuilder(schemaTable, this.Setup);

                var exists = await InternalExistsStoredProcedureAsync(this.GetContext(), tableBuilder, storedProcedureType, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                return exists;

            }
            catch (Exception ex)
            {
                throw GetSyncError(ex);
            }
        }

        /// <summary>
        /// Drop a Stored Procedure
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to drop the Stored Procedure</param>
        /// <param name="storedProcedureType">Stored Procedure type</param>
        public async Task<bool> DropStoredProcedureAsync(SetupTable table, DbStoredProcedureType storedProcedureType, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(SyncStage.Deprovisioning, connection, transaction, cancellationToken).ConfigureAwait(false);
                bool hasBeenDropped = false;

                var schemaTable = new SyncTable(table.TableName, table.SchemaName);
                var schema = new SyncSet();
                schema.Tables.Add(schemaTable);
                schema.EnsureSchema();

                // copy filters from setup
                foreach (var filter in this.Setup.Filters)
                    schema.Filters.Add(filter);

                var tableBuilder = this.GetTableBuilder(schemaTable, this.Setup);

                var existsAndCanBeDeleted = await InternalExistsStoredProcedureAsync(this.GetContext(), tableBuilder, storedProcedureType, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (existsAndCanBeDeleted)
                    hasBeenDropped = await InternalDropStoredProcedureAsync(this.GetContext(), tableBuilder, storedProcedureType, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // Removing cached commands
                var syncAdapter = this.GetSyncAdapter(schemaTable, this.Setup);
                syncAdapter.RemoveCommands();

                await runner.CommitAsync().ConfigureAwait(false);

                return hasBeenDropped;
            }
            catch (Exception ex)
            {
                throw GetSyncError(ex);
            }
        }

        /// <summary>
        /// Drop all Stored Procedures
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to drop all the Stored Procedures</param>
        public async Task<bool> DropStoredProceduresAsync(SetupTable table, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(SyncStage.Deprovisioning, connection, transaction, cancellationToken).ConfigureAwait(false);
                var hasDroppedAtLeastOneStoredProcedure = false;

                // using a fake SyncTable based on SetupTable, since we don't need columns
                var schemaTable = new SyncTable(table.TableName, table.SchemaName);
                var schema = new SyncSet();
                schema.Tables.Add(schemaTable);
                schema.EnsureSchema();

                // copy filters from setup
                foreach (var filter in this.Setup.Filters)
                    schema.Filters.Add(filter);

                // using a fake SyncTable based on SetupTable, since we don't need columns
                var tableBuilder = this.GetTableBuilder(schemaTable, this.Setup);

                // check bulk before
                hasDroppedAtLeastOneStoredProcedure = await InternalDropStoredProceduresAsync(this.GetContext(), tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // Removing cached commands
                var syncAdapter = this.GetSyncAdapter(schemaTable, this.Setup);
                syncAdapter.RemoveCommands();

                await runner.CommitAsync().ConfigureAwait(false);

                return hasDroppedAtLeastOneStoredProcedure;
            }
            catch (Exception ex)
            {
                throw GetSyncError(ex);
            }

        }

        /// <summary>
        /// Internal create Stored Procedure routine
        /// </summary>
        internal async Task<bool> InternalCreateStoredProcedureAsync(SyncContext ctx, DbTableBuilder tableBuilder, DbStoredProcedureType storedProcedureType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (tableBuilder.TableDescription.Columns.Count <= 0)
                throw new MissingsColumnException(tableBuilder.TableDescription.GetFullName());

            if (tableBuilder.TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(tableBuilder.TableDescription.GetFullName());

            var filter = tableBuilder.TableDescription.GetFilter();

            var command = await tableBuilder.GetCreateStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction).ConfigureAwait(false);

            if (command == null)
                return false;

            var action = new StoredProcedureCreatingArgs(ctx, tableBuilder.TableDescription, storedProcedureType, command, connection, transaction);
            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);
            await this.InterceptAsync(new StoredProcedureCreatedArgs(ctx, tableBuilder.TableDescription, storedProcedureType, connection, transaction), cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal drop storedProcedure routine
        /// </summary>
        internal async Task<bool> InternalDropStoredProcedureAsync(SyncContext ctx, DbTableBuilder tableBuilder, DbStoredProcedureType storedProcedureType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var filter = tableBuilder.TableDescription.GetFilter();

            var command = await tableBuilder.GetDropStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction).ConfigureAwait(false);

            if (command == null)
                return false;

            var action = new StoredProcedureDroppingArgs(ctx, tableBuilder.TableDescription, storedProcedureType, command, connection, transaction);
            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await action.Command.ExecuteNonQueryAsync();

            await this.InterceptAsync(new StoredProcedureDroppedArgs(ctx, tableBuilder.TableDescription, storedProcedureType, connection, transaction), cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal exists storedProcedure procedure routine
        /// </summary>
        internal async Task<bool> InternalExistsStoredProcedureAsync(SyncContext ctx, DbTableBuilder tableBuilder, DbStoredProcedureType storedProcedureType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var filter = tableBuilder.TableDescription.GetFilter();

            var existsCommand = await tableBuilder.GetExistsStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction).ConfigureAwait(false);
            if (existsCommand == null)
                return false;

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;
            return exists;
        }

        /// <summary>
        /// Internal drop storedProcedures routine
        /// </summary>
        internal async Task<bool> InternalDropStoredProceduresAsync(SyncContext ctx, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // check bulk before
            var hasDroppedAtLeastOneStoredProcedure = false;

            var listStoredProcedureType = Enum.GetValues(typeof(DbStoredProcedureType)).Cast<DbStoredProcedureType>().OrderBy(sp => (int)sp);

            foreach (DbStoredProcedureType storedProcedureType in Enum.GetValues(typeof(DbStoredProcedureType)))
            {
                var exists = await InternalExistsStoredProcedureAsync(ctx, tableBuilder, storedProcedureType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                if (exists)
                {
                    var dropped = await InternalDropStoredProcedureAsync(ctx, tableBuilder, storedProcedureType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    // If at least one stored proc has been dropped, we're good to return true;
                    if (dropped)
                        hasDroppedAtLeastOneStoredProcedure = true;
                }
            }

            return hasDroppedAtLeastOneStoredProcedure;
        }

        /// <summary>
        /// Internal create storedProcedures routine
        /// </summary>
        internal async Task<bool> InternalCreateStoredProceduresAsync(SyncContext ctx, bool overwrite, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var hasCreatedAtLeastOneStoredProcedure = false;

            // Order Asc is the correct order to Delete
            var listStoredProcedureType = Enum.GetValues(typeof(DbStoredProcedureType)).Cast<DbStoredProcedureType>().OrderBy(sp => (int)sp);

            // we need to drop bulk in order to be sure bulk type is delete after all
            if (overwrite)
            {
                foreach (DbStoredProcedureType storedProcedureType in listStoredProcedureType)
                {
                    var exists = await InternalExistsStoredProcedureAsync(ctx, tableBuilder, storedProcedureType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (exists)
                        await InternalDropStoredProcedureAsync(ctx, tableBuilder, storedProcedureType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
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

                var exists = await InternalExistsStoredProcedureAsync(ctx, tableBuilder, storedProcedureType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Drop storedProcedure if already exists
                if (exists && overwrite)
                    await InternalDropStoredProcedureAsync(ctx, tableBuilder, storedProcedureType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var shouldCreate = !exists || overwrite;

                if (!shouldCreate)
                    continue;

                var created = await InternalCreateStoredProcedureAsync(ctx, tableBuilder, storedProcedureType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // If at least one stored proc has been created, we're good to return true;
                if (created)
                    hasCreatedAtLeastOneStoredProcedure = true;
            }

            return hasCreatedAtLeastOneStoredProcedure;
        }

    }
}
