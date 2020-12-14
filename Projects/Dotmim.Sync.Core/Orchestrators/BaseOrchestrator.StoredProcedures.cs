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
        public Task<bool> CreateStoredProcedureAsync(SetupTable table, DbStoredProcedureType storedProcedureType, bool overwrite = false, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => RunInTransactionAsync(new Func<SyncContext, DbConnection, DbTransaction, Task<bool>>(async (ctx, connection, transaction) =>
            {
                bool hasBeenCreated = false;

                var schema = await this.InternalGetSchemaAsync(ctx, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var schemaTable = schema.Tables[table.TableName, table.SchemaName];

                if (schemaTable == null)
                    throw new MissingTableException(table.GetFullName());

                // Get table builder
                var tableBuilder = this.Provider.GetTableBuilder(schemaTable, this.Setup);

                ctx.SyncStage = SyncStage.Provisioning;

                var exists = await InternalExistsStoredProcedureAsync(ctx, schemaTable, tableBuilder, storedProcedureType, connection, transaction, cancellationToken);

                // should create only if not exists OR if overwrite has been set
                var shouldCreate = !exists || overwrite;

                if (shouldCreate)
                {
                    // Drop storedProcedure if already exists
                    if (exists && overwrite)
                        await InternalDropStoredProcedureAsync(ctx, schemaTable, tableBuilder, storedProcedureType, connection, transaction, cancellationToken).ConfigureAwait(false);

                    hasBeenCreated = await InternalCreateStoredProcedureAsync(ctx, schemaTable, tableBuilder, storedProcedureType, connection, transaction, cancellationToken).ConfigureAwait(false);

                }

                ctx.SyncStage = SyncStage.Provisioned;

                return hasBeenCreated;

            }), cancellationToken);

        /// <summary>
        /// Create a Stored Procedure
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to create the Stored Procedures</param>
        /// <param name="overwrite">If true, drop the existing Stored Procedures then create them all, again</param>
        public Task<bool> CreateStoredProceduresAsync(SetupTable table, bool overwrite = false, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => RunInTransactionAsync(new Func<SyncContext, DbConnection, DbTransaction, Task<bool>>(async (ctx, connection, transaction) =>
            {
                var schema = await this.InternalGetSchemaAsync(ctx, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var schemaTable = schema.Tables[table.TableName, table.SchemaName];

                if (schemaTable == null)
                    throw new MissingTableException(table.GetFullName());

                // Get table builder
                var tableBuilder = this.Provider.GetTableBuilder(schemaTable, this.Setup);

                var listStoredProcedureType = Enum.GetValues(typeof(DbStoredProcedureType));

                ctx.SyncStage = SyncStage.Provisioning;

                var hasCreatedAtLeastOneStoredProcedure = false;

                foreach (DbStoredProcedureType storedProcedureType in listStoredProcedureType)
                {
                    // if we are iterating on bulk, but provider do not support it, just loop through and continue
                    if ((storedProcedureType is DbStoredProcedureType.BulkTableType || storedProcedureType is DbStoredProcedureType.BulkUpdateRows || storedProcedureType is DbStoredProcedureType.BulkDeleteRows)
                        && !this.Provider.SupportBulkOperations)
                        continue;

                    var exists = await InternalExistsStoredProcedureAsync(ctx, schemaTable, tableBuilder, storedProcedureType, connection, transaction, cancellationToken);

                    // Drop storedProcedure if already exists
                    if (exists && overwrite)
                        await InternalDropStoredProcedureAsync(ctx, schemaTable, tableBuilder, storedProcedureType, connection, transaction, cancellationToken).ConfigureAwait(false);

                    var shouldCreate = !exists || overwrite;

                    if (!shouldCreate)
                        continue;

                    var created = await InternalCreateStoredProcedureAsync(ctx, schemaTable, tableBuilder, storedProcedureType, connection, transaction, cancellationToken).ConfigureAwait(false);

                    // If at least one stored proc has been created, we're good to return true;
                    if (created)
                        hasCreatedAtLeastOneStoredProcedure = true;
                }

                ctx.SyncStage = SyncStage.Provisioned;

                return hasCreatedAtLeastOneStoredProcedure;

            }), cancellationToken);

        /// <summary>
        /// Check if a Stored Procedure exists
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to check if the Stored Procedure exists</param>
        /// <param name="storedProcedureType">StoredProcedure type</param>
        public Task<bool> ExistStoredProcedureAsync(SetupTable table, DbStoredProcedureType storedProcedureType, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => RunInTransactionAsync(new Func<SyncContext, DbConnection, DbTransaction, Task<bool>>(async (ctx, connection, transaction) =>
            {
                var schema = await this.InternalGetSchemaAsync(ctx, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var schemaTable = schema.Tables[table.TableName, table.SchemaName];

                if (schemaTable == null)
                    throw new MissingTableException(table.GetFullName());

                // Get table builder
                var tableBuilder = this.Provider.GetTableBuilder(schemaTable, this.Setup);

                var exists = await InternalExistsStoredProcedureAsync(ctx, schemaTable, tableBuilder, storedProcedureType, connection, transaction, cancellationToken);

                return exists;

            }), cancellationToken);

        /// <summary>
        /// Drop a Stored Procedure
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to drop the Stored Procedure</param>
        /// <param name="storedProcedureType">Stored Procedure type</param>
        public Task<bool> DropStoredProcedureAsync(SetupTable table, DbStoredProcedureType storedProcedureType, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
            => RunInTransactionAsync(new Func<SyncContext, DbConnection, DbTransaction, Task<bool>>(async (ctx, connection, transaction) =>
            {
                bool hasBeenDropped = false;

                var schema = await this.InternalGetSchemaAsync(ctx, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var schemaTable = schema.Tables[table.TableName, table.SchemaName];

                if (schemaTable == null)
                    throw new MissingTableException(table.GetFullName());

                // Get table builder
                var tableBuilder = this.Provider.GetTableBuilder(schemaTable, this.Setup);

                ctx.SyncStage = SyncStage.Deprovisioning;

                var existsAndCanBeDeleted = await InternalExistsStoredProcedureAsync(ctx, schemaTable, tableBuilder, storedProcedureType, connection, transaction, cancellationToken);

                if (existsAndCanBeDeleted)
                    hasBeenDropped = await InternalDropStoredProcedureAsync(ctx, schemaTable, tableBuilder, storedProcedureType, connection, transaction, cancellationToken).ConfigureAwait(false);

                ctx.SyncStage = SyncStage.Deprovisioned;

                return hasBeenDropped;

            }), cancellationToken);


        /// <summary>
        /// Drop all Stored Procedures
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to drop all the Stored Procedures</param>
        public Task<bool> DropStoredProceduresAsync(SetupTable table, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => RunInTransactionAsync(new Func<SyncContext, DbConnection, DbTransaction, Task<bool>>(async (ctx, connection, transaction) =>
            {
                var schema = await this.InternalGetSchemaAsync(ctx, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var schemaTable = schema.Tables[table.TableName, table.SchemaName];

                if (schemaTable == null)
                    throw new MissingTableException(table.GetFullName());

                // Get table builder
                var tableBuilder = this.Provider.GetTableBuilder(schemaTable, this.Setup);

                var listStoredProcedureType = Enum.GetValues(typeof(DbStoredProcedureType));

                ctx.SyncStage = SyncStage.Deprovisioning;

                var hasDroppedAtLeastOneStoredProcedure = false;

                foreach (DbStoredProcedureType storedProcedureType in listStoredProcedureType)
                {
                    // if we are iterating on bulk, but provider do not support it, just loop through and continue
                    if ((storedProcedureType is DbStoredProcedureType.BulkTableType || storedProcedureType is DbStoredProcedureType.BulkUpdateRows || storedProcedureType is DbStoredProcedureType.BulkDeleteRows)
                        && !this.Provider.SupportBulkOperations)
                        continue;

                    var exists = await InternalExistsStoredProcedureAsync(ctx, schemaTable, tableBuilder, storedProcedureType, connection, transaction, cancellationToken);

                    if (!exists)
                        continue;

                    var dropped = await InternalDropStoredProcedureAsync(ctx, schemaTable, tableBuilder, storedProcedureType, connection, transaction, cancellationToken).ConfigureAwait(false);

                    // If at least one stored proc has been dropped, we're good to return true;
                    if (dropped)
                        hasDroppedAtLeastOneStoredProcedure = true;
                }

                ctx.SyncStage = SyncStage.Deprovisioned;

                return hasDroppedAtLeastOneStoredProcedure;

            }), cancellationToken);

        /// <summary>
        /// Internal create Stored Procedure routine
        /// </summary>
        internal async Task<bool> InternalCreateStoredProcedureAsync(SyncContext ctx, SyncTable schemaTable, DbTableBuilder tableBuilder, DbStoredProcedureType storedProcedureType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
        {
            var command = await tableBuilder.GetCreateStoredProcedureCommandAsync(storedProcedureType, schemaTable.GetFilter(), connection, transaction).ConfigureAwait(false);

            if (command == null)
                return false;

            this.logger.LogInformation(SyncEventsId.CreateStoredProcedure, new { Table = schemaTable.GetFullName(), StoredProcedureType = storedProcedureType });

            var action = new StoredProcedureCreatingArgs(ctx, schemaTable, storedProcedureType, command, connection, transaction);
            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await action.Command.ExecuteNonQueryAsync();
            await this.InterceptAsync(new StoredProcedureCreatedArgs(ctx, schemaTable, storedProcedureType, connection, transaction), cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal drop storedProcedure routine
        /// </summary>
        internal async Task<bool> InternalDropStoredProcedureAsync(SyncContext ctx, SyncTable schemaTable, DbTableBuilder tableBuilder, DbStoredProcedureType storedProcedureType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
        {
            var command = await tableBuilder.GetDropStoredProcedureCommandAsync(storedProcedureType, schemaTable.GetFilter(), connection, transaction).ConfigureAwait(false);

            if (command == null)
                return false;

            this.logger.LogInformation(SyncEventsId.DropStoredProcedure, new { Table = schemaTable.GetFullName(), StoredProcedureType = storedProcedureType });

            var action = new StoredProcedureDroppingArgs(ctx, schemaTable, storedProcedureType, command, connection, transaction);
            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await action.Command.ExecuteNonQueryAsync();
            await this.InterceptAsync(new StoredProcedureDroppedArgs(ctx, schemaTable, storedProcedureType, connection, transaction), cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal exists storedProcedure procedure routine
        /// </summary>
        internal async Task<bool> InternalExistsStoredProcedureAsync(SyncContext ctx, SyncTable schemaTable, DbTableBuilder tableBuilder, DbStoredProcedureType storedProcedureType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
        {
            var existsCommand = await tableBuilder.GetExistsStoredProcedureCommandAsync(storedProcedureType, connection, transaction).ConfigureAwait(false);

            if (existsCommand == null)
                return false;

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;

            return exists;

        }



    }
}
