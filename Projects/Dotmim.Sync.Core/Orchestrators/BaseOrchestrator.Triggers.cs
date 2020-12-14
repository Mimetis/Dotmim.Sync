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
        /// Create a trigger
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to create the trigger</param>
        /// <param name="triggerType">Trigger type (Insert, Delete, Update)</param>
        /// <param name="overwrite">If true, drop the existing trriger then create again</param>
        public Task<bool> CreateTriggerAsync(SetupTable table, DbTriggerType triggerType, bool overwrite = false, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => RunInTransactionAsync(async (ctx, connection, transaction) =>
            {
                bool hasBeenCreated = false;

                var schema = await this.InternalGetSchemaAsync(ctx, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var schemaTable = schema.Tables[table.TableName, table.SchemaName];

                if (schemaTable == null)
                    throw new MissingTableException(table.GetFullName());

                // Get table builder
                var tableBuilder = this.Provider.GetTableBuilder(schemaTable, this.Setup);

                ctx.SyncStage = SyncStage.Provisioning;

                var exists = await InternalExistsTriggerAsync(ctx, schemaTable, tableBuilder, triggerType, connection, transaction, cancellationToken);

                // should create only if not exists OR if overwrite has been set
                var shouldCreate = !exists || overwrite;

                if (shouldCreate)
                {
                    // Drop trigger if already exists
                    if (exists && overwrite)
                        await InternalDropTriggerAsync(ctx, schemaTable, tableBuilder, triggerType, connection, transaction, cancellationToken).ConfigureAwait(false);

                    hasBeenCreated = await InternalCreateTriggerAsync(ctx, schemaTable, tableBuilder, triggerType, connection, transaction, cancellationToken).ConfigureAwait(false);

                }

                ctx.SyncStage = SyncStage.Provisioned;

                return hasBeenCreated;

            }, cancellationToken);

        /// <summary>
        /// Create a trigger
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to create the triggers</param>
        /// <param name="overwrite">If true, drop the existing triggers then create them all, again</param>
        public Task<bool> CreateTriggersAsync(SetupTable table, bool overwrite = false, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => RunInTransactionAsync(new Func<SyncContext, DbConnection, DbTransaction, Task<bool>>(async (ctx, connection, transaction) =>
            {
                bool hasBeenCreated = false;

                var schema = await this.InternalGetSchemaAsync(ctx, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var schemaTable = schema.Tables[table.TableName, table.SchemaName];

                if (schemaTable == null)
                    throw new MissingTableException(table.GetFullName());

                // Get table builder
                var tableBuilder = this.Provider.GetTableBuilder(schemaTable, this.Setup);

                var listTriggerType = Enum.GetValues(typeof(DbTriggerType));

                ctx.SyncStage = SyncStage.Provisioning;

                var hasCreatedAtLeastOneTrigger = false;

                foreach (DbTriggerType triggerType in listTriggerType)
                {
                    var exists = await InternalExistsTriggerAsync(ctx, schemaTable, tableBuilder, triggerType, connection, transaction, cancellationToken);

                    // Drop trigger if already exists
                    if (exists && overwrite)
                        await InternalDropTriggerAsync(ctx, schemaTable, tableBuilder, triggerType, connection, transaction, cancellationToken).ConfigureAwait(false);

                    var shouldCreate = !exists || overwrite;

                    if (!shouldCreate)
                        continue;

                    hasBeenCreated = await InternalCreateTriggerAsync(ctx, schemaTable, tableBuilder, triggerType, connection, transaction, cancellationToken).ConfigureAwait(false);

                    if (hasBeenCreated)
                        hasCreatedAtLeastOneTrigger = true;
                }

                ctx.SyncStage = SyncStage.Provisioned;

                return hasCreatedAtLeastOneTrigger;

            }), cancellationToken);

        /// <summary>
        /// Check if a trigger exists
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to check if the trigger exists</param>
        /// <param name="triggerType">Trigger type (Insert, Delete, Update)</param>
        public Task<bool> ExistTriggerAsync(SetupTable table, DbTriggerType triggerType, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => RunInTransactionAsync(new Func<SyncContext, DbConnection, DbTransaction, Task<bool>>(async (ctx, connection, transaction) =>
            {
                var schema = await this.InternalGetSchemaAsync(ctx, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var schemaTable = schema.Tables[table.TableName, table.SchemaName];

                if (schemaTable == null)
                    throw new MissingTableException(table.GetFullName());

                // Get table builder
                var tableBuilder = this.Provider.GetTableBuilder(schemaTable, this.Setup);

                var exists = await InternalExistsTriggerAsync(ctx, schemaTable, tableBuilder, triggerType, connection, transaction, cancellationToken);

                return exists;

            }), cancellationToken);

        /// <summary>
        /// Dropping a trigger
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to drop the trigger</param>
        /// <param name="triggerType">Trigger type (Insert, Delete, Update)</param>
        public Task<bool> DropTriggerAsync(SetupTable table, DbTriggerType triggerType, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
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

                var existsAndCanBeDeleted = await InternalExistsTriggerAsync(ctx, schemaTable, tableBuilder, triggerType, connection, transaction, cancellationToken);

                if (existsAndCanBeDeleted)
                    hasBeenDropped = await InternalDropTriggerAsync(ctx, schemaTable, tableBuilder, triggerType, connection, transaction, cancellationToken).ConfigureAwait(false);

                ctx.SyncStage = SyncStage.Deprovisioned;

                return hasBeenDropped;

            }), cancellationToken);


        /// <summary>
        /// Drop all triggers
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to drop all the triggers</param>
        public Task<bool> DropTriggersAsync(SetupTable table, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => RunInTransactionAsync(new Func<SyncContext, DbConnection, DbTransaction, Task<bool>>(async (ctx, connection, transaction) =>
            {
                var schema = await this.InternalGetSchemaAsync(ctx, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var schemaTable = schema.Tables[table.TableName, table.SchemaName];

                if (schemaTable == null)
                    throw new MissingTableException(table.GetFullName());

                // Get table builder
                var tableBuilder = this.Provider.GetTableBuilder(schemaTable, this.Setup);

                var listTriggerType = Enum.GetValues(typeof(DbTriggerType));

                ctx.SyncStage = SyncStage.Deprovisioning;

                var hasDroppeAtLeastOneTrigger = false;

                foreach (DbTriggerType triggerType in listTriggerType)
                {
                    var exists = await InternalExistsTriggerAsync(ctx, schemaTable, tableBuilder, triggerType, connection, transaction, cancellationToken);

                    if (!exists)
                        continue;

                    var dropped = await InternalDropTriggerAsync(ctx, schemaTable, tableBuilder, triggerType, connection, transaction, cancellationToken).ConfigureAwait(false);

                    if (dropped)
                        hasDroppeAtLeastOneTrigger = true;
                }

                ctx.SyncStage = SyncStage.Deprovisioned;

                return hasDroppeAtLeastOneTrigger;

            }), cancellationToken);

        /// <summary>
        /// Internal create trigger routine
        /// </summary>
        internal async Task<bool> InternalCreateTriggerAsync(SyncContext ctx, SyncTable schemaTable, DbTableBuilder tableBuilder, DbTriggerType triggerType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
        {
            var command = await tableBuilder.GetCreateTriggerCommandAsync(triggerType, connection, transaction).ConfigureAwait(false);

            if (command == null)
                return false;

            this.logger.LogInformation(SyncEventsId.CreateTrigger, new { Table = schemaTable.GetFullName(), TriggerType = triggerType });

            var action = new TriggerCreatingArgs(ctx, schemaTable, triggerType, command, connection, transaction);
            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await action.Command.ExecuteNonQueryAsync();
            await this.InterceptAsync(new TriggerCreatedArgs(ctx, schemaTable, triggerType, connection, transaction), cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal drop trigger routine
        /// </summary>
        internal async Task<bool> InternalDropTriggerAsync(SyncContext ctx, SyncTable schemaTable, DbTableBuilder tableBuilder, DbTriggerType triggerType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
        {
            var command = await tableBuilder.GetDropTriggerCommandAsync(triggerType, connection, transaction).ConfigureAwait(false);

            if (command == null)
                return false;

            this.logger.LogInformation(SyncEventsId.DropTrigger, new { Table = schemaTable.GetFullName(), TriggerType = triggerType });

            var action = new TriggerDroppingArgs(ctx, schemaTable, triggerType, command, connection, transaction);

            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await action.Command.ExecuteNonQueryAsync();
            await this.InterceptAsync(new TriggerDroppedArgs(ctx, schemaTable, triggerType, connection, transaction), cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal exists trigger procedure routine
        /// </summary>
        internal async Task<bool> InternalExistsTriggerAsync(SyncContext ctx, SyncTable schemaTable, DbTableBuilder tableBuilder, DbTriggerType triggerType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
        {
            // Get exists command
            var existsCommand = await tableBuilder.GetExistsTriggerCommandAsync(triggerType, connection, transaction).ConfigureAwait(false);

            if (existsCommand == null)
                return false;

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;
            return exists;

        }



    }
}
