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
            => RunInTransactionAsync(SyncStage.Provisioning, async (ctx, connection, transaction) =>
            {
                bool hasBeenCreated = false;

                var schema = await this.InternalGetSchemaAsync(ctx, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var schemaTable = schema.Tables[table.TableName, table.SchemaName];

                if (schemaTable == null)
                    throw new MissingTableException(table.GetFullName());

                // Get table builder
                var tableBuilder = this.Provider.GetTableBuilder(schemaTable, this.Setup);

                var exists = await InternalExistsTriggerAsync(ctx, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // should create only if not exists OR if overwrite has been set
                var shouldCreate = !exists || overwrite;

                if (shouldCreate)
                {
                    // Drop trigger if already exists
                    if (exists && overwrite)
                        await InternalDropTriggerAsync(ctx, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    hasBeenCreated = await InternalCreateTriggerAsync(ctx, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                }

                return hasBeenCreated;

            }, cancellationToken);

        /// <summary>
        /// Create a trigger
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to create the triggers</param>
        /// <param name="overwrite">If true, drop the existing triggers then create them all, again</param>
        public Task<bool> CreateTriggersAsync(SetupTable table, bool overwrite = false, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => RunInTransactionAsync(SyncStage.Provisioning, async (ctx, connection, transaction) =>
            {
                var schema = await this.InternalGetSchemaAsync(ctx, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var schemaTable = schema.Tables[table.TableName, table.SchemaName];

                if (schemaTable == null)
                    throw new MissingTableException(table.GetFullName());

                // Get table builder
                var tableBuilder = this.Provider.GetTableBuilder(schemaTable, this.Setup);

                var r = await this.InternalCreateTriggersAsync(ctx, overwrite, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                return r;
            }, cancellationToken);

        /// <summary>
        /// Check if a trigger exists
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to check if the trigger exists</param>
        /// <param name="triggerType">Trigger type (Insert, Delete, Update)</param>
        public Task<bool> ExistTriggerAsync(SetupTable table, DbTriggerType triggerType, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.None, async (ctx, connection, transaction) =>
        {
            // Fake sync table without column definitions. Not need for making a check exists call
            var schemaTable = new SyncTable(table.TableName, table.SchemaName);

            // Get table builder
            var tableBuilder = this.Provider.GetTableBuilder(schemaTable, this.Setup);

            var exists = await InternalExistsTriggerAsync(ctx, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            return exists;

        }, cancellationToken);

        /// <summary>
        /// Dropping a trigger
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to drop the trigger</param>
        /// <param name="triggerType">Trigger type (Insert, Delete, Update)</param>
        public Task<bool> DropTriggerAsync(SetupTable table, DbTriggerType triggerType, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.Deprovisioning, async (ctx, connection, transaction) =>
        {
            bool hasBeenDropped = false;

            // Fake sync table without column definitions. Not need for making a drop call
            var schemaTable = new SyncTable(table.TableName, table.SchemaName);

            // Get table builder
            var tableBuilder = this.Provider.GetTableBuilder(schemaTable, this.Setup);

            var existsAndCanBeDeleted = await InternalExistsTriggerAsync(ctx, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (existsAndCanBeDeleted)
                hasBeenDropped = await InternalDropTriggerAsync(ctx, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            return hasBeenDropped;

        }, cancellationToken);

        /// <summary>
        /// Drop all triggers
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to drop all the triggers</param>
        public Task<bool> DropTriggersAsync(SetupTable table, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.Deprovisioning, (ctx, connection, transaction) =>
        {
            // Fake sync table without column definitions. Not need for making a drop call
            var schemaTable = new SyncTable(table.TableName, table.SchemaName);

            // Get table builder
            var tableBuilder = this.Provider.GetTableBuilder(schemaTable, this.Setup);

            return this.InternalDropTriggersAsync(ctx, tableBuilder, connection, transaction, cancellationToken, progress);

        }, cancellationToken);

        /// <summary>
        /// Internal create trigger routine
        /// </summary>
        internal async Task<bool> InternalCreateTriggerAsync(SyncContext ctx, DbTableBuilder tableBuilder, DbTriggerType triggerType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (tableBuilder.TableDescription.Columns.Count <= 0)
                throw new MissingsColumnException(tableBuilder.TableDescription.GetFullName());

            if (tableBuilder.TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(tableBuilder.TableDescription.GetFullName());

            var command = await tableBuilder.GetCreateTriggerCommandAsync(triggerType, connection, transaction).ConfigureAwait(false);

            if (command == null)
                return false;

            var action = new TriggerCreatingArgs(ctx, tableBuilder.TableDescription, triggerType, command, connection, transaction);
            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await action.Command.ExecuteNonQueryAsync();
            await this.InterceptAsync(new TriggerCreatedArgs(ctx, tableBuilder.TableDescription, triggerType, connection, transaction), cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal create triggers routine
        /// </summary>
        internal async Task<bool> InternalCreateTriggersAsync(SyncContext ctx, bool overwrite, DbTableBuilder tableBuilder,DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var hasCreatedAtLeastOneTrigger = false;

            var listTriggerType = Enum.GetValues(typeof(DbTriggerType));

            foreach (DbTriggerType triggerType in listTriggerType)
            {
                var exists = await InternalExistsTriggerAsync(ctx, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Drop trigger if already exists
                if (exists && overwrite)
                    await InternalDropTriggerAsync(ctx, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var shouldCreate = !exists || overwrite;

                if (!shouldCreate)
                    continue;

                var hasBeenCreated = await InternalCreateTriggerAsync(ctx, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (hasBeenCreated)
                    hasCreatedAtLeastOneTrigger = true;
            }

            return hasCreatedAtLeastOneTrigger;
        }

        /// <summary>
        /// Internal drop trigger routine
        /// </summary>
        internal async Task<bool> InternalDropTriggerAsync(SyncContext ctx, DbTableBuilder tableBuilder, DbTriggerType triggerType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var command = await tableBuilder.GetDropTriggerCommandAsync(triggerType, connection, transaction).ConfigureAwait(false);

            if (command == null)
                return false;

            var action = new TriggerDroppingArgs(ctx, tableBuilder.TableDescription, triggerType, command, connection, transaction);

            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await action.Command.ExecuteNonQueryAsync();
            await this.InterceptAsync(new TriggerDroppedArgs(ctx, tableBuilder.TableDescription, triggerType, connection, transaction), cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal drop triggers routine
        /// </summary>
        internal async Task<bool> InternalDropTriggersAsync(SyncContext ctx, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var hasDroppeAtLeastOneTrigger = false;

            var listTriggerType = Enum.GetValues(typeof(DbTriggerType));

            foreach (DbTriggerType triggerType in listTriggerType)
            {
                var exists = await InternalExistsTriggerAsync(ctx, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    continue;

                var dropped = await InternalDropTriggerAsync(ctx, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (dropped)
                    hasDroppeAtLeastOneTrigger = true;
            }

            return hasDroppeAtLeastOneTrigger;

        }

        /// <summary>
        /// Internal exists trigger procedure routine
        /// </summary>
        internal async Task<bool> InternalExistsTriggerAsync(SyncContext ctx, DbTableBuilder tableBuilder, DbTriggerType triggerType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
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
