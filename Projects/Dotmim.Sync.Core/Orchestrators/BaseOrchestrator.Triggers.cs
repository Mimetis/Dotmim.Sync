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
        /// Create a trigger
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to create the trigger</param>
        /// <param name="triggerType">Trigger type (Insert, Delete, Update)</param>
        /// <param name="overwrite">If true, drop the existing trriger then create again</param>
        public async Task<bool> CreateTriggerAsync(IScopeInfo scopeInfo, string tableName, string schemaName = null, DbTriggerType triggerType = DbTriggerType.Insert, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || scopeInfo.Schema.Tables.Count <= 0 || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(scopeInfo.Name, SyncMode.Writing, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                bool hasBeenCreated = false;

                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                var exists = await InternalExistsTriggerAsync(scopeInfo, tableBuilder, triggerType, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // should create only if not exists OR if overwrite has been set
                var shouldCreate = !exists || overwrite;

                if (shouldCreate)
                {
                    // Drop trigger if already exists
                    if (exists && overwrite)
                        await InternalDropTriggerAsync(scopeInfo, tableBuilder, triggerType, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    hasBeenCreated = await InternalCreateTriggerAsync(scopeInfo, tableBuilder, triggerType, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                await runner.CommitAsync().ConfigureAwait(false);

                return hasBeenCreated;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeInfo.Name, ex);
            }
        }

        /// <summary>
        /// Create a trigger
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to create the triggers</param>
        /// <param name="overwrite">If true, drop the existing triggers then create them all, again</param>
        public async Task<bool> CreateTriggersAsync(IScopeInfo scopeInfo, string tableName, string schemaName = null, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || scopeInfo.Schema.Tables.Count <= 0 || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(scopeInfo.Name, SyncMode.Writing, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                var r = await this.InternalCreateTriggersAsync(scopeInfo, overwrite, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return r;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeInfo.Name, ex);
            }
        }

        /// <summary>
        /// Check if a trigger exists
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to check if the trigger exists</param>
        /// <param name="triggerType">Trigger type (Insert, Delete, Update)</param>
        public async Task<bool> ExistTriggerAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null, DbTriggerType triggerType = DbTriggerType.Insert, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || scopeInfo.Schema.Tables.Count <= 0 || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(scopeInfo.Name, SyncMode.Reading, SyncStage.None, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                var exists = await InternalExistsTriggerAsync(scopeInfo, tableBuilder, triggerType, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                return exists;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeInfo.Name, ex);
            }
        }

        /// <summary>
        /// Dropping a trigger
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to drop the trigger</param>
        /// <param name="triggerType">Trigger type (Insert, Delete, Update)</param>
        public async Task<bool> DropTriggerAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null, DbTriggerType triggerType = DbTriggerType.Insert, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || scopeInfo.Schema.Tables.Count <= 0 || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(scopeInfo.Name, SyncMode.Writing, SyncStage.Deprovisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                bool hasBeenDropped = false;

                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                var existsAndCanBeDeleted = await InternalExistsTriggerAsync(scopeInfo, tableBuilder, triggerType, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (existsAndCanBeDeleted)
                    hasBeenDropped = await InternalDropTriggerAsync(scopeInfo, tableBuilder, triggerType, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return hasBeenDropped;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeInfo.Name, ex);
            }

        }

        /// <summary>
        /// Drop all triggers
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to drop all the triggers</param>
        public async Task<bool> DropTriggersAsync(IScopeInfo scopeInfo, string tableName, string schemaName = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || scopeInfo.Schema.Tables.Count <= 0 || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(scopeInfo.Name, SyncMode.Writing, SyncStage.Deprovisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                var r = await this.InternalDropTriggersAsync(scopeInfo, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress);

                await runner.CommitAsync().ConfigureAwait(false);

                return r;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeInfo.Name, ex);
            }
        }

        /// <summary>
        /// Internal create trigger routine
        /// </summary>
        internal async Task<bool> InternalCreateTriggerAsync(IScopeInfo scopeInfo, DbTableBuilder tableBuilder, DbTriggerType triggerType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (tableBuilder.TableDescription.Columns.Count <= 0)
                throw new MissingsColumnException(tableBuilder.TableDescription.GetFullName());

            if (tableBuilder.TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(tableBuilder.TableDescription.GetFullName());

            var command = await tableBuilder.GetCreateTriggerCommandAsync(triggerType, connection, transaction).ConfigureAwait(false);

            if (command == null)
                return false;

            var ctx = this.GetContext(scopeInfo.Name);

            var action = await this.InterceptAsync(new TriggerCreatingArgs(ctx, tableBuilder.TableDescription, triggerType, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await this.InterceptAsync(new DbCommandArgs(ctx, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await action.Command.ExecuteNonQueryAsync();
            await this.InterceptAsync(new TriggerCreatedArgs(ctx, tableBuilder.TableDescription, triggerType, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal create triggers routine
        /// </summary>
        internal async Task<bool> InternalCreateTriggersAsync(IScopeInfo scopeInfo, bool overwrite, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var hasCreatedAtLeastOneTrigger = false;

            var listTriggerType = Enum.GetValues(typeof(DbTriggerType));

            var ctx = this.GetContext(scopeInfo.Name);

            foreach (DbTriggerType triggerType in listTriggerType)
            {
                var exists = await InternalExistsTriggerAsync(scopeInfo, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Drop trigger if already exists
                if (exists && overwrite)
                    await InternalDropTriggerAsync(scopeInfo, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var shouldCreate = !exists || overwrite;

                if (!shouldCreate)
                    continue;

                var hasBeenCreated = await InternalCreateTriggerAsync(scopeInfo, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (hasBeenCreated)
                    hasCreatedAtLeastOneTrigger = true;
            }

            return hasCreatedAtLeastOneTrigger;
        }

        /// <summary>
        /// Internal drop trigger routine
        /// </summary>
        internal async Task<bool> InternalDropTriggerAsync(IScopeInfo scopeInfo, DbTableBuilder tableBuilder, DbTriggerType triggerType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var command = await tableBuilder.GetDropTriggerCommandAsync(triggerType, connection, transaction).ConfigureAwait(false);

            if (command == null)
                return false;

            var ctx = this.GetContext(scopeInfo.Name);

            var action = await this.InterceptAsync(new TriggerDroppingArgs(ctx, tableBuilder.TableDescription, triggerType, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await this.InterceptAsync(new DbCommandArgs(ctx, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await action.Command.ExecuteNonQueryAsync();
            await this.InterceptAsync(new TriggerDroppedArgs(ctx, tableBuilder.TableDescription, triggerType, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal drop triggers routine
        /// </summary>
        internal async Task<bool> InternalDropTriggersAsync(IScopeInfo scopeInfo, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var hasDroppeAtLeastOneTrigger = false;

            var listTriggerType = Enum.GetValues(typeof(DbTriggerType));
            var ctx = this.GetContext(scopeInfo.Name);

            foreach (DbTriggerType triggerType in listTriggerType)
            {
                var exists = await InternalExistsTriggerAsync(scopeInfo, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    continue;

                var dropped = await InternalDropTriggerAsync(scopeInfo, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (dropped)
                    hasDroppeAtLeastOneTrigger = true;
            }

            return hasDroppeAtLeastOneTrigger;

        }

        /// <summary>
        /// Internal exists trigger procedure routine
        /// </summary>
        internal async Task<bool> InternalExistsTriggerAsync(IScopeInfo scopeInfo, DbTableBuilder tableBuilder, DbTriggerType triggerType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // Get exists command
            var existsCommand = await tableBuilder.GetExistsTriggerCommandAsync(triggerType, connection, transaction).ConfigureAwait(false);

            if (existsCommand == null)
                return false;
            var ctx = this.GetContext(scopeInfo.Name);

            await this.InterceptAsync(new DbCommandArgs(ctx, existsCommand, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;
            return exists;

        }



    }
}
