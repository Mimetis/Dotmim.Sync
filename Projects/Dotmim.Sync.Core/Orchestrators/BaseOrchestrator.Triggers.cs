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
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);

            try
            {
                if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || scopeInfo.Schema.Tables.Count <= 0 || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                bool hasBeenCreated = false;

                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                bool exists;
                (context, exists) = await InternalExistsTriggerAsync(scopeInfo, context, tableBuilder, triggerType, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // should create only if not exists OR if overwrite has been set
                var shouldCreate = !exists || overwrite;

                if (shouldCreate)
                {
                    // Drop trigger if already exists
                    if (exists && overwrite)
                        (context, _) = await InternalDropTriggerAsync(scopeInfo, context, tableBuilder, triggerType, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    (context, hasBeenCreated) = await InternalCreateTriggerAsync(scopeInfo, context, tableBuilder, triggerType, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
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
        /// Create a trigger
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to create the triggers</param>
        /// <param name="overwrite">If true, drop the existing triggers then create them all, again</param>
        public async Task<bool> CreateTriggersAsync(IScopeInfo scopeInfo, string tableName, string schemaName = null, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);

            try
            {
                if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || scopeInfo.Schema.Tables.Count <= 0 || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                bool created;
                (context, created) = await this.InternalCreateTriggersAsync(scopeInfo, context, overwrite, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return created;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Check if a trigger exists
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to check if the trigger exists</param>
        /// <param name="triggerType">Trigger type (Insert, Delete, Update)</param>
        public async Task<bool> ExistTriggerAsync(IScopeInfo scopeInfo, string tableName, string schemaName = null, DbTriggerType triggerType = DbTriggerType.Insert, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || scopeInfo.Schema.Tables.Count <= 0 || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Reading, SyncStage.None, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                bool exists;
                (context, exists) = await InternalExistsTriggerAsync(scopeInfo, context, tableBuilder, triggerType, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                return exists;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Dropping a trigger
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to drop the trigger</param>
        /// <param name="triggerType">Trigger type (Insert, Delete, Update)</param>
        public async Task<bool> DropTriggerAsync(IScopeInfo scopeInfo, string tableName, string schemaName = null, DbTriggerType triggerType = DbTriggerType.Insert, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);

            try
            {
                if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || scopeInfo.Schema.Tables.Count <= 0 || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Deprovisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                bool hasBeenDropped = false;

                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                bool existsAndCanBeDeleted;
                (context, existsAndCanBeDeleted) = await InternalExistsTriggerAsync(scopeInfo, context, tableBuilder, triggerType, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (existsAndCanBeDeleted)
                    (context, hasBeenDropped) = await InternalDropTriggerAsync(scopeInfo, context, tableBuilder, triggerType, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return hasBeenDropped;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }

        /// <summary>
        /// Drop all triggers
        /// </summary>
        /// <param name="table">A table from your Setup instance, where you want to drop all the triggers</param>
        public async Task<bool> DropTriggersAsync(IScopeInfo scopeInfo, string tableName, string schemaName = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);

            try
            {
                if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || scopeInfo.Schema.Tables.Count <= 0 || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Deprovisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                bool dropped;
                (context, dropped) = await this.InternalDropTriggersAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress);

                await runner.CommitAsync().ConfigureAwait(false);

                return dropped;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Internal create trigger routine
        /// </summary>
        internal async Task<(SyncContext context, bool created)> InternalCreateTriggerAsync(
            IScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbTriggerType triggerType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (tableBuilder.TableDescription.Columns.Count <= 0)
                throw new MissingsColumnException(tableBuilder.TableDescription.GetFullName());

            if (tableBuilder.TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(tableBuilder.TableDescription.GetFullName());

            using var command = await tableBuilder.GetCreateTriggerCommandAsync(triggerType, connection, transaction).ConfigureAwait(false);

            if (command == null)
                return (context, false);

            var action = await this.InterceptAsync(new TriggerCreatingArgs(context, tableBuilder.TableDescription, triggerType, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return (context, false);

            await this.InterceptAsync(new DbCommandArgs(context, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await action.Command.ExecuteNonQueryAsync();
            await this.InterceptAsync(new TriggerCreatedArgs(context, tableBuilder.TableDescription, triggerType, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            action.Command.Dispose();

            return (context, true);
        }

        /// <summary>
        /// Internal create triggers routine
        /// </summary>
        internal async Task<(SyncContext context, bool created)> InternalCreateTriggersAsync(
            IScopeInfo scopeInfo, SyncContext context, bool overwrite, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var hasCreatedAtLeastOneTrigger = false;

            var listTriggerType = Enum.GetValues(typeof(DbTriggerType));

            foreach (DbTriggerType triggerType in listTriggerType)
            {
                bool exists;
                (context, exists) = await InternalExistsTriggerAsync(scopeInfo, context, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Drop trigger if already exists
                if (exists && overwrite)
                    (context, _) = await InternalDropTriggerAsync(scopeInfo, context, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var shouldCreate = !exists || overwrite;

                if (!shouldCreate)
                    continue;

                bool hasBeenCreated;
                (context, hasBeenCreated) = await InternalCreateTriggerAsync(scopeInfo, context, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (hasBeenCreated)
                    hasCreatedAtLeastOneTrigger = true;
            }

            return (context, hasCreatedAtLeastOneTrigger);
        }

        /// <summary>
        /// Internal drop trigger routine
        /// </summary>
        internal async Task<(SyncContext context, bool dropped)> InternalDropTriggerAsync(
            IScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbTriggerType triggerType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            using var command = await tableBuilder.GetDropTriggerCommandAsync(triggerType, connection, transaction).ConfigureAwait(false);

            if (command == null)
                return (context, false);

            var action = await this.InterceptAsync(new TriggerDroppingArgs(context, tableBuilder.TableDescription, triggerType, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return (context, false);

            await this.InterceptAsync(new DbCommandArgs(context, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await action.Command.ExecuteNonQueryAsync();
            await this.InterceptAsync(new TriggerDroppedArgs(context, tableBuilder.TableDescription, triggerType, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            action.Command.Dispose();

            return (context, true);
        }

        /// <summary>
        /// Internal drop triggers routine
        /// </summary>
        internal async Task<(SyncContext context, bool dropped)> InternalDropTriggersAsync(
            IScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var hasDroppeAtLeastOneTrigger = false;

            var listTriggerType = Enum.GetValues(typeof(DbTriggerType));

            foreach (DbTriggerType triggerType in listTriggerType)
            {
                bool exists;
                (context, exists) = await InternalExistsTriggerAsync(scopeInfo, context, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    continue;
                bool dropped;
                (context, dropped) = await InternalDropTriggerAsync(scopeInfo, context, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (dropped)
                    hasDroppeAtLeastOneTrigger = true;
            }

            return (context, hasDroppeAtLeastOneTrigger);

        }

        /// <summary>
        /// Internal exists trigger procedure routine
        /// </summary>
        internal async Task<(SyncContext context, bool exists)> InternalExistsTriggerAsync(
            IScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbTriggerType triggerType, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // Get exists command
            using var existsCommand = await tableBuilder.GetExistsTriggerCommandAsync(triggerType, connection, transaction).ConfigureAwait(false);

            if (existsCommand == null)
                return (context, false);

            await this.InterceptAsync(new DbCommandArgs(context, existsCommand, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;
            return (context, exists);

        }



    }
}
