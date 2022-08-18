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
        /// Create a tracking table
        /// </summary>
        /// <param name="table">A table from your Setup instance you want to create</param>
        public async Task<bool> CreateTrackingTableAsync(IScopeInfo scopeInfo, string tableName, string schemaName = default, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
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
                bool hasBeenCreated = false;

                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                bool schemaExists;
                (context, schemaExists) = await InternalExistsSchemaAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!schemaExists)
                    (context, _) = await InternalCreateSchemaAsync(scopeInfo,context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                bool exists;
                (context, exists) = await InternalExistsTrackingTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // should create only if not exists OR if overwrite has been set
                var shouldCreate = !exists || overwrite;

                if (shouldCreate)
                {
                    // Drop if already exists and we need to overwrite
                    if (exists && overwrite)
                        (context, _) = await InternalDropTrackingTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    (context, hasBeenCreated) = await InternalCreateTrackingTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
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
        /// Check if a tracking table exists
        /// </summary>
        /// <param name="table">A table from your Setup instance, you want to check if the corresponding tracking table exists</param>
        public async Task<bool> ExistTrackingTableAsync(IScopeInfo scopeInfo, string tableName, string schemaName = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
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

                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                bool exists;
                (context, exists) = await InternalExistsTrackingTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                return exists;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Create a tracking table
        /// </summary>
        /// <param name="table">A table from your Setup instance you want to create</param>
        public async Task<bool> CreateTrackingTablesAsync(IScopeInfo scopeInfo, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                var atLeastOneHasBeenCreated = false;

                foreach (var schemaTable in scopeInfo.Schema.Tables)
                {
                    // Get table builder
                    var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                    bool schemaExists;
                    (context, schemaExists) = await InternalExistsSchemaAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (!schemaExists)
                        (context, _) = await InternalCreateSchemaAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    bool exists;
                    (context, exists) = await InternalExistsTrackingTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    // should create only if not exists OR if overwrite has been set
                    var shouldCreate = !exists || overwrite;

                    if (shouldCreate)
                    {
                        // Drop if already exists and we need to overwrite
                        if (exists && overwrite)
                           (context, _) =  await InternalDropTrackingTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                        bool hasBeenCreated;
                        (context, hasBeenCreated) = await InternalCreateTrackingTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

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
        /// Drop a tracking table
        /// </summary>
        /// <param name="table">A table from your Setup instance you want to drop</param>
        public async Task<bool> DropTrackingTableAsync(IScopeInfo scopeInfo, string tableName, string schemaName = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
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
                bool hasBeenDropped = false;

                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                bool exists;
                (context, exists) = await InternalExistsTrackingTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (exists)
                    (context, hasBeenDropped) = await InternalDropTrackingTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);
                return hasBeenDropped;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }

        /// <summary>
        /// Drop all tracking tables
        /// </summary>
        /// <param name="table">A table from your Setup instance you want to create</param>
        public async Task<bool> DropTrackingTablesAsync(IScopeInfo scopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Deprovisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                bool atLeastOneTrackingTableHasBeenDropped = false;

                foreach (var schemaTable in scopeInfo.Schema.Tables.Reverse())
                {
                    // Get table builder
                    var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                    bool exists;
                    (context, exists) = await InternalExistsTrackingTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (exists)
                        (context, atLeastOneTrackingTableHasBeenDropped) = await InternalDropTrackingTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                await runner.CommitAsync().ConfigureAwait(false);

                return atLeastOneTrackingTableHasBeenDropped;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Rename a tracking table
        /// </summary>
        /// <param name="table">A table from your Setup instance you want to rename the tracking table</param>
        //public async Task<bool> RenameTrackingTableAsync(SyncTable syncTable, ParserName oldTrackingTableName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        //{
        //    try
        //    {
        //        await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Writing, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
        //        var tableBuilder = this.GetTableBuilder(syncTable, this.Setup);
        //        await InternalRenameTrackingTableAsync(this.GetContext(), this.Setup, oldTrackingTableName, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
        //        await runner.CommitAsync().ConfigureAwait(false);
        //        return true;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw GetSyncError(ex);
        //    }
        //}

        /// <summary>
        /// Internal create tracking table routine
        /// </summary>
        internal async Task<(SyncContext context, bool crated)> InternalCreateTrackingTableAsync(
            IScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (Provider == null)
                throw new MissingProviderException(nameof(InternalCreateTrackingTableAsync));

            await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (tableBuilder.TableDescription.Columns.Count <= 0)
                throw new MissingsColumnException(tableBuilder.TableDescription.GetFullName());

            if (tableBuilder.TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(tableBuilder.TableDescription.GetFullName());

            using var command = await tableBuilder.GetCreateTrackingTableCommandAsync(connection, transaction).ConfigureAwait(false);

            if (command == null)
                return (context, false);

            var (_, trackingTableName) = this.Provider.GetParsers(tableBuilder.TableDescription, scopeInfo.Setup);

            var action = await this.InterceptAsync(new TrackingTableCreatingArgs(context, tableBuilder.TableDescription, trackingTableName, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return (context, false);

            await this.InterceptAsync(new DbCommandArgs(context, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            action.Command.CommandTimeout = Options.SqlCommandTimeout;

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

            await this.InterceptAsync(new TrackingTableCreatedArgs(context, tableBuilder.TableDescription, trackingTableName, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await runner.CommitAsync().ConfigureAwait(false);

            action.Command.Dispose();

            return (context, true);
        }

        /// <summary>
        /// Internal rename tracking table routine
        /// </summary>
        internal async Task<(SyncContext context, bool renamed)> InternalRenameTrackingTableAsync(
            IScopeInfo scopeInfo, SyncContext context, ParserName oldTrackingTableName, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (tableBuilder.TableDescription.Columns.Count <= 0)
                throw new MissingsColumnException(tableBuilder.TableDescription.GetFullName());

            if (tableBuilder.TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(tableBuilder.TableDescription.GetFullName());

            using var command = await tableBuilder.GetRenameTrackingTableCommandAsync(oldTrackingTableName, connection, transaction).ConfigureAwait(false);

            if (command == null)
                return (context, false);

            var (_, trackingTableName) = this.Provider.GetParsers(tableBuilder.TableDescription, scopeInfo.Setup);


            var action = await this.InterceptAsync(new TrackingTableRenamingArgs(context, tableBuilder.TableDescription, trackingTableName, oldTrackingTableName, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return (context, false);

            await this.InterceptAsync(new DbCommandArgs(context, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            action.Command.CommandTimeout = Options.SqlCommandTimeout;

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

            await this.InterceptAsync(new TrackingTableRenamedArgs(context, tableBuilder.TableDescription, trackingTableName, oldTrackingTableName, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await runner.CommitAsync().ConfigureAwait(false);
            action.Command.Dispose();

            return (context, true);
        }

        /// <summary>
        /// Internal drop tracking table routine
        /// </summary>
        internal async Task<(SyncContext context, bool dropped)> InternalDropTrackingTableAsync(
            IScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Deprovisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            using var command = await tableBuilder.GetDropTrackingTableCommandAsync(connection, transaction).ConfigureAwait(false);

            if (command == null)
                return (context, false);

            var (_, trackingTableName) = this.Provider.GetParsers(tableBuilder.TableDescription, scopeInfo.Setup);

            var action = await this.InterceptAsync(new TrackingTableDroppingArgs(context, tableBuilder.TableDescription, trackingTableName, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return (context, false);

            await this.InterceptAsync(new DbCommandArgs(context, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            action.Command.CommandTimeout = Options.SqlCommandTimeout;

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);
            await this.InterceptAsync(new TrackingTableDroppedArgs(context, tableBuilder.TableDescription, trackingTableName, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await runner.CommitAsync().ConfigureAwait(false);
            action.Command.Dispose();
            return (context, true);
        }

        /// <summary>
        /// Internal exists tracking table procedure routine
        /// </summary>
        internal async Task<(SyncContext context, bool exists)> InternalExistsTrackingTableAsync(IScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            await using var runner = await this.GetConnectionAsync(context, SyncMode.Reading, SyncStage.None, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            // Get exists command
            using var existsCommand = await tableBuilder.GetExistsTrackingTableCommandAsync(connection, transaction).ConfigureAwait(false);

            if (existsCommand == null)
                return (context, false);

            await this.InterceptAsync(new DbCommandArgs(context, existsCommand, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            existsCommand.CommandTimeout = Options.SqlCommandTimeout;

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;
            await runner.CommitAsync().ConfigureAwait(false);
            return (context, exists);

        }
    }
}
