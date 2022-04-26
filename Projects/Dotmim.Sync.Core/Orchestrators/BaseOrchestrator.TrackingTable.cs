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
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(scopeInfo.Name, SyncMode.Writing, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                bool hasBeenCreated = false;
                
                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                var schemaExists = await InternalExistsSchemaAsync(scopeInfo, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!schemaExists)
                    await InternalCreateSchemaAsync(scopeInfo, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                var exists = await InternalExistsTrackingTableAsync(scopeInfo, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // should create only if not exists OR if overwrite has been set
                var shouldCreate = !exists || overwrite;

                if (shouldCreate)
                {
                    // Drop if already exists and we need to overwrite
                    if (exists && overwrite)
                        await InternalDropTrackingTableAsync(scopeInfo, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    hasBeenCreated = await InternalCreateTrackingTableAsync(scopeInfo, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
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
        /// Check if a tracking table exists
        /// </summary>
        /// <param name="table">A table from your Setup instance, you want to check if the corresponding tracking table exists</param>
        public async Task<bool> ExistTrackingTableAsync(IScopeInfo scopeInfo, string tableName, string schemaName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(scopeInfo.Name, SyncMode.Reading, SyncStage.None, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                var exists = await InternalExistsTrackingTableAsync(scopeInfo, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                return exists;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeInfo.Name, ex);
            }
        }

        /// <summary>
        /// Create a tracking table
        /// </summary>
        /// <param name="table">A table from your Setup instance you want to create</param>
        public async Task<bool> CreateTrackingTablesAsync(IScopeInfo scopeInfo, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                await using var runner = await this.GetConnectionAsync(scopeInfo.Name, SyncMode.Writing, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                var atLeastOneHasBeenCreated = false;

                foreach (var schemaTable in scopeInfo.Schema.Tables)
                {
                    // Get table builder
                    var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                    var schemaExists = await InternalExistsSchemaAsync(scopeInfo, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (!schemaExists)
                        await InternalCreateSchemaAsync(scopeInfo, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    var exists = await InternalExistsTrackingTableAsync(scopeInfo, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    // should create only if not exists OR if overwrite has been set
                    var shouldCreate = !exists || overwrite;

                    if (shouldCreate)
                    {
                        // Drop if already exists and we need to overwrite
                        if (exists && overwrite)
                            await InternalDropTrackingTableAsync(scopeInfo, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                        var hasBeenCreated = await InternalCreateTrackingTableAsync(scopeInfo, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (hasBeenCreated)
                            atLeastOneHasBeenCreated = true;

                    }
                }
                await runner.CommitAsync().ConfigureAwait(false);

                return atLeastOneHasBeenCreated;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeInfo.Name, ex);
            }
        }


        /// <summary>
        /// Drop a tracking table
        /// </summary>
        /// <param name="table">A table from your Setup instance you want to drop</param>
        public async Task<bool> DropTrackingTableAsync(IScopeInfo scopeInfo, string tableName, string schemaName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(scopeInfo.Name, SyncMode.Writing, SyncStage.Deprovisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                bool hasBeenDropped = false;

                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                var exists = await InternalExistsTrackingTableAsync(scopeInfo, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (exists)
                    hasBeenDropped = await InternalDropTrackingTableAsync(scopeInfo, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);
                return hasBeenDropped;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeInfo.Name, ex);
            }

        }

        /// <summary>
        /// Drop all tracking tables
        /// </summary>
        /// <param name="table">A table from your Setup instance you want to create</param>
        public async Task<bool> DropTrackingTablesAsync(IScopeInfo scopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                await using var runner = await this.GetConnectionAsync(scopeInfo.Name, SyncMode.Writing, SyncStage.Deprovisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                bool atLeastOneTrackingTableHasBeenDropped = false;

                foreach (var schemaTable in scopeInfo.Schema.Tables.Reverse())
                {
                    // Get table builder
                    var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                    var exists = await InternalExistsTrackingTableAsync(scopeInfo, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (exists)
                        atLeastOneTrackingTableHasBeenDropped = await InternalDropTrackingTableAsync(scopeInfo, tableBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                await runner.CommitAsync().ConfigureAwait(false);

                return atLeastOneTrackingTableHasBeenDropped;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeInfo.Name, ex);
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
        internal async Task<bool> InternalCreateTrackingTableAsync(IScopeInfo scopeInfo, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (tableBuilder.TableDescription.Columns.Count <= 0)
                throw new MissingsColumnException(tableBuilder.TableDescription.GetFullName());

            if (tableBuilder.TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(tableBuilder.TableDescription.GetFullName());

            var command = await tableBuilder.GetCreateTrackingTableCommandAsync(connection, transaction).ConfigureAwait(false);

            if (command == null)
                return false;

            var (_, trackingTableName) = this.Provider.GetParsers(tableBuilder.TableDescription, scopeInfo.Setup);

            var ctx = this.GetContext(scopeInfo.Name);

            var action = await this.InterceptAsync(new TrackingTableCreatingArgs(ctx, tableBuilder.TableDescription, trackingTableName, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await this.InterceptAsync(new DbCommandArgs(ctx, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

            await this.InterceptAsync(new TrackingTableCreatedArgs(ctx, tableBuilder.TableDescription, trackingTableName, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal rename tracking table routine
        /// </summary>
        internal async Task<bool> InternalRenameTrackingTableAsync(IScopeInfo scopeInfo, ParserName oldTrackingTableName, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (tableBuilder.TableDescription.Columns.Count <= 0)
                throw new MissingsColumnException(tableBuilder.TableDescription.GetFullName());

            if (tableBuilder.TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(tableBuilder.TableDescription.GetFullName());

            var command = await tableBuilder.GetRenameTrackingTableCommandAsync(oldTrackingTableName, connection, transaction).ConfigureAwait(false);

            if (command == null)
                return false;

            var (_, trackingTableName) = this.Provider.GetParsers(tableBuilder.TableDescription, scopeInfo.Setup);

            var ctx = this.GetContext(scopeInfo.Name);

            var action = await this.InterceptAsync(new TrackingTableRenamingArgs(ctx, tableBuilder.TableDescription, trackingTableName, oldTrackingTableName, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await this.InterceptAsync(new DbCommandArgs(ctx, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

            await this.InterceptAsync(new TrackingTableRenamedArgs(ctx, tableBuilder.TableDescription, trackingTableName, oldTrackingTableName, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal drop tracking table routine
        /// </summary>
        internal async Task<bool> InternalDropTrackingTableAsync(IScopeInfo scopeInfo, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var command = await tableBuilder.GetDropTrackingTableCommandAsync(connection, transaction).ConfigureAwait(false);

            if (command == null)
                return false;

            var (_, trackingTableName) = this.Provider.GetParsers(tableBuilder.TableDescription, scopeInfo.Setup);
            var ctx = this.GetContext(scopeInfo.Name);

            var action = await this.InterceptAsync(new TrackingTableDroppingArgs(ctx, tableBuilder.TableDescription, trackingTableName, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await this.InterceptAsync(new DbCommandArgs(ctx, action.Command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);
            await this.InterceptAsync(new TrackingTableDroppedArgs(ctx, tableBuilder.TableDescription, trackingTableName, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal exists tracking table procedure routine
        /// </summary>
        internal async Task<bool> InternalExistsTrackingTableAsync(IScopeInfo scopeInfo, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // Get exists command
            var existsCommand = await tableBuilder.GetExistsTrackingTableCommandAsync(connection, transaction).ConfigureAwait(false);

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
