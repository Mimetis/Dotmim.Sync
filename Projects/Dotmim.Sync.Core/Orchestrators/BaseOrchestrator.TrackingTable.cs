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
        public Task<bool> CreateTrackingTableAsync(SetupTable table, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.Provisioning, async (ctx, connection, transaction) =>
        {
            bool hasBeenCreated = false;

            var schema = await this.InternalGetSchemaAsync(ctx, this.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            var schemaTable = schema.Tables[table.TableName, table.SchemaName];

            if (schemaTable == null)
                throw new MissingTableException(table.GetFullName());

            // Get table builder
            var tableBuilder = this.GetTableBuilder(schemaTable, this.Setup);

            var schemaExists = await InternalExistsSchemaAsync(ctx, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (!schemaExists)
                await InternalCreateSchemaAsync(ctx, this.Setup, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            var exists = await InternalExistsTrackingTableAsync(ctx, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            // should create only if not exists OR if overwrite has been set
            var shouldCreate = !exists || overwrite;

            if (shouldCreate)
            {
                // Drop if already exists and we need to overwrite
                if (exists && overwrite)
                    await InternalDropTrackingTableAsync(ctx, this.Setup, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                hasBeenCreated = await InternalCreateTrackingTableAsync(ctx, this.Setup, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            }

            return hasBeenCreated;

        }, connection, transaction, cancellationToken);


        /// <summary>
        /// Check if a tracking table exists
        /// </summary>
        /// <param name="table">A table from your Setup instance, you want to check if the corresponding tracking table exists</param>
        public Task<bool> ExistTrackingTableAsync(SetupTable table, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.None, async (ctx, connection, transaction) =>
        {
            // Fake sync table without column definitions. Not need for making a check exists call
            var schemaTable = new SyncTable(table.TableName, table.SchemaName);

            // Get table builder
            var tableBuilder = this.GetTableBuilder(schemaTable, this.Setup);

            var exists = await InternalExistsTrackingTableAsync(ctx, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            return exists;

        }, connection, transaction, cancellationToken);

        /// <summary>
        /// Create a tracking table
        /// </summary>
        /// <param name="table">A table from your Setup instance you want to create</param>
        public Task<bool> CreateTrackingTablesAsync(bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.Provisioning, async (ctx, connection, transaction) =>
        {
            var atLeastOneHasBeenCreated = false;

            var schema = await this.InternalGetSchemaAsync(ctx, this.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            foreach (var schemaTable in schema.Tables)
            {
                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, this.Setup);

                var schemaExists = await InternalExistsSchemaAsync(ctx, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!schemaExists)
                    await InternalCreateSchemaAsync(ctx, this.Setup, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var exists = await InternalExistsTrackingTableAsync(ctx, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // should create only if not exists OR if overwrite has been set
                var shouldCreate = !exists || overwrite;

                if (shouldCreate)
                {
                    // Drop if already exists and we need to overwrite
                    if (exists && overwrite)
                        await InternalDropTrackingTableAsync(ctx, this.Setup, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    var hasBeenCreated = await InternalCreateTrackingTableAsync(ctx, this.Setup, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (hasBeenCreated)
                        atLeastOneHasBeenCreated = true;

                }
            }

            return atLeastOneHasBeenCreated;

        }, connection, transaction, cancellationToken);

        /// <summary>
        /// Drop a tracking table
        /// </summary>
        /// <param name="table">A table from your Setup instance you want to drop</param>
        public Task<bool> DropTrackingTableAsync(SetupTable table, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.Deprovisioning, async (ctx, connection, transaction) =>
        {
            bool hasBeenDropped = false;

            // Fake sync table without column definitions. Not needed for making drop call
            var schemaTable = new SyncTable(table.TableName, table.SchemaName);

            // Get table builder
            var tableBuilder = this.GetTableBuilder(schemaTable, this.Setup);

            var exists = await InternalExistsTrackingTableAsync(ctx, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (exists)
                hasBeenDropped = await InternalDropTrackingTableAsync(ctx, this.Setup, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            return hasBeenDropped;

        }, connection, transaction, cancellationToken);

        /// <summary>
        /// Drop all tracking tables
        /// </summary>
        /// <param name="table">A table from your Setup instance you want to create</param>
        public Task<bool> DropTrackingTablesAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.Deprovisioning, async (ctx, connection, transaction) =>
        {
            bool atLeastOneTrackingTableHasBeenDropped = false;

            var schemaTables = new List<SyncTable>();
            foreach(var table in this.Setup.Tables.Reverse())
                schemaTables.Add(new SyncTable(table.TableName, table.SchemaName));

            foreach (var schemaTable in schemaTables)
            {
                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, this.Setup);

                var exists = await InternalExistsTrackingTableAsync(ctx, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (exists)
                    atLeastOneTrackingTableHasBeenDropped = await InternalDropTrackingTableAsync(ctx, this.Setup, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            }

            return atLeastOneTrackingTableHasBeenDropped;

        }, connection, transaction, cancellationToken);

        /// <summary>
        /// Rename a tracking table
        /// </summary>
        /// <param name="table">A table from your Setup instance you want to rename the tracking table</param>
        public Task<bool> RenameTrackingTableAsync(SyncTable syncTable, ParserName oldTrackingTableName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.Provisioning, async (ctx, connection, transaction) =>
        {
            // Get table builder
            var tableBuilder = this.GetTableBuilder(syncTable, this.Setup);

            await InternalRenameTrackingTableAsync(ctx, this.Setup, oldTrackingTableName, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            return true;

        }, connection, transaction, cancellationToken);

        /// <summary>
        /// Internal create tracking table routine
        /// </summary>
        internal async Task<bool> InternalCreateTrackingTableAsync(SyncContext ctx, SyncSetup setup, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (tableBuilder.TableDescription.Columns.Count <= 0)
                throw new MissingsColumnException(tableBuilder.TableDescription.GetFullName());

            if (tableBuilder.TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(tableBuilder.TableDescription.GetFullName());

            var command = await tableBuilder.GetCreateTrackingTableCommandAsync(connection, transaction).ConfigureAwait(false);

            if (command == null)
                return false;

            var (_, trackingTableName) = this.Provider.GetParsers(tableBuilder.TableDescription, setup);

            var action = new TrackingTableCreatingArgs(ctx, tableBuilder.TableDescription, trackingTableName, command, connection, transaction);

            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

            var ttca = new TrackingTableCreatedArgs(ctx, tableBuilder.TableDescription, trackingTableName, connection, transaction);

            await this.InterceptAsync(ttca, cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal rename tracking table routine
        /// </summary>
        internal async Task<bool> InternalRenameTrackingTableAsync(SyncContext ctx, SyncSetup setup, ParserName oldTrackingTableName, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (tableBuilder.TableDescription.Columns.Count <= 0)
                throw new MissingsColumnException(tableBuilder.TableDescription.GetFullName());

            if (tableBuilder.TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(tableBuilder.TableDescription.GetFullName());

            var command = await tableBuilder.GetRenameTrackingTableCommandAsync(oldTrackingTableName, connection, transaction).ConfigureAwait(false);

            if (command == null)
                return false;

            var (_, trackingTableName) = this.Provider.GetParsers(tableBuilder.TableDescription, setup);

            var action = new TrackingTableRenamingArgs(ctx, tableBuilder.TableDescription, trackingTableName, oldTrackingTableName, command, connection, transaction);
            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

            await this.InterceptAsync(new TrackingTableRenamedArgs(ctx, tableBuilder.TableDescription, trackingTableName, oldTrackingTableName, connection, transaction), cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal drop tracking table routine
        /// </summary>
        internal async Task<bool> InternalDropTrackingTableAsync(SyncContext ctx, SyncSetup setup, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var command = await tableBuilder.GetDropTrackingTableCommandAsync(connection, transaction).ConfigureAwait(false);

            if (command == null)
                return false;

            var (_, trackingTableName) = this.Provider.GetParsers(tableBuilder.TableDescription, setup);
            var action = new TrackingTableDroppingArgs(ctx, tableBuilder.TableDescription, trackingTableName, command, connection, transaction);
            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);
            await this.InterceptAsync(new TrackingTableDroppedArgs(ctx, tableBuilder.TableDescription, trackingTableName, connection, transaction), cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal exists tracking table procedure routine
        /// </summary>
        internal async Task<bool> InternalExistsTrackingTableAsync(SyncContext ctx, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // Get exists command
            var existsCommand = await tableBuilder.GetExistsTrackingTableCommandAsync(connection, transaction).ConfigureAwait(false);

            if (existsCommand == null)
                return false;

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;
            return exists;

        }



    }
}
