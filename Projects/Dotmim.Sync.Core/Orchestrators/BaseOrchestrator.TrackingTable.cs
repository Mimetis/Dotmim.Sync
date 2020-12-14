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
        public Task<bool> CreateTrackingTableAsync(SetupTable table, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
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

                var schemaExists = await InternalExistsSchemaAsync(ctx, schemaTable, tableBuilder, connection, transaction, cancellationToken);

                if (!schemaExists)
                    await InternalCreateSchemaAsync(ctx, schemaTable, tableBuilder, connection, transaction, cancellationToken);

                var exists = await InternalExistsTrackingTableAsync(ctx, schemaTable, tableBuilder, connection, transaction, cancellationToken);

                if (!exists)
                    hasBeenCreated = await InternalCreateTrackingTableAsync(ctx, schemaTable, tableBuilder, connection, transaction, cancellationToken).ConfigureAwait(false);

                ctx.SyncStage = SyncStage.Provisioned;

                return hasBeenCreated;

            }), cancellationToken);

        /// <summary>
        /// Drop a tracking table
        /// </summary>
        /// <param name="table">A table from your Setup instance you want to drop</param>
        public Task<bool> DropTrackingTableAsync(SetupTable table, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => RunInTransactionAsync(new Func<SyncContext, DbConnection, DbTransaction, Task<bool>>(async (ctx, connection, transaction) =>
             {
                 bool hasBeenDropped = false;

                 var schema = await this.InternalGetSchemaAsync(ctx,connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                 var schemaTable = schema.Tables[table.TableName, table.SchemaName];

                 if (schemaTable == null)
                     throw new MissingTableException(table.GetFullName());

                 // Get table builder
                 var tableBuilder = this.Provider.GetTableBuilder(schemaTable, this.Setup);

                 ctx.SyncStage = SyncStage.Deprovisioning;

                 var exists = await InternalExistsTrackingTableAsync(ctx, schemaTable, tableBuilder, connection, transaction, cancellationToken);

                 if (exists)
                     hasBeenDropped = await InternalDropTrackingTableAsync(ctx, schemaTable, tableBuilder, connection, transaction, cancellationToken).ConfigureAwait(false);

                 ctx.SyncStage = SyncStage.Deprovisioned;

                 return hasBeenDropped;

             }), cancellationToken);

        /// <summary>
        /// Rename a tracking table
        /// </summary>
        /// <param name="table">A table from your Setup instance you want to rename the tracking table</param>
        public Task<bool> RenameTrackingTableAsync(SyncTable syncTable, ParserName oldTrackingTableName, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => RunInTransactionAsync(new Func<SyncContext, DbConnection, DbTransaction, Task<bool>>(async (ctx, connection, transaction) =>
            {
                // Get table builder
                var tableBuilder = this.Provider.GetTableBuilder(syncTable, this.Setup);

                ctx.SyncStage = SyncStage.Provisioning;

                await InternalRenameTrackingTableAsync(ctx, syncTable, oldTrackingTableName, tableBuilder, connection, transaction, cancellationToken).ConfigureAwait(false);

                ctx.SyncStage = SyncStage.Provisioned;

                return true;

            }), cancellationToken);


        /// <summary>
        /// Internal create tracking table routine
        /// </summary>
        internal async Task<bool> InternalCreateTrackingTableAsync(SyncContext ctx, SyncTable schemaTable, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
        {
            var command = await tableBuilder.GetCreateTrackingTableCommandAsync(connection, transaction).ConfigureAwait(false);

            if (command == null)
                return false;

            this.logger.LogInformation(SyncEventsId.CreateTrackingTable, new { TrackingTable = schemaTable.GetFullName() });

            var action = new TrackingTableCreatingArgs(ctx, schemaTable, command, connection, transaction);

            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await action.Command.ExecuteNonQueryAsync();
            await this.InterceptAsync(new TrackingTableCreatedArgs(ctx, schemaTable, connection, transaction), cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal rename tracking table routine
        /// </summary>
        internal async Task<bool> InternalRenameTrackingTableAsync(SyncContext ctx, SyncTable schemaTable, ParserName oldTrackingTableName, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
        {
            var command = await tableBuilder.GetRenameTrackingTableCommandAsync(oldTrackingTableName, connection, transaction).ConfigureAwait(false);

            if (command == null)
                return false;

            this.logger.LogInformation(SyncEventsId.RenameTrackingTable, new { NewTrackingTable = schemaTable.GetFullName(), OldTrackingTable = oldTrackingTableName });

            var action = new TableCreatingArgs(ctx, schemaTable, command, connection, transaction);

            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await action.Command.ExecuteNonQueryAsync();
            await this.InterceptAsync(new TableCreatedArgs(ctx, schemaTable, connection, transaction), cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal drop tracking table routine
        /// </summary>
        internal async Task<bool> InternalDropTrackingTableAsync(SyncContext ctx, SyncTable schemaTable, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
        {
            var command = await tableBuilder.GetDropTrackingTableCommandAsync(connection, transaction).ConfigureAwait(false);

            if (command == null)
                return false;

            this.logger.LogInformation(SyncEventsId.DropTrackingTable, new { Table = schemaTable.GetFullName() });

            var action = new TrackingTableDroppingArgs(ctx, schemaTable, command, connection, transaction);

            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await action.Command.ExecuteNonQueryAsync();
            await this.InterceptAsync(new TrackingTableDroppedArgs(ctx, schemaTable, connection, transaction), cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal exists tracking table procedure routine
        /// </summary>
        internal async Task<bool> InternalExistsTrackingTableAsync(SyncContext ctx, SyncTable schemaTable, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
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
