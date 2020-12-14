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
            => OperationAsync(new Func<SyncContext, SyncTable, DbConnection, DbTransaction, Task<bool>>(async (ctx, syncTable, connection, transaction) =>
            {
                this.logger.LogInformation(SyncEventsId.CreateTable, new { Table = syncTable });

                // Get table builder
                var tableBuilder = this.Provider.GetTableBuilder(syncTable, this.Setup);

                ctx.SyncStage = SyncStage.Provisioning;

                var schemaExists = await InternalExistsSchemaAsync(ctx, syncTable, tableBuilder, connection, transaction, cancellationToken);

                if (!schemaExists)
                    await InternalCreateSchemaAsync(ctx, syncTable, tableBuilder, connection, transaction, cancellationToken);

                var exists = await InternalExistsTrackingTableAsync(ctx, syncTable, tableBuilder, connection, transaction, cancellationToken);

                if (!exists)
                    await InternalCreateTrackingTableAsync(ctx, syncTable, tableBuilder, connection, transaction, cancellationToken).ConfigureAwait(false);

                ctx.SyncStage = SyncStage.Provisioned;

                return !exists;

            }), table, progress, cancellationToken);

        /// <summary>
        /// Drop a tracking table
        /// </summary>
        /// <param name="table">A table from your Setup instance you want to drop</param>
        public Task<bool> DropTrackingTableAsync(SetupTable table, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => OperationAsync(new Func<SyncContext, SyncTable, DbConnection, DbTransaction, Task<bool>>(async (ctx, syncTable, connection, transaction) =>
             {
                 this.logger.LogInformation(SyncEventsId.DropTable, new { Table = syncTable });

                 // Get table builder
                 var tableBuilder = this.Provider.GetTableBuilder(syncTable, this.Setup);

                 ctx.SyncStage = SyncStage.Deprovisioning;

                 var exists = await InternalExistsTrackingTableAsync(ctx, syncTable, tableBuilder, connection, transaction, cancellationToken);

                 if (exists)
                     await InternalDropTrackingTableAsync(ctx, syncTable, tableBuilder, connection, transaction, cancellationToken).ConfigureAwait(false);

                 ctx.SyncStage = SyncStage.Deprovisioned;

                 return exists;

             }), table, progress, cancellationToken);

        /// <summary>
        /// Rename a tracking table
        /// </summary>
        /// <param name="table">A table from your Setup instance you want to rename the tracking table</param>
        public Task<bool> RenameTrackingTableAsync(SyncTable syncTable, ParserName oldTrackingTableName, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => OperationTableAsync(new Func<SyncContext, DbConnection, DbTransaction, Task<bool>>(async (ctx, connection, transaction) =>
            {
                this.logger.LogInformation(SyncEventsId.CreateTable, new { Table = syncTable });

                // Get table builder
                var tableBuilder = this.Provider.GetTableBuilder(syncTable, this.Setup);

                ctx.SyncStage = SyncStage.Provisioning;

                await InternalRenameTrackingTableAsync(ctx, syncTable, oldTrackingTableName, tableBuilder, connection, transaction, cancellationToken).ConfigureAwait(false);

                ctx.SyncStage = SyncStage.Provisioned;

                return true;

            }), syncTable, progress, cancellationToken);


        /// <summary>
        /// Internal create tracking table routine
        /// </summary>
        internal async Task InternalCreateTrackingTableAsync(SyncContext ctx, SyncTable schemaTable, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
        {
            var command = await tableBuilder.GetCreateTrackingTableCommandAsync(connection, transaction).ConfigureAwait(false);

            if (command == null)
                return;

            var action = new TrackingTableCreatingArgs(ctx, schemaTable, command, connection, transaction);

            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);
            if (!action.Cancel && action.Command != null)
            {
                await action.Command.ExecuteNonQueryAsync();
                await this.InterceptAsync(new TrackingTableCreatedArgs(ctx, schemaTable, connection, transaction), cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Internal rename tracking table routine
        /// </summary>
        internal async Task InternalRenameTrackingTableAsync(SyncContext ctx, SyncTable schemaTable, ParserName oldTrackingTableName, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
        {
            var command = await tableBuilder.GetRenameTrackingTableCommandAsync(oldTrackingTableName, connection, transaction).ConfigureAwait(false);

            if (command == null)
                return;

            var action = new TableCreatingArgs(ctx, schemaTable, command, connection, transaction);

            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);
            if (!action.Cancel && action.Command != null)
            {
                await action.Command.ExecuteNonQueryAsync();
                await this.InterceptAsync(new TableCreatedArgs(ctx, schemaTable, connection, transaction), cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Internal drop tracking table routine
        /// </summary>
        internal async Task InternalDropTrackingTableAsync(SyncContext ctx, SyncTable schemaTable, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
        {
            var command = await tableBuilder.GetDropTrackingTableCommandAsync(connection, transaction).ConfigureAwait(false);

            if (command == null)
                return;

            var action = new TrackingTableDroppingArgs(ctx, schemaTable, command, connection, transaction);

            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);
            if (!action.Cancel && action.Command != null)
            {
                await action.Command.ExecuteNonQueryAsync();
                await this.InterceptAsync(new TrackingTableDroppedArgs(ctx, schemaTable, connection, transaction), cancellationToken).ConfigureAwait(false);
            }
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
