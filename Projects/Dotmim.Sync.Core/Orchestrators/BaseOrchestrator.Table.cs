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
        /// Create a table
        /// </summary>
        /// <param name="table">A table from your Setup instance you want to create</param>
        public Task<bool> CreateTableAsync(SyncTable syncTable, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => RunInTransactionAsync(async (ctx, connection, transaction) =>
            {
                var hasBeenCreated = false;

                // Get table builder
                var tableBuilder = this.Provider.GetTableBuilder(syncTable, this.Setup);

                ctx.SyncStage = SyncStage.Provisioning;

                var schemaExists = await InternalExistsSchemaAsync(ctx, syncTable, tableBuilder, connection, transaction, cancellationToken);

                if (!schemaExists)
                    await InternalCreateSchemaAsync(ctx, syncTable, tableBuilder, connection, transaction, cancellationToken);

                var exists = await InternalExistsTableAsync(ctx, syncTable, tableBuilder, connection, transaction, cancellationToken);

                if (!exists)
                    hasBeenCreated = await InternalCreateTableAsync(ctx, syncTable, tableBuilder, connection, transaction, cancellationToken).ConfigureAwait(false);

                ctx.SyncStage = SyncStage.Provisioned;

                return hasBeenCreated;

            }, cancellationToken);

        /// <summary>
        /// Drop a table
        /// </summary>
        /// <param name="table">A table from your Setup instance you want to drop</param>
        public Task<bool> DropTableAsync(SetupTable table, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => RunInTransactionAsync(async (ctx, connection, transaction) =>
             {
                 var hasBeenDropped = false;

                 var schema = await this.InternalGetSchemaAsync(ctx, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                 var schemaTable = schema.Tables[table.TableName, table.SchemaName];

                 if (schemaTable == null)
                     throw new MissingTableException(table.GetFullName());

                 // Get table builder
                 var tableBuilder = this.Provider.GetTableBuilder(schemaTable, this.Setup);

                 ctx.SyncStage = SyncStage.Deprovisioning;

                 var exists = await InternalExistsTableAsync(ctx, schemaTable, tableBuilder, connection, transaction, cancellationToken);

                 if (exists)
                     hasBeenDropped = await InternalDropTableAsync(ctx, schemaTable, tableBuilder, connection, transaction, cancellationToken).ConfigureAwait(false);

                 ctx.SyncStage = SyncStage.Deprovisioned;

                 return hasBeenDropped;

             }, cancellationToken);

        /// <summary>
        /// Internal create table routine
        /// </summary>
        internal async Task<bool> InternalCreateTableAsync(SyncContext ctx, SyncTable schemaTable, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
        {
            var command = await tableBuilder.GetCreateTableCommandAsync(connection, transaction).ConfigureAwait(false);

            if (command == null)
                return false;

            this.logger.LogInformation(SyncEventsId.CreateTable, new { Table = schemaTable.GetFullName() });

            var action = new TableCreatingArgs(ctx, schemaTable, command, connection, transaction);

            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await action.Command.ExecuteNonQueryAsync();
            await this.InterceptAsync(new TableCreatedArgs(ctx, schemaTable, connection, transaction), cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal create table routine
        /// </summary>
        internal async Task<bool> InternalCreateSchemaAsync(SyncContext ctx, SyncTable schemaTable, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
        {
            var command = await tableBuilder.GetCreateSchemaCommandAsync(connection, transaction).ConfigureAwait(false);

            if (command == null)
                return false;

            this.logger.LogInformation(SyncEventsId.CreateSchemaName, new { Table = schemaTable.GetFullName() });

            var action = new SchemaCreatingArgs(ctx, schemaTable, command, connection, transaction);

            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await action.Command.ExecuteNonQueryAsync();
            await this.InterceptAsync(new SchemaCreatedArgs(ctx, schemaTable, connection, transaction), cancellationToken).ConfigureAwait(false);

            return true;
        }

        /// <summary>
        /// Internal drop table routine
        /// </summary>
        internal async Task<bool> InternalDropTableAsync(SyncContext ctx, SyncTable schemaTable, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
        {
            var command = await tableBuilder.GetDropTableCommandAsync(connection, transaction).ConfigureAwait(false);

            if (command == null)
                return false;

            this.logger.LogInformation(SyncEventsId.DropTable, new { Table = schemaTable.GetFullName() });

            var action = new TableDroppingArgs(ctx, schemaTable, command, connection, transaction);

            await this.InterceptAsync(action, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return false;

            await action.Command.ExecuteNonQueryAsync();
            await this.InterceptAsync(new TableDroppedArgs(ctx, schemaTable, connection, transaction), cancellationToken).ConfigureAwait(false);

            return true;

        }

        /// <summary>
        /// Internal exists table procedure routine
        /// </summary>
        internal async Task<bool> InternalExistsTableAsync(SyncContext ctx, SyncTable schemaTable, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
        {
            // Get exists command
            var existsCommand = await tableBuilder.GetExistsTableCommandAsync(connection, transaction).ConfigureAwait(false);

            if (existsCommand == null)
                return false;

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;
            return exists;

        }

        /// <summary>
        /// Internal exists schema procedure routine
        /// </summary>
        internal async Task<bool> InternalExistsSchemaAsync(SyncContext ctx, SyncTable schemaTable, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
        {
            // Get exists command
            var existsCommand = await tableBuilder.GetExistsSchemaCommandAsync(connection, transaction).ConfigureAwait(false);

            if (existsCommand == null)
                return false;

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;
            return exists;

        }



    }
}
