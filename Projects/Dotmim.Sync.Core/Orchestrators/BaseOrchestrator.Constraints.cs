using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {
        /// <summary>
        /// Reset a table, deleting rows from table and tracking_table
        /// </summary>
        public Task<bool> ResetTableAsync(SetupTable table, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.None, async (ctx, connection, transaction) =>
        {
            // using a fake SyncTable based on SetupTable, since we don't need columns
            var schemaTable = new SyncTable(table.TableName, table.SchemaName);

            var syncAdapter = this.GetSyncAdapter(schemaTable, this.Setup);

            await this.InternalResetTableAsync(ctx, syncAdapter, connection, transaction).ConfigureAwait(false);

            return true;
        }, connection, transaction, cancellationToken);

        /// <summary>
        /// Disabling constraints on one table
        /// </summary>
        public Task<bool> DisableConstraintsAsync(SetupTable table, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.None, async (ctx, connection, transaction) =>
        {
            // using a fake SyncTable based on SetupTable, since we don't need columns
            var schemaTable = new SyncTable(table.TableName, table.SchemaName);

            var syncAdapter = this.GetSyncAdapter(schemaTable, this.Setup);

            await this.InternalDisableConstraintsAsync(ctx, syncAdapter, connection, transaction).ConfigureAwait(false);

            return true;
        }, connection, transaction, cancellationToken);

        /// <summary>
        /// Enabling constraints on one table
        /// </summary>
        public Task<bool> EnableConstraintsAsync(SetupTable table, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.None, async (ctx, connection, transaction) =>
        {
            // using a fake SyncTable based on SetupTable, since we don't need columns
            var schemaTable = new SyncTable(table.TableName, table.SchemaName);

            var syncAdapter = this.GetSyncAdapter(schemaTable, this.Setup);

            await this.InternalEnableConstraintsAsync(ctx, syncAdapter, connection, transaction).ConfigureAwait(false);

            return true;
        }, connection, transaction, cancellationToken);

        /// <summary>
        /// Disabling all constraints on synced tables
        /// </summary>
        internal async Task InternalDisableConstraintsAsync(SyncContext context, DbSyncAdapter syncAdapter, DbConnection connection, DbTransaction transaction = null)
        {
            var (command, _) = await syncAdapter.GetCommandAsync(DbCommandType.DisableConstraints, connection, transaction).ConfigureAwait(false);

            if (command == null) return;

            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Enabling all constraints on synced tables
        /// </summary>
        internal async Task InternalEnableConstraintsAsync(SyncContext context, DbSyncAdapter syncAdapter, DbConnection connection, DbTransaction transaction)
        {
            var (command, _) = await syncAdapter.GetCommandAsync(DbCommandType.EnableConstraints, connection, transaction).ConfigureAwait(false);

            if (command == null) return;

            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Reset a table, deleting rows from table and tracking_table
        /// </summary>
        internal async Task<bool> InternalResetTableAsync(SyncContext context, DbSyncAdapter syncAdapter, DbConnection connection, DbTransaction transaction)
        {
            var (command, _) = await syncAdapter.GetCommandAsync(DbCommandType.Reset, connection, transaction);

            if (command != null)
            {
                var rowCount = await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                return rowCount > 0;
            }

            return true;
        }

    }
}
