using Dotmim.Sync.Args;
using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


namespace Dotmim.Sync
{
    public partial class LocalOrchestrator : BaseOrchestrator
    {
        /// <summary>
        /// Update all untracked rows from the client database
        /// </summary>
        public virtual async Task<long> UpdateUntrackedRowsAsync(string scopeName)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);

            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.ChangesApplying).ConfigureAwait(false);

                ScopeInfo clientScopeInfo;
                (context, clientScopeInfo) = await this.InternalLoadScopeInfoAsync(context, 
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (clientScopeInfo.Schema == null || clientScopeInfo.Schema.Tables == null || clientScopeInfo.Schema.Tables.Count <= 0 || !clientScopeInfo.Schema.HasColumns)
                    throw new MissingTablesException(scopeName);

                long totalUpdates = 0L;
                // Update untracked rows
                foreach (var table in clientScopeInfo.Schema.Tables)
                {
                    long updates = 0L;
                    (context, updates) = await this.InternalUpdateUntrackedRowsAsync(clientScopeInfo, context, table, 
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
                    totalUpdates += updates;
                }

                await runner.CommitAsync().ConfigureAwait(false);

                return totalUpdates;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Update all untracked rows from the client database
        /// </summary>
        public virtual Task<long> UpdateUntrackedRowsAsync() => this.UpdateUntrackedRowsAsync(SyncOptions.DefaultScopeName);


        /// <summary>
        /// Internal update untracked rows routine
        /// </summary>
        internal async Task<(SyncContext context, int updated)> InternalUpdateUntrackedRowsAsync(ScopeInfo scopeInfo, SyncContext context,
            SyncTable schemaTable, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            // Get table builder
            var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

            await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            // Check if tracking table exists
            bool trackingTableExists;
            (context, trackingTableExists) = await this.InternalExistsTrackingTableAsync(scopeInfo, context, tableBuilder, 
                runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            if (!trackingTableExists)
                throw new MissingTrackingTableException(tableBuilder.TableDescription.GetFullName());

            // Get correct Select incremental changes command 
            var (command, _) = await this.InternalGetCommandAsync(scopeInfo, context, schemaTable, DbCommandType.UpdateUntrackedRows, null,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            if (command == null) return (context, 0);

            // Parametrized command timeout established if exist
            if (Options.DbCommandTimeout.HasValue)
                command.CommandTimeout = Options.DbCommandTimeout.Value;

            await this.InterceptAsync(new ExecuteCommandArgs(context, command, DbCommandType.UpdateUntrackedRows, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

            // Execute
            var rowAffected = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

            // Check if we have a return value instead
            var syncRowCountParam = GetParameter(command, "sync_row_count");

            if (syncRowCountParam != null)
                rowAffected = (int)syncRowCountParam.Value;
            
            command.Dispose();

            await runner.CommitAsync().ConfigureAwait(false);

            return (context, rowAffected);
        }

    }
}
