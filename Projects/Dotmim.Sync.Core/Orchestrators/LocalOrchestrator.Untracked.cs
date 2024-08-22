using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains the logic to update all the rows that are not tracked on the client side.
    /// </summary>
    public partial class LocalOrchestrator : BaseOrchestrator
    {
        /// <summary>
        /// Update all untracked rows from the client database.
        /// </summary>
        public virtual async Task<long> UpdateUntrackedRowsAsync(
            string scopeName,
            DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);

            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.ChangesApplying, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    ScopeInfo cScopeInfo;
                    (context, cScopeInfo) = await this.InternalLoadScopeInfoAsync(
                        context,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (cScopeInfo.Schema == null || cScopeInfo.Schema.Tables == null || cScopeInfo.Schema.Tables.Count <= 0 || !cScopeInfo.Schema.HasColumns)
                        throw new MissingTablesException();

                    var totalUpdates = 0L;

                    // Update untracked rows
                    foreach (var table in cScopeInfo.Schema.Tables)
                    {
                        var updates = 0L;
                        (context, updates) = await this.InternalUpdateUntrackedRowsAsync(cScopeInfo, context, table,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                        totalUpdates += updates;
                    }

                    await runner.CommitAsync().ConfigureAwait(false);

                    return totalUpdates;
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Update all untracked rows from the client database.
        /// </summary>
        public virtual Task<long> UpdateUntrackedRowsAsync(DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
            => this.UpdateUntrackedRowsAsync(SyncOptions.DefaultScopeName, connection, transaction, progress, cancellationToken);

        /// <summary>
        /// Internal update untracked rows routine.
        /// </summary>
        internal async Task<(SyncContext Context, int Updated)> InternalUpdateUntrackedRowsAsync(ScopeInfo scopeInfo, SyncContext context,
            SyncTable schemaTable, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {

            using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
            await using (runner.ConfigureAwait(false))
            {
                var syncAdapter = this.GetSyncAdapter(schemaTable, scopeInfo);
                var tableBuilder = syncAdapter.GetTableBuilder();

                // Check if tracking table exists
                bool trackingTableExists;
                (context, trackingTableExists) = await this.InternalExistsTrackingTableAsync(scopeInfo, context, tableBuilder,
                    runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                if (!trackingTableExists)
                    throw new MissingTrackingTableException(tableBuilder.TableDescription.GetFullName());

                // Get correct Select incremental changes command
                var (command, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, DbCommandType.UpdateUntrackedRows,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                if (command == null)
                    return (context, 0);

                await this.InterceptAsync(new ExecuteCommandArgs(context, command, DbCommandType.UpdateUntrackedRows, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                // Execute
                var rowAffected = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

                // Check if we have a return value instead
                var syncRowCountParam = syncAdapter.GetParameter(context, command, "sync_row_count");

                if (syncRowCountParam != null)
                    rowAffected = (int)syncRowCountParam.Value;

                command.Dispose();

                await runner.CommitAsync().ConfigureAwait(false);

                return (context, rowAffected);
            }
        }
    }
}