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
        public virtual async Task<long> UpdateUntrackedRowsAsync(string scopeName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);

            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                ClientScopeInfo clientScopeInfo;
                (context, clientScopeInfo) = await this.InternalLoadClientScopeInfoAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (clientScopeInfo.Schema == null || clientScopeInfo.Schema.Tables == null || clientScopeInfo.Schema.Tables.Count <= 0 || !clientScopeInfo.Schema.HasColumns)
                    throw new MissingTablesException(scopeName);

                long totalUpdates = 0L;
                // Update untracked rows
                foreach (var table in clientScopeInfo.Schema.Tables)
                {
                    var syncAdapter = this.GetSyncAdapter(table, clientScopeInfo);
                    long updates = 0L;
                    (context, updates) = await this.InternalUpdateUntrackedRowsAsync(clientScopeInfo, context, syncAdapter, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
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
        public virtual Task<long> UpdateUntrackedRowsAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => this.UpdateUntrackedRowsAsync(SyncOptions.DefaultScopeName, connection, transaction, cancellationToken);



        /// <summary>
        /// Internal update untracked rows routine
        /// </summary>
        internal async Task<(SyncContext context, int updated)> InternalUpdateUntrackedRowsAsync(IScopeInfo scopeInfo, SyncContext context, DbSyncAdapter syncAdapter, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            // Get table builder
            var tableBuilder = this.GetTableBuilder(syncAdapter.TableDescription, scopeInfo);

            // Check if tracking table exists
            bool trackingTableExists;
            (context, trackingTableExists) = await this.InternalExistsTrackingTableAsync(scopeInfo, context, tableBuilder, connection, transaction, CancellationToken.None, null).ConfigureAwait(false);

            if (!trackingTableExists)
                throw new MissingTrackingTableException(tableBuilder.TableDescription.GetFullName());

            // Get correct Select incremental changes command 
            var (command, _) = await syncAdapter.GetCommandAsync(DbCommandType.UpdateUntrackedRows, connection, transaction);

            if (command == null) return (context, 0);

            await this.InterceptAsync(new DbCommandArgs(context, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            // Execute
            var rowAffected = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

            // Check if we have a return value instead
            var syncRowCountParam = DbSyncAdapter.GetParameter(command, "sync_row_count");

            if (syncRowCountParam != null)
                rowAffected = (int)syncRowCountParam.Value;

            return (context, rowAffected);
        }

    }
}
