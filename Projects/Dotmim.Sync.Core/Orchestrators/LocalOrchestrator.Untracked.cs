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
        public virtual async Task<bool> UpdateUntrackedRowsAsync(string scopeName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Writing, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var scopeInfo = await this.InternalGetScopeAsync(scopeName, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || scopeInfo.Schema.Tables.Count <= 0 || !scopeInfo.Schema.HasColumns)
                    throw new MissingTablesException(scopeName);

                // Update untracked rows
                foreach (var table in scopeInfo.Schema.Tables)
                {
                    var syncAdapter = this.GetSyncAdapter(table, scopeInfo);
                    await this.InternalUpdateUntrackedRowsAsync(scopeInfo, syncAdapter, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                await runner.CommitAsync().ConfigureAwait(false);

                return true;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeName, ex);
            }
        }

        /// <summary>
        /// Update all untracked rows from the client database
        /// </summary>
        public virtual Task<bool> UpdateUntrackedRowsAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null) 
            => this.UpdateUntrackedRowsAsync(SyncOptions.DefaultScopeName, connection, transaction, cancellationToken);



        /// <summary>
        /// Internal update untracked rows routine
        /// </summary>
        internal async Task<int> InternalUpdateUntrackedRowsAsync(IScopeInfo scopeInfo, DbSyncAdapter syncAdapter, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            // Get table builder
            var tableBuilder = this.GetTableBuilder(syncAdapter.TableDescription, scopeInfo);

            // Check if tracking table exists
            var trackingTableExists = await this.InternalExistsTrackingTableAsync(scopeInfo, tableBuilder, connection, transaction, CancellationToken.None, null).ConfigureAwait(false);

            if (!trackingTableExists)
                throw new MissingTrackingTableException(tableBuilder.TableDescription.GetFullName());

            // Get correct Select incremental changes command 
            var (command, _) = await syncAdapter.GetCommandAsync(DbCommandType.UpdateUntrackedRows, connection, transaction);

            if (command == null) return 0;

            var ctx = this.GetContext(scopeInfo.Name);

            await this.InterceptAsync(new DbCommandArgs(ctx, command, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            // Execute
            var rowAffected = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

            // Check if we have a return value instead
            var syncRowCountParam = DbSyncAdapter.GetParameter(command, "sync_row_count");

            if (syncRowCountParam != null)
                rowAffected = (int)syncRowCountParam.Value;

            return rowAffected;
        }

    }
}
