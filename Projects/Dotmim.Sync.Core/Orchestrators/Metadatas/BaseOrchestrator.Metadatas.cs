
using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {


        internal virtual async Task<(SyncContext context, DatabaseMetadatasCleaned databaseMetadatasCleaned)>
            InternalDeleteMetadatasAsync(
                    IEnumerable<ScopeInfo> scopeInfos, SyncContext context, long timestampLimit,
                    DbConnection connection, DbTransaction transaction,
                    CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            context.SyncStage = SyncStage.MetadataCleaning;

            var databaseMetadatasCleaned = new DatabaseMetadatasCleaned { TimestampLimit = timestampLimit };

            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.MetadataCleaning, connection, transaction).ConfigureAwait(false);

                await this.InterceptAsync(new MetadataCleaningArgs(context, scopeInfos, timestampLimit,
                                            runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                // contains all tables already processed
                var doneList = new List<SetupTable>();

                foreach (var scopeInfo in scopeInfos)
                {
                    if (scopeInfo.Setup?.Tables == null || scopeInfo.Setup.Tables.Count <= 0)
                        continue;

                    foreach (var setupTable in scopeInfo.Setup.Tables)
                    {
                        var isDone = doneList.Any(t => t.EqualsByName(setupTable));

                        if (isDone)
                            continue;

                        var syncTable = scopeInfo.Schema.Tables[setupTable.TableName, setupTable.SchemaName];
                        var syncAdapter = this.GetSyncAdapter(scopeInfo.Name, syncTable, scopeInfo.Setup);

                        var (command, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, DbCommandType.DeleteMetadata, 
                            runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        if (command != null)
                        {
                            // Set the special parameters for delete metadata
                            command = this.InternalSetCommandParametersValues(context, command, DbCommandType.DeleteMetadata, syncAdapter, 
                                runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress,
                                sync_min_timestamp: timestampLimit);
                            
                            await this.InterceptAsync(new ExecuteCommandArgs(context, command, DbCommandType.DeleteMetadata, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                            int rowsCleanedCount = 0;
                            rowsCleanedCount = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                            // Check if we have a return value instead
                            var syncRowCountParam = syncAdapter.GetParameter(context, command, "sync_row_count");

                            if (syncRowCountParam != null && syncRowCountParam.Value != null && syncRowCountParam.Value != DBNull.Value)
                                rowsCleanedCount = (int)syncRowCountParam.Value;

                            // Only add a new table metadata stats object, if we have, at least, purged 1 or more rows
                            if (rowsCleanedCount > 0)
                            {
                                var tableMetadatasCleaned = new TableMetadatasCleaned(syncTable.TableName, syncTable.SchemaName)
                                {
                                    RowsCleanedCount = rowsCleanedCount,
                                    TimestampLimit = timestampLimit
                                };

                                databaseMetadatasCleaned.Tables.Add(tableMetadatasCleaned);
                            }

                            command.Dispose();
                        }

                        doneList.Add(setupTable);
                    }
                }

                await this.InterceptAsync(new MetadataCleanedArgs(context, databaseMetadatasCleaned, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return (context, databaseMetadatasCleaned);
            }
            catch (Exception ex)
            {
                string message = null;

                message += $"TimestampLimit:{timestampLimit}.";

                throw GetSyncError(context, ex, message);
            }
        }



        /// <summary>
        /// Update a metadata row
        /// </summary>
        internal async Task<(SyncContext context, bool metadataUpdated, Exception exception)>InternalUpdateMetadatasAsync(ScopeInfo scopeInfo, SyncContext context, 
                                SyncRow row, SyncTable schemaTable, Guid? senderScopeId, bool forceWrite, 
                                DbConnection connection, DbTransaction transaction,
                                CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            context.SyncStage = SyncStage.ChangesApplying;
            
            var syncAdapter = this.GetSyncAdapter(scopeInfo.Name, schemaTable, scopeInfo.Setup);

            await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.MetadataCleaning, connection, transaction).ConfigureAwait(false);

            var (command, _) = await this.InternalGetCommandAsync(scopeInfo, context, syncAdapter, DbCommandType.UpdateMetadata, 
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            if (command == null) return (context, false, null);

            // Set the parameters value from row 
            this.InternalSetCommandParametersValues(context,  command, DbCommandType.UpdateMetadata, syncAdapter, connection, transaction, cancellationToken, progress,
                row, senderScopeId, 0, row.RowState == SyncRowState.Deleted, forceWrite);

            await this.InterceptAsync(new ExecuteCommandArgs(context, command, DbCommandType.UpdateMetadata, runner.Connection, runner.Transaction)).ConfigureAwait(false);

            Exception exception = null;
            int metadataUpdatedRowsCount = 0;
            try
            {
                metadataUpdatedRowsCount = await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                // Check if we have a return value instead
                var syncRowCountParam = syncAdapter.GetParameter(context, command, "sync_row_count");

                if (syncRowCountParam != null && syncRowCountParam.Value != null && syncRowCountParam.Value != DBNull.Value)
                    metadataUpdatedRowsCount = (int)syncRowCountParam.Value;

                command.Dispose();
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            return (context, metadataUpdatedRowsCount > 0, exception);
        }


    }
}
