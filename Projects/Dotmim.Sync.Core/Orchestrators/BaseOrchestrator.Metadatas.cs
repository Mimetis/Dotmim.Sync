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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {

        /// <summary>
        /// Delete metadatas items from tracking tables
        /// </summary>
        /// <param name="timeStampStart">Timestamp start. Used to limit the delete metadatas rows from now to this timestamp</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <param name="progress">Progress args</param>
        public virtual Task<DatabaseMetadatasCleaned> DeleteMetadatasAsync(long timeStampStart, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(async (ctx, connection, transaction) =>
        {
            ctx.SyncStage = SyncStage.MetadataCleaning;

            await this.InterceptAsync(new MetadataCleaningArgs(ctx, this.Setup, timeStampStart, connection, transaction), cancellationToken).ConfigureAwait(false);

            // Create a dummy schema to be able to call the DeprovisionAsync method on the provider
            // No need columns or primary keys to be able to deprovision a table
            SyncSet schema = new SyncSet(this.Setup);

            var databaseMetadatasCleaned = await this.InternalDeleteMetadatasAsync(ctx, schema, this.Setup, timeStampStart, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            ctx.SyncStage = SyncStage.MetadataCleaned;

            var args = new MetadataCleanedArgs(ctx, databaseMetadatasCleaned, connection);
            await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);
            this.ReportProgress(ctx, progress, args);

            return databaseMetadatasCleaned;

        }, cancellationToken);


        internal virtual async Task<DatabaseMetadatasCleaned> InternalDeleteMetadatasAsync(SyncContext context, SyncSet schema, SyncSetup setup, long timestampLimit,
                    DbConnection connection, DbTransaction transaction,
                    CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            DatabaseMetadatasCleaned databaseMetadatasCleaned = new DatabaseMetadatasCleaned { TimestampLimit = timestampLimit };

            this.logger.LogInformation(SyncEventsId.MetadataCleaning, new { connection.Database, TimestampLimit = timestampLimit });

            foreach (var syncTable in schema.Tables)
            {
                // Create sync adapter
                var syncAdapter = this.Provider.GetSyncAdapter(syncTable, setup);

                // Delete metadatas
                var rowsCleanedCount = await syncAdapter.DeleteMetadatasAsync(timestampLimit, connection, transaction).ConfigureAwait(false);

                // Only add a new table metadata stats object, if we have, at least, purged 1 or more rows
                if (rowsCleanedCount > 0)
                {
                    var tableMetadatasCleaned = new TableMetadatasCleaned(syncTable.TableName, syncTable.SchemaName)
                    {
                        RowsCleanedCount = rowsCleanedCount,
                        TimestampLimit = timestampLimit
                    };

                    this.logger.LogDebug(SyncEventsId.MetadataCleaning, tableMetadatasCleaned);
                    databaseMetadatasCleaned.Tables.Add(tableMetadatasCleaned);
                }

            }

            this.logger.LogDebug(SyncEventsId.MetadataCleaning, databaseMetadatasCleaned);

            return databaseMetadatasCleaned;
        }

    }
}
