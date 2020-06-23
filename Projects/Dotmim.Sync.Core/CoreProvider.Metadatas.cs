using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class CoreProvider
    {
        public virtual async Task<(SyncContext syncContext, DatabaseMetadatasCleaned databaseMetadatasCleaned)> DeleteMetadatasAsync(SyncContext context, SyncSet schema, SyncSetup setup, long timestampLimit,
                            DbConnection connection, DbTransaction transaction,
                            CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            DatabaseMetadatasCleaned databaseMetadatasCleaned = new DatabaseMetadatasCleaned { TimestampLimit = timestampLimit };

            foreach (var syncTable in schema.Tables)
            {
                // get table builder
                var tableBuilder = this.GetTableBuilder(syncTable, setup);

                var tableHelper = tableBuilder.CreateTableBuilder();

                // check if table exists
                // If not, kindly continue, without exception
                if (await tableHelper.NeedToCreateTableAsync(connection, transaction).ConfigureAwait(false))
                    continue;

                // Create sync adapter
                var syncAdapter = tableBuilder.CreateSyncAdapter();

                // Delete metadatas
                var rowsCleanedCount = await syncAdapter.DeleteMetadatasAsync(timestampLimit, connection, transaction).ConfigureAwait(false);

                // Only add a new table metadata stats object, if we have, at least, purged 1 or more rows
                if (rowsCleanedCount > 0)
                {
                    var tableMetadatasCleaned = new TableMetadatasCleaned(syncTable.TableName, syncTable.SchemaName);
                    tableMetadatasCleaned.RowsCleanedCount = rowsCleanedCount;
                    tableMetadatasCleaned.TimestampLimit = timestampLimit;

                    this.Orchestrator.logger.LogDebug(SyncEventsId.MetadataCleaning, tableMetadatasCleaned);
                    databaseMetadatasCleaned.Tables.Add(tableMetadatasCleaned);
                }

            }

            this.Orchestrator.logger.LogDebug(SyncEventsId.MetadataCleaning, databaseMetadatasCleaned);

            return (context, databaseMetadatasCleaned);
        }

    }
}
