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
        /// update configuration object with tables desc from server database
        /// </summary>
        internal virtual async Task<(SyncContext, BatchInfo)> InternalCreateSnapshotAsync(SyncContext context, SyncSet schema, SyncSetup setup,
                             DbConnection connection, DbTransaction transaction, string snapshotDirectory, int batchSize, long remoteClientTimestamp,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            // create local directory
            if (!Directory.Exists(snapshotDirectory))
            {
                this.logger.LogDebug(SyncEventsId.CreateDirectory, new { SnapshotDirectory = snapshotDirectory });
                Directory.CreateDirectory(snapshotDirectory);
            }

            // cleansing scope name
            var directoryScopeName = new string(context.ScopeName.Where(char.IsLetterOrDigit).ToArray());

            var directoryFullPath = Path.Combine(snapshotDirectory, directoryScopeName);

            // create local directory with scope inside
            if (!Directory.Exists(directoryFullPath))
            {
                this.logger.LogDebug(SyncEventsId.CreateDirectory, new { DirectoryFullPath = directoryFullPath });
                Directory.CreateDirectory(directoryFullPath);
            }

            // numbers of batch files generated
            var batchIndex = 0;

            // create the in memory changes set
            var changesSet = new SyncSet();

            var sb = new StringBuilder();
            var underscore = "";

            if (context.Parameters != null)
            {
                foreach (var p in context.Parameters.OrderBy(p => p.Name))
                {
                    var cleanValue = new string(p.Value.ToString().Where(char.IsLetterOrDigit).ToArray());
                    var cleanName = new string(p.Name.Where(char.IsLetterOrDigit).ToArray());

                    sb.Append($"{underscore}{cleanName}_{cleanValue}");
                    underscore = "_";
                }
            }

            var directoryName = sb.ToString();
            directoryName = string.IsNullOrEmpty(directoryName) ? "ALL" : directoryName;

            // batchinfo generate a schema clone with scope columns if needed
            var batchInfo = new BatchInfo(false, schema, directoryFullPath, directoryName);

            // Delete directory if already exists
            directoryFullPath = Path.Combine(directoryFullPath, directoryName);

            if (Directory.Exists(directoryFullPath))
            {
                this.logger.LogDebug(SyncEventsId.DropDirectory, new { DirectoryFullPath = directoryFullPath });
                Directory.Delete(directoryFullPath, true);
            }

            foreach (var table in schema.Tables)
            {
                var syncAdapter = this.Provider.GetSyncAdapter(table, setup);

                // launch interceptor if any
                await this.InterceptAsync(new TableChangesSelectingArgs(context, table, connection, transaction), cancellationToken).ConfigureAwait(false);

                // Get Select initialize changes command
                var selectIncrementalChangesCommand = await this.GetSelectChangesCommandAsync(context, syncAdapter, table, true, connection, transaction);

                // Set parameters
                this.SetSelectChangesCommonParameters(context, table, null, true, 0, selectIncrementalChangesCommand);

                // log
                this.logger.LogDebug(SyncEventsId.CreateSnapshot, new
                {
                    SelectChangesCommandText = selectIncrementalChangesCommand.CommandText,
                    ExcludingScopeId = Guid.Empty,
                    IsNew = true,
                    LastTimestamp = 0
                });

                // Get the reader
                using (var dataReader = await selectIncrementalChangesCommand.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    // memory size total
                    double rowsMemorySize = 0L;

                    // Create a chnages table with scope columns
                    var changesSetTable = SyncAdapter.CreateChangesTable(schema.Tables[table.TableName, table.SchemaName], changesSet);

                    while (dataReader.Read())
                    {
                        // Create a row from dataReader
                        var row = CreateSyncRowFromReader(dataReader, changesSetTable);

                        // Add the row to the changes set
                        changesSetTable.Rows.Add(row);

                        // Log trace row
                        this.logger.LogTrace(SyncEventsId.CreateSnapshot, row);

                        var fieldsSize = ContainerTable.GetRowSizeFromDataRow(row.ToArray());
                        var finalFieldSize = fieldsSize / 1024d;

                        if (finalFieldSize > batchSize)
                            throw new RowOverSizedException(finalFieldSize.ToString());

                        // Calculate the new memory size
                        rowsMemorySize += finalFieldSize;

                        // Next line if we don't reach the batch size yet.
                        if (rowsMemorySize <= batchSize)
                            continue;

                        // add changes to batchinfo
                        await batchInfo.AddChangesAsync(changesSet, batchIndex, false, this).ConfigureAwait(false);

                        this.logger.LogDebug(SyncEventsId.CreateBatch, changesSet);

                        // increment batch index
                        batchIndex++;

                        // we know the datas are serialized here, so we can flush  the set
                        changesSet.Clear();

                        // Recreate an empty ContainerSet and a ContainerTable
                        changesSet = new SyncSet();

                        changesSetTable = SyncAdapter.CreateChangesTable(schema.Tables[table.TableName, table.SchemaName], changesSet);

                        // Init the row memory size
                        rowsMemorySize = 0L;
                    }
                }

                //selectIncrementalChangesCommand.Dispose();
            }


            if (changesSet != null && changesSet.HasTables)
            {
                await batchInfo.AddChangesAsync(changesSet, batchIndex, true, this).ConfigureAwait(false);
                this.logger.LogDebug(SyncEventsId.CreateBatch, changesSet);
            }

            // Check the last index as the last batch
            batchInfo.EnsureLastBatch();

            batchInfo.Timestamp = remoteClientTimestamp;

            // Serialize on disk.
            var jsonConverter = new JsonConverter<BatchInfo>();

            var summaryFileName = Path.Combine(directoryFullPath, "summary.json");

            using (var f = new FileStream(summaryFileName, FileMode.CreateNew, FileAccess.ReadWrite))
            {
                this.logger.LogDebug(SyncEventsId.CreateSnapshotSummary, batchInfo);
                var bytes = await jsonConverter.SerializeAsync(batchInfo).ConfigureAwait(false);
                f.Write(bytes, 0, bytes.Length);
            }

            return (context, batchInfo);
        }

    }
}
