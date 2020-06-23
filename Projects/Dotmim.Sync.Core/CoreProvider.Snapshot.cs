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
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class CoreProvider
    {
        /// <summary>
        /// update configuration object with tables desc from server database
        /// </summary>
        public virtual async Task<(SyncContext, BatchInfo)> CreateSnapshotAsync(SyncContext context, SyncSet schema, SyncSetup setup,
                             DbConnection connection, DbTransaction transaction, string snapshotDirectory, int batchSize, long remoteClientTimestamp,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            // create local directory
            if (!Directory.Exists(snapshotDirectory))
            {
                this.Orchestrator.logger.LogDebug(SyncEventsId.CreateDirectory, new { SnapshotDirectory = snapshotDirectory });
                Directory.CreateDirectory(snapshotDirectory);
            }

            // cleansing scope name
            var directoryScopeName = new string(context.ScopeName.Where(char.IsLetterOrDigit).ToArray());

            var directoryFullPath = Path.Combine(snapshotDirectory, directoryScopeName);

            // create local directory with scope inside
            if (!Directory.Exists(directoryFullPath))
            {
                this.Orchestrator.logger.LogDebug(SyncEventsId.CreateDirectory, new { DirectoryFullPath = directoryFullPath });
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
                this.Orchestrator.logger.LogDebug(SyncEventsId.DropDirectory, new { DirectoryFullPath = directoryFullPath });
                Directory.Delete(directoryFullPath, true);
            }

            foreach (var syncTable in schema.Tables)
            {
                var tableBuilder = this.GetTableBuilder(syncTable, setup);
                var syncAdapter = tableBuilder.CreateSyncAdapter();

                // launch interceptor if any
                await this.Orchestrator.InterceptAsync(new TableChangesSelectingArgs(context, syncTable, connection, transaction), cancellationToken).ConfigureAwait(false);

                // Get Select initialize changes command
                var selectIncrementalChangesCommand = await this.GetSelectChangesCommandAsync(context, syncAdapter, syncTable, true, connection, transaction);

                // Set parameters
                this.SetSelectChangesCommonParameters(context, syncTable, null, true, 0, selectIncrementalChangesCommand);

                // log
                this.Orchestrator.logger.LogDebug(SyncEventsId.CreateSnapshot, new
                {
                    SelectChangesCommandText = selectIncrementalChangesCommand.CommandText,
                    ExcludingScopeId = Guid.Empty,
                    IsNew = true,
                    LastTimestamp = 0
                });

                // Get the reader
                using (var dataReader = selectIncrementalChangesCommand.ExecuteReader())
                {
                    // memory size total
                    double rowsMemorySize = 0L;

                    // Create a chnages table with scope columns
                    var changesSetTable = DbSyncAdapter.CreateChangesTable(schema.Tables[syncTable.TableName, syncTable.SchemaName], changesSet);

                    while (dataReader.Read())
                    {
                        // Create a row from dataReader
                        var row = CreateSyncRowFromReader(dataReader, changesSetTable);

                        // Add the row to the changes set
                        changesSetTable.Rows.Add(row);

                        // Log trace row
                        this.Orchestrator.logger.LogTrace(SyncEventsId.CreateSnapshot, row);

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
                        await batchInfo.AddChangesAsync(changesSet, batchIndex, false, this.Orchestrator).ConfigureAwait(false);

                        this.Orchestrator.logger.LogDebug(SyncEventsId.CreateBatch, changesSet);

                        // increment batch index
                        batchIndex++;

                        // we know the datas are serialized here, so we can flush  the set
                        changesSet.Clear();

                        // Recreate an empty ContainerSet and a ContainerTable
                        changesSet = new SyncSet();

                        changesSetTable = DbSyncAdapter.CreateChangesTable(schema.Tables[syncTable.TableName, syncTable.SchemaName], changesSet);

                        // Init the row memory size
                        rowsMemorySize = 0L;
                    }
                }

                //selectIncrementalChangesCommand.Dispose();
            }


            if (changesSet != null && changesSet.HasTables)
            {
                await batchInfo.AddChangesAsync(changesSet, batchIndex, true, this.Orchestrator).ConfigureAwait(false);
                this.Orchestrator.logger.LogDebug(SyncEventsId.CreateBatch, changesSet);
            }

            // Check the last index as the last batch
            batchInfo.EnsureLastBatch();

            batchInfo.Timestamp = remoteClientTimestamp;

            // Serialize on disk.
            var jsonConverter = new JsonConverter<BatchInfo>();

            var summaryFileName = Path.Combine(directoryFullPath, "summary.json");

            using (var f = new FileStream(summaryFileName, FileMode.CreateNew, FileAccess.ReadWrite))
            {
                this.Orchestrator.logger.LogDebug(SyncEventsId.CreateSnapshotSummary, batchInfo);
                var bytes = await jsonConverter.SerializeAsync(batchInfo).ConfigureAwait(false);
                f.Write(bytes, 0, bytes.Length);
            }

            return (context, batchInfo);
        }


        /// <summary>
        /// Gets a batch of changes to synchronize when given batch size, 
        /// destination knowledge, and change data retriever parameters.
        /// </summary>
        /// <returns>A DbSyncContext object that will be used to retrieve the modified data.</returns>
        public virtual async Task<(SyncContext, BatchInfo)> GetSnapshotAsync(
                             SyncContext context, SyncSet schema, string snapshotDirectory,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            // TODO : Get a snapshot based on scope name

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

            this.Orchestrator.logger.LogDebug(SyncEventsId.GetSnapshot, new { DirectoryName = directoryName });

            // cleansing scope name
            var directoryScopeName = new string(context.ScopeName.Where(char.IsLetterOrDigit).ToArray());

            // Get full path
            var directoryFullPath = Path.Combine(snapshotDirectory, directoryScopeName, directoryName);

            // if no snapshot present, just return null value.
            if (!Directory.Exists(directoryFullPath))
            {
                this.Orchestrator.logger.LogDebug(SyncEventsId.DirectoryNotExists, new { DirectoryPath = directoryFullPath });
                return (context, null);
            }

            // Serialize on disk.
            var jsonConverter = new JsonConverter<BatchInfo>();

            var summaryFileName = Path.Combine(directoryFullPath, "summary.json");

            BatchInfo batchInfo = null;

            // Create the schema changeset
            var changesSet = new SyncSet();

            // Create a Schema set without readonly columns, attached to memory changes
            foreach (var table in schema.Tables)
                DbSyncAdapter.CreateChangesTable(schema.Tables[table.TableName, table.SchemaName], changesSet);

            using (var fs = new FileStream(summaryFileName, FileMode.Open, FileAccess.Read))
            {
                this.Orchestrator.logger.LogDebug(SyncEventsId.LoadSnapshotSummary, new { FileName = summaryFileName });
                batchInfo = await jsonConverter.DeserializeAsync(fs).ConfigureAwait(false);
                this.Orchestrator.logger.LogDebug(SyncEventsId.LoadSnapshotSummary, batchInfo);
            }

            batchInfo.SanitizedSchema = changesSet;

            return (context, batchInfo);
        }
    }
}
