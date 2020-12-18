using Dotmim.Sync.Batch;
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
    public partial class RemoteOrchestrator : BaseOrchestrator
    {

        /// <summary>
        /// Create a snapshot, based on the Setup object. 
        /// </summary>
        /// <param name="syncParameters">if not parameters are found in the SyncContext instance, will use thes sync parameters instead</param>
        /// <returns>Instance containing all information regarding the snapshot</returns>
        public virtual Task<BatchInfo> CreateSnapshotAsync(SyncParameters syncParameters = null, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.SnapshotCreating, async (ctx, connection, transaction) =>
        {
            if (string.IsNullOrEmpty(this.Options.SnapshotsDirectory) || this.Options.BatchSize <= 0)
                throw new SnapshotMissingMandatariesOptionsException();

            // check parameters
            // If context has no parameters specified, and user specifies a parameter collection we switch them
            if ((ctx.Parameters == null || ctx.Parameters.Count <= 0) && syncParameters != null && syncParameters.Count > 0)
                ctx.Parameters = syncParameters;

            // 1) Get Schema from remote provider
            var schema = await this.InternalGetSchemaAsync(ctx, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            // 2) Ensure databases are ready
            var provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

            // Provision everything
            schema = await InternalProvisionAsync(ctx, schema, provision, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            // 3) Getting the most accurate timestamp
            var remoteClientTimestamp = await this.InternalGetLocalTimestampAsync(ctx, connection, transaction, cancellationToken, progress);

            await this.InterceptAsync(new SnapshotCreatingArgs(ctx, schema, this.Options.SnapshotsDirectory, this.Options.BatchSize, remoteClientTimestamp, connection, transaction), cancellationToken).ConfigureAwait(false);

            // 4) Create the snapshot
            var batchInfo = await this.InternalCreateSnapshotAsync(ctx, schema, this.Setup, connection, transaction, this.Options.SnapshotsDirectory,
                    this.Options.BatchSize, remoteClientTimestamp, cancellationToken, progress).ConfigureAwait(false);

            var snapshotCreated = new SnapshotCreatedArgs(ctx, batchInfo, connection);
            await this.InterceptAsync(snapshotCreated, cancellationToken).ConfigureAwait(false);
            this.ReportProgress(ctx, progress, snapshotCreated);


            return batchInfo;
        },  cancellationToken);

        
        /// <summary>
        /// Get a snapshot
        /// </summary>
        public virtual async Task<(long RemoteClientTimestamp, BatchInfo ServerBatchInfo)>
            GetSnapshotAsync(SyncSet schema = null, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            // TODO: Get snapshot based on version and scopename

            // Get context or create a new one
            var ctx = this.GetContext();

            BatchInfo serverBatchInfo = null;
            try
            {
                var connection = this.Provider.CreateConnection();

                if (string.IsNullOrEmpty(this.Options.SnapshotsDirectory))
                    return (0, null);

                //Direction set to Download
                ctx.SyncWay = SyncWay.Download;

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Get Schema from remote provider if no schema passed from args
                if (schema == null)
                {
                    var serverScopeInfo = await this.EnsureSchemaAsync(cancellationToken, progress).ConfigureAwait(false);
                    schema = serverScopeInfo.Schema;
                }

                // When we get the changes from server, we create the batches if it's requested by the client
                // the batch decision comes from batchsize from client
                // TODO : Get a snapshot based on scope name

                var sb = new StringBuilder();
                var underscore = "";

                if (ctx.Parameters != null)
                {
                    foreach (var p in ctx.Parameters.OrderBy(p => p.Name))
                    {
                        var cleanValue = new string(p.Value.ToString().Where(char.IsLetterOrDigit).ToArray());
                        var cleanName = new string(p.Name.Where(char.IsLetterOrDigit).ToArray());

                        sb.Append($"{underscore}{cleanName}_{cleanValue}");
                        underscore = "_";
                    }
                }

                var directoryName = sb.ToString();

                directoryName = string.IsNullOrEmpty(directoryName) ? "ALL" : directoryName;

                this.logger.LogDebug(SyncEventsId.GetSnapshot, new { DirectoryName = directoryName });

                // cleansing scope name
                var directoryScopeName = new string(ctx.ScopeName.Where(char.IsLetterOrDigit).ToArray());

                // Get full path
                var directoryFullPath = Path.Combine(this.Options.SnapshotsDirectory, directoryScopeName, directoryName);

                // if no snapshot present, just return null value.
                if (!Directory.Exists(directoryFullPath))
                {
                    this.logger.LogDebug(SyncEventsId.DirectoryNotExists, new { DirectoryPath = directoryFullPath });
                }
                else
                {

                    // Serialize on disk.
                    var jsonConverter = new Serialization.JsonConverter<BatchInfo>();

                    var summaryFileName = Path.Combine(directoryFullPath, "summary.json");

                    // Create the schema changeset
                    var changesSet = new SyncSet();

                    // Create a Schema set without readonly columns, attached to memory changes
                    foreach (var table in schema.Tables)
                        DbSyncAdapter.CreateChangesTable(schema.Tables[table.TableName, table.SchemaName], changesSet);

                    using (var fs = new FileStream(summaryFileName, FileMode.Open, FileAccess.Read))
                    {
                        this.logger.LogDebug(SyncEventsId.LoadSnapshotSummary, new { FileName = summaryFileName });
                        serverBatchInfo = await jsonConverter.DeserializeAsync(fs).ConfigureAwait(false);
                        this.logger.LogDebug(SyncEventsId.LoadSnapshotSummary, serverBatchInfo);
                    }

                    serverBatchInfo.SanitizedSchema = changesSet;
                }
            }
            catch (Exception ex)
            {
                RaiseError(ex);
            }

            if (serverBatchInfo == null)
                return (0, null);

            return (serverBatchInfo.Timestamp, serverBatchInfo);
        }


        /// <summary>
        /// update configuration object with tables desc from server database
        /// </summary>
        internal virtual async Task<BatchInfo> InternalCreateSnapshotAsync(SyncContext context, SyncSet schema, SyncSetup setup,
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
                // TODO : supply command
                await this.InterceptAsync(new TableChangesSelectingArgs(context, table, null, connection, transaction), cancellationToken).ConfigureAwait(false);

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
                    var changesSetTable = DbSyncAdapter.CreateChangesTable(schema.Tables[table.TableName, table.SchemaName], changesSet);

                    while (dataReader.Read())
                    {
                        // Create a row from dataReader
                        var row = this.CreateSyncRowFromReader(dataReader, changesSetTable);

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

                        changesSetTable = DbSyncAdapter.CreateChangesTable(schema.Tables[table.TableName, table.SchemaName], changesSet);

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

            this.logger.LogInformation(SyncEventsId.CreateSnapshot, batchInfo);

            return batchInfo;
        }

    }
}
