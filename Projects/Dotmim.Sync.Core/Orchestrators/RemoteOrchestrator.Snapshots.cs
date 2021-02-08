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
        public virtual Task<BatchInfo> CreateSnapshotAsync(SyncParameters syncParameters = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
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
            schema = await InternalProvisionAsync(ctx, false, schema, provision, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            // 3) Getting the most accurate timestamp
            var remoteClientTimestamp = await this.InternalGetLocalTimestampAsync(ctx, connection, transaction, cancellationToken, progress);

            await this.InterceptAsync(new SnapshotCreatingArgs(ctx, schema, this.Options.SnapshotsDirectory, this.Options.BatchSize, remoteClientTimestamp, connection, transaction), cancellationToken).ConfigureAwait(false);

            // 4) Create the snapshot
            var batchInfo = await this.InternalCreateSnapshotAsync(ctx, schema, connection, transaction, remoteClientTimestamp, cancellationToken, progress).ConfigureAwait(false);

            var snapshotCreated = new SnapshotCreatedArgs(ctx, batchInfo, connection);
            await this.InterceptAsync(snapshotCreated, cancellationToken).ConfigureAwait(false);
            this.ReportProgress(ctx, progress, snapshotCreated);


            return batchInfo;
        }, connection, transaction, cancellationToken);


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
                if (string.IsNullOrEmpty(this.Options.SnapshotsDirectory))
                    return (0, null);

                //Direction set to Download
                ctx.SyncWay = SyncWay.Download;

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Get Schema from remote provider if no schema passed from args
                if (schema == null)
                {
                    var serverScopeInfo = await this.EnsureSchemaAsync(default, default, cancellationToken, progress).ConfigureAwait(false);
                    schema = serverScopeInfo.Schema;
                }

                // When we get the changes from server, we create the batches if it's requested by the client
                // the batch decision comes from batchsize from client
                // TODO : Get a snapshot based on scope name

                var (rootDirectory, nameDirectory) = await this.InternalGetSnapshotDirectoryAsync(ctx, cancellationToken, progress).ConfigureAwait(false);

                if (!string.IsNullOrEmpty(rootDirectory))
                {
                    var directoryFullPath = Path.Combine(rootDirectory, nameDirectory);

                    // if no snapshot present, just return null value.
                    if (Directory.Exists(directoryFullPath))
                    {
                        // Serialize on disk.
                        var jsonConverter = new JsonConverter<BatchInfo>();

                        var summaryFileName = Path.Combine(directoryFullPath, "summary.json");

                        // Create the schema changeset
                        var changesSet = new SyncSet();

                        // Create a Schema set without readonly columns, attached to memory changes
                        foreach (var table in schema.Tables)
                            DbSyncAdapter.CreateChangesTable(schema.Tables[table.TableName, table.SchemaName], changesSet);

                        using (var fs = new FileStream(summaryFileName, FileMode.Open, FileAccess.Read))
                        {
                            serverBatchInfo = await jsonConverter.DeserializeAsync(fs).ConfigureAwait(false);
                        }

                        serverBatchInfo.SanitizedSchema = changesSet;
                    }
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
        internal virtual async Task<BatchInfo> InternalCreateSnapshotAsync(SyncContext context, SyncSet schema,
                             DbConnection connection, DbTransaction transaction, long remoteClientTimestamp,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {


            // Call interceptor
            await this.InterceptAsync(new DatabaseChangesSelectingArgs(context, null, connection, transaction), cancellationToken).ConfigureAwait(false);

            // create local directory
            if (!Directory.Exists(this.Options.SnapshotsDirectory))
            {
                Directory.CreateDirectory(this.Options.SnapshotsDirectory);
            }

            var (rootDirectory, nameDirectory) = await this.InternalGetSnapshotDirectoryAsync(context, cancellationToken, progress).ConfigureAwait(false);

            if (string.IsNullOrEmpty(rootDirectory))
                return null;

            // create local directory with scope inside
            if (!Directory.Exists(rootDirectory))
                Directory.CreateDirectory(rootDirectory);

            // numbers of batch files generated
            var batchIndex = 0;

            // create the in memory changes set
            var changesSet = new SyncSet();


            // batchinfo generate a schema clone with scope columns if needed
            var batchInfo = new BatchInfo(false, schema, rootDirectory, nameDirectory);

            // Delete directory if already exists
            var directoryFullPath = Path.Combine(rootDirectory, nameDirectory);

            if (Directory.Exists(directoryFullPath))
                Directory.Delete(directoryFullPath, true);

            // Create stats object to store changes count
            var changes = new DatabaseChangesSelected();

            foreach (var table in schema.Tables)
            {
                // Get Select initialize changes command
                var selectIncrementalChangesCommand = await this.GetSelectChangesCommandAsync(context, table, this.Setup, true, connection, transaction);

                // Set parameters
                this.SetSelectChangesCommonParameters(context, table, null, true, null, selectIncrementalChangesCommand);

                // launch interceptor if any
                var args = new TableChangesSelectingArgs(context, table, selectIncrementalChangesCommand, connection, transaction);
                await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);

                if (!args.Cancel && args.Command != null)
                {
                    // Statistics
                    var tableChangesSelected = new TableChangesSelected(table.TableName, table.SchemaName);

                    // Get the reader
                    using var dataReader = await selectIncrementalChangesCommand.ExecuteReaderAsync().ConfigureAwait(false);

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

                        // Set the correct state to be applied
                        if (row.RowState == DataRowState.Deleted)
                            tableChangesSelected.Deletes++;
                        else if (row.RowState == DataRowState.Modified)
                            tableChangesSelected.Upserts++;

                        var fieldsSize = ContainerTable.GetRowSizeFromDataRow(row.ToArray());
                        var finalFieldSize = fieldsSize / 1024d;

                        if (finalFieldSize > this.Options.BatchSize)
                            throw new RowOverSizedException(finalFieldSize.ToString());

                        // Calculate the new memory size
                        rowsMemorySize += finalFieldSize;

                        // Next line if we don't reach the batch size yet.
                        if (rowsMemorySize <= this.Options.BatchSize)
                            continue;

                        // Check interceptor
                        var batchTableChangesSelectedArgs = new TableChangesSelectedArgs(context, changesSetTable, tableChangesSelected, connection, transaction);
                        await this.InterceptAsync(batchTableChangesSelectedArgs, cancellationToken).ConfigureAwait(false);

                        // add changes to batchinfo
                        await batchInfo.AddChangesAsync(changesSet, batchIndex, false, this).ConfigureAwait(false);

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

                    dataReader.Close();

                    // We don't report progress if no table changes is empty, to limit verbosity
                    if (tableChangesSelected.Deletes > 0 || tableChangesSelected.Upserts > 0)
                        changes.TableChangesSelected.Add(tableChangesSelected);

                    // even if no rows raise the interceptor
                    var tableChangesSelectedArgs = new TableChangesSelectedArgs(context, changesSetTable, tableChangesSelected, connection, transaction);
                    await this.InterceptAsync(tableChangesSelectedArgs, cancellationToken).ConfigureAwait(false);

                    // only raise report progress if we have something
                    if (tableChangesSelectedArgs.TableChangesSelected.TotalChanges > 0)
                        this.ReportProgress(context, progress, tableChangesSelectedArgs);

                }
            }

            if (changesSet != null && changesSet.HasTables)
            {
                await batchInfo.AddChangesAsync(changesSet, batchIndex, true, this).ConfigureAwait(false);
            }

            //Set the total rows count contained in the batch info
            batchInfo.RowsCount = changes.TotalChangesSelected;

            // Check the last index as the last batch
            batchInfo.EnsureLastBatch();

            batchInfo.Timestamp = remoteClientTimestamp;


            // Serialize on disk.
            var jsonConverter = new JsonConverter<BatchInfo>();

            var summaryFileName = Path.Combine(directoryFullPath, "summary.json");

            using (var f = new FileStream(summaryFileName, FileMode.CreateNew, FileAccess.ReadWrite))
            {
                var bytes = await jsonConverter.SerializeAsync(batchInfo).ConfigureAwait(false);
                f.Write(bytes, 0, bytes.Length);
            }

            // Raise database changes selected
            if (changes.TotalChangesSelected > 0 || changes.TotalChangesSelectedDeletes > 0 || changes.TotalChangesSelectedUpdates > 0)
            {
                // Raise database changes selected
                var databaseChangesSelectedArgs = new DatabaseChangesSelectedArgs(context, remoteClientTimestamp, batchInfo, changes, connection);
                this.ReportProgress(context, progress, databaseChangesSelectedArgs);
                await this.InterceptAsync(databaseChangesSelectedArgs, cancellationToken).ConfigureAwait(false);
            }
            return batchInfo;
        }

    }
}
