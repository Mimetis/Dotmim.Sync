using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
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
        public virtual async Task<BatchInfo> CreateSnapshotAsync(SyncParameters syncParameters = null,
            ILocalSerializer localSerializer = default, DbConnection connection = default, DbTransaction transaction = default,
            CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            await using var runner = await this.GetConnectionAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
            var ctx = this.GetContext();
            ctx.SyncStage = SyncStage.SnapshotCreating;
            try
            {
                if (string.IsNullOrEmpty(this.Options.SnapshotsDirectory) || this.Options.BatchSize <= 0)
                    throw new SnapshotMissingMandatariesOptionsException();

                // check parameters
                // If context has no parameters specified, and user specifies a parameter collection we switch them
                if ((this.syncContext.Parameters == null || this.syncContext.Parameters.Count <= 0) && syncParameters != null && syncParameters.Count > 0)
                    this.syncContext.Parameters = syncParameters;

                // 1) Get Schema from remote provider
                var schema = await this.InternalGetSchemaAsync(this.syncContext, this.Setup, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // 2) Ensure databases are ready
                //    Even if we are using only stored procedures, we need tracking tables and triggers
                //    for tracking the rows inserted / updated after the snapshot
                var provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

                // 3) Provision everything
                var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                var exists = await this.InternalExistsScopeInfoTableAsync(this.syncContext, DbScopeType.Server, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(this.syncContext, DbScopeType.Server, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                var serverScopeInfo = await this.InternalGetScopeAsync<ServerScopeInfo>(this.syncContext, DbScopeType.Server, this.ScopeName, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                schema = await InternalProvisionAsync(this.syncContext, false, schema, this.Setup, provision, serverScopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // 4) Getting the most accurate timestamp
                var remoteClientTimestamp = await this.InternalGetLocalTimestampAsync(this.syncContext, runner.Connection, runner.Transaction, cancellationToken, progress);

                await runner.CommitAsync();

                await this.InterceptAsync(new SnapshotCreatingArgs(this.GetContext(), schema, this.Options.SnapshotsDirectory, this.Options.BatchSize, remoteClientTimestamp, runner.Connection, runner.Transaction), cancellationToken).ConfigureAwait(false);

                // 5) Create the snapshot with
                localSerializer = localSerializer == null ? new LocalJsonSerializer() : localSerializer;

                var batchInfo = await this.InternalCreateSnapshotAsync(this.GetContext(), schema, this.Setup, localSerializer, remoteClientTimestamp, cancellationToken, progress).ConfigureAwait(false);

                var snapshotCreated = new SnapshotCreatedArgs(this.GetContext(), batchInfo, runner.Connection);
                await this.InterceptAsync(snapshotCreated, cancellationToken).ConfigureAwait(false);
                this.ReportProgress(this.GetContext(), progress, snapshotCreated);

                return batchInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(ex);
            }

        }


        /// <summary>
        /// Get a snapshot
        /// </summary>
        public virtual async Task<(long RemoteClientTimestamp, BatchInfo ServerBatchInfo, DatabaseChangesSelected DatabaseChangesSelected)>
            GetSnapshotAsync(SyncSet schema = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            // Get context or create a new one
            var ctx = this.GetContext();
            var changesSelected = new DatabaseChangesSelected();

            BatchInfo serverBatchInfo = null;
            try
            {
                if (string.IsNullOrEmpty(this.Options.SnapshotsDirectory))
                    return (0, null, changesSelected);

                //Direction set to Download
                ctx.SyncWay = SyncWay.Download;

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Get Schema from remote provider if no schema passed from args
                if (schema == null)
                {
                    var serverScopeInfo = await this.EnsureSchemaAsync(connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    schema = serverScopeInfo.Schema;
                }

                // When we get the changes from server, we create the batches if it's requested by the client
                // the batch decision comes from batchsize from client
                var (rootDirectory, nameDirectory) = await this.InternalGetSnapshotDirectoryAsync(ctx, cancellationToken, progress).ConfigureAwait(false);

                if (!string.IsNullOrEmpty(rootDirectory))
                {
                    var directoryFullPath = Path.Combine(rootDirectory, nameDirectory);

                    // if no snapshot present, just return null value.
                    if (Directory.Exists(directoryFullPath))
                    {
                        // Serialize on disk.
                        var jsonConverter = new Serialization.JsonConverter<BatchInfo>();

                        var summaryFileName = Path.Combine(directoryFullPath, "summary.json");

                        using (var fs = new FileStream(summaryFileName, FileMode.Open, FileAccess.Read))
                        {
                            serverBatchInfo = await jsonConverter.DeserializeAsync(fs).ConfigureAwait(false);
                        }

                        // Create the schema changeset
                        var changesSet = new SyncSet();

                        // Create a Schema set without readonly columns, attached to memory changes
                        foreach (var table in schema.Tables)
                        {
                            DbSyncAdapter.CreateChangesTable(schema.Tables[table.TableName, table.SchemaName], changesSet);

                            // Get all stats about this table
                            var bptis = serverBatchInfo.BatchPartsInfo.SelectMany(bpi => bpi.Tables.Where(t =>
                            {
                                var sc = SyncGlobalization.DataSourceStringComparison;

                                var sn = t.SchemaName == null ? string.Empty : t.SchemaName;
                                var otherSn = table.SchemaName == null ? string.Empty : table.SchemaName;

                                return table.TableName.Equals(t.TableName, sc) && sn.Equals(otherSn, sc);

                            }));

                            if (bptis != null)
                            {
                                // Statistics
                                var tableChangesSelected = new TableChangesSelected(table.TableName, table.SchemaName)
                                {
                                    // we are applying a snapshot where it can't have any deletes, obviously
                                    Upserts = bptis.Sum(bpti => bpti.RowsCount)
                                };

                                if (tableChangesSelected.Upserts > 0)
                                    changesSelected.TableChangesSelected.Add(tableChangesSelected);
                            }


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
                return (0, null, changesSelected);

            return (serverBatchInfo.Timestamp, serverBatchInfo, changesSelected);
        }



        internal virtual async Task<BatchInfo> InternalCreateSnapshotAsync(SyncContext context, SyncSet schema, SyncSetup setup,
                ILocalSerializer serializer, long remoteClientTimestamp,
                CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            // Call interceptor
            await this.InterceptAsync(new DatabaseChangesSelectingArgs(context, null, null, null), cancellationToken).ConfigureAwait(false);

            // create local directory
            if (!Directory.Exists(this.Options.SnapshotsDirectory))
                Directory.CreateDirectory(this.Options.SnapshotsDirectory);

            var (rootDirectory, nameDirectory) = await this.InternalGetSnapshotDirectoryAsync(context, cancellationToken, progress).ConfigureAwait(false);

            if (string.IsNullOrEmpty(rootDirectory))
                return null;

            // create local directory with scope inside
            if (!Directory.Exists(rootDirectory))
                Directory.CreateDirectory(rootDirectory);

            // Delete directory if already exists
            var directoryFullPath = Path.Combine(rootDirectory, nameDirectory);

            if (Directory.Exists(directoryFullPath))
                Directory.Delete(directoryFullPath, true);

            Directory.CreateDirectory(directoryFullPath);

            // Create stats object to store changes count
            var changes = new DatabaseChangesSelected();
            var batchInfo = new BatchInfo(schema, rootDirectory, nameDirectory);

            //await schema.Tables.ForEachAsync(async table =>
            var schemaTables = schema.Tables.SortByDependencies(tab => tab.GetRelations().Select(r => r.GetParentTable()));

            await schemaTables.ForEachAsync(async table =>
            {
                var batchIndex = 0;

                // Get Select initialize changes command
                await using var runner = await this.GetConnectionAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                try
                {
                    var selectIncrementalChangesCommand = await this.GetSelectChangesCommandAsync(context, table, setup, true, runner.Connection, runner.Transaction).ConfigureAwait(false);

                    if (selectIncrementalChangesCommand == null)
                        return;

                    // generate a new path
                    var (fullPath, fileName) = batchInfo.GetNewBatchPartInfoPath(table, batchIndex, serializer.Extension);

                    var schemaChangesTable = DbSyncAdapter.CreateChangesTable(table);

                    // open the file and write table header
                    await serializer.OpenFileAsync(fullPath, schemaChangesTable);

                    // Statistics
                    var tableChangesSelected = new TableChangesSelected(table.TableName, table.SchemaName);

                    var rowsCountInBatch = 0;
                    // We are going to batch select, if needed by the provider

                    // Set parameters
                    this.SetSelectChangesCommonParameters(context, table, null, true, null, selectIncrementalChangesCommand);

                    // launch interceptor if any
                    var args = new TableChangesSelectingArgs(context, table, selectIncrementalChangesCommand, runner.Connection, runner.Transaction);
                    await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);

                    if (!args.Cancel && args.Command != null)
                    {
                        // Get the reader
                        using var dataReader = await selectIncrementalChangesCommand.ExecuteReaderAsync().ConfigureAwait(false);

                        while (await dataReader.ReadAsync())
                        {
                            // Create a row from dataReader
                            var syncRow = this.CreateSyncRowFromReader2(dataReader, schemaChangesTable);
                            rowsCountInBatch++;

                            // Set the correct state to be applied
                            if (syncRow.RowState == DataRowState.Deleted)
                                tableChangesSelected.Deletes++;
                            else
                                tableChangesSelected.Upserts++;

                            await serializer.WriteRowToFileAsync(syncRow, schemaChangesTable);

                            var currentBatchSize = await serializer.GetCurrentFileSizeAsync();

                            // Next line if we don't reach the batch size yet.
                            if (currentBatchSize <= this.Options.BatchSize)
                                continue;

                            var bpi = new BatchPartInfo { FileName = fileName };

                            // Create the info on the batch part
                            BatchPartTableInfo tableInfo = new BatchPartTableInfo
                            {
                                TableName = tableChangesSelected.TableName,
                                SchemaName = tableChangesSelected.SchemaName,
                                RowsCount = rowsCountInBatch

                            };

                            bpi.Tables = new BatchPartTableInfo[] { tableInfo };
                            bpi.RowsCount = rowsCountInBatch;
                            bpi.IsLastBatch = false;
                            bpi.Index = batchIndex;
                            batchInfo.RowsCount += rowsCountInBatch;
                            batchInfo.BatchPartsInfo.Add(bpi);

                            // Close file
                            await serializer.CloseFileAsync(fullPath, schemaChangesTable);

                            // increment batch index
                            batchIndex++;
                            // Reinit rowscount in batch
                            rowsCountInBatch = 0;

                            // generate a new path
                            (fullPath, fileName) = batchInfo.GetNewBatchPartInfoPath(schemaChangesTable, batchIndex, serializer.Extension);

                            // open a new file and write table header
                            await serializer.OpenFileAsync(fullPath, schemaChangesTable);

                            // Raise progress
                            var tmpTableChangesSelectedArgs = new TableChangesSelectedArgs(context, null, tableChangesSelected, runner.Connection, runner.Transaction);
                            await this.InterceptAsync(tmpTableChangesSelectedArgs, cancellationToken).ConfigureAwait(false);

                            // only raise report progress if we have something
                            if (tmpTableChangesSelectedArgs.TableChangesSelected.TotalChanges > 0)
                                this.ReportProgress(context, progress, tmpTableChangesSelectedArgs);

                        }

                        dataReader.Close();

                    }

                    var bpi2 = new BatchPartInfo { FileName = fileName };

                    // Create the info on the batch part
                    BatchPartTableInfo tableInfo2 = new BatchPartTableInfo
                    {
                        TableName = tableChangesSelected.TableName,
                        SchemaName = tableChangesSelected.SchemaName,
                        RowsCount = rowsCountInBatch
                    };
                    bpi2.Tables = new BatchPartTableInfo[] { tableInfo2 };
                    bpi2.RowsCount = rowsCountInBatch;
                    bpi2.IsLastBatch = true;
                    bpi2.Index = batchIndex;
                    batchInfo.RowsCount += rowsCountInBatch;
                    batchInfo.BatchPartsInfo.Add(bpi2);
                    batchIndex++;

                    // Close file
                    await serializer.CloseFileAsync(fullPath, schemaChangesTable);

                    // Raise progress
                    var tableChangesSelectedArgs = new TableChangesSelectedArgs(context, null, tableChangesSelected, runner.Connection, runner.Transaction);
                    await this.InterceptAsync(tableChangesSelectedArgs, cancellationToken).ConfigureAwait(false);

                    changes.TableChangesSelected.Add(tableChangesSelected);

                    // only raise report progress if we have something
                    if (tableChangesSelectedArgs.TableChangesSelected.TotalChanges > 0)
                        this.ReportProgress(context, progress, tableChangesSelectedArgs);
                }
                catch (Exception ex)
                {
                    throw GetSyncError(ex);
                }
            }, 1);

            // Check the last index as the last batch
            batchInfo.EnsureLastBatch();
            batchInfo.Timestamp = remoteClientTimestamp;

            // delete all empty batchparts (empty tables)
            foreach (var bpi in batchInfo.BatchPartsInfo.ToArray())
            {
                if (bpi.RowsCount <= 0)
                {
                    var fullPathToDelete = Path.Combine(directoryFullPath, bpi.FileName);
                    File.Delete(fullPathToDelete);
                    batchInfo.BatchPartsInfo.Remove(bpi);
                }
            }

            // Generate a good index order to be compliant with previous versions
            var lstBatchPartInfos = new List<BatchPartInfo>();
            foreach (var table in schemaTables)
            {
                foreach (var bpi in batchInfo.BatchPartsInfo.Where(
                    bpi => bpi.Tables[0].EqualsByName(
                        new BatchPartTableInfo(table.TableName, table.SchemaName))).OrderBy(bpi => bpi.Index).ToArray())
                {
                    lstBatchPartInfos.Add(bpi);
                }
            }
            var newBatchIndex = 0;
            foreach (var bpi in lstBatchPartInfos)
            {
                bpi.Index = newBatchIndex;
                newBatchIndex++;
                bpi.IsLastBatch = newBatchIndex == lstBatchPartInfos.Count;
            }

            // Serialize on disk.
            var jsonConverter = new Serialization.JsonConverter<BatchInfo>();

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
                var c = this.Provider.CreateConnection();
                var databaseChangesSelectedArgs = new DatabaseChangesSelectedArgs(context, remoteClientTimestamp, batchInfo, changes, c);
                this.ReportProgress(context, progress, databaseChangesSelectedArgs);
                await this.InterceptAsync(databaseChangesSelectedArgs, cancellationToken).ConfigureAwait(false);
            }
            return batchInfo;
        }
    }
}
