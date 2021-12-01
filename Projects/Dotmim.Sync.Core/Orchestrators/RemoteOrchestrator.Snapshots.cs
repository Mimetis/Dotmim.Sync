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
        public virtual Task<BatchInfo> CreateSnapshotAsync(SyncParameters syncParameters = null,
            ISerializerFactory serializerFactory = default,
            DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.SnapshotCreating, async (ctx, connection, transaction) =>
        {
            if (string.IsNullOrEmpty(this.Options.SnapshotsDirectory) || this.Options.BatchSize <= 0)
                throw new SnapshotMissingMandatariesOptionsException();

            // Default serialization to json
            if (serializerFactory == default)
                serializerFactory = SerializersCollection.JsonSerializer;

            // check parameters
            // If context has no parameters specified, and user specifies a parameter collection we switch them
            if ((ctx.Parameters == null || ctx.Parameters.Count <= 0) && syncParameters != null && syncParameters.Count > 0)
                ctx.Parameters = syncParameters;

            // 1) Get Schema from remote provider
            var schema = await this.InternalGetSchemaAsync(ctx, this.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            // 2) Ensure databases are ready
            var provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

            // 3) Provision everything
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.Server, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (!exists)
                await this.InternalCreateScopeInfoTableAsync(ctx, DbScopeType.Server, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            var serverScopeInfo = await this.InternalGetScopeAsync<ServerScopeInfo>(ctx, DbScopeType.Server, this.ScopeName, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            schema = await InternalProvisionAsync(ctx, false, schema, this.Setup, provision, serverScopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            // 4) Getting the most accurate timestamp
            var remoteClientTimestamp = await this.InternalGetLocalTimestampAsync(ctx, connection, transaction, cancellationToken, progress);

            await this.InterceptAsync(new SnapshotCreatingArgs(ctx, schema, this.Options.SnapshotsDirectory, this.Options.BatchSize, remoteClientTimestamp, connection, transaction), cancellationToken).ConfigureAwait(false);

            // 5) Create the snapshot
            var batchInfo = await this.InternalCreateSnapshotAsync(ctx, schema, this.Setup, this.Options.SerializerFactory, connection, transaction, remoteClientTimestamp, cancellationToken, progress).ConfigureAwait(false);

            var snapshotCreated = new SnapshotCreatedArgs(ctx, batchInfo, connection);
            await this.InterceptAsync(snapshotCreated, cancellationToken).ConfigureAwait(false);
            this.ReportProgress(ctx, progress, snapshotCreated);


            return batchInfo;
        }, connection, transaction, cancellationToken);


        /// <summary>
        /// Get a snapshot
        /// </summary>
        public virtual async Task<(long RemoteClientTimestamp, BatchInfo ServerBatchInfo, DatabaseChangesSelected DatabaseChangesSelected)>
            GetSnapshotAsync(SyncSet schema = null, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
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
                    var serverScopeInfo = await this.EnsureSchemaAsync(default, default, cancellationToken, progress).ConfigureAwait(false);
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



        ///// <summary>
        ///// update configuration object with tables desc from server database
        ///// </summary>
        //internal virtual async Task<BatchInfo> InternalCreateSnapshotAsync1(SyncContext context, SyncSet schema, SyncSetup setup, ISerializerFactory serializerFactory,
        //                     DbConnection connection, DbTransaction transaction, long remoteClientTimestamp,
        //                     CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        //{


        //    // Call interceptor
        //    await this.InterceptAsync(new DatabaseChangesSelectingArgs(context, null, connection, transaction), cancellationToken).ConfigureAwait(false);

        //    // create local directory
        //    if (!Directory.Exists(this.Options.SnapshotsDirectory))
        //    {
        //        Directory.CreateDirectory(this.Options.SnapshotsDirectory);
        //    }

        //    var (rootDirectory, nameDirectory) = await this.InternalGetSnapshotDirectoryAsync(context, cancellationToken, progress).ConfigureAwait(false);

        //    if (string.IsNullOrEmpty(rootDirectory))
        //        return null;

        //    // create local directory with scope inside
        //    if (!Directory.Exists(rootDirectory))
        //        Directory.CreateDirectory(rootDirectory);

        //    // numbers of batch files generated
        //    var batchIndex = 0;

        //    // create the in memory changes set
        //    var changesSet = new SyncSet();

        //    // batchinfo generate a schema clone with scope columns if needed
        //    var batchInfo = new BatchInfo(schema, rootDirectory, nameDirectory);

        //    // Delete directory if already exists
        //    var directoryFullPath = Path.Combine(rootDirectory, nameDirectory);

        //    if (Directory.Exists(directoryFullPath))
        //        Directory.Delete(directoryFullPath, true);

        //    // Create stats object to store changes count
        //    var changes = new DatabaseChangesSelected();

        //    foreach (var table in schema.Tables)
        //    {
        //        // Get Select initialize changes command
        //        var selectIncrementalChangesCommand = await this.GetSelectChangesCommandAsync(context, table, setup, true, connection, transaction);

        //        if (selectIncrementalChangesCommand == null) continue;

        //        // Set parameters
        //        this.SetSelectChangesCommonParameters(context, table, null, true, null, selectIncrementalChangesCommand);

        //        // launch interceptor if any
        //        var args = new TableChangesSelectingArgs(context, table, selectIncrementalChangesCommand, connection, transaction);
        //        await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);

        //        if (!args.Cancel && args.Command != null)
        //        {
        //            // Statistics
        //            var tableChangesSelected = new TableChangesSelected(table.TableName, table.SchemaName);

        //            // Get the reader
        //            using var dataReader = await selectIncrementalChangesCommand.ExecuteReaderAsync().ConfigureAwait(false);

        //            // memory size total
        //            double rowsMemorySize = 0L;

        //            // Create a chnages table with scope columns
        //            var changesSetTable = DbSyncAdapter.CreateChangesTable(schema.Tables[table.TableName, table.SchemaName], changesSet);

        //            while (dataReader.Read())
        //            {
        //                // Create a row from dataReader
        //                var row = this.CreateSyncRowFromReader(dataReader, changesSetTable);

        //                // Add the row to the changes set
        //                changesSetTable.Rows.Add(row);

        //                // Set the correct state to be applied
        //                if (row.RowState == DataRowState.Deleted)
        //                    tableChangesSelected.Deletes++;
        //                else if (row.RowState == DataRowState.Modified)
        //                    tableChangesSelected.Upserts++;

        //                var fieldsSize = ContainerTable.GetRowSizeFromDataRow(row.ToArray());
        //                var finalFieldSize = fieldsSize / 1024d;

        //                if (finalFieldSize > this.Options.BatchSize)
        //                    throw new RowOverSizedException(finalFieldSize.ToString());

        //                // Calculate the new memory size
        //                rowsMemorySize += finalFieldSize;

        //                // Next line if we don't reach the batch size yet.
        //                if (rowsMemorySize <= this.Options.BatchSize)
        //                    continue;

        //                // Check interceptor
        //                var batchTableChangesSelectedArgs = new TableChangesSelectedArgs(context, changesSetTable, tableChangesSelected, connection, transaction);
        //                await this.InterceptAsync(batchTableChangesSelectedArgs, cancellationToken).ConfigureAwait(false);

        //                // add changes to batchinfo
        //                await batchInfo.AddChangesAsync(changesSet, batchIndex, false, serializerFactory, this).ConfigureAwait(false);

        //                // increment batch index
        //                batchIndex++;

        //                // we know the datas are serialized here, so we can flush  the set
        //                changesSet.Dispose();
        //                changesSetTable.Dispose();

        //                // Recreate an empty ContainerSet and a ContainerTable
        //                changesSet = new SyncSet();

        //                changesSetTable = DbSyncAdapter.CreateChangesTable(schema.Tables[table.TableName, table.SchemaName], changesSet);

        //                // Init the row memory size
        //                rowsMemorySize = 0L;

        //                GC.Collect();
        //            }

        //            dataReader.Close();
        //            GC.Collect();

        //            // We don't report progress if no table changes is empty, to limit verbosity
        //            if (tableChangesSelected.Deletes > 0 || tableChangesSelected.Upserts > 0)
        //                changes.TableChangesSelected.Add(tableChangesSelected);

        //            // even if no rows raise the interceptor
        //            var tableChangesSelectedArgs = new TableChangesSelectedArgs(context, changesSetTable, tableChangesSelected, connection, transaction);
        //            await this.InterceptAsync(tableChangesSelectedArgs, cancellationToken).ConfigureAwait(false);

        //            // only raise report progress if we have something
        //            if (tableChangesSelectedArgs.TableChangesSelected.TotalChanges > 0)
        //                this.ReportProgress(context, progress, tableChangesSelectedArgs);

        //        }
        //    }

        //    if (changesSet != null && changesSet.HasTables)
        //    {
        //        await batchInfo.AddChangesAsync(changesSet, batchIndex, true, serializerFactory, this).ConfigureAwait(false);
        //    }

        //    //Set the total rows count contained in the batch info
        //    batchInfo.RowsCount = changes.TotalChangesSelected;

        //    // Check the last index as the last batch
        //    batchInfo.EnsureLastBatch();

        //    batchInfo.Timestamp = remoteClientTimestamp;


        //    // Serialize on disk.
        //    var jsonConverter = new Serialization.JsonConverter<BatchInfo>();

        //    var summaryFileName = Path.Combine(directoryFullPath, "summary.json");

        //    using (var f = new FileStream(summaryFileName, FileMode.CreateNew, FileAccess.ReadWrite))
        //    {
        //        var bytes = await jsonConverter.SerializeAsync(batchInfo).ConfigureAwait(false);
        //        f.Write(bytes, 0, bytes.Length);
        //    }

        //    // Raise database changes selected
        //    if (changes.TotalChangesSelected > 0 || changes.TotalChangesSelectedDeletes > 0 || changes.TotalChangesSelectedUpdates > 0)
        //    {
        //        // Raise database changes selected
        //        var databaseChangesSelectedArgs = new DatabaseChangesSelectedArgs(context, remoteClientTimestamp, batchInfo, changes, connection);
        //        this.ReportProgress(context, progress, databaseChangesSelectedArgs);
        //        await this.InterceptAsync(databaseChangesSelectedArgs, cancellationToken).ConfigureAwait(false);
        //    }
        //    return batchInfo;
        //}

        //internal virtual async Task<BatchInfo> InternalCreateSnapshotAsync2(SyncContext context, SyncSet schema, SyncSetup setup, ISerializerFactory serializerFactory,
        //     DbConnection connection, DbTransaction transaction, long remoteClientTimestamp,
        //     CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        //{
        //    // Call interceptor
        //    await this.InterceptAsync(new DatabaseChangesSelectingArgs(context, null, connection, transaction), cancellationToken).ConfigureAwait(false);

        //    // create local directory
        //    if (!Directory.Exists(this.Options.SnapshotsDirectory))
        //    {
        //        Directory.CreateDirectory(this.Options.SnapshotsDirectory);
        //    }

        //    var (rootDirectory, nameDirectory) = await this.InternalGetSnapshotDirectoryAsync(context, cancellationToken, progress).ConfigureAwait(false);

        //    if (string.IsNullOrEmpty(rootDirectory))
        //        return null;

        //    // create local directory with scope inside
        //    if (!Directory.Exists(rootDirectory))
        //        Directory.CreateDirectory(rootDirectory);

        //    // batchinfo generate a schema clone with scope columns if needed
        //    var batchInfo = new BatchInfo(schema, rootDirectory, nameDirectory);

        //    // Delete directory if already exists
        //    var directoryFullPath = Path.Combine(rootDirectory, nameDirectory);

        //    if (Directory.Exists(directoryFullPath))
        //        Directory.Delete(directoryFullPath, true);

        //    Directory.CreateDirectory(directoryFullPath);

        //    var taskRunners = new List<Task>();

        //    for (var tableIndex = 0; tableIndex < schema.Tables.Count; tableIndex++)
        //    {
        //        var table = schema.Tables[tableIndex];

        //        var tableName = ParserName.Parse(table);
        //        var fileName = $"{tableName.Unquoted().Normalized()}_{Path.GetRandomFileName().Replace(".", "_")}.json";
        //        var fullPath = Path.Combine(directoryFullPath, fileName);


        //        // Get Select initialize changes command
        //        var selectIncrementalChangesCommand = await this.GetSelectChangesCommandAsync(context, table, setup, true, connection,transaction);

        //        if (selectIncrementalChangesCommand != null)
        //        {
        //            // Set parameters
        //            this.SetSelectChangesCommonParameters(context, table, null, true, null, selectIncrementalChangesCommand);

        //            // launch interceptor if any
        //            var args = new TableChangesSelectingArgs(context, table, selectIncrementalChangesCommand, dbConnection, dbTransaction);
        //            await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);

        //            // Statistics
        //            var tableChangesSelected = new TableChangesSelected(table.TableName, table.SchemaName);

        //            if (!args.Cancel && args.Command != null)
        //            {
        //                using StreamWriter sw = new StreamWriter(fullPath);
        //                using JsonTextWriter writer = new JsonTextWriter(sw);
        //                writer.WriteStartObject();
        //                writer.WritePropertyName("table");

        //                writer.WriteStartObject();
        //                writer.WritePropertyName("tableName");
        //                writer.WriteValue(table.TableName);
        //                writer.WritePropertyName("schemaName");
        //                writer.WriteValue(table.SchemaName);
        //                writer.WriteEndObject();
        //                writer.WriteWhitespace(Environment.NewLine);

        //                writer.WritePropertyName("rows");

        //                // Get the reader
        //                using var dataReader = await selectIncrementalChangesCommand.ExecuteReaderAsync().ConfigureAwait(false);
        //                writer.WriteStartArray();

        //                while (dataReader.Read())
        //                {
        //                    writer.WriteStartArray();
        //                    bool isTombstone = false;

        //                    for (var i = 0; i < dataReader.FieldCount; i++)
        //                    {
        //                        var columnName = dataReader.GetName(i);

        //                        // if we have the tombstone value, do not add it to the table
        //                        if (columnName == "sync_row_is_tombstone")
        //                        {
        //                            isTombstone = Convert.ToInt64(dataReader.GetValue(i)) > 0;
        //                            continue;
        //                        }
        //                        if (columnName == "sync_update_scope_id")
        //                            continue;

        //                        var columnValueObject = dataReader.GetValue(i);
        //                        var columnValue = columnValueObject == DBNull.Value ? null : columnValueObject;

        //                        if (columnValue != null)
        //                            writer.WriteValue(columnValue);
        //                    }

        //                    // Set the correct state to be applied
        //                    if (isTombstone)
        //                        tableChangesSelected.Deletes++;
        //                    else
        //                        tableChangesSelected.Upserts++;

        //                    writer.WriteEndArray();
        //                    writer.WriteWhitespace(Environment.NewLine);

        //                    // Check interceptor
        //                    var batchTableChangesSelectedArgs = new TableChangesSelectedArgs(context, null, tableChangesSelected, connection, transaction);
        //                    await this.InterceptAsync(batchTableChangesSelectedArgs, cancellationToken).ConfigureAwait(false);

        //                }

        //                dataReader.Close();

        //                writer.WriteEndArray();
        //                writer.WriteWhitespace(Environment.NewLine);

        //                writer.WriteEndObject();

        //                // even if no rows raise the interceptor
        //                var tableChangesSelectedArgs = new TableChangesSelectedArgs(context, null, tableChangesSelected, connection, transaction);
        //                await this.InterceptAsync(tableChangesSelectedArgs, cancellationToken).ConfigureAwait(false);

        //                // only raise report progress if we have something
        //                if (tableChangesSelectedArgs.TableChangesSelected.TotalChanges > 0)
        //                    this.ReportProgress(context, progress, tableChangesSelectedArgs);

        //            }
        //        }

        //    }
        //    await Task.WhenAll(taskRunners);
        //    return batchInfo;
        //}

        internal virtual async Task<BatchInfo> InternalCreateSnapshotAsync(SyncContext context, SyncSet schema, SyncSetup setup, ISerializerFactory serializerFactory,
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

            // create the in memory changes set
            var changesSet = new SyncSet();

            // Delete directory if already exists
            var directoryFullPath = Path.Combine(rootDirectory, nameDirectory);

            if (Directory.Exists(directoryFullPath))
                Directory.Delete(directoryFullPath, true);

            Directory.CreateDirectory(directoryFullPath);

            // Create stats object to store changes count
            var changes = new DatabaseChangesSelected();
            var batchInfo = new BatchInfo(schema, rootDirectory, nameDirectory);

            using (BlockingCollection<(int state, TableChangesSelected tableChanges, object[] row, JsonTextWriter Writer, StreamWriter StreamWriter, BatchInfo batchInfo, Exception ex)> bc
                = new BlockingCollection<(int state, TableChangesSelected tableChanges, object[] row, JsonTextWriter Writer, StreamWriter StreamWriter, BatchInfo batchInfo, Exception ex)>())
            {
                var producerTask = Task.Run(async () =>
                {
                    try
                    {
                        var batchIndex = 0;
                        //await schema.Tables.ForEachAsync(async table =>
                        var schemaTables = schema.Tables.SortByDependencies(tab => tab.GetRelations().Select(r => r.GetParentTable()));

                        foreach (var table in schemaTables)
                        {
                            // Get Select initialize changes command
                            var selectIncrementalChangesCommand = await this.GetSelectChangesCommandAsync(context, table, setup, true, connection, transaction);

                            if (selectIncrementalChangesCommand == null) return;

                            // Set parameters
                            this.SetSelectChangesCommonParameters(context, table, null, true, null, selectIncrementalChangesCommand);

                            // launch interceptor if any
                            var args = new TableChangesSelectingArgs(context, table, selectIncrementalChangesCommand, connection, transaction);
                            await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);

                            var tableName = ParserName.Parse(table).Unquoted().Normalized().ToString();
                            var fileName = BatchInfo.GenerateNewFileName2(tableName, "json");
                            var fullPath = Path.Combine(directoryFullPath, fileName);

                            var sw = new StreamWriter(fullPath);
                            var writer = new JsonTextWriter(sw) { CloseOutput = true };

                            // Statistics
                            var tableChangesSelected = new TableChangesSelected(table.TableName, table.SchemaName);

                            // memory size total
                            double rowsMemorySize = 0L;

                            if (!args.Cancel && args.Command != null)
                            {
                                // Get the reader
                                using var dataReader = await selectIncrementalChangesCommand.ExecuteReaderAsync().ConfigureAwait(false);

                                // Create a chnages table with scope columns
                                var changesSetTable = DbSyncAdapter.CreateChangesTable(schema.Tables[table.TableName, table.SchemaName], changesSet);

                                // Open file
                                bc.Add((0, tableChangesSelected, null, writer, sw, batchInfo, null));

                                var rowsCountInBatch = 0;
                                while (dataReader.Read())
                                {
                                    // Create a row from dataReader
                                    var row = this.CreateSyncRowFromReader2(dataReader, changesSetTable);
                                    rowsCountInBatch++;

                                    var fieldsSize = ContainerTable.GetRowSizeFromDataRow(row);

                                    var finalFieldSize = fieldsSize / 1024d;

                                    if (finalFieldSize > this.Options.BatchSize)
                                        throw new RowOverSizedException(finalFieldSize.ToString());

                                    // Set the correct state to be applied
                                    if ((int)row[0] == (int)DataRowState.Deleted)
                                        tableChangesSelected.Deletes++;
                                    else
                                        tableChangesSelected.Upserts++;

                                    bc.Add((1, tableChangesSelected, row, writer, sw, batchInfo, null));

                                    // Calculate the new memory size
                                    rowsMemorySize += finalFieldSize;

                                    // Next line if we don't reach the batch size yet.
                                    if (rowsMemorySize <= this.Options.BatchSize)
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
                                    bc.Add((2, tableChangesSelected, null, writer, sw, batchInfo, null));

                                    // Init the row memory size
                                    rowsMemorySize = 0L;

                                    fileName = BatchInfo.GenerateNewFileName2(tableName, "json");
                                    fullPath = Path.Combine(directoryFullPath, fileName);

                                    sw = new StreamWriter(fullPath);
                                    writer = new JsonTextWriter(sw) { CloseOutput = true };
                                    rowsCountInBatch = 0;
                                    batchIndex++;
                                    // Open file
                                    bc.Add((0, tableChangesSelected, null, writer, sw, batchInfo, null));

                                }

                                dataReader.Close();

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
                                bc.Add((2, tableChangesSelected, null, writer, sw, batchInfo, null));
                            }
                            //}, 1);


                            // Raise progress
                            bc.Add((3, tableChangesSelected, null, null, null, batchInfo, null));

                        }
                    }
                    catch (Exception exception)
                    {
                        bc.Add((99, null, null, null, null, batchInfo, exception));
                    }
                    finally
                    {
                        bc.CompleteAdding();
                    }

                });

                var consumerTask = Task.Run(async () =>
                {
                    try
                    {
                        foreach (var (state, tableChangesSelected, syncRow, writer, sw, batchInfo, ex) in bc.GetConsumingEnumerable())
                        {
                            if (state == 0)
                            {
                                writer.WriteStartObject();
                                writer.WritePropertyName("t");
                                writer.WriteStartArray();
                                writer.WriteStartObject();
                                writer.WritePropertyName("n");
                                writer.WriteValue(tableChangesSelected.TableName);
                                writer.WritePropertyName("s");
                                writer.WriteValue(tableChangesSelected.SchemaName);
                                writer.WritePropertyName("r");
                                writer.WriteStartArray();
                                writer.WriteWhitespace(Environment.NewLine);
                            }
                            else if (state == 1)
                            {
                                writer.WriteStartArray();

                                for (var i = 0; i < syncRow.Length; i++)
                                    writer.WriteValue(syncRow[i]);

                                writer.WriteEndArray();
                                writer.WriteWhitespace(Environment.NewLine);
                                writer.Flush();
                            }
                            else if (state == 2)
                            {
                                writer.WriteEndArray();
                                writer.WriteEndObject();

                                writer.WriteEndArray();
                                writer.WriteEndObject();
                                writer.Flush();
                                writer.Close();

                            }
                            else if (state == 3)
                            {
                                var tableChangesSelectedArgs = new TableChangesSelectedArgs(context, null, tableChangesSelected, connection, transaction);
                                await this.InterceptAsync(tableChangesSelectedArgs, cancellationToken).ConfigureAwait(false);

                                changes.TableChangesSelected.Add(tableChangesSelected);

                                // only raise report progress if we have something
                                if (tableChangesSelectedArgs.TableChangesSelected.TotalChanges > 0)
                                    this.ReportProgress(context, progress, tableChangesSelectedArgs);

                            }
                            else if (state == 99)
                            {
                                throw ex;
                            }

                        }
                    }
                    catch (InvalidOperationException)
                    {
                        // An InvalidOperationException means that Take() was called on a completed collection
                    }

                });

                await Task.WhenAll(producerTask, consumerTask);

            }

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
                var databaseChangesSelectedArgs = new DatabaseChangesSelectedArgs(context, remoteClientTimestamp, batchInfo, changes, connection);
                this.ReportProgress(context, progress, databaseChangesSelectedArgs);
                await this.InterceptAsync(databaseChangesSelectedArgs, cancellationToken).ConfigureAwait(false);
            }
            return batchInfo;
        }

    }
}
