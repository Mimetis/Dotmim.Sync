
using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {


        /// <summary>
        /// Load all batch infos from the batch directory (see <see cref="SyncOptions.BatchDirectory"/>)
        /// <example>
        /// <code>
        /// var batchInfos = agent.LocalOrchestrator.LoadBatchInfos();
        /// 
        /// foreach (var batchInfo in batchInfos)
        ///     Console.WriteLine(batchInfo.RowsCount);
        /// </code>
        /// </example>
        /// </summary>
        /// <returns>
        /// 
        /// </returns>
        public virtual List<BatchInfo> LoadBatchInfos()
        {
            var localSerializer = new LocalJsonSerializer();

            var directoryInfo = new DirectoryInfo(this.Options.BatchDirectory);

            if (directoryInfo == null || !directoryInfo.Exists)
                return null;

            List<BatchInfo> batchInfos = new List<BatchInfo>();

            foreach (var directory in directoryInfo.EnumerateDirectories())
            {
                var batchInfo = new BatchInfo(directoryInfo.FullName, directory.Name);

                foreach (var file in directory.GetFiles())
                {
                    var (schemaTable, rowsCount) = localSerializer.GetSchemaTableFromFile(file.FullName);
                    batchInfo.BatchPartsInfo.Add(new BatchPartInfo(file.Name, schemaTable.TableName, schemaTable.SchemaName, rowsCount));
                }
                batchInfos.Add(batchInfo);
            }
            return batchInfos;
        }


        /// <inheritdoc cref="LoadTablesFromBatchInfo(string, BatchInfo, SyncRowState?)"/>
        public virtual IEnumerable<SyncTable> LoadTablesFromBatchInfo(BatchInfo batchInfo, SyncRowState? syncRowState = default)
            => LoadTablesFromBatchInfo(SyncOptions.DefaultScopeName, batchInfo, syncRowState);


        /// <summary>
        /// Load all tables from a batch info. All rows serialized on disk are loaded in memory once you are iterating
        /// 
        /// <code>
        /// var batchInfos = await agent.LocalOrchestrator.LoadBatchInfos();
        /// foreach (var batchInfo in batchInfos)
        /// {
        ///    // Load all rows from error tables specifying the specific SyncRowState states
        ///    var allTables = agent.LocalOrchestrator.LoadTablesFromBatchInfo(batchInfo, SyncRowState.ApplyDeletedFailed | SyncRowState.ApplyModifiedFailed);
        ///
        ///    // Enumerate all rows in error
        ///    foreach (var table in allTables)
        ///      foreach (var row in table.Rows)
        ///        Console.WriteLine(row);
        /// }
        /// </code>   
        /// </summary>
        public virtual IEnumerable<SyncTable> LoadTablesFromBatchInfo(string scopeName, BatchInfo batchInfo, SyncRowState? syncRowState = default)
        {
            if (batchInfo == null || batchInfo.BatchPartsInfo == null || batchInfo.BatchPartsInfo.Count == 0)
                yield break;

            var context = new SyncContext(Guid.NewGuid(), scopeName);
            var bpiGroupedTables = batchInfo.BatchPartsInfo.GroupBy(st => st.TableName + st.SchemaName);

            var localSerializer = new LocalJsonSerializer();

            var interceptorsReading = this.interceptors.GetInterceptors<DeserializingRowArgs>();
            if (interceptorsReading.Count > 0)
            {
                localSerializer.OnReadingRow(async (schemaTable, rowString) =>
                {
                    var args = new DeserializingRowArgs(context, schemaTable, rowString);
                    await this.InterceptAsync(args).ConfigureAwait(false);
                    return args.Result;
                });
            }

            SyncTable currentTable = null;

            foreach (var bpiGroupedTable in bpiGroupedTables)
            {
                var bpiTable = bpiGroupedTable.FirstOrDefault();

                if (bpiTable == null)
                    continue;

                // Gets all BPI containing this table
                foreach (var bpi in batchInfo.GetBatchPartsInfo(bpiTable.TableName, bpiTable.SchemaName))
                {
                    try
                    {
                        // Get full path of my batchpartinfo
                        var fullPath = batchInfo.GetBatchPartInfoPath(bpi).FullPath;

                        if (!File.Exists(fullPath))
                            continue;

                        var (syncTable, _) = localSerializer.GetSchemaTableFromFile(fullPath);

                        // on first iteration, creating the return table
                        currentTable ??= syncTable.Clone();

                        foreach (var syncRow in localSerializer.GetRowsFromFile(fullPath, syncTable))
                            if (!syncRowState.HasValue || syncRowState == default || (syncRowState.HasValue && syncRowState.Value.HasFlag(syncRow.RowState)))
                                currentTable.Rows.Add(syncRow);
                    }
                    catch (Exception ex)
                    {
                        throw GetSyncError(context, ex);
                    }
                }

                yield return currentTable;
            }
        }

        //-------------------------------------------------------
        // Load Batch Info for a given Table Name into SyncTable
        //-------------------------------------------------------

        public virtual SyncTable LoadTableFromBatchInfo(BatchInfo batchInfo, string tableName, string schemaName = default, SyncRowState? syncRowState = default)
            => LoadTableFromBatchInfo(SyncOptions.DefaultScopeName, batchInfo, tableName, schemaName, syncRowState);


        /// <summary>
        /// Load a table with all rows from a <see cref="BatchInfo"/> instance. You need a <see cref="ScopeInfoClient"/> instance to be able to load rows for this client.
        /// <para>
        /// Once loaded, all rows are in memory.
        /// </para>
        /// <example>
        /// <code>
        /// // get the local client scope info
        /// var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName, parameters);
        /// // get all changes from server
        /// var changes = await remoteOrchestrator.GetChangesAsync(cScopeInfoClient);
        /// // load changes for table ProductCategory in memory
        /// var productCategoryTable = await localOrchestrator.LoadBatchInfo(scopeName, changes, "ProductCategory")
        /// foreach (var productCategoryRow in productCategoryTable.Rows)
        /// {
        ///    ....
        /// }
        /// </code>
        /// </example>
        /// </summary>
        public virtual SyncTable LoadTableFromBatchInfo(string scopeName, BatchInfo batchInfo, string tableName, string schemaName = default, SyncRowState? syncRowState = default)
        {
            if (batchInfo == null)
                return null;

            var context = new SyncContext(Guid.NewGuid(), scopeName);

            try
            {
                return InternalLoadTableFromBatchInfo(context, batchInfo, tableName, schemaName, syncRowState);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }


        }

        internal SyncTable InternalLoadTableFromBatchInfo(SyncContext context, BatchInfo batchInfo, string tableName, string schemaName = default, SyncRowState? syncRowState = default)
        {
            var localSerializer = new LocalJsonSerializer();

            var interceptorsReading = this.interceptors.GetInterceptors<DeserializingRowArgs>();
            if (interceptorsReading.Count > 0)
            {
                localSerializer.OnReadingRow(async (schemaTable, rowString) =>
                {
                    var args = new DeserializingRowArgs(context, schemaTable, rowString);
                    await this.InterceptAsync(args).ConfigureAwait(false);
                    return args.Result;
                });
            }
            SyncTable syncTable = null;

            // Gets all BPI containing this table
            foreach (var bpi in batchInfo.GetBatchPartsInfo(tableName, schemaName))
            {
                // Get full path of my batchpartinfo
                var fullPath = batchInfo.GetBatchPartInfoPath(bpi).FullPath;

                if (!File.Exists(fullPath))
                    continue;

                // Get table from file
                if (syncTable == null)
                    (syncTable, _) = localSerializer.GetSchemaTableFromFile(fullPath);

                foreach (var syncRow in localSerializer.GetRowsFromFile(fullPath, syncTable))
                    if (!syncRowState.HasValue || syncRowState == default || (syncRowState.HasValue && syncRowState.Value.HasFlag(syncRow.RowState)))
                        syncTable.Rows.Add(syncRow);
            }
            return syncTable;

        }

        //-------------------------------------------------------
        // Load Batch Part Info into SyncTable
        //-------------------------------------------------------

        /// <summary>
        /// Load a table from a batch part info
        /// </summary>
        public virtual SyncTable LoadTableFromBatchPartInfo(string path, SyncRowState? syncRowState = default, DbConnection connection = null, DbTransaction transaction = null)
            => LoadTableFromBatchPartInfo(SyncOptions.DefaultScopeName, path, syncRowState, connection, transaction);

        /// <summary>
        /// Load a table from a batch part info
        /// </summary>
        public virtual SyncTable LoadTableFromBatchPartInfo(string scopeName, string path, SyncRowState? syncRowState = default, DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                return InternalLoadTableFromBatchPartInfo(context, path, syncRowState, connection, transaction);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Load the Batch part info in memory, in a SyncTable
        /// </summary>
        internal SyncTable InternalLoadTableFromBatchPartInfo(SyncContext context, string fullPath, SyncRowState? syncRowState = default, DbConnection connection = null, DbTransaction transaction = null)
        {
            if (!File.Exists(fullPath))
                return null;

            var localSerializer = new LocalJsonSerializer();

            var interceptorsReading = this.interceptors.GetInterceptors<DeserializingRowArgs>();
            if (interceptorsReading.Count > 0)
            {
                localSerializer.OnReadingRow(async (schemaTable, rowString) =>
                {
                    var args = new DeserializingRowArgs(context, schemaTable, rowString);
                    await this.InterceptAsync(args).ConfigureAwait(false);
                    return args.Result;
                });
            }

            // Get table from file
            var (syncTable, rowsCount) = localSerializer.GetSchemaTableFromFile(fullPath);

            foreach (var syncRow in localSerializer.GetRowsFromFile(fullPath, syncTable))
                if (!syncRowState.HasValue || syncRowState == default || (syncRowState.HasValue && syncRowState.Value.HasFlag(syncRow.RowState)))
                    syncTable.Rows.Add(syncRow);

            return syncTable;
        }





        /// <summary>
        /// Save a batch part info containing all rows from a sync table
        /// </summary>
        /// <param name="batchInfo">Represents the directory containing all batch parts and the schema associated</param>
        /// <param name="batchPartInfo">Represents the table to serialize in a batch part</param>
        /// <param name="syncTable">The table to serialize</param>
        public virtual Task SaveTableToBatchPartInfoAsync(BatchInfo batchInfo, BatchPartInfo batchPartInfo, SyncTable syncTable)
            => SaveTableToBatchPartInfoAsync(SyncOptions.DefaultScopeName, batchInfo, batchPartInfo, syncTable);

        /// <inheritdoc cref="SaveTableToBatchPartInfoAsync(BatchInfo, BatchPartInfo, SyncTable)"/>
        public virtual Task SaveTableToBatchPartInfoAsync(string scopeName, BatchInfo batchInfo, BatchPartInfo batchPartInfo, SyncTable syncTable)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {

                return InternalSaveTableToBatchPartInfoAsync(context, batchInfo, batchPartInfo, syncTable);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        internal async Task InternalSaveTableToBatchPartInfoAsync(SyncContext context, BatchInfo batchInfo, BatchPartInfo batchPartInfo, SyncTable syncTable)
        {
            var localSerializer = new LocalJsonSerializer();

            // Get full path of my batchpartinfo
            var fullPath = batchInfo.GetBatchPartInfoPath(batchPartInfo).FullPath;

            if (File.Exists(fullPath))
                File.Delete(fullPath);

            if (syncTable?.Rows != null && syncTable.Rows.Count <= 0)
            {
                var interceptorsWriting = this.interceptors.GetInterceptors<SerializingRowArgs>();
                if (interceptorsWriting.Count > 0)
                {
                    localSerializer.OnWritingRow(async (schemaTable, rowArray) =>
                    {
                        var args = new SerializingRowArgs(context, schemaTable, rowArray);
                        await this.InterceptAsync(args).ConfigureAwait(false);
                        return args.Result;
                    });
                }
                // open the file and write table header
                await localSerializer.OpenFileAsync(fullPath, syncTable).ConfigureAwait(false);

                foreach (var row in syncTable.Rows)
                    await localSerializer.WriteRowToFileAsync(row, syncTable).ConfigureAwait(false);

                // Close file
                await localSerializer.CloseFileAsync().ConfigureAwait(false);
            }
        }

    }
}
