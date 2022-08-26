using Dotmim.Sync.Args;
using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {

        public virtual Task<SyncTable> LoadTableFromBatchInfoAsync(BatchInfo batchInfo, string tableName, string schemaName = default, DataRowState? dataRowState = default)
            => LoadTableFromBatchInfoAsync(SyncOptions.DefaultScopeName, batchInfo, tableName, schemaName, dataRowState);

        public virtual async Task<SyncTable> LoadTableFromBatchInfoAsync(string scopeName, BatchInfo batchInfo, string tableName, string schemaName = default, DataRowState? dataRowState = default)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                SyncTable syncTable = null;

                (_, syncTable) = await InternalLoadTableFromBatchInfoAsync(context, batchInfo, tableName, schemaName, dataRowState).ConfigureAwait(false);

                return syncTable;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }


        /// <summary>
        /// Load the Batch part info in memory, in a SyncTable
        /// </summary>
        internal virtual async Task<(SyncContext context, SyncTable syncTable)> InternalLoadTableFromBatchInfoAsync(
            SyncContext context, BatchInfo batchInfo, string tableName, string schemaName = default, DataRowState? dataRowState = default)
        {
            if (batchInfo == null)
                return (context, null);

            ScopeInfo scopeInfo = null;
            (context, scopeInfo) = await this.InternalGetScopeInfoAsync(context, default, default, default, default).ConfigureAwait(false);

            if (scopeInfo == null)
                throw new MissingSchemaInScopeException();

            // get the sanitazed table (without any readonly / non updatable columns) from batchinfo
            var originalSchemaTable = scopeInfo.Schema.Tables[tableName, schemaName];
            var changesSet = originalSchemaTable.Schema.Clone(false);
            var table = BaseOrchestrator.CreateChangesTable(originalSchemaTable, changesSet);


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
            // Gets all BPI containing this table
            foreach (var bpi in batchInfo.GetBatchPartsInfo(tableName, schemaName))
            {
                // Get full path of my batchpartinfo
                var fullPath = batchInfo.GetBatchPartInfoPath(bpi).FullPath;

                if (!File.Exists(fullPath))
                    continue;

                if (bpi.Tables == null || bpi.Tables.Count() < 1)
                    return (context, null);

                foreach (var syncRow in localSerializer.ReadRowsFromFile(fullPath, table))
                    if (!dataRowState.HasValue || dataRowState == default || syncRow.RowState == dataRowState)
                        table.Rows.Add(syncRow);
            }


            return (context, table);
        }


        public virtual Task<SyncTable> LoadTableFromBatchPartInfoAsync(BatchInfo batchInfo, BatchPartInfo batchPartInfo, DataRowState? dataRowState = default)
            => LoadTableFromBatchPartInfoAsync(SyncOptions.DefaultScopeName, batchInfo, batchPartInfo, dataRowState);


        public virtual Task<SyncTable> LoadTableFromBatchPartInfoAsync(string scopeName, BatchInfo batchInfo, BatchPartInfo batchPartInfo, DataRowState? dataRowState = default)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                return InternalLoadTableFromBatchPartInfoAsync(context, batchInfo, batchPartInfo, dataRowState);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Load the Batch part info in memory, in a SyncTable
        /// </summary>
        internal async Task<SyncTable> InternalLoadTableFromBatchPartInfoAsync(SyncContext context, BatchInfo batchInfo, BatchPartInfo batchPartInfo, DataRowState? dataRowState = default)
        {
            if (batchInfo == null)
                return null;

            var localSerializer = new LocalJsonSerializer();

            // Get full path of my batchpartinfo
            var fullPath = batchInfo.GetBatchPartInfoPath(batchPartInfo).FullPath;

            if (!File.Exists(fullPath))
                return null;

            if (batchPartInfo.Tables == null || batchPartInfo.Tables.Count() < 1)
                return null;

            ScopeInfo scopeInfo = null;
            (context, scopeInfo) = await this.InternalGetScopeInfoAsync(context, default, default, default, default).ConfigureAwait(false);

            // get the sanitazed table (without any readonly / non updatable columns) from batchinfo
            var originalSchemaTable = scopeInfo.Schema.Tables[batchPartInfo.Tables[0].TableName, batchPartInfo.Tables[0].SchemaName];
            var changesSet = originalSchemaTable.Schema.Clone(false);
            var table = BaseOrchestrator.CreateChangesTable(originalSchemaTable, changesSet);


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

            foreach (var syncRow in localSerializer.ReadRowsFromFile(fullPath, table))
                if (!dataRowState.HasValue || dataRowState == default || syncRow.RowState == dataRowState)
                    table.Rows.Add(syncRow);

            return table;
        }


        /// <summary>
        /// Save a batch part info containing all rows from a sync table
        /// </summary>
        /// <param name="batchInfo">Represents the directory containing all batch parts and the schema associated</param>
        /// <param name="batchPartInfo">Represents the table to serialize in a batch part</param>
        /// <param name="syncTable">The table to serialize</param>
        public virtual Task<SyncContext> SaveTableToBatchPartInfoAsync(BatchInfo batchInfo, BatchPartInfo batchPartInfo, SyncTable syncTable)
            => SaveTableToBatchPartInfoAsync(SyncOptions.DefaultScopeName, batchInfo, batchPartInfo, syncTable);

        public virtual Task<SyncContext> SaveTableToBatchPartInfoAsync(string scopeName, BatchInfo batchInfo, BatchPartInfo batchPartInfo, SyncTable syncTable)
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

        internal async Task<SyncContext> InternalSaveTableToBatchPartInfoAsync(SyncContext context, BatchInfo batchInfo, BatchPartInfo batchPartInfo, SyncTable syncTable)
        {
            var localSerializer = new LocalJsonSerializer();

            // Get full path of my batchpartinfo
            var fullPath = batchInfo.GetBatchPartInfoPath(batchPartInfo).FullPath;

            if (File.Exists(fullPath))
                File.Delete(fullPath);

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
            await localSerializer.CloseFileAsync(fullPath, syncTable).ConfigureAwait(false);

            return context;
        }

    }
}
