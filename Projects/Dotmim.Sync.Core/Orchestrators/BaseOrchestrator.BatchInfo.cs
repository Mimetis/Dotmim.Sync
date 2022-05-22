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
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {

        public virtual Task<(SyncContext context, SyncTable syncTable)> LoadTableFromBatchInfoAsync(BatchInfo batchInfo, string tableName, string schemaName = default)
            => LoadTableFromBatchInfoAsync(SyncOptions.DefaultScopeName, batchInfo, tableName, schemaName);

        public virtual Task<(SyncContext context, SyncTable syncTable)> LoadTableFromBatchInfoAsync(string scopeName, BatchInfo batchInfo, string tableName, string schemaName = default)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {

                return InternalLoadTableFromBatchInfoAsync(context, batchInfo, tableName, schemaName);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }


        /// <summary>
        /// Load the Batch part info in memory, in a SyncTable
        /// </summary>
        internal virtual Task<(SyncContext context, SyncTable syncTable)> InternalLoadTableFromBatchInfoAsync(SyncContext context, BatchInfo batchInfo, string tableName, string schemaName = default)
        {
            if (batchInfo == null || batchInfo.SanitizedSchema == null)
                return Task.FromResult<(SyncContext, SyncTable)>((context, null));

            // get the sanitazed table (without any readonly / non updatable columns) from batchinfo
            var schemaTable = batchInfo.SanitizedSchema.Tables[tableName, schemaName];
            var table = schemaTable.Clone();

            var localSerializer = new LocalJsonSerializer();

            var interceptorReading = this.interceptors.GetInterceptor<DeserializingRowArgs>();
            if (!interceptorReading.IsEmpty)
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
                    return Task.FromResult<(SyncContext, SyncTable)>((context, null));

                foreach (var syncRow in localSerializer.ReadRowsFromFile(fullPath, schemaTable))
                    table.Rows.Add(syncRow);
            }


            return Task.FromResult((context, table));
        }




        public virtual Task<SyncTable> LoadTableFromBatchPartInfoAsync(BatchInfo batchInfo, BatchPartInfo batchPartInfo)
            => LoadTableFromBatchPartInfoAsync(SyncOptions.DefaultScopeName, batchInfo, batchPartInfo);


        public virtual Task<SyncTable> LoadTableFromBatchPartInfoAsync(string scopeName, BatchInfo batchInfo, BatchPartInfo batchPartInfo)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {

                return InternalLoadTableFromBatchPartInfoAsync(context, batchInfo, batchPartInfo);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Load the Batch part info in memory, in a SyncTable
        /// </summary>
        internal Task<SyncTable> InternalLoadTableFromBatchPartInfoAsync(SyncContext context, BatchInfo batchInfo, BatchPartInfo batchPartInfo)
        {
            if (batchInfo == null || batchInfo.SanitizedSchema == null)
                return Task.FromResult<SyncTable>(null);

            var localSerializer = new LocalJsonSerializer();

            // Get full path of my batchpartinfo
            var fullPath = batchInfo.GetBatchPartInfoPath(batchPartInfo).FullPath;

            if (!File.Exists(fullPath))
                return Task.FromResult<SyncTable>(null);

            if (batchPartInfo.Tables == null || batchPartInfo.Tables.Count() < 1)
                return Task.FromResult<SyncTable>(null);

            var schemaTable = batchInfo.SanitizedSchema.Tables[batchPartInfo.Tables[0].TableName, batchPartInfo.Tables[0].SchemaName];

            var table = schemaTable.Clone();

            var interceptorReading = this.interceptors.GetInterceptor<DeserializingRowArgs>();
            if (!interceptorReading.IsEmpty)
            {
                localSerializer.OnReadingRow(async (schemaTable, rowString) =>
                {
                    var args = new DeserializingRowArgs(context, schemaTable, rowString);
                    await this.InterceptAsync(args).ConfigureAwait(false);
                    return args.Result;
                });
            }

            foreach (var syncRow in localSerializer.ReadRowsFromFile(fullPath, schemaTable))
                table.Rows.Add(syncRow);

            return Task.FromResult(table);
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

            var interceptorWriting = this.interceptors.GetInterceptor<SerializingRowArgs>();
            if (!interceptorWriting.IsEmpty)
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
