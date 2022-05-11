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

        /// <summary>
        /// Load the Batch part info in memory, in a SyncTable
        /// </summary>
        public Task<SyncTable> LoadBatchInfoAsync(BatchInfo batchInfo, string tableName, string schemaName = default)
        {
            if (batchInfo == null || batchInfo.SanitizedSchema == null)
                return Task.FromResult<SyncTable>(null);

            // get the sanitazed table (without any readonly / non updatable columns) from batchinfo
            var schemaTable = batchInfo.SanitizedSchema.Tables[tableName, schemaName];
            var table = schemaTable.Clone();

            var localSerializer = new LocalJsonSerializer();

            // Gets all BPI containing this table
            foreach (var bpi in batchInfo.GetBatchPartsInfo(tableName, schemaName))
            {
                // Get full path of my batchpartinfo
                var fullPath = batchInfo.GetBatchPartInfoPath(bpi).FullPath;

                if (!File.Exists(fullPath))
                    continue;

                if (bpi.Tables == null || bpi.Tables.Count() < 1)
                    return Task.FromResult<SyncTable>(null);

                foreach (var syncRow in localSerializer.ReadRowsFromFile(fullPath, schemaTable))
                    table.Rows.Add(syncRow);
            }


            return Task.FromResult(table);
        }


        /// <summary>
        /// Load the Batch part info in memory, in a SyncTable
        /// </summary>
        public Task<SyncTable> LoadBatchPartInfoAsync(BatchInfo batchInfo, BatchPartInfo batchPartInfo)
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

            foreach (var syncRow in localSerializer.ReadRowsFromFile(fullPath, schemaTable))
                table.Rows.Add(syncRow);

            return Task.FromResult(table);
        }

        public async Task SaveBatchPartInfoAsync(BatchInfo batchInfo, BatchPartInfo batchPartInfo)
        {
            if (batchInfo == null || batchInfo.SanitizedSchema == null)
                return;

            var localSerializer = new LocalJsonSerializer();

            // Get full path of my batchpartinfo
            var fullPath = batchInfo.GetBatchPartInfoPath(batchPartInfo).FullPath;

            if (!File.Exists(fullPath))
                return;

            File.Delete(fullPath);

            var schemaTable = batchInfo.SanitizedSchema.Tables[batchPartInfo.Tables[0].TableName, batchPartInfo.Tables[0].SchemaName];


            // open the file and write table header
            await localSerializer.OpenFileAsync(fullPath, schemaTable).ConfigureAwait(false);

            foreach (var row in syncTable.Rows)
                await localSerializer.WriteRowToFileAsync(row, syncTable).ConfigureAwait(false);

            // Close file
            await localSerializer.CloseFileAsync(fullPath, syncTable).ConfigureAwait(false);

        }

    }
}
