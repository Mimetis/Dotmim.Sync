using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;

namespace Dotmim.Sync.Web.Client
{
    public partial class WebClientOrchestrator : RemoteOrchestrator
    {
        /// <summary>
        /// Http Client is not able to load batches from server
        /// </summary>
        public override Task<(SyncContext context, SyncTable syncTable)> LoadTableFromBatchInfoAsync(BatchInfo batchInfo, string tableName, string schemaName = default)
              => throw new NotImplementedException();

        /// <summary>
        /// Http Client is not able to load batches from server
        /// </summary>
        public override Task<(SyncContext context, SyncTable syncTable)> LoadTableFromBatchInfoAsync(string scopeName, BatchInfo batchInfo, string tableName, string schemaName = default)
               => throw new NotImplementedException();

        /// <summary>
        /// Http Client is not able to load batches from server
        /// </summary>
        public override Task<SyncTable> LoadTableFromBatchPartInfoAsync(BatchInfo batchInfo, BatchPartInfo batchPartInfo)
               => throw new NotImplementedException();

        /// <summary>
        /// Http Client is not able to load batches from server
        /// </summary>
        public override Task<SyncTable> LoadTableFromBatchPartInfoAsync(string scopeName, BatchInfo batchInfo, BatchPartInfo batchPartInfo)
               => throw new NotImplementedException();

        public override Task<SyncContext> SaveTableToBatchPartInfoAsync(BatchInfo batchInfo, BatchPartInfo batchPartInfo, SyncTable syncTable)
               => throw new NotImplementedException();

        public override Task<SyncContext> SaveTableToBatchPartInfoAsync(string scopeName, BatchInfo batchInfo, BatchPartInfo batchPartInfo, SyncTable syncTable)
               => throw new NotImplementedException();



    }
}
