using Dotmim.Sync.Batch;
using System;
using System.Threading.Tasks;

namespace Dotmim.Sync.Web.Client
{
    /// <summary>
    /// Contains the forbidden logic to handle batch info on the server side.
    /// </summary>
    public partial class WebRemoteOrchestrator : RemoteOrchestrator
    {
        /// <summary>
        /// You are not allowed to save a batch info on the server from the client side.
        /// </summary>
        public override Task SaveTableToBatchPartInfoAsync(BatchInfo batchInfo, BatchPartInfo batchPartInfo, SyncTable syncTable)
               => throw new NotImplementedException();

        /// <summary>
        /// You are not allowed to save a batch info on the server from the client side.
        /// </summary>
        public override Task SaveTableToBatchPartInfoAsync(string scopeName, BatchInfo batchInfo, BatchPartInfo batchPartInfo, SyncTable syncTable)
               => throw new NotImplementedException();
    }
}