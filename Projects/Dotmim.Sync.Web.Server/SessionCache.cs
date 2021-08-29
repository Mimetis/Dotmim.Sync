
using Newtonsoft.Json;
using System;
using System.Collections.Specialized;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Dotmim.Sync.Batch;

namespace Dotmim.Sync.Web.Server
{
    /// <summary>
    /// Cache object used by each client to cache sync process batches
    /// </summary>
    public class SessionCache
    {
        public long RemoteClientTimestamp { get; set; }

        public BatchInfo ServerBatchInfo { get; set; }
        public BatchInfo ClientBatchInfo { get; set; }

        public DatabaseChangesSelected ServerChangesSelected { get; set; }
        public DatabaseChangesApplied ClientChangesApplied { get; set; }



    }


}
