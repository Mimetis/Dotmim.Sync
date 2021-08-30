
using Newtonsoft.Json;
using System;
using System.Collections.Specialized;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Dotmim.Sync.Batch;
using System.Text;

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


        public override string ToString()
        {
            var serverBatchInfoStr = "Null";
            if (ServerBatchInfo != null)
                serverBatchInfoStr = $"Parts:{ServerBatchInfo.BatchPartsInfo.Count}. Rows Count:{ServerBatchInfo.RowsCount}. Tables:{ServerBatchInfo.SanitizedSchema.Tables.Count}";

            var clientBatchInfoStr = "Null";
            if (ClientBatchInfo != null)
                clientBatchInfoStr = $"Parts:{ClientBatchInfo.BatchPartsInfo.Count}. Rows Count:{ClientBatchInfo.RowsCount}. Tables:{ClientBatchInfo.SanitizedSchema.Tables.Count}";

            var debug = new StringBuilder();
            debug.AppendLine("{");
            debug.AppendLine($" \"RemoteClientTimestamp\":\"{RemoteClientTimestamp}\"");
            debug.AppendLine($" \"ClientBatchInfo\":\"{clientBatchInfoStr}\"");
            debug.AppendLine($" \"ServerBatchInfo\":\"{serverBatchInfoStr}\"");

            debug.AppendLine("}");

            return debug.ToString();
            
        }

    }


}
