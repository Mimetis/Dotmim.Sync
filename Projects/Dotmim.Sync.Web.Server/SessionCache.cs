
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
            var serverBatchPartsCountStr = ServerBatchInfo.BatchPartsInfo == null ? "Null" : ServerBatchInfo.BatchPartsInfo.Count.ToString();
            var serverBatchTablesCountStr = ServerBatchInfo.SanitizedSchema == null ? "Null" : ServerBatchInfo.SanitizedSchema.Tables.Count.ToString();

            if (ServerBatchInfo != null)
                serverBatchInfoStr = $"Parts:{serverBatchPartsCountStr}. Rows Count:{ServerBatchInfo.RowsCount}. Tables:{serverBatchTablesCountStr}";

            var clientBatchInfoStr = "Null";
            var clientBatchPartsCountStr = ClientBatchInfo.BatchPartsInfo == null ? "Null" : ClientBatchInfo.BatchPartsInfo.Count.ToString();
            var clientBatchTablesCountStr = ClientBatchInfo.SanitizedSchema == null ? "Null" : ClientBatchInfo.SanitizedSchema.Tables.Count.ToString();
            if (ClientBatchInfo != null)
                clientBatchInfoStr = $"Parts:{clientBatchPartsCountStr}. Rows Count:{ClientBatchInfo.RowsCount}. Tables:{clientBatchTablesCountStr}";

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
