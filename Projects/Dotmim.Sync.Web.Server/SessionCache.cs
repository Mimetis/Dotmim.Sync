
using Newtonsoft.Json;
using System;
using System.Collections.Specialized;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Dotmim.Sync.Batch;
using System.Text;
using System.Runtime.Serialization;

namespace Dotmim.Sync.Web.Server
{
    /// <summary>
    /// Cache object used by each client to cache sync process batches
    /// </summary>
    [DataContract(Name = "sc"), Serializable]
    public class SessionCache
    {
        [DataMember(Name = "rct", IsRequired = false, EmitDefaultValue = false, Order = 1)]
        public long RemoteClientTimestamp { get; set; }
        [DataMember(Name = "sbi", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public BatchInfo ServerBatchInfo { get; set; }
        [DataMember(Name = "cbi", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public BatchInfo ClientBatchInfo { get; set; }
        [DataMember(Name = "dcs", IsRequired = false, EmitDefaultValue = false, Order = 4)]
        public DatabaseChangesSelected ServerChangesSelected { get; set; }
        [DataMember(Name = "dca", IsRequired = false, EmitDefaultValue = false, Order = 5)]
        public DatabaseChangesApplied ClientChangesApplied { get; set; }


        public override string ToString()
        {
            var serverBatchInfoStr = "Null";
            if (ServerBatchInfo != null)
            {
                var serverBatchPartsCountStr = ServerBatchInfo.BatchPartsInfo == null ? "Null" : ServerBatchInfo.BatchPartsInfo.Count.ToString();
                serverBatchInfoStr = $"Parts:{serverBatchPartsCountStr}. Rows Count:{ServerBatchInfo.RowsCount}.";
            }

            var clientBatchInfoStr = "Null";
            if (ClientBatchInfo != null)
            {
                var clientBatchPartsCountStr = ClientBatchInfo.BatchPartsInfo == null ? "Null" : ClientBatchInfo.BatchPartsInfo.Count.ToString();
                clientBatchInfoStr = $"Parts:{clientBatchPartsCountStr}. Rows Count:{ClientBatchInfo.RowsCount}.";
            }

            var debug = new StringBuilder();
            debug.Append($" \"RemoteClientTimestamp\":\"{RemoteClientTimestamp}\",");
            debug.Append($" \"ClientBatchInfo\":\"{clientBatchInfoStr}\",");
            debug.Append($" \"ServerBatchInfo\":\"{serverBatchInfoStr}\"");

            return debug.ToString();

        }

    }


}
