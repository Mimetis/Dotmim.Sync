using Dotmim.Sync.Batch;
using System;
using System.Globalization;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync.Web.Server
{
    /// <summary>
    /// Cache object used by each client to cache sync process batches.
    /// </summary>
    [DataContract(Name = "sc"), Serializable]
    public class SessionCache
    {
        /// <summary>
        /// Gets or Sets the remote client timestamp.
        /// </summary>
        [DataMember(Name = "rct", IsRequired = false, Order = 1)]
        public long RemoteClientTimestamp { get; set; }

        /// <summary>
        /// Gets or Sets the server batch info.
        /// </summary>
        [DataMember(Name = "sbi", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public BatchInfo ServerBatchInfo { get; set; }

        /// <summary>
        /// Gets or Sets the client batch info.
        /// </summary>
        [DataMember(Name = "cbi", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public BatchInfo ClientBatchInfo { get; set; }

        /// <summary>
        /// Gets or Sets the server changes selected.
        /// </summary>
        [DataMember(Name = "dcs", IsRequired = false, EmitDefaultValue = false, Order = 4)]
        public DatabaseChangesSelected ServerChangesSelected { get; set; }

        /// <summary>
        /// Gets or Sets the client changes applied.
        /// </summary>
        [DataMember(Name = "dca", IsRequired = false, EmitDefaultValue = false, Order = 5)]
        public DatabaseChangesApplied ClientChangesApplied { get; set; }

        /// <inheritdoc />
        public override string ToString()
        {
            var serverBatchInfoStr = "Null";
            if (this.ServerBatchInfo != null)
            {
                var serverBatchPartsCountStr = this.ServerBatchInfo.BatchPartsInfo == null ? "Null" : this.ServerBatchInfo.BatchPartsInfo.Count.ToString(CultureInfo.InvariantCulture);
                serverBatchInfoStr = $"Parts:{serverBatchPartsCountStr}. Rows Count:{this.ServerBatchInfo.RowsCount}.";
            }

            var clientBatchInfoStr = "Null";
            if (this.ClientBatchInfo != null)
            {
                var clientBatchPartsCountStr = this.ClientBatchInfo.BatchPartsInfo == null ? "Null" : this.ClientBatchInfo.BatchPartsInfo.Count.ToString(CultureInfo.InvariantCulture);
                clientBatchInfoStr = $"Parts:{clientBatchPartsCountStr}. Rows Count:{this.ClientBatchInfo.RowsCount}.";
            }

            var debug = new StringBuilder();
            debug.Append(CultureInfo.InvariantCulture, $" \"RemoteClientTimestamp\":\"{this.RemoteClientTimestamp}\",");
            debug.Append(CultureInfo.InvariantCulture, $" \"ClientBatchInfo\":\"{clientBatchInfoStr}\",");
            debug.Append(CultureInfo.InvariantCulture, $" \"ServerBatchInfo\":\"{serverBatchInfoStr}\"");

            return debug.ToString();
        }
    }
}