

using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Batch
{
    /// <summary>
    /// Info about a BatchPart
    /// Will be serialized in the BatchInfo file
    /// </summary>
    [DataContract(Name = "batchpartinfo"), Serializable]
    public class BatchPartInfo
    {
        [DataMember(Name = "file", IsRequired = true, Order = 1)]
        public string FileName { get; set; }

        [DataMember(Name = "index", IsRequired = true, Order = 2)]
        public int Index { get; set; }

        [DataMember(Name = "last", IsRequired = true, Order = 3)]
        public bool IsLastBatch { get; set; }

        /// <summary>
        /// Tables contained in the SyncSet (serialiazed or not) (NEW v0.9.3 : Only One table per file)
        /// </summary>
        [DataMember(Name = "tables", IsRequired = true, Order = 4)]
        public BatchPartTableInfo[] Tables { get; set; }

        /// <summary>
        /// Tables contained rows count
        /// </summary>
        [DataMember(Name = "rc", IsRequired = false, Order = 5)]
        public int RowsCount { get; set; }

        public BatchPartInfo()
        {
        }
    }

}
