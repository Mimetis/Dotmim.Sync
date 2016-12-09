using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Core.Batch
{
    internal class SyncBatchDirectoryHeader
    {
        internal Version Version { get; set; }
        internal ulong DataCacheSizeInBytes { get; set; }
        internal string StartBatchFileName { get; set; }
        internal string EndBatchFileName { get; set; }
        internal List<string> BatchFileNames { get; set; }

        public SyncBatchDirectoryHeader()
        {
        }

        public override string ToString()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append("\n\t\tVersion                :").Append(this.Version);
            stringBuilder.Append("\n\t\tDataCacheSize          :").Append(this.DataCacheSizeInBytes);
            stringBuilder.Append("\n\t\tStart Batch File       :").Append(this.StartBatchFileName);
            stringBuilder.Append("\n\t\tLast Batch File        :").Append(this.EndBatchFileName);
            stringBuilder.Append("\n\t\tBatch File Names       :");
            foreach (string _batchFileName in this.BatchFileNames)
                stringBuilder.Append(_batchFileName).Append(", ");

            return stringBuilder.ToString();
        }
    }
}
