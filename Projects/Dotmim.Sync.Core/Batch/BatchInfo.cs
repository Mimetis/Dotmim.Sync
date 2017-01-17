using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.Core.Batch
{
    /// <summary>
    /// Represents a Batch, for a complete change set
    /// FullName like : [Guid].batchinfo
    /// </summary>
    public class BatchInfo
    {

        /// <summary>
        /// Create a new BatchInfo, containing all BatchPartInfo
        /// </summary>
        public BatchInfo()
        {
            this.BatchPartsInfo = new List<BatchPartInfo>();
        }

        /// <summary>
        /// All Parts of the batch
        /// Each part is the size of download batch size
        /// </summary>
        public List<BatchPartInfo> BatchPartsInfo { get; set; }

        /// <summary>
        /// Get all parts containing this table
        /// Could be multiple parts, since the table may be spread across multiples files
        /// </summary>
        public IEnumerable<DmTable> GetTable(string tableName)
        {
            foreach (var batchPartinInfo in this.BatchPartsInfo)
            {
                if (batchPartinInfo.Tables.Contains(tableName))
                {
                    // Batch not readed, so we deserialized the batch and get the table
                    if (batchPartinInfo.Set == null)
                    {
                        // Set is not already deserialized so we try to get the batch
                        BatchPart batchPart = batchPartinInfo.GetBatch();

                        // Unserialized and set in memory the DmSet
                        batchPartinInfo.Set  = batchPart.DmSetSurrogate.ConvertToDmSet();
                    }

                    // return the table
                    if (batchPartinInfo.Set.Tables.Contains(tableName))
                        yield return batchPartinInfo.Set.Tables[tableName];
                }
            }

        }


    }
}
