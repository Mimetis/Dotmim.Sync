using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
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
        /// Get the directory where all files are stored (if InMemory == false)
        /// </summary>
        public String Directory { get; set; }


        /// <summary>
        /// Is the batch parts are in memory
        /// If true, only one BPI
        /// If false, several serialized BPI
        /// </summary>
        public bool InMemory { get; set; }

        /// <summary>
        /// Get the current batch index (if InMemory == false)
        /// </summary>
        public int BatchIndex { get; set; }

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
                bool isSerialized = false;

                if (batchPartinInfo.Tables.Contains(tableName))
                {
                    // Batch not readed, so we deserialized the batch and get the table
                    if (batchPartinInfo.Set == null)
                    {
                        // Set is not already deserialized so we try to get the batch
                        BatchPart batchPart = batchPartinInfo.GetBatch();

                        // Unserialized and set in memory the DmSet
                        batchPartinInfo.Set = batchPart.DmSetSurrogate.ConvertToDmSet();

                        isSerialized = true;
                    }

                    // return the table
                    if (batchPartinInfo.Set.Tables.Contains(tableName))
                    {
                        yield return batchPartinInfo.Set.Tables[tableName];
                        if (isSerialized)
                        {
                            batchPartinInfo.Set.Clear();
                            batchPartinInfo.Set = null;
                        }

                    }
                }
            }

        }


        /// <summary>
        /// Generate a new BatchPartInfo and add it to the current batchInfo
        /// </summary>
        internal BatchPartInfo GenerateBatchInfo(int batchIndex, DmSet changesSet, string batchDirectory )
        {
            var hasData = true;

            if (changesSet == null || changesSet.Tables.Count == 0)
                hasData = false;
            else
                hasData = changesSet.Tables.Any(t => t.Rows.Count > 0);

            if (!hasData)
                return null;

            BatchPartInfo bpi = null;
            // Create a batch part
            // The batch part creation process will serialize the changesSet to the disk
            if (!InMemory)
            {
                var bpId = GenerateNewFileName(batchIndex.ToString());
                var fileName = Path.Combine(batchDirectory, this.Directory, bpId);

                bpi = BatchPartInfo.CreateBatchPartInfo(batchIndex, changesSet, fileName, false, false);
            }
            else
            {
                bpi = BatchPartInfo.CreateBatchPartInfo(batchIndex, changesSet, null, true, true);
            }

            // add the batchpartinfo tp the current batchinfo
            this.BatchPartsInfo.Add(bpi);

            return bpi;
        }


        internal static string GenerateNewDirectoryName()
        {
            return String.Concat(DateTime.UtcNow.ToString("yyyy_MM_dd_ss"), Path.GetRandomFileName().Replace(".", ""));
        }

        internal static string GenerateNewFileName(string batchIndex)
        {
            if (batchIndex.Length == 1)
                batchIndex = $"00{batchIndex}";
            else if (batchIndex.Length == 2)
                batchIndex = $"0{batchIndex}";
            else if (batchIndex.Length == 3)
                batchIndex = $"{batchIndex}";
            else
                throw new OverflowException("too much batches !!!");

            return $"{batchIndex}_{Path.GetRandomFileName().Replace(".", "_")}.batch";
        }

        public static BatchInfo DeserializeFromDmSet(DmSet set)
        {
            if (set == null)
                return null;

            if (!set.Tables.Contains("DotmimSync__BatchInfo"))
                return null;

            var dmRow = set.Tables["DotmimSync__BatchInfo"].Rows[0];
            BatchInfo bi = new BatchInfo();

            bi.Directory= dmRow["Directory"] as string;
            bi.InMemory = (Boolean)dmRow["InMemory"];
            bi.BatchIndex = (int)dmRow["BatchIndex"];

            return bi;

        }
        internal static void SerializeInDmSet(DmSet set, BatchInfo bi)
        {
            if (set == null)
                return;

            DmTable dmTableBatchInfo = null;

            if (!set.Tables.Contains("DotmimSync__BatchInfo"))
            {
                dmTableBatchInfo = new DmTable("DotmimSync__BatchInfo");
                set.Tables.Add(dmTableBatchInfo);
            }

            dmTableBatchInfo = set.Tables["DotmimSync__BatchInfo"];

            dmTableBatchInfo.Columns.Add<String>("Directory");
            dmTableBatchInfo.Columns.Add<int>("BatchIndex");
            dmTableBatchInfo.Columns.Add<Boolean>("InMemory");

            var dmRow = dmTableBatchInfo.NewRow();
            dmRow["Directory"] = bi.Directory;
            dmRow["BatchIndex"] = bi.BatchIndex;
            dmRow["InMemory"] = bi.InMemory;

            dmTableBatchInfo.Rows.Add(dmRow);


        }

    }
}
