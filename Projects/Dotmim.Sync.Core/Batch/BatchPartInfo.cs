using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.Core.Batch
{
    /// <summary>
    /// Info about a BatchPart
    /// Will be serialized in the BatchInfo file
    /// </summary>
    public class BatchPartInfo
    {
        private BatchPart batch;
        public BatchPart GetBatch()
        {
            if (batch != null)
                return batch;

            if (String.IsNullOrEmpty(this.FileName))
                throw new ArgumentException("Cant get a batchpart if filename is null");

            // Get a Batch part, and deserialise the file into a DmSetSurrogate
            batch = BatchPart.Deserialize(this.FileName);

            return batch;
        }

        public String FileName { get; set; }

        public int Index { get; set; }

        public Boolean IsLastBatch { get; set; }

        /// <summary>
        /// Tables contained in the DmSet (serialiazed or not)
        /// </summary>
        public String[] Tables { get; set; }

        /// <summary>
        /// Gets or Sets the DmSet from the batch associated once the DmSetSurrogate is deserialized
        /// </summary>
        public DmSet Set { get; set; }


        internal BatchPartInfo()
        {
        }

        /// <summary>
        /// Deserialize the BPI WITHOUT the DmSet
        /// </summary>
        public static BatchPartInfo DeserializeFromDmSet(DmSet set)
        {
            if (set == null)
                return null;

            if (!set.Tables.Contains("DotmimSync__BatchPartsInfo"))
                return null;

            var dmRow = set.Tables["DotmimSync__BatchPartsInfo"].Rows[0];

            var bpi = new BatchPartInfo();
            bpi.Index = (int)dmRow["Index"];
            bpi.FileName = dmRow["FileName"] as string;
            bpi.IsLastBatch = (Boolean)dmRow["IsLastBatch"];

            if (dmRow["Tables"] != null)
            {
                var stringTables = dmRow["Tables"] as string;
                var tables = stringTables.Split(new char[] { '|' }, StringSplitOptions.RemoveEmptyEntries);
                bpi.Tables = tables;
            }

            return bpi;
        }

        /// <summary>
        /// Serialize the BatchPartInfo WITHOUT the DmSet
        /// </summary>
        internal static void SerializeInDmSet(DmSet set, BatchPartInfo bpi)
        {
            if (set == null)
                return;

            DmTable dmTableBatchPartsInfo = null;

            if (!set.Tables.Contains("DotmimSync__BatchPartsInfo"))
            {
                dmTableBatchPartsInfo = new DmTable("DotmimSync__BatchPartsInfo");
                set.Tables.Add(dmTableBatchPartsInfo);
            }

            dmTableBatchPartsInfo = set.Tables["DotmimSync__BatchPartsInfo"];

            dmTableBatchPartsInfo.Columns.Add<String>("FileName");
            dmTableBatchPartsInfo.Columns.Add<int>("Index");
            dmTableBatchPartsInfo.Columns.Add<Boolean>("IsLastBatch");
            dmTableBatchPartsInfo.Columns.Add<String>("Tables");

            var dmRow = dmTableBatchPartsInfo.NewRow();
            dmRow["FileName"] = bpi.FileName;
            dmRow["Index"] = bpi.Index;
            dmRow["IsLastBatch"] = bpi.IsLastBatch;

            if (bpi.Tables != null && bpi.Tables.Length > 0)
            {
                var tablesString = String.Join("|", bpi.Tables);
                dmRow["Tables"] = tablesString;
            }

            dmTableBatchPartsInfo.Rows.Add(dmRow);

        }

        /// <summary>
        /// Create a new BPI, and serialize the changeset if not in memory
        /// </summary>
        internal static BatchPartInfo CreateBatchPartInfo(int batchIndex, DmSet changesSet, string fileName, Boolean isLastBatch, Boolean inMemory)
        {
            BatchPartInfo bpi = null;

            // Create a batch part
            // The batch part creation process will serialize the changesSet to the disk
            if (!inMemory)
            {
                // Serialize the file !
                BatchPart.Serialize(new DmSetSurrogate(changesSet), fileName);

                bpi = new BatchPartInfo { FileName = fileName };
            }
            else
            {
                bpi = new BatchPartInfo { Set = changesSet };
            }

            bpi.Index = batchIndex;
            bpi.IsLastBatch = isLastBatch;

            // Even if the set is empty (serialized on disk), we should retain the tables names
            bpi.Tables = changesSet.Tables.Select(t => t.TableName).ToArray();

            return bpi;
        }

    }
}
