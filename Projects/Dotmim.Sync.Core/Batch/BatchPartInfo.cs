using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
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

        public bool InMemory { get; set; }

        /// <summary>
        /// Initializing a BatchPartInfo with an existing file
        /// So it's a serialized batch
        /// </summary>
        public BatchPartInfo(string fileName)
        {
            this.FileName = fileName;
            this.InMemory = true;
        }


        /// <summary>
        /// Initializing a BatchPart with an existing DmSet
        /// So it's an in memory batch
        /// </summary>
        public BatchPartInfo(DmSet set)
        {
            // no need of a batch here since we are in memory
            this.Set = set;
            this.InMemory = false;
        }

    }
}
