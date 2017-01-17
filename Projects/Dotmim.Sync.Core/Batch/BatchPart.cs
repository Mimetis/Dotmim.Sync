using DmBinaryFormatter;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Dotmim.Sync.Core.Batch
{
    /// <summary>
    /// Batch Part
    /// FullName like : [Guid].batch
    /// </summary>
    public class BatchPart
    {

        /// <summary>
        /// get the DmSetSurrogate associated with this batch part
        /// </summary>
        public DmSetSurrogate DmSetSurrogate { get; private set; }

        public static BatchPart Deserialize(string fileName)
        {
            if (String.IsNullOrEmpty(fileName))
                throw new ArgumentException("Cant get a Batch part if fileName doesn't exist");

            if (!File.Exists(fileName))
                throw new ArgumentException($"file {fileName} doesn't exist");

            BatchPart bp = new BatchPart();

            using (FileStream fs = new FileStream(fileName, FileMode.Open, FileAccess.Read))
            {
                DmSerializer serializer = new DmSerializer();
                bp = serializer.Deserialize<BatchPart>(fs);
            }

            return bp;
        }

        /// <summary>
        /// Create a new batch part with an existing DmSet
        /// </summary>
        public static void Serialize(DmSet set, string fileName)
        {
            using (DmSetSurrogate dss = new DmSetSurrogate(set))
            {
                BatchPart bp = new BatchPart();
                bp.DmSetSurrogate = new DmSetSurrogate(set);
                DmSerializer serializer = new DmSerializer();
                // Serialize on disk.
                using (var f = new FileStream(fileName, FileMode.CreateNew, FileAccess.ReadWrite))
                {
                    serializer.Serialize(bp, typeof(BatchPart), f);
                }
                bp.Clear();
            }
        }

        /// <summary>
        /// Initializing a BatchPart with an existing file
        /// So it's a serialized batch
        /// </summary>
        private BatchPart()
        {
        }

        /// <summary>
        /// Clear the in memory Surrogate
        /// </summary>
        internal void Clear()
        {
            this.DmSetSurrogate.Dispose();
            this.DmSetSurrogate = null;
        }
    }
}
