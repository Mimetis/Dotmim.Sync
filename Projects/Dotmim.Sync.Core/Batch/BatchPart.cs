using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Serialization;
using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace Dotmim.Sync.Batch
{
    /// <summary>
    /// Batch Part
    /// FullName like : [Guid].batch
    /// </summary>
    [Serializable]
    public class BatchPart
    {
        /// <summary>
        /// get the DmSetSurrogate associated with this batch part
        /// </summary>
        public DmSetSurrogate DmSetSurrogate { get; private set; }

        public static BatchPart Deserialize(string fileName)
        {
            if (String.IsNullOrEmpty(fileName))
                throw new ArgumentNullException("Cant get a Batch part if fileName doesn't exist");

            if (!File.Exists(fileName))
                throw new ArgumentNullException($"file {fileName} doesn't exist");

            BatchPart bp = new BatchPart();

            using (FileStream fs = new FileStream(fileName, FileMode.Open, FileAccess.Read))
            {
                BinaryFormatter serializer = new BinaryFormatter();
                bp.DmSetSurrogate = serializer.Deserialize(fs) as DmSetSurrogate;
            }

            return bp;
        }


        public static void Serialize(DmSetSurrogate set, string fileName)
        {

            FileInfo fi = new FileInfo(fileName);

            if (!Directory.Exists(fi.Directory.FullName))
                Directory.CreateDirectory(fi.Directory.FullName);

            // Serialize on disk.
            using (var f = new FileStream(fileName, FileMode.CreateNew, FileAccess.ReadWrite))
            {
                BinaryFormatter serializer = new BinaryFormatter();
                serializer.Serialize(f, set);
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
            if (this.DmSetSurrogate != null)
            {
                this.DmSetSurrogate.Dispose();
                this.DmSetSurrogate = null;
            }
        }
    }
}
