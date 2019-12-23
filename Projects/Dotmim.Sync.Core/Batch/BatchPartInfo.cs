using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace Dotmim.Sync.Batch
{
    /// <summary>
    /// Info about a BatchPart
    /// Will be serialized in the BatchInfo file
    /// </summary>
    public class BatchPartInfo
    {
        /// <summary>
        /// Loads the batch file and set the DmSet in memory
        /// </summary>
        public void LoadBatch(SyncSet schema)
        {
            if (this.Data != null && this.Data.Tables != null && this.Data.Tables.Count > 0)
                return;

            if (string.IsNullOrEmpty(this.FileName))
                return;


            // Clone the schema to get a unique instance
            var set = schema.Clone();

            // Get a Batch part, and deserialise the file into a the BatchPartInfo Set property
            var data = Deserialize(this.FileName);

            // Import data in a typed Set
            set.ImportContainerSet(data);

        }

        /// <summary>
        /// Delete the DmSet surrogate affiliated with the BatchPart, if exists.
        /// </summary>
        public void Clear()
        {
            if (this.Data != null)
                this.Data.Dispose();
        }

        public string FileName { get; set; }

        public int Index { get; set; }

        public bool IsLastBatch { get; set; }

        /// <summary>
        /// Tables contained in the DmSet (serialiazed or not)
        /// </summary>
        public (string tableName, string schemaName)[] Tables { get; set; }

        /// <summary>
        /// Get a SyncSet corresponding to this batch part info
        /// </summary>
        public SyncSet Data { get; set; }


        public BatchPartInfo()
        {
        }


        private static ContainerSet Deserialize(string fileName)
        {
            if (string.IsNullOrEmpty(fileName))
                throw new ArgumentNullException("Cant get a Batch part if fileName doesn't exist");

            if (!File.Exists(fileName))
                throw new ArgumentNullException($"file {fileName} doesn't exist");

            using (var fs = new FileStream(fileName, FileMode.Open, FileAccess.Read))
            {
                var serializer = new DataContractSerializer(typeof(ContainerSet));
                return serializer.ReadObject(fs) as ContainerSet;
            }
        }

        /// <summary>
        /// Serialize the DmSet data (acutally serialize a DmSetSurrogate)
        /// </summary>
        private static void Serialize(ContainerSet set, string fileName)
        {
            if (set == null)
                return;

            var fi = new FileInfo(fileName);

            if (!Directory.Exists(fi.Directory.FullName))
                Directory.CreateDirectory(fi.Directory.FullName);

            // Serialize on disk.
            using (var f = new FileStream(fileName, FileMode.CreateNew, FileAccess.ReadWrite))
            {
                var serializer = new DataContractSerializer(typeof(ContainerSet));
                serializer.WriteObject(f, set);
            }
        }

        /// <summary>
        /// Create a new BPI, and serialize the changeset if not in memory
        /// </summary>
        internal static BatchPartInfo CreateBatchPartInfo(int batchIndex, SyncSet set, string fileName, bool isLastBatch)
        {
            BatchPartInfo bpi = null;

            // Create a batch part
            // The batch part creation process will serialize the changesSet to the disk

            // Serialize the file !
            Serialize(set.GetContainerSet(), fileName);

            bpi = new BatchPartInfo { FileName = fileName };

            bpi.Index = batchIndex;
            bpi.IsLastBatch = isLastBatch;

            // Even if the set is empty (serialized on disk), we should retain the tables names
            if (set != null)
                bpi.Tables = set.Tables.Select(t => (t.TableName, t.SchemaName)).ToArray();

            return bpi;
        }

    }
}
