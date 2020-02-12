

using Dotmim.Sync.Serialization;
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
    [DataContract(Name = "batchpartinfo"), Serializable]
    public class BatchPartInfo
    {
        // TODO : Set the serializer to the one choosed by user

        /// <summary>
        /// Loads the batch file and import the rows in a SyncSet instance
        /// </summary>
        public void LoadBatch(SyncSet schema, string directoryFullPath)
        {

            if (string.IsNullOrEmpty(this.FileName))
                return;

            // Clone the schema to get a unique instance
            var set = schema.Clone();

            // Get a Batch part, and deserialise the file into a the BatchPartInfo Set property
            var data = Deserialize(this.FileName, directoryFullPath);

            // Import data in a typed Set
            set.ImportContainerSet(data, true);

            this.Data = set;
        }

        /// <summary>
        /// Delete the SyncSet affiliated with the BatchPart, if exists.
        /// </summary>
        public void Clear()
        {
            if (this.Data != null)
                this.Data.Dispose();
        }

        [DataMember(Name = "file", IsRequired = true, Order = 1)]
        public string FileName { get; set; }

        [DataMember(Name = "index", IsRequired = true, Order = 2)]
        public int Index { get; set; }

        [DataMember(Name = "last", IsRequired = true, Order = 3)]
        public bool IsLastBatch { get; set; }

        /// <summary>
        /// Tables contained in the SyncSet (serialiazed or not)
        /// </summary>
        [DataMember(Name = "tables", IsRequired = true, Order = 4)]
        public BatchPartTableInfo[] Tables { get; set; }

        /// <summary>
        /// Get a SyncSet corresponding to this batch part info
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Data { get; set; }


        public BatchPartInfo()
        {
        }


        private static ContainerSet Deserialize(string fileName, string directoryFullPath)
        {
            if (string.IsNullOrEmpty(fileName))
                throw new ArgumentNullException(fileName);
            if (string.IsNullOrEmpty(directoryFullPath))
                throw new ArgumentNullException(directoryFullPath);

            var fullPath = Path.Combine(directoryFullPath, fileName);

            if (!File.Exists(fullPath))
                throw new MissingFileException(fullPath);

            var jsonConverter = new JsonConverter<ContainerSet>();
            using (var fs = new FileStream(fullPath, FileMode.Open, FileAccess.Read))
            {
                return jsonConverter.Deserialize(fs);
            }
        }

        /// <summary>
        /// Serialize a container set instance
        /// </summary>
        internal static void Serialize(ContainerSet set, string fileName, string directoryFullPath)
        {
            if (set == null)
                return;

            var fullPath = Path.Combine(directoryFullPath, fileName);

            var fi = new FileInfo(fullPath);

            if (!Directory.Exists(fi.Directory.FullName))
                Directory.CreateDirectory(fi.Directory.FullName);

            // Serialize on disk.
            var jsonConverter = new JsonConverter<ContainerSet>();

            using (var f = new FileStream(fullPath, FileMode.CreateNew, FileAccess.ReadWrite))
            {
                var bytes = jsonConverter.Serialize(set);
                f.Write(bytes, 0, bytes.Length);
            }
        }


        /// <summary>
        /// Create a new BPI, and serialize the changeset if not in memory
        /// </summary>
        internal static BatchPartInfo CreateBatchPartInfo2(int batchIndex, SyncSet set, string fileName, bool isLastBatch)
        {
            BatchPartInfo bpi = null;

            // Create a batch part
            // The batch part creation process will serialize the changesSet to the disk
            bpi = new BatchPartInfo { FileName = fileName };

            bpi.Index = batchIndex;
            bpi.IsLastBatch = isLastBatch;

            // Even if the set is empty (serialized on disk), we should retain the tables names
            if (set != null)
                bpi.Tables = set.Tables.Select(t => new BatchPartTableInfo(t.TableName, t.SchemaName)).ToArray();

            return bpi;
        }

        /// <summary>
        /// Create a new BPI, and serialize the changeset if not in memory
        /// </summary>
        internal static BatchPartInfo CreateBatchPartInfo(int batchIndex, SyncSet set, string fileName, string directoryFullPath, bool isLastBatch)
        {
            BatchPartInfo bpi = null;

            // Create a batch part
            // The batch part creation process will serialize the changesSet to the disk

            // Serialize the file !
            Serialize(set.GetContainerSet(), fileName, directoryFullPath);

            bpi = new BatchPartInfo { FileName = fileName };

            bpi.Index = batchIndex;
            bpi.IsLastBatch = isLastBatch;

            // Even if the set is empty (serialized on disk), we should retain the tables names
            if (set != null)
                bpi.Tables = set.Tables.Select(t => new BatchPartTableInfo(t.TableName, t.SchemaName)).ToArray();

            return bpi;
        }

    }
}
