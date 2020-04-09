

using Dotmim.Sync.Serialization;
using System;
using System.Collections.Generic;
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
        /// <summary>
        /// Loads the batch file and import the rows in a SyncSet instance
        /// </summary>
        public async Task LoadBatchAsync(SyncSet sanitizedSchema, string directoryFullPath, BaseOrchestrator orchestrator = null)
        {
            if (this.Data != null)
                return;

            if (string.IsNullOrEmpty(this.FileName))
                return;

            // Clone the schema to get a unique instance
            var set = sanitizedSchema.Clone();

            // Get a Batch part, and deserialise the file into a the BatchPartInfo Set property
            var data = await DeserializeAsync(this.FileName, directoryFullPath, orchestrator);

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


        private static async Task<ContainerSet> DeserializeAsync(string fileName, string directoryFullPath, BaseOrchestrator orchestrator = null)
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
                ContainerSet set = null;

                if (orchestrator != null)
                {
                    var interceptorArgs = new DeserializingSetArgs(orchestrator.GetContext(), fs, fileName, directoryFullPath);
                    await orchestrator.InterceptAsync(interceptorArgs, default);
                    set = interceptorArgs.Result;
                }

                if (set == null)
                    set = await jsonConverter.DeserializeAsync(fs);

                return set;
            }
        }

        /// <summary>
        /// Serialize a container set instance
        /// </summary>
        private static async Task SerializeAsync(ContainerSet set, string fileName, string directoryFullPath, BaseOrchestrator orchestrator = null)
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
                byte[] serializedBytes = null;

                if (orchestrator != null)
                {
                    var interceptorArgs = new SerializingSetArgs(orchestrator.GetContext(), set, fileName, directoryFullPath);
                    await orchestrator.InterceptAsync(interceptorArgs, default);
                    serializedBytes = interceptorArgs.Result;
                }

                if (serializedBytes == null)
                    serializedBytes = await jsonConverter.SerializeAsync(set);


                f.Write(serializedBytes, 0, serializedBytes.Length);
            }
        }

        /// <summary>
        /// Create a new BPI, and serialize the changeset if not in memory
        /// </summary>
        internal static async Task<BatchPartInfo> CreateBatchPartInfoAsync(int batchIndex, SyncSet set, string fileName, string directoryFullPath, bool isLastBatch, BaseOrchestrator orchestrator = null)
        {
            BatchPartInfo bpi = null;

            // Create a batch part
            // The batch part creation process will serialize the changesSet to the disk

            // Serialize the file !
            await SerializeAsync(set.GetContainerSet(), fileName, directoryFullPath, orchestrator);

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
