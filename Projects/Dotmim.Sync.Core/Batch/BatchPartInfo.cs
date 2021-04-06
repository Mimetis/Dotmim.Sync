

using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
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
        public async Task LoadBatchAsync(SyncSet sanitizedSchema, string directoryFullPath, ISerializerFactory serializerFactory = default, BaseOrchestrator orchestrator = null)
        {
            if (this.Data != null)
                return;

            if (string.IsNullOrEmpty(this.FileName))
                return;

            // Clone the schema to get a unique instance
            var set = sanitizedSchema.Clone();

            // Get a Batch part, and deserialise the file into a the BatchPartInfo Set property
            var data = await DeserializeAsync(this.FileName, directoryFullPath, serializerFactory, orchestrator);

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

            this.Data = null;
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
        /// Tables contained rows count
        /// </summary>
        [DataMember(Name = "rc", IsRequired = false, Order = 5)]
        public int RowsCount { get; set; }

        /// <summary>
        /// Get a SyncSet corresponding to this batch part info
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Data { get; set; }

        /// <summary>
        /// Gets or Sets the serialized type
        /// </summary>
        [IgnoreDataMember]
        public Type SerializedType { get; set; }


        public BatchPartInfo()
        {
        }


        private async Task<ContainerSet> DeserializeAsync(string fileName, string directoryFullPath, ISerializerFactory serializerFactory = default, BaseOrchestrator orchestrator = null)
        {
            if (string.IsNullOrEmpty(fileName))
                throw new ArgumentNullException(fileName);
            if (string.IsNullOrEmpty(directoryFullPath))
                throw new ArgumentNullException(directoryFullPath);

            var fullPath = Path.Combine(directoryFullPath, fileName);

            if (!File.Exists(fullPath))
                throw new MissingFileException(fullPath);

            // backward compatibility
            if (serializerFactory == default)
                serializerFactory = SerializersCollection.JsonSerializer;

            // backward compatibility
            if (this.SerializedType == default)
                this.SerializedType = typeof(ContainerSet);

            Debug.WriteLine($"Deserialize file {fileName}");

            using var fs = new FileStream(fullPath, FileMode.Open, FileAccess.Read);

            ContainerSet set = null;

            if (orchestrator != null)
            {
                var interceptorArgs = new DeserializingSetArgs(orchestrator.GetContext(), fs, serializerFactory, fileName, directoryFullPath);
                await orchestrator.InterceptAsync(interceptorArgs, default);
                set = interceptorArgs.Result;
            }


            if (set == null)
            {
                if (this.SerializedType == typeof(ContainerSet))
                {
                    var serializer = serializerFactory.GetSerializer<ContainerSet>();
                    set = await serializer.DeserializeAsync(fs);
                }
                else
                {
                    var serializer = serializerFactory.GetSerializer<ContainerSetBoilerPlate>();
                    var jobject = await serializer.DeserializeAsync(fs);
                    set = jobject.Changes;
                }
            }

            return set;
        }

        /// <summary>
        /// Serialize a container set instance
        /// </summary>
        private static async Task SerializeAsync(ContainerSet set, string fileName, string directoryFullPath, ISerializerFactory serializerFactory = default, BaseOrchestrator orchestrator = null)
        {
            if (set == null)
                return;

            var fullPath = Path.Combine(directoryFullPath, fileName);

            var fi = new FileInfo(fullPath);

            if (!Directory.Exists(fi.Directory.FullName))
                Directory.CreateDirectory(fi.Directory.FullName);

            if (serializerFactory == default)
                serializerFactory = SerializersCollection.JsonSerializer;

            var serializer = serializerFactory.GetSerializer<ContainerSet>();

            using var f = new FileStream(fullPath, FileMode.CreateNew, FileAccess.ReadWrite);

            byte[] serializedBytes = null;

            if (orchestrator != null)
            {
                var interceptorArgs = new SerializingSetArgs(orchestrator.GetContext(), set, serializerFactory, fileName, directoryFullPath);
                await orchestrator.InterceptAsync(interceptorArgs, default);
                serializedBytes = interceptorArgs.Result;
            }

            if (serializedBytes == null)
                serializedBytes = await serializer.SerializeAsync(set);


            f.Write(serializedBytes, 0, serializedBytes.Length);

            //await f.FlushAsync();
        }

        /// <summary>
        /// Create a new BPI, and serialize the changeset if not in memory
        /// </summary>
        internal static async Task<BatchPartInfo> CreateBatchPartInfoAsync(int batchIndex, SyncSet set, string fileName, string directoryFullPath, bool isLastBatch, ISerializerFactory serializerFactory = default, BaseOrchestrator orchestrator = null)
        {
            BatchPartInfo bpi = null;

            // Create a batch part
            // The batch part creation process will serialize the changesSet to the disk

            // Serialize the file !
            await SerializeAsync(set.GetContainerSet(), fileName, directoryFullPath, serializerFactory, orchestrator);

            bpi = new BatchPartInfo { FileName = fileName };
            bpi.Index = batchIndex;
            bpi.IsLastBatch = isLastBatch;

            // Even if the set is empty (serialized on disk), we should retain the tables names
            if (set != null)
            {
                bpi.Tables = set.Tables.Select(t => new BatchPartTableInfo(t.TableName, t.SchemaName, t.Rows.Count)).ToArray();
                bpi.RowsCount = set.Tables.Sum(t => t.Rows.Count);
            }

            return bpi;
        }

    }

    /// <summary>
    /// Boiler plate for backward compatibility with HttpMessageSendChangesResponse
    /// </summary>
    [DataContract(Name = "changesres"), Serializable]
    public class ContainerSetBoilerPlate
    {
        public ContainerSetBoilerPlate() { }

        /// <summary>
        /// Gets the BatchParInfo send from the server 
        /// </summary>
        [DataMember(Name = "changes", IsRequired = true, Order = 7)]
        public ContainerSet Changes { get; set; } // BE CAREFUL : Order is coming from "HttpMessageSendChangesResponse"

    }
}
