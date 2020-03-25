

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace Dotmim.Sync.Batch
{
    /// <summary>
    /// Represents a Batch, containing a full or serialized change set
    /// </summary>
    [DataContract(Name = "bi"), Serializable]
    public class BatchInfo
    {

        /// <summary>
        /// Ctor for serializer
        /// </summary>
        public BatchInfo()
        {

        }

        /// <summary>
        /// Create a new BatchInfo, containing all BatchPartInfo
        /// </summary>
        public BatchInfo(bool isInMemory, SyncSet inSchema, string rootDirectory = null, string directoryName = null)
        {
            this.InMemory = isInMemory;

            // We need to create a change table set, containing table with columns not readonly
            foreach (var table in inSchema.Tables)
                DbSyncAdapter.CreateChangesTable(inSchema.Tables[table.TableName, table.SchemaName], this.SanitizedSchema);

            // If not in memory, generate a directory name and initialize batch parts list
            if (!this.InMemory)
            {
                this.DirectoryRoot = rootDirectory;
                this.BatchPartsInfo = new List<BatchPartInfo>();
                this.DirectoryName = string.IsNullOrEmpty(directoryName) ? string.Concat(DateTime.UtcNow.ToString("yyyy_MM_dd_ss"), Path.GetRandomFileName().Replace(".", "")) : directoryName;
            }
        }

        /// <summary>
        /// Internally setting schema
        /// </summary>
        [IgnoreDataMember]
        public SyncSet SanitizedSchema { get; set; } = new SyncSet();

        /// <summary>
        /// Is the batch parts are in memory
        /// If true, only one BPI
        /// If false, several serialized BPI
        /// </summary>
        [IgnoreDataMember]
        public bool InMemory { get; set; }

        /// <summary>
        /// If in memory, return the in memory Dm
        /// </summary>
        [IgnoreDataMember]
        public SyncSet InMemoryData { get; set; }

        /// <summary>
        /// Gets or Sets directory name
        /// </summary>
        [DataMember(Name = "dirname", IsRequired = true, Order = 1)]
        public string DirectoryName { get; set; }

        /// <summary>
        /// Gets or sets directory root
        /// </summary>
        [DataMember(Name = "dir", IsRequired = true, Order = 2)]
        public string DirectoryRoot { get; set; }

        /// <summary>
        /// Gets or sets server timestamp
        /// </summary>
        [DataMember(Name = "ts", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public long Timestamp { get; set; }

        /// <summary>
        /// List of batch parts if not in memory
        /// </summary>
        [DataMember(Name = "parts", IsRequired = true, Order = 4)]
        public List<BatchPartInfo> BatchPartsInfo { get; set; }


        /// <summary>
        /// Get the full path of the Batch directory
        /// </summary>
        /// <returns></returns>
        public string GetDirectoryFullPath()
        {
            if (this.InMemory)
                return null;

            return Path.Combine(this.DirectoryRoot, this.DirectoryName);
        }


        /// <summary>
        /// Check if this batchinfo has some data (in memory or not)
        /// </summary>
        public async Task<bool> HasDataAsync()
        {
            if (this.SanitizedSchema == null)
                throw new NullReferenceException("Batch info schema should not be null");

            if (InMemory && InMemoryData != null && InMemoryData.HasTables && InMemoryData.HasRows)
                return true;

            if (!InMemory && BatchPartsInfo != null && BatchPartsInfo.Count > 0)
            {
                foreach (var bpi in BatchPartsInfo)
                {
                    await bpi.LoadBatchAsync(this.SanitizedSchema, GetDirectoryFullPath()).ConfigureAwait(false);

                    var hasData = bpi.Data.HasRows;

                    bpi.Clear();
                    bpi.Data = null;

                    return hasData;
                }
            }

            return false;
        }


        /// <summary>
        /// Get all parts containing this table
        /// Could be multiple parts, since the table may be spread across multiples files
        /// </summary>
        public IEnumerable<SyncTable> GetTable(string tableName, string schemaName)
        {
            if (this.SanitizedSchema == null)
                throw new NullReferenceException("Batch info schema should not be null");

            var tableInfo = new BatchPartTableInfo(tableName, schemaName);

            if (InMemory)
            {
                if (this.InMemoryData.HasTables)
                    yield return this.InMemoryData.Tables[tableName, schemaName];
            }
            else
            {
                foreach (var batchPartinInfo in this.BatchPartsInfo.OrderBy(bpi => bpi.Index))
                {
                    if (batchPartinInfo.Tables != null && batchPartinInfo.Tables.Any(t => t == tableInfo))
                    {
                        batchPartinInfo.LoadBatchAsync(this.SanitizedSchema, GetDirectoryFullPath()).ConfigureAwait(false).GetAwaiter().GetResult();

                        // Get the table from the batchPartInfo
                        // generate a tmp SyncTable for 
                        var batchTable = batchPartinInfo.Data.Tables.FirstOrDefault(bt => bt == new SyncTable(tableName, schemaName));

                        if (batchTable != null)
                        {
                            yield return batchTable;

                            // Once read, clear it
                            batchPartinInfo.Data.Clear();
                            batchPartinInfo.Data = null;

                        }
                    }
                }
            }
        }


        /// <summary>
        /// Ensure the last batch part (if not in memory) has the correct IsLastBatch flag
        /// </summary>
        public void EnsureLastBatch()
        {
            if (this.InMemory)
                return;

            if (this.BatchPartsInfo.Count == 0)
                return;

            // get last index
            var maxIndex = this.BatchPartsInfo.Max(tBpi => tBpi.Index);

            // Set corret last batch 
            foreach (var bpi in this.BatchPartsInfo)
                bpi.IsLastBatch = bpi.Index == maxIndex;

        }

        /// <summary>
        /// Add changes to batch info.
        /// </summary>
        public async Task AddChangesAsync(SyncSet changes, int batchIndex = 0, bool isLastBatch = true)
        {
            if (this.InMemory)
            {
                this.InMemoryData = changes;
            }
            else
            {
                var bpId = this.GenerateNewFileName(batchIndex.ToString());
                //var fileName = Path.Combine(this.GetDirectoryFullPath(), bpId);
                var bpi = await BatchPartInfo.CreateBatchPartInfoAsync(batchIndex, changes, bpId, GetDirectoryFullPath(), isLastBatch).ConfigureAwait(false);

                // add the batchpartinfo tp the current batchinfo
                this.BatchPartsInfo.Add(bpi);

            }
        }

        /// <summary>
        /// generate a batch file name
        /// </summary>
        internal string GenerateNewFileName(string batchIndex)
        {
            if (batchIndex.Length == 1)
                batchIndex = $"000{batchIndex}";
            else if (batchIndex.Length == 2)
                batchIndex = $"00{batchIndex}";
            else if (batchIndex.Length == 3)
                batchIndex = $"0{batchIndex}";
            else if (batchIndex.Length == 4)
                batchIndex = $"{batchIndex}";
            else
                throw new OverflowException("too much batches !!!");

            return $"{batchIndex}_{Path.GetRandomFileName().Replace(".", "_")}.batch";
        }


        /// <summary>
        /// try to delete the Batch tmp directory and all the files stored in it
        /// </summary>
        public void TryRemoveDirectory()
        {
            // Once we have applied all the batch, we can safely remove the temp dir and all it's files
            if (!this.InMemory && !string.IsNullOrEmpty(this.DirectoryRoot) && !string.IsNullOrEmpty(this.DirectoryName))
            {
                var tmpDirectory = new DirectoryInfo(this.GetDirectoryFullPath());

                if (tmpDirectory == null || !tmpDirectory.Exists)
                    return;

                try
                {
                    tmpDirectory.Delete(true);
                }
                // do nothing here 
                catch { }
            }
        }


        /// <summary>
        /// Clear all batch parts info and try to delete tmp folder if needed
        /// </summary>
        public void Clear(bool deleteFolder)
        {
            if (this.InMemory)
            {
                this.InMemoryData.Dispose();
                return;
            }

            // Delete folders before deleting batch parts
            if (deleteFolder)
                this.TryRemoveDirectory();

            foreach (var bpi in this.BatchPartsInfo)
                bpi.Clear();

            this.BatchPartsInfo.Clear();

        }
    }
}
