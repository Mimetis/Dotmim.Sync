using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Dotmim.Sync.Batch
{
    /// <summary>
    /// Represents a Batch, containing a full or serialized change set
    /// </summary>
    public class BatchInfo
    {
        private string directoryName;
        private string directoryRoot;
        private SyncSet schema;

        /// <summary>
        /// Create a new BatchInfo, containing all BatchPartInfo
        /// </summary>
        public BatchInfo(bool isInMemory, SyncSet schema = null,string rootDirectory = null)
        {
            this.InMemory = isInMemory;
            this.schema = schema;

            // If not in memory, generate a directory name and initialize batch parts list
            if (!this.InMemory)
            {
                this.directoryRoot = rootDirectory;
                this.BatchPartsInfo = new List<BatchPartInfo>();
                // To simplify things, even if not used, just generate a random directory name for this batchinfo
                this.directoryName = string.Concat(DateTime.UtcNow.ToString("yyyy_MM_dd_ss"), Path.GetRandomFileName().Replace(".", ""));
            }
        }

        /// <summary>
        /// List of batch parts if not in memory
        /// </summary>
        public List<BatchPartInfo> BatchPartsInfo { get; set; }

        /// <summary>
        /// Is the batch parts are in memory
        /// If true, only one BPI
        /// If false, several serialized BPI
        /// </summary>
        public bool InMemory { get; set; }

        /// <summary>
        /// If in memory, return the in memory Dm
        /// </summary>
        public SyncSet InMemoryData { get; set; }


        /// <summary>
        /// Get the full path of the Batch directory
        /// </summary>
        /// <returns></returns>
        public string GetDirectoryFullPath()
        {
            if (this.InMemory)
                return null;

            return Path.Combine(this.directoryRoot, this.directoryName);
        }


        /// <summary>
        /// Check if this batchinfo has some data (in memory or not)
        /// </summary>
        /// <returns></returns>
        public bool HasData()
        {
            if (InMemory && InMemoryData != null && InMemoryData.HasTables && InMemoryData.HasRows)
                return true;

            if (!InMemory && BatchPartsInfo != null && BatchPartsInfo.Count > 0)
            {
                foreach (var bpi in BatchPartsInfo)
                {
                    bpi.LoadBatch(schema);

                    if (bpi.Data.HasRows)
                    {
                        bpi.Clear();
                        return true;
                    }

                    bpi.Clear();
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
            if (InMemory)
            {
                if (this.InMemoryData.HasTables)
                    yield return this.InMemoryData.Tables[tableName, schemaName];
            }
            else
            {
                foreach (var batchPartinInfo in this.BatchPartsInfo.OrderBy(bpi => bpi.Index))
                {
                    if (batchPartinInfo.Tables != null && batchPartinInfo.Tables.Contains((tableName, schemaName)))
                    {
                        batchPartinInfo.LoadBatch(schema);

                        // Get the table from the batchPartInfo
                        var batchTable = batchPartinInfo.Data.Tables.FirstOrDefault(tableName, schemaName);

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
        public void AddChanges(SyncSet changes, int batchIndex = 0, bool isLastBatch = true)
        {
            if (this.InMemory)
            {
                this.InMemoryData = changes;
            }
            else
            {
                var bpId = this.GenerateNewFileName(batchIndex.ToString());
                var fileName = Path.Combine(this.GetDirectoryFullPath(), bpId);
                var bpi = BatchPartInfo.CreateBatchPartInfo(batchIndex, changes, fileName, isLastBatch);

                // add the batchpartinfo tp the current batchinfo
                this.BatchPartsInfo.Add(bpi);

            }
        }

        /// <summary>
        /// generate a batch file name
        /// </summary>
        private string GenerateNewFileName(string batchIndex)
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


        /// <summary>
        /// try to delete the Batch tmp directory and all the files stored in it
        /// </summary>
        public void TryRemoveDirectory()
        {
            // Once we have applied all the batch, we can safely remove the temp dir and all it's files
            if (!this.InMemory && !string.IsNullOrEmpty(this.directoryRoot) && !string.IsNullOrEmpty(this.directoryName))
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
