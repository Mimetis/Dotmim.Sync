using Dotmim.Sync.Data;
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

        /// <summary>
        /// Create a new BatchInfo, containing all BatchPartInfo
        /// </summary>
        public BatchInfo(bool isInMemory, string rootDirectory)
        {
            this.BatchPartsInfo = new List<BatchPartInfo>();
            this.DirectoryRoot = rootDirectory;
            this.InMemory = isInMemory;

            // To simplify things, even if not used, just generate a random directory name for this batchinfo
            this.DirectoryName = string.Concat(DateTime.UtcNow.ToString("yyyy_MM_dd_ss"), Path.GetRandomFileName().Replace(".", ""));
        }

        /// <summary>
        /// Get the directory where all files are stored (if InMemory == false)
        /// </summary>
        public string DirectoryName { get; set; }

        /// <summary>
        /// Get the Directory full path
        /// </summary>
        public string DirectoryRoot { get; set; }

        /// <summary>
        /// Get the full path of the Batch directory
        /// </summary>
        /// <returns></returns>
        public string GetDirectoryFullPath() => Path.Combine(this.DirectoryRoot, this.DirectoryName);

        /// <summary>
        /// Is the batch parts are in memory
        /// If true, only one BPI
        /// If false, several serialized BPI
        /// </summary>
        public bool InMemory { get; set; }

        /// <summary>
        /// Get the current batch index (if InMemory == false)
        /// </summary>
        public int BatchIndex { get; set; }

        /// <summary>
        /// All Parts of the batch
        /// Each part is the size of download batch size
        /// </summary>
        public List<BatchPartInfo> BatchPartsInfo { get; set; }

        /// <summary>
        /// Get all parts containing this table
        /// Could be multiple parts, since the table may be spread across multiples files
        /// </summary>
        public IEnumerable<DmTable> GetTable(string tableName)
        {
            foreach (var batchPartinInfo in this.BatchPartsInfo)
            {
                var isLoadedFromDisk = false;

                if (batchPartinInfo.Tables != null && batchPartinInfo.Tables.Contains(tableName))
                {
                    // Batch not readed, so we deserialized the batch and get the table
                    if (batchPartinInfo.Set == null && !this.InMemory)
                    {
                        // Deserialize and Set DmSet in memory
                        batchPartinInfo.LoadBatch();

                        isLoadedFromDisk = true;
                    }

                    // return the table
                    if (batchPartinInfo.Set.Tables.Contains(tableName))
                    {
                        yield return batchPartinInfo.Set.Tables[tableName];

                        if (isLoadedFromDisk)
                        {
                            batchPartinInfo.Set.Clear();
                            batchPartinInfo.Set = null;
                        }

                    }
                }
            }

        }

        /// <summary>
        /// Generate a new BatchPartInfo and add it to the current batchInfo
        /// </summary>
        public BatchPartInfo GenerateBatchInfo(int batchIndex, DmSet changesSet)
        {
            BatchPartInfo bpi;

            // Create a batch part
            // The batch part creation process will serialize the changesSet to the disk
            if (!this.InMemory)
            {
                var bpId = this.GenerateNewFileName(batchIndex.ToString());
                var fileName = Path.Combine(this.GetDirectoryFullPath(), bpId);

                bpi = BatchPartInfo.CreateBatchPartInfo(batchIndex, changesSet, fileName, false, false);
            }
            else
            {
                bpi = BatchPartInfo.CreateBatchPartInfo(batchIndex, changesSet, null, true, true);
            }

            // add the batchpartinfo tp the current batchinfo
            this.BatchPartsInfo.Add(bpi);

            return bpi;
        }


        /// <summary>
        /// generate a batch file name
        /// </summary>
        public string GenerateNewFileName(string batchIndex)
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
            // Delete folders before deleting batch parts
            if (deleteFolder)
                this.TryRemoveDirectory();

            foreach (var bpi in this.BatchPartsInfo)
                bpi.Clear();

            this.BatchPartsInfo.Clear();

        }
    }
}
