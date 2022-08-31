

using Dotmim.Sync.Builders;
using Dotmim.Sync.Serialization;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
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
            this.BatchPartsInfo = new List<BatchPartInfo>();
            this.DirectoryName = string.Concat(DateTime.UtcNow.ToString("yyyy_MM_dd_ss"), Path.GetRandomFileName().Replace(".", ""));
        }

        /// <summary>
        /// Create a new BatchInfo, containing all BatchPartInfo
        /// </summary>
        public BatchInfo(string rootDirectory, string directoryName = null) : this()
        {
            // We need to create a change table set, containing table with columns not readonly
            this.DirectoryRoot = rootDirectory;
            this.DirectoryName = string.IsNullOrEmpty(directoryName) ? string.Concat(DateTime.UtcNow.ToString("yyyy_MM_dd_ss"), Path.GetRandomFileName().Replace(".", "")) : directoryName;
        }

        /// <summary>
        /// Gets or Sets directory name
        /// </summary>
        [DataMember(Name = "dirname", IsRequired = false, EmitDefaultValue = false, Order = 1)]
        public string DirectoryName { get; set; }

        /// <summary>
        /// Gets or sets directory root
        /// </summary>
        [DataMember(Name = "dir", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public string DirectoryRoot { get; set; }

        /// <summary>
        /// Gets or sets server timestamp
        /// </summary>
        [DataMember(Name = "ts", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public long Timestamp { get; set; }

        /// <summary>
        /// List of batch parts
        /// </summary>
        [DataMember(Name = "parts", IsRequired = false, EmitDefaultValue = false, Order = 4)]
        public List<BatchPartInfo> BatchPartsInfo { get; set; }

        /// <summary>
        /// Gets or Sets the rows count contained in the batch info
        /// </summary>
        [DataMember(Name = "count", IsRequired = true, Order = 5)]
        public int RowsCount { get; set; }

        /// <summary>
        /// Gets or Sets the Serialization Factory Key used to serialize this batch info
        /// </summary>
        [DataMember(Name = "ser", IsRequired = false, EmitDefaultValue = false, Order = 6)]
        public string SerializerFactoryKey { get; set; }


        /// <summary>
        /// Only Used for Backward compatibility for version < 0.9.6
        /// </summary>
        [DataMember(Name = "schema", IsRequired = false, EmitDefaultValue = false, Order = 7)]
        public SyncSet SanitizedSchema { get; set; }
        /// <summary>
        /// Get the full path of the Batch directory
        /// </summary>
        /// <returns></returns>
        public string GetDirectoryFullPath() => Path.Combine(this.DirectoryRoot, this.DirectoryName);


        /// <summary>
        /// Create batch info directory
        /// </summary>
        public void CreateDirectory()
        {
            if (!Directory.Exists(this.GetDirectoryFullPath()))
                Directory.CreateDirectory(this.GetDirectoryFullPath());
        }

        /// <summary>
        /// Check if this batchinfo has some data
        /// </summary>
        public bool HasData()
        {
            if (BatchPartsInfo != null && BatchPartsInfo.Count > 0)
            {
                var rowsCount = BatchPartsInfo.Sum(bpi => bpi.RowsCount);

                return rowsCount > 0;
            }

            return false;
        }

        /// <summary>
        /// Generate a new full path to store a new batch part info file
        /// </summary>
        public (string FullPath, string FileName) GetNewBatchPartInfoPath(SyncTable syncTable, int batchIndex, string extension)
        {
            var tableName = ParserName.Parse(syncTable).Unquoted().Schema().Normalized().ToString();
            var fileName = GenerateNewFileName(batchIndex.ToString(), tableName, extension);
            var fullPath = Path.Combine(this.GetDirectoryFullPath(), fileName);
            return (fullPath, fileName);
        }

        public (string FullPath, string FileName) GetBatchPartInfoPath(BatchPartInfo batchPartInfo)
        {
            if (BatchPartsInfo == null)
                return (default, default);

            var fullPath = Path.Combine(this.GetDirectoryFullPath(), batchPartInfo.FileName);

            return (fullPath, batchPartInfo.FileName);

        }

        /// <summary>
        /// Check if this batchinfo has some data
        /// </summary>
        public bool HasData(string tableName, string schemaName)
        {
            if (BatchPartsInfo != null && BatchPartsInfo.Count > 0)
            {
                var tableInfo = new BatchPartTableInfo(tableName, schemaName);

                var bptis = BatchPartsInfo.SelectMany(bpi => bpi.Tables.Where(t => t.EqualsByName(tableInfo)));

                if (bptis == null)
                    return false;


                return bptis.Sum(bpti => bpti.RowsCount) > 0;
            }

            return false;
        }

        /// <summary>
        /// Get all batch part for 1 particular table
        /// </summary>
        public IEnumerable<BatchPartInfo> GetBatchPartsInfo(SyncTable syncTable)
        {
            if (syncTable == null) return Enumerable.Empty<BatchPartInfo>();

            if (BatchPartsInfo == null) return Enumerable.Empty<BatchPartInfo>();

            // Get all batch part
            var bpiTables = this.BatchPartsInfo.Where(bpi => bpi.RowsCount > 0
                                            && bpi.Tables.Any(t => t.EqualsByName(new BatchPartTableInfo(syncTable.TableName, syncTable.SchemaName)))).OrderBy(t => t.Index);

            if (bpiTables == null) return Enumerable.Empty<BatchPartInfo>();

            return bpiTables;
        }

        /// <summary>
        /// Get all batch part for 1 particular table
        /// </summary>
        public IEnumerable<BatchPartInfo> GetBatchPartsInfo(string tableName, string schemaName = default) =>
            GetBatchPartsInfo(new SyncTable(tableName, schemaName));

        /// <summary>
        /// Ensure the last batch part has the correct IsLastBatch flag
        /// </summary>
        public void EnsureLastBatch()
        {
            if (this.BatchPartsInfo.Count == 0)
                return;

            // get last index
            var maxIndex = this.BatchPartsInfo.Max(tBpi => tBpi.Index);

            // Set corret last batch 
            foreach (var bpi in this.BatchPartsInfo)
                bpi.IsLastBatch = bpi.Index == maxIndex;
        }

        /// <summary>
        /// generate a batch file name
        /// </summary>
        public static string GenerateNewFileName(string batchIndex, string tableName, string extension)
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

            return $"{tableName}_{batchIndex}_{Path.GetRandomFileName().Replace(".", "_")}.{extension}";
        }



        //public async Task SaveBatchPartInfoAsync(BatchPartInfo batchPartInfo, SyncTable syncTable)
        //{
        //     var localSerializer = new LocalJsonSerializer();

        //    // Get full path of my batchpartinfo
        //    var fullPath = this.GetBatchPartInfoPath(batchPartInfo).FullPath;

        //    if (!File.Exists(fullPath))
        //        return;

        //    File.Delete(fullPath);

        //    // open the file and write table header
        //    await localSerializer.OpenFileAsync(fullPath, syncTable).ConfigureAwait(false);

        //    foreach (var row in syncTable.Rows)
        //        await localSerializer.WriteRowToFileAsync(row, syncTable).ConfigureAwait(false);

        //    // Close file
        //    await localSerializer.CloseFileAsync(fullPath, syncTable).ConfigureAwait(false);

        //}

        /// <summary>
        /// try to delete the Batch tmp directory and all the files stored in it
        /// </summary>
        public void TryRemoveDirectory()
        {
            // Once we have applied all the batch, we can safely remove the temp dir and all it's files
            if (!string.IsNullOrEmpty(this.DirectoryRoot) && !string.IsNullOrEmpty(this.DirectoryName))
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
        }

    }
}
