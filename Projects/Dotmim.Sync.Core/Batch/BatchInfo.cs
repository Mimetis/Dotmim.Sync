using Dotmim.Sync.DatabaseStringParsers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;

namespace Dotmim.Sync.Batch
{
    /// <summary>
    /// Represents a collection of files serialized in a directory, and containing all the batch parts.
    /// </summary>
    [DataContract(Name = "bi"), Serializable]
    public class BatchInfo
    {

        /// <summary>
        /// Initializes a new instance of the <see cref="BatchInfo"/> class.
        /// <inheritdoc cref="BatchInfo"/>
        /// By default, the batch directory is the user temp directory.
        /// </summary>
        public BatchInfo()
        {
            this.BatchPartsInfo = [];
            this.DirectoryRoot = SyncOptions.GetDefaultUserBatchDirectory();
#if NET6_0_OR_GREATER
            this.DirectoryName = string.Concat(DateTime.UtcNow.ToString("yyyy_MM_dd_ss", CultureInfo.InvariantCulture), Path.GetRandomFileName().Replace(".", string.Empty, SyncGlobalization.DataSourceStringComparison));
#else
            this.DirectoryName = string.Concat(DateTime.UtcNow.ToString("yyyy_MM_dd_ss", CultureInfo.InvariantCulture), Path.GetRandomFileName().Replace(".", string.Empty));
#endif
        }

        /// <inheritdoc cref="BatchInfo"/>
        public BatchInfo(string rootDirectory, string directoryName = null, string info = null)
            : this()
        {
            // We need to create a change table set, containing table with columns not readonly
            this.DirectoryRoot = rootDirectory;

#if NET6_0_OR_GREATER
            var randomName = string.Concat(DateTime.UtcNow.ToString("yyyy_MM_dd_ss", CultureInfo.InvariantCulture), Path.GetRandomFileName().Replace(".", string.Empty, SyncGlobalization.DataSourceStringComparison));
#else
            var randomName = string.Concat(DateTime.UtcNow.ToString("yyyy_MM_dd_ss", CultureInfo.InvariantCulture), Path.GetRandomFileName().Replace(".", string.Empty));
#endif
            randomName = string.IsNullOrEmpty(info) ? randomName : $"{info}_{randomName}";
            this.DirectoryName = string.IsNullOrEmpty(directoryName) ? randomName : directoryName;
        }

        /// <summary>
        /// generate a batch file name.
        /// </summary>
        public static string GenerateNewFileName(string batchIndex, string tableName, string extension, string info)
        {
#if NET6_0_OR_GREATER
            var randomFileName = Path.GetRandomFileName().Replace(".", "_", SyncGlobalization.DataSourceStringComparison);
#else
            var randomFileName = Path.GetRandomFileName().Replace(".", "_");
#endif
            info = string.IsNullOrEmpty(info) ? string.Empty : $"_{info}";

            batchIndex = string.IsNullOrEmpty(batchIndex)
                ? $"0001"
                : batchIndex.Length switch
                {
                    1 => $"000{batchIndex}",
                    2 => $"00{batchIndex}",
                    3 => $"0{batchIndex}",
                    4 => $"{batchIndex}",
                    _ => throw new OverflowException("too much batches !!! You have reached the maximum amount of batch files generated. You need to increase the batch file value from the SyncOptions instance"),
                };

            return $"{tableName}_{batchIndex}{info}_{randomFileName}.{extension}";
        }

        /// <summary>
        /// Gets or Sets directory name.
        /// </summary>
        [DataMember(Name = "dirname", IsRequired = false, EmitDefaultValue = false, Order = 1)]
        public string DirectoryName { get; set; }

        /// <summary>
        /// Gets or sets directory root.
        /// </summary>
        [DataMember(Name = "dir", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public string DirectoryRoot { get; set; }

        /// <summary>
        /// Gets or sets server timestamp.
        /// </summary>
        [DataMember(Name = "ts", IsRequired = false, Order = 3)]
        public long Timestamp { get; set; }

        /// <summary>
        /// Gets or sets list of batch parts.
        /// </summary>
        [DataMember(Name = "parts", IsRequired = false, EmitDefaultValue = false, Order = 4)]
        public IList<BatchPartInfo> BatchPartsInfo { get; set; }

        /// <summary>
        /// Gets or Sets the rows count contained in the batch info.
        /// </summary>
        [DataMember(Name = "count", IsRequired = true, Order = 5)]
        public int RowsCount { get; set; }

        /// <summary>
        /// Gets or Sets the Serialization Factory Key used to serialize this batch info.
        /// </summary>
        [DataMember(Name = "ser", IsRequired = false, EmitDefaultValue = false, Order = 6)]
        public string SerializerFactoryKey { get; set; }

        /// <summary>
        /// Get the full path of the Batch directory.
        /// </summary>
        public string GetDirectoryFullPath() => Path.Combine(this.DirectoryRoot, this.DirectoryName);

        /// <summary>
        /// Check if this batchinfo has some data.
        /// </summary>
        public bool HasData()
        {
            if (this.BatchPartsInfo != null && this.BatchPartsInfo.Count > 0)
            {
                var rowsCount = this.BatchPartsInfo.Sum(bpi => bpi.RowsCount);

                return rowsCount > 0;
            }

            return false;
        }

        /// <summary>
        /// Generate a new full path to store a new batch part info file.
        /// </summary>
        public (string FullPath, string FileName) GetNewBatchPartInfoPath(SyncTable syncTable, int batchIndex, string extension, string info)
        {
            // parsing the table name
            var tableBuilder = new TableParser(syncTable.GetFullName());

            var tableName = tableBuilder.NormalizedFullName;
            var fileName = GenerateNewFileName(batchIndex.ToString(CultureInfo.InvariantCulture), tableName, extension, info);
            var fullPath = Path.Combine(this.GetDirectoryFullPath(), fileName);
            return (fullPath, fileName);
        }

        /// <summary>
        /// Gets the full path + file name for a given batch part info.
        /// </summary>
        public string GetBatchPartInfoFullPath(BatchPartInfo batchPartInfo)
        {
            if (batchPartInfo == null)
                return null;

            var fullPath = Path.Combine(this.GetDirectoryFullPath(), batchPartInfo.FileName);

            return fullPath;
        }

        /// <summary>
        /// Check if this batchinfo has some data.
        /// </summary>
        public bool HasData(string tableName, string schemaName)
        {
            if (this.BatchPartsInfo != null && this.BatchPartsInfo.Count > 0)
            {
                // fake batchpartinfo just for comparison
                var tmpBpi = new BatchPartInfo { TableName = tableName, SchemaName = schemaName };

                var bptis = this.BatchPartsInfo.Where(bpi => bpi.EqualsByName(tmpBpi));

                return bptis == null ? false : bptis.Sum(bpti => bpti.RowsCount) > 0;
            }

            return false;
        }

        /// <summary>
        /// Get all batch part for 1 particular table.
        /// </summary>
        public IEnumerable<BatchPartInfo> GetBatchPartsInfos(SyncTable syncTable)
        {
            return syncTable == null ? [] : this.GetBatchPartsInfos(syncTable.TableName, syncTable.SchemaName);
        }

        /// <summary>
        /// Get all batch part for 1 particular table.
        /// </summary>
        public IEnumerable<BatchPartInfo> GetBatchPartsInfos(string tableName, string schemaName = default)
        {
            if (this.BatchPartsInfo == null)
                return [];

            // fake for comparison
            var tmpBpi = new BatchPartInfo { TableName = tableName, SchemaName = schemaName };

            IEnumerable<BatchPartInfo> bpiTables = null;

            bpiTables = this.BatchPartsInfo.Where(bpi => bpi.RowsCount > 0 && bpi.EqualsByName(tmpBpi)).OrderBy(bpi => bpi.Index);

            return bpiTables ?? [];
        }

        /// <summary>
        /// Ensure the last batch part has the correct IsLastBatch flag.
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

        // public async Task SaveBatchPartInfoAsync(BatchPartInfo batchPartInfo, SyncTable syncTable)
        // {
        //    using var localSerializer = new LocalJsonSerializer();

        // // Get full path of my batchpartinfo
        //    var fullPath = this.GetBatchPartInfoFullPath(batchPartInfo).FullPath;

        // if (!File.Exists(fullPath))
        //        return;

        // File.Delete(fullPath);

        // // open the file and write table header
        //    await localSerializer.OpenFileAsync(fullPath, syncTable).ConfigureAwait(false);

        // foreach (var row in syncTable.Rows)
        //        await localSerializer.WriteRowToFileAsync(row, syncTable).ConfigureAwait(false);
        // }

        /// <summary>
        /// try to delete the Batch tmp directory and all the files stored in it.
        /// </summary>
        public void TryRemoveDirectory()
        {
            // Once we have applied all the batch, we can safely remove the temp dir and all it's files
            if (!string.IsNullOrEmpty(this.DirectoryRoot) && !string.IsNullOrEmpty(this.DirectoryName))
            {
                var tmpDirectory = new DirectoryInfo(this.GetDirectoryFullPath());

                if (!tmpDirectory.Exists)
                    return;

                try
                {
                    tmpDirectory.Delete(true);
                }

                // do nothing here
                catch
                {
                }
            }
        }

        /// <summary>
        /// Get the full path of the Batch directory and number of rows contained in the batchinfo.
        /// </summary>
        public override string ToString() => $"{this.GetDirectoryFullPath()} [{this.RowsCount}]";
    }
}