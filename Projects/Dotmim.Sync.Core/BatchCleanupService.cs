using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Default implementation of IBatchCleanupService for cleaning up orphaned batch directories.
    /// </summary>
    public class BatchCleanupService : IBatchCleanupService
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="options"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public Task<int> CleanupExpiredBatchesAsync(SyncOptions options, CancellationToken cancellationToken = default)
        {
            return this.CleanupExpiredBatchesAsync(options.BatchDirectory, options.BatchRetentionPeriod, cancellationToken);
        }

        /// <inheritdoc />
        public Task<int> CleanupExpiredBatchesAsync(SyncOptions options, TimeSpan retentionPeriod,
            CancellationToken cancellationToken = default)
        {
            return this.CleanupExpiredBatchesAsync(options.BatchDirectory, retentionPeriod, cancellationToken);
        }

        /// <inheritdoc/>
        public async Task<int> CleanupExpiredBatchesAsync(string batchDirectory, TimeSpan retentionPeriod, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(batchDirectory))
                throw new ArgumentException("Batch directory cannot be null or empty.", nameof(batchDirectory));

            if (!Directory.Exists(batchDirectory))
                return 0;

            if (retentionPeriod == TimeSpan.Zero)
                return 0; // Time-based cleanup disabled

            var expiredDirectories = await GetExpiredBatchDirectoriesAsync(batchDirectory, retentionPeriod, cancellationToken).ConfigureAwait(false);

            int cleanedCount = 0;

            foreach (var directory in expiredDirectories)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    if (Directory.Exists(directory))
                    {
                        Directory.Delete(directory, recursive: true);
                        cleanedCount++;
                    }
                }
                catch (Exception)
                {
                    // Log if needed, but don't fail the entire operation for one directory
                    // Individual directory cleanup failures should not break the batch cleanup
                }
            }

            return cleanedCount;
        }

        /// <inheritdoc />
        public Task<IList<string>> GetExpiredBatchDirectoriesAsync(SyncOptions options, TimeSpan retentionPeriod,
            CancellationToken cancellationToken = default)
        {
            return this.GetExpiredBatchDirectoriesAsync(options.BatchDirectory, retentionPeriod, cancellationToken);
        }

        /// <inheritdoc/>
        public async Task<IList<string>> GetExpiredBatchDirectoriesAsync(string batchDirectory, TimeSpan retentionPeriod, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(batchDirectory))
                throw new ArgumentException("Batch directory cannot be null or empty.", nameof(batchDirectory));

            if (!Directory.Exists(batchDirectory))
                return new List<string>();

            if (retentionPeriod == TimeSpan.Zero)
                return new List<string>(); // Time-based cleanup disabled

            // Calculate cutoff timestamp and format it as yyyyMMddHHmm
            var cutoffTime = DateTime.UtcNow - retentionPeriod;
            var cutoffTimestamp = cutoffTime.ToString("yyyyMMddHHmm", CultureInfo.InvariantCulture);

            return await Task.Run(() =>
            {
                try
                {
                    return Directory.EnumerateDirectories(batchDirectory)
                        .Where(dir =>
                        {
                            var directoryName = Path.GetFileName(dir);
                            // Directory must start with yyyyMMddHHmm format and be older than cutoff
                            return directoryName.Length >= 12 &&
                                   directoryName.Substring(0, 12).All(char.IsDigit) &&
                                   string.Compare(directoryName.Substring(0, 12), cutoffTimestamp, StringComparison.Ordinal) <= 0;
                        })
                        .OrderBy(Path.GetFileName)
                        .ToList();
                }
                catch (Exception)
                {
                    // Return empty list if directory enumeration fails
                    return new List<string>();
                }
            }, cancellationToken).ConfigureAwait(false);
        }
    }
}