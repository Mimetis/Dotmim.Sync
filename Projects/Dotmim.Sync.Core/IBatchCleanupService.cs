using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Service for cleaning up orphaned batch directories based on retention policies.
    /// Helps prevent disk space issues from failed sync sessions and network interruptions.
    /// </summary>
    public interface IBatchCleanupService
    {
        
        /// <summary>
        /// Cleans up batch directories older than the specified retention period.
        /// Parses timestamps from directory names to determine age.
        /// </summary>
        /// <param name="options">The sync options we can get the batchDirectory from</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Number of directories cleaned up</returns>
        Task<int> CleanupExpiredBatchesAsync(SyncOptions options, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Cleans up batch directories older than the specified retention period.
        /// Parses timestamps from directory names to determine age.
        /// </summary>
        /// <param name="options">The sync options we can get the batchDirectory from</param>
        /// <param name="retentionPeriod">Allows to override the retention period configured in SyncOptionsBatchRetentionPeriod.  Age threshold for cleanup (directories older than this will be removed)</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Number of directories cleaned up</returns>
        Task<int> CleanupExpiredBatchesAsync(SyncOptions options, TimeSpan retentionPeriod, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Cleans up batch directories older than the specified retention period.
        /// Parses timestamps from directory names to determine age.
        /// </summary>
        /// <param name="batchDirectory">Root batch directory path</param>
        /// <param name="retentionPeriod">Age threshold for cleanup (directories older than this will be removed)</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Number of directories cleaned up</returns>
        Task<int> CleanupExpiredBatchesAsync(string batchDirectory, TimeSpan retentionPeriod, CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets a list of batch directories that are eligible for cleanup based on retention period.
        /// Does not perform actual cleanup, only identifies candidates.
        /// </summary>
        /// <param name="options">The sync options we can get the batchDirectory from</param>
        /// <param name="retentionPeriod">Age threshold for cleanup eligibility</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>List of directory paths eligible for cleanup</returns>
        Task<IList<string>> GetExpiredBatchDirectoriesAsync(SyncOptions options, TimeSpan retentionPeriod, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Gets a list of batch directories that are eligible for cleanup based on retention period.
        /// Does not perform actual cleanup, only identifies candidates.
        /// </summary>
        /// <param name="batchDirectory">Root batch directory path</param>
        /// <param name="retentionPeriod">Age threshold for cleanup eligibility</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>List of directory paths eligible for cleanup</returns>
        Task<IList<string>> GetExpiredBatchDirectoriesAsync(string batchDirectory, TimeSpan retentionPeriod, CancellationToken cancellationToken = default);

    }
}