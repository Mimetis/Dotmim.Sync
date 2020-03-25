using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Numerics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Base orchestrator.
    /// </summary>
    public interface IOrchestrator
    {
        /// <summary>
        /// Gets or Sets the provider used by this orchestrator
        /// </summary>
        CoreProvider Provider { get; set; }

        /// <summary>
        /// Gets or Sets the options used by this orchestrator
        /// </summary>
        SyncOptions Options { get; set; }

        /// <summary>
        /// Gets or Sets  the Setup used by this orchestrator
        /// </summary>
        SyncSetup Setup { get; set; }

        /// <summary>
        /// Gets or Sets  the scope name used by this orchestrator
        /// </summary>
        string ScopeName { get; set; }

        /// <summary>
        /// Gets or Sets the start time for this orchestrator
        /// </summary>
        DateTime? StartTime { get; set; }

        /// <summary>
        /// Gets or Sets the end time for this orchestrator
        /// </summary>
        DateTime? CompleteTime { get; set; }


        /// <summary>
        /// Sets the context if needed
        /// </summary>
        /// <param name=""></param>
        void SetContext(SyncContext syncContext);

        /// <summary>
        /// Gets the local context
        /// </summary>
        /// <returns></returns>
        SyncContext GetContext();

        /// <summary>
        /// Set an interceptor to get info on the current sync process
        /// </summary>
        void On<T>(Func<T, Task> interceptorFunc) where T : ProgressArgs;

        /// <summary>
        /// Set an interceptor to get info on the current sync process
        /// </summary>
        void On<T>(Action<T> interceptorAction) where T : ProgressArgs;

        /// <summary>
        /// Set a collection of interceptors
        /// </summary>
        /// <param name="interceptorBase"></param>
        void On(Interceptors interceptors);


        /// <summary>
        /// Launch an interceptor
        /// </summary>
        Task InterceptAsync<T>(T args, CancellationToken cancellationToken) where T : ProgressArgs;

        /// <summary>
        /// Report progress through a IProgress<T></T> instance
        /// </summary>
        void ReportProgress(SyncContext context, IProgress<ProgressArgs> progress, ProgressArgs args, DbConnection connection = null, DbTransaction transaction = null);

    }


    /// <summary>
    /// Remote provider
    /// </summary>
    public interface IRemoteOrchestrator : IOrchestrator
    {

        /// <summary>
        /// Ensure scope is created (both scope_info_server and scope_info_history tables)
        /// </summary>
        Task<ServerScopeInfo> EnsureScopesAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null);

        /// <summary>
        /// Get configuration from remote to ensure local provider has everything needed
        /// </summary>
        Task<(SyncSet Schema, string Version)> EnsureSchemaAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null);

        /// <summary>
        /// Send all changes and get new changes in return
        /// </summary>
        Task<(long remoteClientTimestamp,
              BatchInfo serverBatchInfo,
              ConflictResolutionPolicy serverPolicy,
              DatabaseChangesApplied clientChangesApplied,
              DatabaseChangesSelected serverChangesSelected)>
            ApplyThenGetChangesAsync(ScopeInfo clientScope, BatchInfo clientBatchInfo, 
                                     CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null);


        /// <summary>
        /// Gets a snapshot for initialization
        /// </summary>
        Task<(long remoteClientTimestamp, BatchInfo serverBatchInfo)> GetSnapshotAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null);


        /// <summary>
        /// Delete all metadatas from tracking tables that are below a timestamp
        /// </summary>
        Task DeleteMetadatasAsync(long timeStampStart, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null);
    }
}
