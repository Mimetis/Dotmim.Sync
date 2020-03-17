using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Messages;
using System;
using System.Collections.Generic;
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



    }


    /// <summary>
    /// Remote provider
    /// </summary>
    public interface IRemoteOrchestrator : IOrchestrator
    {
        /// <summary>
        /// Get configuration from remote to ensure local provider has everything needed
        /// </summary>
        /// <returns></returns>
        Task<SyncSet> EnsureSchemaAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null);

        /// <summary>
        /// Send all changes and get new changes in return
        /// </summary>
        Task<(long remoteClientTimestamp,
              BatchInfo serverBatchInfo,
              DatabaseChangesSelected serverChangesSelected)>
            ApplyThenGetChangesAsync(ScopeInfo clientScope, SyncSet schema, BatchInfo clientBatchInfo,
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
