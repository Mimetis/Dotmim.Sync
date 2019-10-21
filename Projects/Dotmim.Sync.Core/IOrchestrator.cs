using Dotmim.Sync.Batch;
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
    public interface IOrchestrator<T> where T : CoreProvider
    {

        /// <summary>
        /// Gets a reference to the provider
        /// </summary>
        T Provider { get; }

        /// <summary>
        /// Set the provider used for this orchestrator
        /// </summary>
        void SetProvider(T coreProvider);


    }

    /// <summary>
    /// Remote provider
    /// </summary>
    public interface ILocalOrchestrator<T> : IOrchestrator<T> where T : CoreProvider
    {

        /// <summary>
        /// Get configuration to ensure local provider has everything needed
        /// </summary>
        /// <returns></returns>
        Task<(SyncContext context,
              ScopeInfo localScopeInfo)>
            EnsureScopeAsync(SyncContext context, SyncSchema schema, SyncOptions options,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null);




        /// <summary>
        /// Send all changes and get new changes in return
        /// </summary>
        Task<(SyncContext context,
              long clientTimestamp,
              BatchInfo clientBatchInfo,
              DatabaseChangesSelected clientChangesSelected)>
            GetChangesAsync(SyncContext context, SyncSchema schema, ScopeInfo localScopeInfo, ScopeInfo serverScopeInfo,
                            CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null);

        /// <summary>
        /// Save changes locally
        /// </summary>
        Task<(SyncContext context,
              DatabaseChangesApplied clientChangesApplied)>
            ApplyChangesAsync(SyncContext context,
                              long serverTimestamp, long clientTimestamp,
                              ScopeInfo serverScopeInfo, ScopeInfo localScopeInfo,
                              BatchInfo serverBatchInfo,
                              CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null);

    }


    /// <summary>
    /// Remote provider
    /// </summary>
    public interface IRemoteOrchestrator<T> : IOrchestrator<T> where T : CoreProvider
    {

        /// <summary>
        /// Get configuration from remote to ensure local provider has everything needed
        /// </summary>
        /// <returns></returns>
        Task<(SyncContext context,
              ScopeInfo serverScopeInfo,
              ScopeInfo localScopeReferenceInfo,
              SyncSchema schema)>
            EnsureScopeAsync(SyncContext context, SyncSchema schema, SyncOptions options, Guid clientScopeId,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null);

        /// <summary>
        /// Send all changes and get new changes in return
        /// </summary>
        Task<(SyncContext context,
              long serverTimestamp,
              BatchInfo serverBatchInfo,
              DatabaseChangesSelected serverChangesSelected)>
            ApplyThenGetChangesAsync(SyncContext context, 
                                     ScopeInfo localScopeInfo, ScopeInfo localScopeReferenceInfo,
                                     ScopeInfo serverScopeInfo, BatchInfo clientBatchInfo,
                                     CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null);



    }


}
