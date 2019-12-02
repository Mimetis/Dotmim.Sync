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
        /// Gets a reference to the provider
        /// </summary>
        CoreProvider Provider { get; set; }

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
    public interface ILocalOrchestrator : IOrchestrator
    {
        /// <summary>
        /// Get configuration to ensure local provider has everything needed
        /// </summary>
        /// <returns></returns>
        Task<(SyncContext context, ScopeInfo localScopeInfo)>
            EnsureScopeAsync(SyncContext context, SyncSchema schema, SyncOptions options,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null);

        /// <summary>
        /// Send all changes and get new changes in return
        /// </summary>
        Task<(SyncContext context,
              long clientTimestamp,
              BatchInfo clientBatchInfo,
              DatabaseChangesSelected clientChangesSelected)>
            GetChangesAsync(SyncContext context, SyncSchema schema, ScopeInfo localScopeInfo, int batchSize, string batchDirectory,
                            CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null);

        /// <summary>
        /// Save changes locally
        /// </summary>
        Task<(SyncContext context,
              DatabaseChangesApplied clientChangesApplied)>
            ApplyChangesAsync(SyncContext context, ScopeInfo scope, SyncSchema schema, BatchInfo serverBatchInfo,
                              ConflictResolutionPolicy clientPolicy, long clientTimestamp, long remoteClientTimestamp,
                              bool disableConstraintsOnApplyChanges, bool useBulkOperations, bool cleanMetadatas, string scopeInfoTableName,
                              CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null);
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
        Task<(SyncContext context, SyncSchema schema)>
            EnsureSchemaAsync(SyncContext context, SyncSchema schema, 
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null);

        /// <summary>
        /// Send all changes and get new changes in return
        /// </summary>
        Task<(SyncContext context,
              long remoteClientTimestamp,
              BatchInfo serverBatchInfo,
              DatabaseChangesSelected serverChangesSelected)>
            ApplyThenGetChangesAsync(SyncContext context, ScopeInfo scope, SyncSchema schema, BatchInfo clientBatchInfo,
                                     bool disableConstraintsOnApplyChanges, bool useBulkOperations, bool cleanMetadatas,
                                     int clientBatchSize, string batchDirectory,
                                     CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null);

    }
}
