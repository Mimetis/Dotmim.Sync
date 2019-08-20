using Dotmim.Sync.Batch;
using Dotmim.Sync.Data;
using Dotmim.Sync.Messages;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public interface IProvider2
    {
        /// <summary>
        /// Set Options parameters
        /// </summary>
        void SetOptions(Action<SyncOptions> options);

        /// <summary>
        /// Set Sync Configuration parameters
        /// </summary>
        void SetConfiguration(Action<SyncConfiguration> configuration);

        /// <summary>
        /// Set the token for the current operation
        /// </summary>
        void SetCancellationToken(CancellationToken token);

        /// <summary>
        /// set the progress action used to get progression on the provider
        /// </summary>
        void SetProgress(IProgress<ProgressArgs> progress);

        /// <summary>
        /// set the Interceptor class to intercepts multipes events during the sync process
        /// </summary>
        void SetInterceptor(InterceptorBase interceptor);

        ///// <summary>
        ///// Subecribe an apply changes failed action
        ///// </summary>
        //void InterceptApplyChangesFailed(Func<ApplyChangesFailedArgs, Task> action);

        ///// <summary>
        ///// Subecribe an apply changes failed action
        ///// </summary>
        //void InterceptApplyChangesFailed(Action<ApplyChangesFailedArgs> action);


        /// <summary>
        /// Begin Session. if Configuration is set locally, then send it to the server
        /// On Server side, you can override Configuration and then send back the new configuration to apply on local machine
        /// </summary>
        Task<(SyncContext, SyncConfiguration)> BeginSessionAsync(SyncContext context, MessageBeginSession message);

        /// <summary>
        /// Ensure scopes are created on both local and remote 
        /// If clientReferenceId is specified, we are on the server side and we need the client reference scope (with server timestamp)
        /// </summary>
        Task<(SyncContext, List<ScopeInfo>)> EnsureScopesAsync(SyncContext context, MessageEnsureScopes messsage);

        /// <summary>
        /// Ensure tables are get from datastore
        /// </summary>
        Task<(SyncContext, DmSet)> EnsureSchemaAsync(SyncContext context, MessageEnsureSchema message);

        /// <summary>
        /// Ensure database is ready and created
        /// </summary>
        Task<SyncContext> EnsureDatabaseAsync(SyncContext context, MessageEnsureDatabase message);

        /// <summary>
        /// Apply changes to the local storage, coming from this scope
        /// </summary>
        Task<(SyncContext, DatabaseChangesApplied)> ApplyChangesAsync(SyncContext context, MessageApplyChanges message);

        /// <summary>
        /// Get Changes to be applied 
        /// </summary>
        Task<(SyncContext, BatchInfo, DatabaseChangesSelected)> GetChangeBatchAsync(SyncContext context, MessageGetChangesBatch message);

        /// <summary>
        /// Update scope to reflect last changed timestamp
        /// </summary>
        Task<SyncContext> WriteScopesAsync(SyncContext context, MessageWriteScopes message);

        /// <summary>
        /// End Session
        /// </summary>
        Task<SyncContext> EndSessionAsync(SyncContext context);

        /// <summary>
        /// Get a local timestamp
        /// </summary>
        Task<(SyncContext, long)> GetLocalTimestampAsync(SyncContext context, MessageTimestamp message);

    }
}
