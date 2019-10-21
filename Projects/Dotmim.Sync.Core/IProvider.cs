using Dotmim.Sync.Batch;
using Dotmim.Sync.Data;
using Dotmim.Sync.Messages;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    //public interface IProvider
    //{
    //    /// <summary>
    //    /// Gets a boolean indicating if the provider can use bulk operations
    //    /// </summary>
    //    bool SupportBulkOperations { get; }

    //    /// <summary>
    //    /// Gets a boolean indicating if the provider can be a server side provider
    //    /// </summary>
    //    bool CanBeServerProvider { get; }


    //    /// <summary>
    //    /// Begin Session. if Configuration is set locally, then send it to the server
    //    /// On Server side, you can override Configuration and then send back the new configuration to apply on local machine
    //    /// </summary>
    //    Task<SyncContext> BeginSessionAsync(SyncContext context,
    //                                        CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null);

    //    /// <summary>
    //    /// Ensure scopes are created on both local and remote 
    //    /// If clientReferenceId is specified, we are on the server side and we need the client reference scope (with server timestamp)
    //    /// </summary>
    //    Task<(SyncContext, List<ScopeInfo>)> EnsureScopesAsync(SyncContext context, MessageEnsureScopes messsage,
    //                         CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null);

    //    /// <summary>
    //    /// Ensure tables are get from datastore
    //    /// </summary>
    //    Task<(SyncContext, DmSet)> EnsureSchemaAsync(SyncContext context, MessageEnsureSchema message,
    //                         CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null);

    //    /// <summary>
    //    /// Ensure database is ready and created
    //    /// </summary>
    //    Task<SyncContext> EnsureDatabaseAsync(SyncContext context, MessageEnsureDatabase message,
    //                         CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null);

    //    /// <summary>
    //    /// Apply changes to the local storage, coming from this scope
    //    /// </summary>
    //    Task<(SyncContext, DatabaseChangesApplied)> ApplyChangesAsync(SyncContext context, MessageApplyChanges message,
    //                         CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null);

    //    /// <summary>
    //    /// Get Changes to be applied 
    //    /// </summary>
    //    Task<(SyncContext, BatchInfo, DatabaseChangesSelected)> GetChangeBatchAsync(SyncContext context, MessageGetChangesBatch message,
    //                         CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null);

    //    /// <summary>
    //    /// Update scope to reflect last changed timestamp
    //    /// </summary>
    //    Task<SyncContext> WriteScopesAsync(SyncContext context, MessageWriteScopes message,
    //                         CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null);

    //    /// <summary>
    //    /// End Session
    //    /// </summary>
    //    Task<SyncContext> EndSessionAsync(SyncContext context,
    //                         CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null);

    //    /// <summary>
    //    /// Get a local timestamp
    //    /// </summary>
    //    Task<(SyncContext, long)> GetLocalTimestampAsync(SyncContext context, MessageTimestamp message,
    //                         CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null);

    //}
}
