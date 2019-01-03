using Dotmim.Sync.Batch;
using Dotmim.Sync.Data;
using Dotmim.Sync.Messages;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public interface IProvider
    {
        //event EventHandler<ProgressEventArgs> SyncProgress;
        //event EventHandler<BeginSessionEventArgs> BeginSession;
        //event EventHandler<EndSessionEventArgs> EndSession;
        //event EventHandler<ScopeEventArgs> ScopeLoading;
        //event EventHandler<ScopeEventArgs> ScopeSaved;
        //event EventHandler<DatabaseApplyingEventArgs> DatabaseApplying;
        //event EventHandler<DatabaseAppliedEventArgs> DatabaseApplied;
        //event EventHandler<DatabaseTableApplyingEventArgs> DatabaseTableApplying;
        //event EventHandler<DatabaseTableAppliedEventArgs> DatabaseTableApplied;
        //event EventHandler<SchemaApplyingEventArgs> SchemaApplying;
        //event EventHandler<SchemaAppliedEventArgs> SchemaApplied;
        //event EventHandler<TableChangesSelectingEventArgs> TableChangesSelecting;
        //event EventHandler<TableChangesSelectedEventArgs> TableChangesSelected;
        //event EventHandler<TableChangesApplyingEventArgs> TableChangesApplying;
        //event EventHandler<TableChangesAppliedEventArgs> TableChangesApplied;
        event EventHandler<ApplyChangeFailedEventArgs> ApplyChangedFailed;

        /// <summary>
        /// Gets or Sets the options used on this provider
        /// </summary>
        SyncOptions Options { get; set; }

        /// <summary>
        /// Set the token for the current operation
        /// </summary>
        void SetCancellationToken(CancellationToken token);

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
        Task<(SyncContext, ChangesApplied)> ApplyChangesAsync(SyncContext context, MessageApplyChanges message);

        /// <summary>
        /// Get Changes to be applied 
        /// </summary>
        Task<(SyncContext, BatchInfo, ChangesSelected)> GetChangeBatchAsync(SyncContext context, MessageGetChangesBatch message);

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
