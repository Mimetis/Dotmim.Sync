using Dotmim.Sync.Core.Batch;
using Dotmim.Sync.Core.Builders;
using Dotmim.Sync.Core.Scope;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Core
{
    public interface IResponseHandler
    {
        /// <summary>
        /// Begin Session
        /// </summary>
        void BeginSession();

        /// <summary>
        /// Event Progress
        /// </summary>
        event EventHandler<ScopeProgressEventArgs> SyncProgress;

        /// <summary>
        /// Ensure Scopes are created
        /// </summary>
        (ScopeInfo serverScope, ScopeInfo clientScope) EnsureScopes(string serverScopeName, string clientScopeName = null);


        /// <summary>
        /// Get a configuration from the provider. 
        /// Eventually it's only used on the remote provider
        /// </summary>
        ServiceConfiguration GetConfiguration();

        /// <summary>
        /// Actually only used on the local provider
        /// </summary>
        /// <param name="configuration">if null, we are on the remote machine, configuration already known</param>
        void ApplyConfiguration(ServiceConfiguration configuration= null);

        /// <summary>
        /// Ensure database is ready and created
        /// </summary>
        void EnsureDatabase(DbBuilderOption options);

        /// <summary>
        /// Apply changes to the local storage, coming from this scope
        /// </summary>
        void ApplyChanges(ScopeInfo fromScope, BatchInfo changes);

        /// <summary>
        /// Get Changes to be applied 
        /// </summary>
        BatchInfo GetChangeBatch();

        /// <summary>
        /// Update scope to reflect last changed timestamp
        /// </summary>
        void WriteScopes();

        /// <summary>
        /// End Session
        /// </summary>
        void EndSession();

        /// <summary>
        /// Get a local timestamp
        /// </summary>
        long GetLocalTimestamp();
    }
}
