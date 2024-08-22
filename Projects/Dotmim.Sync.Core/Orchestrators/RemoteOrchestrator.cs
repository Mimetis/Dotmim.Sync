using Dotmim.Sync.Enumerations;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Remote orchestrator, used to orchestrates the whole sync on the server side.
    /// </summary>
    public partial class RemoteOrchestrator : BaseOrchestrator
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="RemoteOrchestrator"/> class.
        /// Create a remote orchestrator, used to orchestrates the whole sync on the server side.
        /// </summary>
        public RemoteOrchestrator(CoreProvider provider, SyncOptions options)
            : base(provider, options)
        {
            if (this.Provider != null && !this.Provider.CanBeServerProvider)
                throw this.GetSyncError(null, new UnsupportedServerProviderException(this.Provider.GetProviderTypeName()));
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RemoteOrchestrator"/> class.
        /// Create a remote orchestrator, used to orchestrates the whole sync on the server side.
        /// </summary>
        public RemoteOrchestrator(CoreProvider provider)
            : base(provider, new SyncOptions())
        {
            if (this.Provider != null && !this.Provider.CanBeServerProvider)
                throw this.GetSyncError(null, new UnsupportedServerProviderException(this.Provider.GetProviderTypeName()));
        }

        /// <summary>
        /// Called when a new synchronization session has started. Initialize the SyncContext instance, used for this session.
        /// </summary>
        public virtual Task BeginSessionAsync(string scopeName = SyncOptions.DefaultScopeName, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            // Create a new context
            var context = new SyncContext(Guid.NewGuid(), scopeName);

            return this.InternalBeginSessionAsync(context, progress, cancellationToken);
        }

        /// <summary>
        /// Called when the synchronization session is over.
        /// </summary>
        public virtual Task EndSessionAsync(SyncResult syncResult, string scopeName = SyncOptions.DefaultScopeName, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            // Create a new context
            var ctx = new SyncContext(Guid.NewGuid(), scopeName);

            return this.InternalEndSessionAsync(ctx, syncResult, null, null, progress, cancellationToken);
        }

        /// <summary>
        /// Called when a session is starting.
        /// </summary>
        internal virtual async Task<SyncContext> InternalBeginSessionAsync(SyncContext context, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            context.SyncStage = SyncStage.BeginSession;

            var connection = this.Provider?.CreateConnection();

            // Progress & interceptor
            await this.InterceptAsync(new SessionBeginArgs(context, connection), progress, cancellationToken).ConfigureAwait(false);

            return context;
        }

        /// <summary>
        /// Called when the sync is over.
        /// </summary>
        internal virtual async Task<SyncContext> InternalEndSessionAsync(SyncContext context, SyncResult result, ServerSyncChanges serverSyncChanges, SyncException syncException = default,
            IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            context.SyncStage = SyncStage.EndSession;

            try
            {
                var connection = this.Provider?.CreateConnection();

                if (this.Options.CleanFolder && serverSyncChanges?.ServerBatchInfo != null)
                {
                    var cleanFolder = await this.InternalCanCleanFolderAsync(context.ScopeName, context.Parameters, serverSyncChanges.ServerBatchInfo, default, cancellationToken).ConfigureAwait(false);

                    if (cleanFolder)
                        serverSyncChanges.ServerBatchInfo.TryRemoveDirectory();
                }

                // Progress & interceptor
                await this.InterceptAsync(new SessionEndArgs(context, result, syncException, connection), progress, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // silent throw
            }

            return context;
        }
    }
}