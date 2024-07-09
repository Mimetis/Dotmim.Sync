using Dotmim.Sync.Enumerations;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// <para>
    /// The <c>LocalOrchestrator</c> object allows you to interact with the local datsource, (using a sync provider to access it).
    /// </para>
    /// <para>
    /// Use a <c>LocalOrchestrator</c> object only when you want to interact with a client datasource.
    /// If you want to interact with your server datasource, consider using a <c>RemoteOrchestrator</c> instead.
    /// </para>
    /// <example>
    /// You can access your <c>LocalOrchestrator</c> instance from your <see cref="SyncAgent"/>:
    /// <code>
    /// var syncAgent = new SyncAgent(clientProvider, serverProvider, options);
    /// var localOrchestrator = syncAgent.LocalOrchestrator;
    /// </code>
    /// You can also create an instance, without using a <see cref="SyncAgent"/>:
    /// <code>
    /// var localOrchestrator = new LocalOrchestrator(clientProvider, options);
    /// </code>
    /// </example>
    /// </summary>
    public partial class LocalOrchestrator : BaseOrchestrator
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="LocalOrchestrator"/> class.
        /// Create a local orchestrator, used to orchestrate the whole sync on the client side.
        /// </summary>
        public LocalOrchestrator(CoreProvider provider, SyncOptions options)
            : base(provider, options)
        {
            if (provider == null)
                throw this.GetSyncError(null, new MissingProviderException(nameof(LocalOrchestrator)));
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="LocalOrchestrator"/> class.
        /// Create a local orchestrator, used to orchestrate the whole sync on the client side.
        /// </summary>
        public LocalOrchestrator(CoreProvider provider)
            : base(provider, new SyncOptions())
        {
            if (provider == null)
                throw this.GetSyncError(null, new MissingProviderException(nameof(LocalOrchestrator)));
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
        public Task EndSessionAsync(SyncResult syncResult, string scopeName = SyncOptions.DefaultScopeName, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            // Create a new context
            var ctx = new SyncContext(Guid.NewGuid(), scopeName);

            return this.InternalEndSessionAsync(ctx, syncResult, null, null, progress, cancellationToken);
        }

        /// <summary>
        /// Called when a session is starting.
        /// </summary>
        internal async Task<SyncContext> InternalBeginSessionAsync(SyncContext context, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            context.SyncStage = SyncStage.BeginSession;

            var connection = this.Provider.CreateConnection();

            // Progress & interceptor
            await this.InterceptAsync(new SessionBeginArgs(context, connection), progress, cancellationToken).ConfigureAwait(false);

            return context;
        }

        /// <summary>
        /// Called when the sync is over.
        /// </summary>
        internal async Task<SyncContext> InternalEndSessionAsync(SyncContext context, SyncResult result, ClientSyncChanges clientSyncChanges, SyncException syncException = default, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            context.SyncStage = SyncStage.EndSession;

            try
            {

                var connection = this.Provider.CreateConnection();

                if (this.Options.CleanFolder && clientSyncChanges?.ClientBatchInfo != null)
                {
                    var cleanFolder = await this.InternalCanCleanFolderAsync(context.ScopeName, context.Parameters, clientSyncChanges.ClientBatchInfo, default, cancellationToken).ConfigureAwait(false);

                    if (cleanFolder)
                        clientSyncChanges.ClientBatchInfo.TryRemoveDirectory();
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