using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Net.Mime;
using System.Text;
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
        /// Create a local orchestrator, used to orchestrate the whole sync on the client side
        /// </summary>
        public LocalOrchestrator(CoreProvider provider, SyncOptions options) : base(provider, options)
        {
            if (provider == null)
                throw GetSyncError(null, new MissingProviderException(nameof(LocalOrchestrator)));

        }
        
        /// <summary>
        /// Create a local orchestrator, used to orchestrate the whole sync on the client side
        /// </summary>
        public LocalOrchestrator(CoreProvider provider) : base(provider, new SyncOptions())
        {
            if (provider == null)
                throw GetSyncError(null, new MissingProviderException(nameof(LocalOrchestrator)));
        }

        /// <summary>
        /// Called when a new synchronization session has started. Initialize the SyncContext instance, used for this session.
        /// </summary>
        public virtual Task BeginSessionAsync(string scopeName = SyncOptions.DefaultScopeName, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            // Create a new context
            var context = new SyncContext(Guid.NewGuid(), scopeName);

            return InternalBeginSessionAsync(context, cancellationToken, progress);
        }

        /// <summary>
        /// Called when the synchronization session is over.
        /// </summary>
        public Task EndSessionAsync(SyncResult syncResult, string scopeName = SyncOptions.DefaultScopeName, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            // Create a new context
            var ctx = new SyncContext(Guid.NewGuid(), scopeName);

            return InternalEndSessionAsync(ctx, syncResult, null, null, cancellationToken, progress);
        }

        /// <summary>
        /// Called when a session is starting
        /// </summary>
        internal async Task<SyncContext> InternalBeginSessionAsync(SyncContext context, CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            context.SyncStage = SyncStage.BeginSession;

            var connection = this.Provider.CreateConnection();

            // Progress & interceptor
            await this.InterceptAsync(new SessionBeginArgs(context, connection), progress, cancellationToken).ConfigureAwait(false);

            return context;

        }

        /// <summary>
        /// Called when the sync is over
        /// </summary>
        internal async Task<SyncContext> InternalEndSessionAsync(SyncContext context, SyncResult result, ClientSyncChanges clientSyncChanges, ServerSyncChanges serverSyncChanges, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            context.SyncStage = SyncStage.EndSession;

            var connection = this.Provider.CreateConnection();

            if (this.Options.CleanFolder && clientSyncChanges?.ClientBatchInfo != null)
            {
                var cleanFolder = await this.InternalCanCleanFolderAsync(context.ScopeName, context.Parameters, clientSyncChanges.ClientBatchInfo, default).ConfigureAwait(false);

                if (cleanFolder)
                    clientSyncChanges.ClientBatchInfo.TryRemoveDirectory();

            }

            if (this.Options.CleanFolder && serverSyncChanges?.ServerBatchInfo != null)
            {
                var cleanFolder = await this.InternalCanCleanFolderAsync(context.ScopeName, context.Parameters, serverSyncChanges.ServerBatchInfo, default).ConfigureAwait(false);

                if (cleanFolder)
                    serverSyncChanges.ServerBatchInfo.TryRemoveDirectory();

            }

            // Progress & interceptor
            await this.InterceptAsync(new SessionEndArgs(context, result, connection), progress, cancellationToken).ConfigureAwait(false);

            return context;
        }
    }
}
