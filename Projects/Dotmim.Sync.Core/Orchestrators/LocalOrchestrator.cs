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
    public partial class LocalOrchestrator : BaseOrchestrator
    {
        public override SyncSide Side => SyncSide.ClientSide;

        /// <summary>
        /// Create a local orchestrator, used to orchestrates the whole sync on the client side
        /// </summary>
        public LocalOrchestrator(CoreProvider provider, SyncOptions options) : base(provider, options)
        {
            if (provider == null)
                throw GetSyncError(null, new MissingProviderException(nameof(LocalOrchestrator)));

        }
        /// <summary>
        /// Create a local orchestrator, used to orchestrates the whole sync on the client side
        /// </summary>
        public LocalOrchestrator(CoreProvider provider) : base(provider, new SyncOptions())
        {
            if (provider == null)
                throw GetSyncError(null, new MissingProviderException(nameof(LocalOrchestrator)));
        }

        /// <summary>
        /// Called by the  to indicate that a 
        /// synchronization session has started.
        /// </summary>
        public virtual Task BeginSessionAsync(string scopeName = SyncOptions.DefaultScopeName, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            // Create a new context
            var context = new SyncContext(Guid.NewGuid(), scopeName);

            return InternalBeginSessionAsync(context, cancellationToken, progress);
        }

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
        public Task EndSessionAsync(SyncResult syncResult, string scopeName = SyncOptions.DefaultScopeName, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            // Create a new context
            var ctx = new SyncContext(Guid.NewGuid(), scopeName);

            return InternalEndSessionAsync(ctx, syncResult, cancellationToken, progress);
        }

        /// <summary>
        /// Called when the sync is over
        /// </summary>
        public async Task<SyncContext> InternalEndSessionAsync(SyncContext context, SyncResult result, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            context.SyncStage = SyncStage.EndSession;

            var connection = this.Provider.CreateConnection();

            // Progress & interceptor
            await this.InterceptAsync(new SessionEndArgs(context, result, connection), progress, cancellationToken).ConfigureAwait(false);

            return context;
        }



        ///// <summary>
        ///// Migrate an old setup configuration to a new one. This method is usefull if you are changing your SyncSetup when a database has been already configured previously
        ///// </summary>
        //public virtual async Task<ScopeInfo> MigrationAsync(ScopeInfo oldScopeInfo, ServerScopeInfo newScopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        //{
        //    try
        //    {
        //        await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Writing, SyncStage.Migrating, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
        //        // If schema does not have any table, just return
        //        if (newScopeInfo == null || newScopeInfo.Schema == null || newScopeInfo.Schema.Tables == null || !newScopeInfo.Schema.HasTables)
        //            throw new MissingTablesException();

        //        // Migrate the db structure
        //        await this.InternalMigrationAsync(this.GetContext(), oldScopeInfo, newScopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        ScopeInfo localScope = null;

        //        var exists = await this.InternalExistsScopeInfoTableAsync(this.GetContext(), DbScopeType.Client, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        if (!exists)
        //            await this.InternalCreateScopeInfoTableAsync(this.GetContext(), DbScopeType.Client, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        localScope = await this.InternalGetScopeAsync<ScopeInfo>(this.GetContext(), DbScopeType.Client, this.ScopeName, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        if (localScope == null)
        //        {
        //            localScope = await this.InternalCreateScopeAsync<ScopeInfo>(this.GetContext(), DbScopeType.Client, this.ScopeName, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
        //            localScope = await this.InternalSaveScopeAsync(localScope, DbScopeType.Client, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        }

        //        localScope.Setup = newScopeInfo.Setup;
        //        localScope.Schema = newScopeInfo.Schema;
        //        localScope.Name = newScopeInfo.Name;

        //        await this.InternalSaveScopeAsync(this.GetContext(), DbScopeType.Client, localScope, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        await runner.CommitAsync().ConfigureAwait(false);

        //        return localScope;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw GetSyncError(ex);
        //    }
        //}


    }
}
