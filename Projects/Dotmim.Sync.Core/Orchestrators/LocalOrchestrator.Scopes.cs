using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


namespace Dotmim.Sync
{
    public partial class LocalOrchestrator : BaseOrchestrator
    {

        /// <summary> 
        /// Get the local configuration, ensures the local scope is created
        /// </summary>
        /// <returns>current context, the local scope info created or get from the database and the configuration from the client if changed </returns>
        public Task<ScopeInfo> GetClientScopeAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => RunInTransactionAsync(SyncStage.ScopeLoading, async (ctx, connection, transaction) =>
            {
                var scopeBuilder = this.Provider.GetScopeBuilder(this.Options.ScopeInfoTableName);

                var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.Client, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(ctx, DbScopeType.Client, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var localScope = await this.InternalGetScopeAsync<ScopeInfo>(ctx, DbScopeType.Client, this.ScopeName, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                return localScope;

            },  cancellationToken);

        /// <summary>
        /// Write a server scope 
        /// </summary>
        public virtual Task<ScopeInfo> UpsertClientScopeAsync(ScopeInfo scopeInfo, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.ScopeWriting, async (ctx, connection, transaction) =>
        {
            var scopeBuilder = this.Provider.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.Client, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (!exists)
                await this.InternalCreateScopeInfoTableAsync(ctx, DbScopeType.Client, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            // Write scopes locally
            await this.InternalUpsertScopeAsync(ctx, DbScopeType.Client, scopeInfo, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            return scopeInfo;

        }, cancellationToken);



    }
}
