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

        public virtual async Task<ScopeInfo> GetClientScopeAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            await using var runner = await this.GetConnectionAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
            var ctx = this.GetContext();
            ctx.SyncStage = SyncStage.ScopeLoading;
            try
            {
                var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                var exists = await this.InternalExistsScopeInfoTableAsync(this.GetContext(), DbScopeType.Client, scopeBuilder,
                    runner.Connection, runner.Transaction, runner.CancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(this.GetContext(), DbScopeType.Client, scopeBuilder,
                        runner.Connection, runner.Transaction, runner.CancellationToken, progress).ConfigureAwait(false);

                var localScope = await this.InternalGetScopeAsync<ScopeInfo>(this.GetContext(), DbScopeType.Client, this.ScopeName, scopeBuilder,
                    runner.Connection, runner.Transaction, runner.CancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync();

                return localScope;
            }
            catch (Exception ex)
            {
                throw GetSyncError(ex);
            }
        }

        /// <summary>
        /// Write a server scope 
        /// </summary> 
        public virtual async Task<ScopeInfo> SaveClientScopeAsync(ScopeInfo scopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            await using var runner = await this.GetConnectionAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
            var ctx = this.GetContext();
            ctx.SyncStage = SyncStage.ScopeWriting;
            try
            {
                var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.Client, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(ctx, DbScopeType.Client, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // Write scopes locally

                await this.InternalSaveScopeAsync(ctx, DbScopeType.Client, scopeInfo, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return scopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(ex);
            }
        }

    }
}
