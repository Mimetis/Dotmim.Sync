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
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


namespace Dotmim.Sync
{
    public partial class LocalOrchestrator : BaseOrchestrator
    {

        public virtual Task<ScopeInfo> GetClientScopeAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => GetClientScopeAsync(SyncOptions.DefaultScopeName, default, connection, transaction, cancellationToken, progress);

        public virtual Task<ScopeInfo> GetClientScopeAsync(SyncSetup setup, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => GetClientScopeAsync(SyncOptions.DefaultScopeName, setup, connection, transaction, cancellationToken, progress);



        public virtual async Task<ScopeInfo> GetClientScopeAsync(string scopeName, SyncSetup setup = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Reading, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var exists = await this.InternalExistsScopeInfoTableAsync(scopeName, DbScopeType.Client, runner.Connection, runner.Transaction, runner.CancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(scopeName, DbScopeType.Client, runner.Connection, runner.Transaction, runner.CancellationToken, progress).ConfigureAwait(false);

                // Get scope from local client 
                var localScope = await this.InternalGetScopeAsync(scopeName, DbScopeType.Client, runner.Connection, runner.Transaction, runner.CancellationToken, progress).ConfigureAwait(false);

                if (localScope == null)
                {
                    localScope = this.InternalCreateScope(scopeName, DbScopeType.Client, cancellationToken, progress);
                    localScope = await this.InternalSaveScopeAsync(localScope, DbScopeType.Client, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                // if localScope is empty, grab the schema locally from setup 
                if (localScope.Setup == null && localScope.Schema == null && setup != null && setup.Tables.Count > 0)
                {
                    var schema = await this.InternalGetSchemaAsync(scopeName, setup, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                    localScope.Setup = setup;
                    localScope.Schema = schema;

                    // Write scopes locally
                    await this.InternalSaveScopeAsync(localScope, DbScopeType.Client, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                await runner.CommitAsync().ConfigureAwait(false);

                return localScope as ScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeName, ex);
            }
        }

        /// <summary>
        /// Write a server scope 
        /// </summary> 
        public virtual async Task<ScopeInfo> SaveClientScopeAsync(ScopeInfo scopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(scopeInfo.Name, SyncMode.Writing, SyncStage.ScopeWriting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var exists = await this.InternalExistsScopeInfoTableAsync(scopeInfo.Name, DbScopeType.Client, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(scopeInfo.Name, DbScopeType.Client, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // Write scopes locally
                await this.InternalSaveScopeAsync(scopeInfo, DbScopeType.Client, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return scopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeInfo.Name, ex);
            }
        }

    }
}
