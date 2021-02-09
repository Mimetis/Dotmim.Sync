using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Newtonsoft.Json;
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
    public partial class RemoteOrchestrator : BaseOrchestrator
    {

        /// <summary>
        /// Get the server scope histories
        /// </summary>
        public virtual Task<List<ServerHistoryScopeInfo>> GetServerHistoryScopes(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
           => RunInTransactionAsync(SyncStage.ScopeLoading, async (ctx, connection, transaction) =>
           {
               List<ServerHistoryScopeInfo> serverHistoryScopes = null;

               // Get Scope Builder
               var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

               var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.ServerHistory, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

               if (!exists)
                   await this.InternalCreateScopeInfoTableAsync(ctx, DbScopeType.ServerHistory, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

               // Get scope if exists
               serverHistoryScopes = await this.InternalGetAllScopesAsync<ServerHistoryScopeInfo>(ctx, DbScopeType.ServerHistory, this.ScopeName, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

               return serverHistoryScopes;

           }, connection, transaction, cancellationToken);


        /// <summary>
        /// Get the local configuration, ensures the local scope is created
        /// </summary>
        /// <returns>Server scope info, containing all scopes names, version, setup and related schema infos</returns>
        public virtual Task<ServerScopeInfo> GetServerScopeAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.ScopeLoading, async (ctx, connection, transaction) =>
        {
            ServerScopeInfo serverScopeInfo = null;

            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.Server, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (!exists)
                await this.InternalCreateScopeInfoTableAsync(ctx, DbScopeType.Server, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.ServerHistory, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (!exists)
                await this.InternalCreateScopeInfoTableAsync(ctx, DbScopeType.ServerHistory, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            serverScopeInfo = await this.InternalGetScopeAsync<ServerScopeInfo>(ctx, DbScopeType.Server, this.ScopeName, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            // if serverscopeinfo is a new, because we never run any sync before, just return serverscope empty
            if (serverScopeInfo.Setup == null && serverScopeInfo.Schema == null)
                return serverScopeInfo;

            // Compare serverscope setup with current
            if (!serverScopeInfo.Setup.EqualsByProperties(this.Setup))
            {
                SyncSet schema;
                // 1) Get Schema from remote provider
                schema = await this.InternalGetSchemaAsync(ctx, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Migrate the old setup (serverScopeInfo.Setup) to the new setup (this.Setup) based on the new schema 
                await this.InternalMigrationAsync(ctx, schema, serverScopeInfo.Setup, this.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                serverScopeInfo.Setup = this.Setup;
                serverScopeInfo.Schema = schema;

                // Write scopes locally
                await this.InternalSaveScopeAsync(ctx, DbScopeType.Server, serverScopeInfo, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            }

            return serverScopeInfo;

        }, connection, transaction, cancellationToken);



        /// <summary>
        /// Update or Insert a server scope row
        /// </summary>
        public virtual Task<ServerScopeInfo> SaveServerScopeAsync(ServerScopeInfo scopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.ScopeWriting, async (ctx, connection, transaction) =>
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.Server, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (!exists)
                await this.InternalCreateScopeInfoTableAsync(ctx, DbScopeType.Server, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            // Write scopes locally
            var scopeInfoUpdated = await this.InternalSaveScopeAsync(ctx, DbScopeType.Server, scopeInfo, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            return scopeInfoUpdated;

        }, connection, transaction, cancellationToken);

        /// <summary>
        /// Update or Insert a server scope row
        /// </summary>
        public virtual Task<ServerHistoryScopeInfo> SaveServerHistoryScopeAsync(ServerHistoryScopeInfo scopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.ScopeWriting, async (ctx, connection, transaction) =>
        {
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.ServerHistory, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (!exists)
                await this.InternalCreateScopeInfoTableAsync(ctx, DbScopeType.ServerHistory, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            // Write scopes locally
            var scopeInfoUpdated = await this.InternalSaveScopeAsync(ctx, DbScopeType.ServerHistory, scopeInfo, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            return scopeInfoUpdated;

        }, connection, transaction, cancellationToken);

    }
}