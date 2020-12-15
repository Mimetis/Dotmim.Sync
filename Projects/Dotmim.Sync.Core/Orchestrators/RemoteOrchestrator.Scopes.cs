using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
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
        public virtual Task<List<ServerHistoryScopeInfo>> GetServerHistoryScopes(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
           => RunInTransactionAsync(async (ctx, connection, transaction) =>
           {
               List<ServerHistoryScopeInfo> serverHistoryScopes = null;

               ctx.SyncStage = SyncStage.ScopeLoading;
               // Get Scope Builder
               var scopeBuilder = this.Provider.GetScopeBuilder(this.Options.ScopeInfoTableName);

               var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.ServerHistory, scopeBuilder, connection, transaction, cancellationToken).ConfigureAwait(false);

               if (exists)
                   await this.InternalDropScopeInfoTableAsync(ctx, DbScopeType.ServerHistory, scopeBuilder, connection, transaction, cancellationToken).ConfigureAwait(false);

               // Get scope if exists
               serverHistoryScopes = await this.InternalGetAllScopesAsync<ServerHistoryScopeInfo>(ctx, DbScopeType.ServerHistory, this.ScopeName, scopeBuilder, connection, transaction, cancellationToken).ConfigureAwait(false);

               ctx.SyncStage = SyncStage.ScopeLoaded;

               return serverHistoryScopes;

           }, cancellationToken);


        /// <summary>
        /// Get the local configuration, ensures the local scope is created
        /// </summary>
        /// <returns>Server scope info, containing all scopes names, version, setup and related schema infos</returns>
        public virtual Task<ServerScopeInfo> GetServerScopeAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(async (ctx, connection, transaction) =>
        {
            ServerScopeInfo serverScopeInfo = null;

            ctx.SyncStage = SyncStage.ScopeLoading;

            var scopeBuilder = this.Provider.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.Server, scopeBuilder, connection, transaction, cancellationToken).ConfigureAwait(false);

            if (!exists)
                await this.InternalCreateScopeInfoTableAsync(ctx, DbScopeType.Server, scopeBuilder, connection, transaction, cancellationToken).ConfigureAwait(false);

            exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.ServerHistory, scopeBuilder, connection, transaction, cancellationToken).ConfigureAwait(false);

            if (!exists)
                await this.InternalCreateScopeInfoTableAsync(ctx, DbScopeType.ServerHistory, scopeBuilder, connection, transaction, cancellationToken).ConfigureAwait(false);

            serverScopeInfo = await this.InternalGetScopeAsync<ServerScopeInfo>(ctx, DbScopeType.Server, this.ScopeName, scopeBuilder, connection, transaction, cancellationToken).ConfigureAwait(false);

            // if serverscopeinfo is a new, because we never run any sync before, just return serverscope empty
            if (serverScopeInfo.Setup == null && serverScopeInfo.Schema == null)
                return serverScopeInfo;

            // Compare serverscope setup with current
            if (!serverScopeInfo.Setup.EqualsByProperties(this.Setup))
            {
                this.logger.LogDebug(SyncEventsId.GetScopeInfo, $"[{ctx.SyncStage}] database {connection.Database}. serverScopeInfo.Setup != this.Setup. Need migrations ");

                SyncSet schema;
                // 1) Get Schema from remote provider
                schema = await this.InternalGetSchemaAsync(ctx, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Launch InterceptAsync on Migrating
                await this.InterceptAsync(new DatabaseMigratingArgs(ctx, schema, serverScopeInfo.Setup, this.Setup, connection, transaction), cancellationToken).ConfigureAwait(false);

                // Migrate the old setup (serverScopeInfo.Setup) to the new setup (this.Setup) based on the new schema 
                await this.Provider.MigrationAsync(ctx, schema, serverScopeInfo.Setup, this.Setup, false, connection, transaction, cancellationToken, progress);

                // Now call the ProvisionAsync() to provision new tables
                var provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

                await this.InterceptAsync(new DatabaseProvisioningArgs(ctx, provision, schema, connection, transaction), cancellationToken).ConfigureAwait(false);

                // Provision everything
                ctx = await InternalProvisionAsync(ctx, schema, provision, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                serverScopeInfo.Setup = this.Setup;
                serverScopeInfo.Schema = schema;

                // Write scopes locally
                await this.InternalUpsertScopeAsync(ctx, DbScopeType.Server, serverScopeInfo, scopeBuilder, connection, transaction, cancellationToken);

                var args = new DatabaseProvisionedArgs(ctx, provision, schema, connection, transaction);
                await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);
                this.ReportProgress(ctx, progress, args);

                // InterceptAsync Migrated
                var args2 = new DatabaseMigratedArgs(ctx, schema, this.Setup, connection, transaction);
                await this.InterceptAsync(args2, cancellationToken).ConfigureAwait(false);
                this.ReportProgress(ctx, progress, args2);
            }

            ctx.SyncStage = SyncStage.ScopeLoaded;

            var scopeArgs = new ScopeLoadedArgs<ServerScopeInfo>(ctx, this.ScopeName, DbScopeType.Server, serverScopeInfo, connection, transaction);
            this.ReportProgress(ctx, progress, scopeArgs);

            return serverScopeInfo;

        }, cancellationToken);


        /// <summary>
        /// Update or Insert a server scope row
        /// </summary>
        public virtual Task<ServerScopeInfo> UpsertServerScopeAsync(ServerScopeInfo scopeInfo, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(async (ctx, connection, transaction) =>
        {
            ctx.SyncStage = SyncStage.ScopeWriting;

            var scopeBuilder = this.Provider.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.Server, scopeBuilder, connection, transaction, cancellationToken);

            if (!exists)
                await this.InternalCreateScopeInfoTableAsync(ctx, DbScopeType.Server, scopeBuilder, connection, transaction, cancellationToken);

            // Write scopes locally
            var scopeInfoUpdated = await this.InternalUpsertScopeAsync(ctx, DbScopeType.Server, scopeInfo, scopeBuilder, connection, transaction, cancellationToken);

            ctx.SyncStage = SyncStage.ScopeWrited;

            return scopeInfoUpdated;

        }, cancellationToken);

        /// <summary>
        /// Update or Insert a server scope row
        /// </summary>
        public virtual Task<ServerHistoryScopeInfo> UpsertServerHistoryScopeAsync(ServerHistoryScopeInfo scopeInfo, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(async (ctx, connection, transaction) =>
        {
            ctx.SyncStage = SyncStage.ScopeWriting;

            var scopeBuilder = this.Provider.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.ServerHistory, scopeBuilder, connection, transaction, cancellationToken);

            if (!exists)
                await this.InternalCreateScopeInfoTableAsync(ctx, DbScopeType.ServerHistory, scopeBuilder, connection, transaction, cancellationToken);

            // Write scopes locally
            var scopeInfoUpdated = await this.InternalUpsertScopeAsync(ctx, DbScopeType.ServerHistory, scopeInfo, scopeBuilder, connection, transaction, cancellationToken);

            ctx.SyncStage = SyncStage.ScopeWrited;

            return scopeInfoUpdated;

        }, cancellationToken);

    }
}