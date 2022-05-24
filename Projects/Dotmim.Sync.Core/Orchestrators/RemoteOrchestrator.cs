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
        /// Gets the sync side of this Orchestrator. RemoteOrchestrator is always used on server side
        /// </summary>
        public override SyncSide Side => SyncSide.ServerSide;


        /// <summary>
        /// Create a remote orchestrator, used to orchestrates the whole sync on the server side
        /// </summary>
        public RemoteOrchestrator(CoreProvider provider, SyncOptions options) : base(provider, options)
        {
            if (this.Provider != null && !this.Provider.CanBeServerProvider)
                throw new UnsupportedServerProviderException(this.Provider.GetProviderTypeName());
        }

        /// <summary>
        /// Create a remote orchestrator, used to orchestrates the whole sync on the server side
        /// </summary>
        public RemoteOrchestrator(CoreProvider provider) : base(provider, new SyncOptions())
        {
            if (this.Provider != null && !this.Provider.CanBeServerProvider)
                throw new UnsupportedServerProviderException(this.Provider.GetProviderTypeName());
        }

        /// <summary>
        /// Ensure the schema is readed from the server, based on the Setup instance.
        /// Creates all required tables (server_scope tables) and provision all tables (tracking, stored proc, triggers and so on...)
        /// Then return the schema readed
        /// </summary>
        /// <returns>current context, the local scope info created or get from the database and the configuration from the client if changed </returns>
        //internal virtual async Task<ServerScopeInfo> EnsureSchemaAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        //{
        //    try
        //    {
        //        await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Writing, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

        //        var exists = await this.InternalExistsScopeInfoTableAsync(this.GetContext(), DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
        //        if (!exists)
        //            await this.InternalCreateScopeInfoTableAsync(this.GetContext(), DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        exists = await this.InternalExistsScopeInfoTableAsync(this.GetContext(), DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
        //        if (!exists)
        //            await this.InternalCreateScopeInfoTableAsync(this.GetContext(), DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        var serverScopeInfo = await this.InternalGetScopeAsync<ServerScopeInfo>(this.GetContext(), DbScopeType.Server, this.ScopeName, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        SyncSet schema;
        //        // Let's compare this serverScopeInfo with the current Setup
        //        // If schema is null :
        //        // - Read the schema from database based on this.Setup
        //        // - Provision the database with this schema
        //        // - Write the scope with this.Setup and schema
        //        // If schema is not null :
        //        // - Compare saved setup with current setup
        //        // - If not equals:
        //        // - Read schema from database based on this.Setup
        //        if (serverScopeInfo.Schema == null)
        //        {
        //            // So far, we don't have already a database provisionned
        //            this.GetContext().SyncStage = SyncStage.Provisioning;

        //            // 1) Get Schema from remote provider
        //            schema = await this.InternalGetSchemaAsync(this.GetContext(), this.Setup, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //            // 2) Provision
        //            var provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;
        //            schema = await InternalProvisionAsync(this.GetContext(), false, schema, this.Setup, provision, serverScopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
        //        }
        //        else
        //        {
        //            // Setup stored on local or remote is different from the one provided.
        //            // So, we can migrate
        //            if (!serverScopeInfo.Setup.EqualsByProperties(this.Setup))
        //            {
        //                // 1) Get Schema from remote provider
        //                schema = await this.InternalGetSchemaAsync(this.GetContext(), this.Setup, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //                // Migrate the old setup (serverScopeInfo.Setup) to the new setup (this.Setup) based on the new schema 
        //                await this.InternalMigrationAsync(this.GetContext(), schema, serverScopeInfo.Setup, this.Setup, runner.Connection, runner.Transaction, cancellationToken, progress);

        //                serverScopeInfo.Setup = this.Setup;
        //                serverScopeInfo.Schema = schema;

        //                // Write scopes locally
        //                await this.InternalSaveScopeAsync(this.GetContext(), DbScopeType.Server, serverScopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
        //            }

        //            // Get the schema saved on server
        //            schema = serverScopeInfo.Schema;
        //        }

        //        await runner.CommitAsync().ConfigureAwait(false);

        //        return serverScopeInfo;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw GetSyncError(ex);
        //    }
        //}


        /// <summary>
        /// Migrate an old setup configuration to a new one. This method is usefull if you are changing your SyncSetup when a database has been already configured previously
        /// </summary>
        //public virtual async Task<ServerScopeInfo> MigrationAsync(SyncSetup oldSetup, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        //{
        //    try
        //    {
        //        await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Writing, SyncStage.Migrating, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

        //        SyncSet schema = await this.InternalGetSchemaAsync(this.GetContext(), this.Setup, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        // Migrate the db structure
        //        await this.InternalMigrationAsync(this.GetContext(), schema, oldSetup, this.Setup, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        var exists = await this.InternalExistsScopeInfoTableAsync(this.GetContext(), DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        if (!exists)
        //            await this.InternalCreateScopeInfoTableAsync(this.GetContext(), DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        var remoteScope = await this.InternalGetScopeAsync<ServerScopeInfo>(this.GetContext(), DbScopeType.Server, this.ScopeName, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        if (remoteScope == null)
        //            remoteScope = await this.InternalCreateScopeAsync<ServerScopeInfo>(this.GetContext(), DbScopeType.Server, this.ScopeName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

        //        remoteScope.Setup = this.Setup;
        //        remoteScope.Schema = schema;

        //        // Write scopes locally
        //        await this.InternalSaveScopeAsync(this.GetContext(), DbScopeType.Server, remoteScope, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

        //        await runner.CommitAsync().ConfigureAwait(false);

        //        return remoteScope;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw GetSyncError(ex);
        //    }
        //}

  }
}