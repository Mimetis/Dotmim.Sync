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
        public virtual async Task<List<ServerHistoryScopeInfo>> GetServerHistoryScopesAsync(string scopeName = SyncOptions.DefaultScopeName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Reading, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var exists = await this.InternalExistsScopeInfoTableAsync(scopeName, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(scopeName, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // Get scope if exists
                var scopes = await this.InternalGetAllScopesAsync(scopeName, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                var serverHistoryScopes = new List<ServerHistoryScopeInfo>();
                foreach (var scope in scopes)
                    serverHistoryScopes.Add(scope as ServerHistoryScopeInfo);

                return serverHistoryScopes;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeName, ex);
            }
        }

        public virtual Task<ServerScopeInfo> GetServerScopeAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => GetServerScopeAsync(SyncOptions.DefaultScopeName, null, connection, transaction, cancellationToken, progress);

        public virtual Task<ServerScopeInfo> GetServerScopeAsync(SyncSetup setup , DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => GetServerScopeAsync(SyncOptions.DefaultScopeName, setup, connection, transaction, cancellationToken, progress);

        /// <summary>
        /// Get the server scope info, ensures the scope is created.
        /// Provision is setup is defined (and scope does not exists in the database yet)
        /// </summary>
        /// <returns>Server scope info, containing scope name, version, setup and related schema infos</returns>
        public virtual async Task<ServerScopeInfo> GetServerScopeAsync(string scopeName, SyncSetup setup = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Writing, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var exists = await this.InternalExistsScopeInfoTableAsync(scopeName, DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(scopeName, DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                exists = await this.InternalExistsScopeInfoTableAsync(scopeName, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(scopeName, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                var serverScopeInfo = await this.InternalGetScopeAsync(scopeName, DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false) as ServerScopeInfo;


                if (serverScopeInfo == null)
                {
                    serverScopeInfo = this.InternalCreateScope(scopeName, DbScopeType.Server, cancellationToken, progress) as ServerScopeInfo;
                    serverScopeInfo = await this.InternalSaveScopeAsync(serverScopeInfo, DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false) as ServerScopeInfo;
                }

                // Raise error only on server side, since we can't do nothing if we don't have any tables provisionned and no setup provided
                if ((serverScopeInfo.Setup == null || serverScopeInfo.Schema == null) && (setup == null || setup.Tables.Count <= 0))
                    throw new Exception($"Setup does not exist on server, for scope name {scopeName}. Please provision server side.");

                // if serverscopeinfo is a new, because we never run any sync before, grab schema and affect setup
                if (serverScopeInfo.Setup == null && serverScopeInfo.Schema == null && setup != null && setup.Tables.Count > 0)
                {
                    var schema = await this.InternalGetSchemaAsync(scopeName, setup, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                    serverScopeInfo.Setup = setup;
                    serverScopeInfo.Schema = schema;

                    // 2) Provision
                    var provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;
                    await this.InternalProvisionAsync(serverScopeInfo, false, provision, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    // Write scopes locally
                    await this.InternalSaveScopeAsync(serverScopeInfo, DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                }


                if (!serverScopeInfo.Setup.EqualsByProperties(setup))
                    throw new Exception("Seems you are trying another Setup tables that what is stored in your server scope database. Please make a migration or create a new scope");

                await runner.CommitAsync().ConfigureAwait(false);

                return serverScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeName, ex);
            }

        }

        /// <summary>
        /// Update or Insert a server scope row
        /// </summary>
        public virtual async Task<ServerScopeInfo> SaveServerScopeAsync(ServerScopeInfo scopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(scopeInfo.Name, SyncMode.Writing, SyncStage.ScopeWriting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);


                var exists = await this.InternalExistsScopeInfoTableAsync(scopeInfo.Name, DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(scopeInfo.Name, DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // Write scopes locally
                var scopeInfoUpdated = await this.InternalSaveScopeAsync(scopeInfo, DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return scopeInfoUpdated as ServerScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeInfo.Name, ex);
            }
        }

        /// <summary>
        /// Update or Insert a server scope row
        /// </summary>
        public virtual async Task<ServerHistoryScopeInfo> SaveServerHistoryScopeAsync(ServerHistoryScopeInfo scopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(scopeInfo.Name, SyncMode.Writing, SyncStage.ScopeWriting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var exists = await this.InternalExistsScopeInfoTableAsync(scopeInfo.Name, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(scopeInfo.Name, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // Write scopes locally
                var scopeInfoUpdated = await this.InternalSaveScopeAsync(scopeInfo, DbScopeType.ServerHistory, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return scopeInfoUpdated as ServerHistoryScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeInfo.Name, ex);
            }
        }
    }
}