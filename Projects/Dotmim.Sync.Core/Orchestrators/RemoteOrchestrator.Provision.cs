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
    public partial class RemoteOrchestrator : BaseOrchestrator
    {


        public virtual Task<ServerScopeInfo> ProvisionAsync(SyncProvision provision = default, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => ProvisionAsync(SyncOptions.DefaultScopeName, provision, overwrite, connection, transaction, cancellationToken, progress);

        /// <summary>
        /// Provision the remote database 
        /// </summary>
        /// <param name="overwrite">Overwrite existing objects</param>
        public virtual Task<ServerScopeInfo> ProvisionAsync(string scopeName, SyncProvision provision = default, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (provision == SyncProvision.None)
                provision = SyncProvision.ServerScope | SyncProvision.ServerHistoryScope | SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable;

            return this.ProvisionAsync(scopeName, null, provision, overwrite, connection, transaction, cancellationToken, progress);
        }


        public virtual Task<ServerScopeInfo> ProvisionAsync(SyncSetup setup, SyncProvision provision = default, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => ProvisionAsync(SyncOptions.DefaultScopeName, setup, provision, overwrite, connection, transaction, cancellationToken, progress);


        /// <summary>
        /// Provision the remote database based on the Setup parameter, and the provision enumeration
        /// </summary>
        /// <param name="provision">Provision enumeration to determine which components to apply</param>
        /// <param name="serverScopeInfo">server scope. Will be saved once provision is done</param>
        /// <returns>Full schema with table and columns properties</returns>
        public virtual async Task<ServerScopeInfo> ProvisionAsync(string scopeName, SyncSetup setup = null, SyncProvision provision = default, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                // Check incompatibility with the flags
                if (provision.HasFlag(SyncProvision.ClientScope))
                    throw new InvalidProvisionForRemoteOrchestratorException();

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                ServerScopeInfo serverScopeInfo;
                (context, serverScopeInfo) = await this.InternalGetServerScopeInfoAsync(context, setup, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // 2) Provision
                if (provision == SyncProvision.None)
                    provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

                (context, _) = await this.InternalProvisionAsync(serverScopeInfo, context, false, provision, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return serverScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        public virtual async Task< ServerScopeInfo> ProvisionAsync(ServerScopeInfo serverScopeInfo, SyncProvision provision = default, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), serverScopeInfo.Name);
            try
            {
                (_, serverScopeInfo) = await InternalProvisionServerAsync(serverScopeInfo, context, provision, overwrite, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                return serverScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }


        internal virtual async Task<(SyncContext context, ServerScopeInfo serverScopeInfo)>
          InternalProvisionServerAsync(ServerScopeInfo serverScopeInfo, SyncContext context, SyncProvision provision = default, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                if (serverScopeInfo.Schema == null)
                    throw new Exception($"No Schema in your server scopeInfo {serverScopeInfo.Name}");

                if (serverScopeInfo.Schema == null)
                    throw new Exception($"No Setup in your server scopeInfo {serverScopeInfo.Name}");

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Check incompatibility with the flags
                if (provision.HasFlag(SyncProvision.ClientScope))
                    throw new InvalidProvisionForRemoteOrchestratorException();

                if (provision == SyncProvision.None)
                    provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

                (context, _) = await this.InternalProvisionAsync(serverScopeInfo, context, false, provision, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // Write scopes locally
                (context, serverScopeInfo) = await this.InternalSaveServerScopeInfoAsync(serverScopeInfo, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return (context, serverScopeInfo);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }

        /// <summary>
        /// Deprovision the remote database. Schema tables are retrieved through the default scope.
        /// </summary>
        public virtual Task<bool> 
               DeprovisionAsync(SyncProvision provision = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => DeprovisionAsync(SyncOptions.DefaultScopeName, provision, connection, transaction, cancellationToken, progress);

        /// <summary>
        /// Deprovision the remote database. Schema tables are retrieved through scope name.
        /// </summary>
        public virtual async Task<bool> 
            DeprovisionAsync(string scopeName, SyncProvision provision = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                if (provision == default)
                    provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Deprovisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                ServerScopeInfo serverScopeInfo;
                (context, serverScopeInfo) = await this.InternalLoadServerScopeInfoAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (serverScopeInfo == null || serverScopeInfo.Schema == null || serverScopeInfo.Schema.Tables == null || serverScopeInfo.Schema.Tables.Count <= 0 || !serverScopeInfo.Schema.HasColumns)
                    throw new MissingSchemaInScopeException();

                bool isDeprovisioned;
                (context, isDeprovisioned) = await InternalDeprovisionAsync(serverScopeInfo, context, provision, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return isDeprovisioned;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }

        public virtual Task<bool> DeprovisionAsync(SyncSetup setup, SyncProvision provision = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => DeprovisionAsync(SyncOptions.DefaultScopeName, setup, provision, connection, transaction, cancellationToken, progress);

        /// <summary>
        /// Deprovision the remote database. Schema tables are retrieved through setup in parameter.
        /// </summary>
        public virtual async Task<bool> DeprovisionAsync(string scopeName, SyncSetup setup, SyncProvision provision = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                if (provision == default)
                    provision = SyncProvision.ServerScope | SyncProvision.ServerHistoryScope | SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Deprovisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Creating a fake scope info
                var serverScopeInfo = this.InternalCreateScopeInfo(scopeName, DbScopeType.Server);
                serverScopeInfo.Setup = setup;

                bool isDeprovisioned;
                (context, isDeprovisioned) = await InternalDeprovisionAsync(serverScopeInfo, context, provision, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return isDeprovisioned;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }

    }
}
