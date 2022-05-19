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

        public virtual async Task<ClientScopeInfo>
            ProvisionAsync(ClientScopeInfo clientScopeInfo, SyncProvision provision = default, bool overwrite = true, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), clientScopeInfo.Name);
            try
            {
                (context, clientScopeInfo) = await InternalProvisionClientAsync(clientScopeInfo, context, provision, overwrite, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                return clientScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }


        /// <summary>
        /// Provision the local database based on the scope info parameter.
        /// Scope info parameter should contains Schema and Setup properties
        /// </summary>
        public virtual async Task<(SyncContext context, ClientScopeInfo clientScopeInfo)>
                    InternalProvisionClientAsync(ClientScopeInfo clientScopeInfo, SyncContext context, SyncProvision provision = default, bool overwrite = true, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (clientScopeInfo.Schema == null)
                throw new Exception($"No Schema in your scopeInfo {clientScopeInfo.Name}");

            if (clientScopeInfo.Schema == null)
                throw new Exception($"No Setup in your scopeInfo {clientScopeInfo.Name}");

            await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            // Check incompatibility with the flags
            if (provision.HasFlag(SyncProvision.ServerHistoryScope) || provision.HasFlag(SyncProvision.ServerScope))
                throw new InvalidProvisionForLocalOrchestratorException();

            // 2) Provision
            if (provision == SyncProvision.None)
                provision = SyncProvision.Table | SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable;

            (context, _) = await this.InternalProvisionAsync(clientScopeInfo, context, overwrite, provision, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

            // Write scopes locally
            (context, clientScopeInfo) = await this.InternalSaveClientScopeInfoAsync(clientScopeInfo, context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

            await runner.CommitAsync().ConfigureAwait(false);

            return (context, clientScopeInfo);

        }


        public virtual Task<ClientScopeInfo> ProvisionAsync(SyncSetup setup = null, SyncProvision provision = default, bool overwrite = true, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => ProvisionAsync(SyncOptions.DefaultScopeName, setup, provision, overwrite, connection, transaction, cancellationToken, progress);

        /// <summary>
        /// Provision the local database based on the setup in parameter
        /// The schema should exists in your local database
        /// </summary>
        public virtual async Task<ClientScopeInfo> ProvisionAsync(string scopeName, SyncSetup setup = null, SyncProvision provision = default, bool overwrite = true, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                ClientScopeInfo clientScopeInfo;
                (context, clientScopeInfo) = await InternalProvisionClientAsync(scopeName, context, setup, provision, overwrite, connection, transaction, cancellationToken, progress);
                return clientScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        internal virtual async Task<(SyncContext context, ClientScopeInfo clientScopeInfo)>
            InternalProvisionClientAsync(string scopeName, SyncContext context, SyncSetup setup = null, SyncProvision provision = default, bool overwrite = true, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            // Check incompatibility with the flags
            if (provision.HasFlag(SyncProvision.ServerHistoryScope) || provision.HasFlag(SyncProvision.ServerScope))
                throw new InvalidProvisionForLocalOrchestratorException();

            // get client scope and create tables / row if needed
            ClientScopeInfo clientScopeInfo;
            (context, clientScopeInfo) = await this.InternalGetClientScopeInfoAsync(context, setup, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

            if (clientScopeInfo.Schema == null || !clientScopeInfo.Schema.HasTables || !clientScopeInfo.Schema.HasColumns)
                throw new MissingTablesException(scopeName);

            // 2) Provision
            if (provision == SyncProvision.None)
                provision = SyncProvision.Table | SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable;

            (context, _) = await this.InternalProvisionAsync(clientScopeInfo, context, overwrite, provision, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

            // Write scopes locally
            (context, clientScopeInfo) = await this.InternalSaveClientScopeInfoAsync(clientScopeInfo, context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

            await runner.CommitAsync().ConfigureAwait(false);

            return (context, clientScopeInfo);

        }


        /// <summary>
        /// Deprovision the orchestrator database based on the schema argument, and the provision enumeration
        /// </summary>
        /// <param name="schema">Schema to be deprovisioned from the database managed by the orchestrator, through the provider.</param>
        /// <param name="provision">Provision enumeration to determine which components to deprovision</param>
        public virtual Task<(SyncContext context, bool deprovisioned)> DeprovisionAsync(ClientScopeInfo clientScopeInfo, SyncProvision provision = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), clientScopeInfo.Name);
            try
            {
                return InternalDeprovisionClientAsync(clientScopeInfo, context, provision, connection, transaction, cancellationToken, progress);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        public virtual async Task<(SyncContext context, bool deprovisioned)>
            InternalDeprovisionClientAsync(ClientScopeInfo clientScopeInfo, SyncContext context, SyncProvision provision = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (provision == default)
                provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;

            await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Deprovisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (clientScopeInfo == null || clientScopeInfo.Schema == null || !clientScopeInfo.Schema.HasTables || !clientScopeInfo.Schema.HasColumns)
                return (context, false);

            bool isDeprovisioned;
            (context, isDeprovisioned) = await InternalDeprovisionAsync(clientScopeInfo, context, provision, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            await runner.CommitAsync().ConfigureAwait(false);

            return (context, isDeprovisioned);
        }


        /// <summary>
        /// Deprovision the orchestrator database based on the setup argument, and the provision enumeration
        /// </summary>
        public virtual async Task<(SyncContext context, bool deprovisioned)> DeprovisionAsync(string scopeName = SyncOptions.DefaultScopeName, SyncProvision provision = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                if (provision == default)
                    provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Deprovisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // get client scope and create tables / row if needed

                ClientScopeInfo clientScopeInfo;
                (context, clientScopeInfo) = await this.InternalLoadClientScopeInfoAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (clientScopeInfo == null || clientScopeInfo.Schema == null || !clientScopeInfo.Schema.HasTables || !clientScopeInfo.Schema.HasColumns)
                    return (context, false);

                bool isDeprovisioned;
                (context, isDeprovisioned) = await InternalDeprovisionAsync(clientScopeInfo, context, provision, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return (context, isDeprovisioned);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

    }
}
